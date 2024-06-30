use std::time::Duration;
use std::{collections::VecDeque, net::SocketAddr, sync::Arc};

use moka::sync::Cache;
use tokio::select;
use tokio::sync::{Mutex, Notify};
use tonic::{Request, Response, Status};
use tracing::info;

pub use coordinator::coordinator_server::{Coordinator, CoordinatorServer};
use coordinator::*;
pub use worker::{worker_client::WorkerClient, AckRequest, ReceivedWorkRequest};

use crate::jobs::{Job, JobQueue};
use crate::minio::{Client, ClientConfig};
use crate::worker_info::WorkerID;
use crate::{
    jobs,
    worker_info::{Worker, WorkerState},
    worker_registry::WorkerRegistry,
};

pub mod coordinator {
    tonic::include_proto!("coordinator");
}

pub mod worker {
    tonic::include_proto!("worker");
}

struct Submit {
    input: String,
    workload: String,
    output: String,
    args: Vec<String>,
    timeout: u32,
}

pub enum WorkType {
    Map,
    Reduce,
}

#[derive(Debug)]
pub struct MRCoordinator {
    pub s3_client: Client,
    jobs: VecDeque<jobs::Job>,
    worker_registry: Arc<Mutex<WorkerRegistry>>,
    job_queue: Arc<Mutex<JobQueue>>,
    job_queue_notifier: Arc<Notify>,
    object_locks: Cache<String, WorkerID>,
}

impl MRCoordinator {
    pub fn new(s3_config: ClientConfig) -> Self {
        MRCoordinator {
            s3_client: Client::from_conf(s3_config),
            jobs: VecDeque::new(),
            worker_registry: Arc::new(Mutex::new(WorkerRegistry::default())),
            job_queue: Arc::new(Mutex::new(JobQueue::new())),
            job_queue_notifier: Arc::new(Notify::new()),
            object_locks: Cache::builder()
                .time_to_live(Duration::from_secs(5))
                .max_capacity(1000)
                .build(),
        }
    }

    async fn get_registry(&self) -> tokio::sync::MutexGuard<'_, WorkerRegistry> {
        self.worker_registry.lock().await
    }

    pub fn clone_job_queue(&self) -> Arc<Mutex<JobQueue>> {
        self.job_queue.clone()
    }
    pub fn clone_job_queue_notifier(&self) -> Arc<Notify> {
        self.job_queue_notifier.clone()
    }

    pub async fn get_job_queue(&self) -> tokio::sync::MutexGuard<'_, JobQueue> {
        self.job_queue.lock().await
    }

    pub fn clone_registry(&self) -> Arc<Mutex<WorkerRegistry>> {
        self.worker_registry.clone()
    }

    async fn add_free_worker(&self, worker_id: WorkerID) {
        let mut registry = self.get_registry().await;
        registry.set_worker_state(worker_id, WorkerState::Free);
    }

    async fn status(&self) -> Vec<String> {
        let registry = self.get_registry().await;
        let number_of_workers = registry.len();
        let workers = registry.get_workers();

        let workers_registered = format!("Workers Registered {}", number_of_workers);

        let mut data = vec![workers_registered];

        for worker in workers {
            let index = Worker::get_worker_index(worker.id);
            let worker_status = format!("Worker (ID={:0>4}) - {:?}", index, worker.state);
            data.push(worker_status);
        }
        data
    }

    /// Returns the jobs of this [`MRCoordinator`].
    async fn jobs(&self) -> Vec<String> {
        let job_queue = self.get_job_queue().await;

        let number_of_pending_jobs = job_queue.number_of_jobs_pending();
        let number_of_processed_jobs = job_queue.number_of_jobs_processed();

        let number_of_processed_jobs = format!("Completed {}", number_of_processed_jobs);
        let number_of_pending_jobs = format!("Pending   {}", number_of_pending_jobs);

        let mut data = vec![number_of_pending_jobs, number_of_processed_jobs];

        let jobs = job_queue.get_all_jobs();

        for (offset, job) in jobs.iter().enumerate() {
            let job_status = format!("Job (ID={:0>3}) {:?}", offset, job.get_state());
            data.push(job_status);
        }

        data
    }
}

#[tonic::async_trait]
impl Coordinator for MRCoordinator {
    async fn jobs(&self, request: Request<JobsRequest>) -> Result<Response<JobsResponse>, Status> {
        info!("[REQUEST] JOBS from {:?}", request.remote_addr());
        let data = self.jobs().await;
        let reply = JobsResponse { data };
        Ok(Response::new(reply))
    }
    async fn status(
        &self,
        request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        info!("[REQUEST] STATUS from {:?}", request.remote_addr());
        let data = self.status().await;
        let reply = StatusResponse { data };
        Ok(Response::new(reply))
    }

    /// Worker requests to join the workforce.
    async fn worker_join(
        &self,
        request: Request<WorkerJoinRequest>,
    ) -> Result<Response<WorkerJoinResponse>, Status> {
        // Construct address for worker's gRPC server.
        let worker_ip = request.remote_addr().unwrap().ip();
        let addr = SocketAddr::new(worker_ip, request.into_inner().port as u16);

        // Create a new worker, generate and assign an ID to it.
        let worker_id = {
            let mut registry = self.get_registry().await;
            registry.register_worker(addr).await?
        };

        // Server ack.
        let mut client = WorkerClient::connect(format!("http://{}", addr).to_string())
            .await
            .map_err(|_| Status::unknown("Unable to connect to client"))?;
        let request = Request::new(AckRequest {
            worker_id: worker_id as u32,
        });

        let resp = client.ack(request).await;
        if resp.is_err() {
            return Err(Status::unknown("Worker did not acknowledge connection."));
        } else {
            info!("Worker joined (ID={})", Worker::get_worker_index(worker_id));
        }

        // Send the generated worker ID to be saved
        // on the worker, so they know how they are
        // identified.
        let reply = WorkerJoinResponse { worker_id };
        Ok(Response::new(reply))
    }

    /// Worker requests to leave the workforce.
    async fn worker_leave(
        &self,
        request: Request<WorkerLeaveRequest>,
    ) -> Result<Response<WorkerLeaveResponse>, Status> {
        let worker_id = request.into_inner().worker_id;

        {
            // Only invalidate the ID. No need to touch WorkerInfo.
            let mut registry = self.get_registry().await;
            registry.delete_worker(worker_id);
        };

        info!("Worker exited (ID={})", Worker::get_worker_index(worker_id));

        let reply = WorkerLeaveResponse {};
        Ok(Response::new(reply))
    }

    /// Set worker with specified ID to `Free` state.
    async fn worker_task(
        &self,
        request: Request<WorkerTaskRequest>,
    ) -> Result<Response<WorkerTaskResponse>, Status> {
        let worker_id = request.into_inner().worker_id;

        let _ = self.add_free_worker(worker_id).await;

        let reply = WorkerTaskResponse { success: true };
        Ok(Response::new(reply))
    }

    /// Adding a job into queue
    async fn add_job(
        &self,
        request: Request<AddJobRequest>,
    ) -> Result<Response<AddJobResponse>, Status> {
        let task_request = request.into_inner();
        let job = Job::from_request(task_request);

        {
            // Add job to the queue.
            let mut job_queue = self.get_job_queue().await;
            job_queue.push_job(job.clone());
            self.job_queue_notifier.notify_one();
        }

        let reply = AddJobResponse { success: true };
        Ok(Response::new(reply))
    }

    /// Update the status in the registry when the worker is done
    async fn worker_done(
        &self,
        request: Request<WorkerDoneRequest>,
    ) -> Result<Response<WorkerDoneResponse>, Status> {
        let worker_done_request = request.into_inner();
        let worker_id = worker_done_request.worker_id;

        info!("Worker done (ID={})", Worker::get_worker_index(worker_id));

        let mut registry = self.get_registry().await;
        if let Some(worker) = registry.get_worker_mut(worker_id) {
            dbg!(&worker_id);
            worker.set_state(WorkerState::Free)
        }

        let reply = WorkerDoneResponse { success: true };
        Ok(Response::new(reply))
    }

    async fn acquire_lock(
        &self,
        request: Request<AcquireLockRequest>,
    ) -> Result<Response<AcquireLockResponse>, Status> {
        let request = request.into_inner();
        let object_key = request.object_key;

        // may want to time out this
        while self.object_locks.contains_key(&object_key) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        self.object_locks.insert(object_key, 0);

        let reply = AcquireLockResponse { lock: true };
        Ok(Response::new(reply))
    }

    async fn invalidate_lock(
        &self,
        request: Request<InvalidateLockRequest>,
    ) -> Result<Response<InvalidateLockResponse>, Status> {
        let request = request.into_inner();
        let object_key = request.object_key;

        self.object_locks.invalidate(&object_key);

        let reply = InvalidateLockResponse {};
        Ok(Response::new(reply))
    }
}
