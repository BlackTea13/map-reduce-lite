use std::{collections::VecDeque, net::SocketAddr, sync::Arc};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tracing::info;

pub use coordinator::coordinator_server::{Coordinator, CoordinatorServer};
use coordinator::*;
pub use worker::{worker_client::WorkerClient, AckRequest, ReceivedWorkRequest};

use crate::core::worker::received_work_request::JobMessage;
use crate::core::worker::MapJobRequest;
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
}

impl MRCoordinator {
    pub fn new(s3_config: ClientConfig) -> Self {
        MRCoordinator {
            s3_client: Client::from_conf(s3_config),
            jobs: VecDeque::new(),
            worker_registry: Arc::new(Mutex::new(WorkerRegistry::default())),
            job_queue: Arc::new(Mutex::new(JobQueue::new())),
        }
    }

    async fn get_registry(&self) -> tokio::sync::MutexGuard<'_, WorkerRegistry> {
        self.worker_registry.lock().await
    }

    async fn get_job_queue(&self) -> tokio::sync::MutexGuard<'_, JobQueue> {
        self.job_queue.lock().await
    }

    async fn add_free_worker(&self, worker_id: WorkerID) {
        let mut registry = self.get_registry().await;
        registry.set_worker_state(worker_id, WorkerState::Free);
    }

    // TODO: partition input/output for mapper and reducers.
    async fn _assign_work(
        &self,
        worker_id: WorkerID,
        input_file: String,
        output_file: String,
        work_type: WorkType,
        workload: String,
        aux: Vec<String>,
    ) {
        let mut registry = self.get_registry().await;
        let work_state = WorkerState::from_work_type(work_type);

        registry.set_worker_state(worker_id, work_state);

        // TODO: send Map work for now, someone handle this when for reduce
        if let Some(worker) = registry.get_worker_mut(worker_id) {
            let map_message = MapJobRequest {
                input_files: input_file,
                workload,
                aux,
            };

            worker.set_state(WorkerState::Mapping);

            let message = JobMessage::MapMessage(map_message);
            let request = Request::new(ReceivedWorkRequest {
                job_message: Some(message),
            });

            let response = worker.client.received_work(request).await.unwrap();
        }
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
        let request = Request::new(AckRequest { worker_id });

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
            // Only invalid the ID. No need to touch WorkerInfo.
            let mut registry = self.get_registry().await;
            registry.delete_worker(worker_id);
        };

        info!("Worker exited (ID={})", Worker::get_worker_index(worker_id));

        let reply = WorkerLeaveResponse {};
        Ok(Response::new(reply))
    }

    /// Adding free worker into the registry
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
            job_queue.push_job(job);
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

        let mut registry = self.get_registry().await;
        if let Some(worker) = registry.get_worker_mut(worker_id) {
            match worker.state {
                WorkerState::Mapping => worker.set_state(WorkerState::Reducing),
                _ => worker.set_state(WorkerState::Free)
            }
        }

        let reply = WorkerDoneResponse { success: true };
        Ok(Response::new(reply))
    }
}
