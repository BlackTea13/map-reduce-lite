use std::{collections::VecDeque, net::SocketAddr, sync::Arc};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tracing::info;

pub use coordinator::coordinator_server::{Coordinator, CoordinatorServer};
use coordinator::*;
pub use worker::{worker_client::WorkerClient, AckRequest, ReceivedWorkRequest};

use crate::worker_info::WorkerID;
use crate::{
    jobs,
    worker_info::{Worker, WorkerState},
    worker_registry::WorkerRegistry,
};
use crate::core::worker::MapJobRequest;
use crate::core::worker::received_work_request::JobMessage;
use crate::minio::{Client, ClientConfig};

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

enum WorkType {
    Map,
    Reduce,
}

#[derive(Debug)]
pub struct MRCoordinator {
    pub s3_client: Client,
    jobs: VecDeque<jobs::Job>,
    worker_registry: Arc<Mutex<WorkerRegistry>>,
}

impl MRCoordinator {
    pub(crate) fn new(s3_config: ClientConfig) -> Self {
        MRCoordinator {
            s3_client: Client::from_conf(s3_config),
            jobs: VecDeque::new(),
            worker_registry: Arc::new(Mutex::new(WorkerRegistry::default())),
        }
    }

    async fn get_registry(&self) -> tokio::sync::MutexGuard<'_, WorkerRegistry> {
        self.worker_registry.lock().await
    }

    async fn add_free_worker(&self, worker_id: WorkerID) {
        let mut registry = self.get_registry().await;
        registry.set_worker_state(worker_id, WorkerState::Free);
    }

    // TODO: partition input/output for mapper and reducers.
    async fn assign_work(
        &self,
        worker_id: WorkerID,
        input_file: String,
        output_file: String,
        work_type: WorkType,
        workload: String,
        aux: Vec<String>,
    ) {
        let mut registry = self.get_registry().await;
        let work_state = match work_type {
            WorkType::Map => WorkerState::Mapping,
            WorkType::Reduce => WorkerState::Reducing,
        };
        registry.set_worker_state(worker_id, work_state);

        // TODO: send Map work for now, someone handle this when for reduce
        if let Some(worker) = registry.get_worker_mut(worker_id) {
            let map_message = MapJobRequest {
                input_files: input_file,
                workload: workload,
                presigned_url: self.s3_client.presigned_get_uri("myjob", "testcases/books/yang.txt", 50000).await.unwrap(),
                aux: aux,
            };

            let message = JobMessage::MapMessage(map_message);
            let request = Request::new(ReceivedWorkRequest {
                job_message: Some(message)
            });

            let response = worker.client.received_work(request).await.unwrap();
        }
    }
}

#[tonic::async_trait]
impl Coordinator for MRCoordinator {
    async fn jobs(&self, request: Request<JobsRequest>) -> Result<Response<JobsResponse>, Status> {
        info!("Got a request from {:?}", request.remote_addr());

        let reply = JobsResponse {
            job_count: self.jobs.len() as u32,
        };
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
        let request = tonic::Request::new(AckRequest {});

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

    async fn worker_task(
        &self,
        request: Request<WorkerTaskRequest>,
    ) -> Result<Response<WorkerTaskResponse>, Status> {
        let worker_id = request.into_inner().worker_id;

        let _ = self.add_free_worker(worker_id).await;

        let reply = WorkerTaskResponse { success: true };
        Ok(Response::new(reply))
    }

    async fn start_task(
        &self,
        request: Request<StartTaskRequest>,
    ) -> Result<Response<StartTaskResponse>, Status> {
        let start_task_request = request.into_inner();

        let input_files = start_task_request.input_files;
        let output_files = start_task_request.output_files;
        let workload = start_task_request.workload;
        let aux = start_task_request.aux;

        let no_splits = 1;

        let splitted_free_workers = {
            let mut registry = self.get_registry().await;
            let free_workers = registry.get_free_workers();
            free_workers[..no_splits].to_vec()
        };

        for worker_id in splitted_free_workers {
            let _ = self
                .assign_work(
                    worker_id,
                    String::from("input_files"),
                    String::from("output_files"),
                    WorkType::Map,
                    String::from("workload"),
                    Vec::new(),
                )
                .await;
        }

        let reply = StartTaskResponse { success: true };
        Ok(Response::new(reply))
    }

    async fn worker_done(
        &self,
        request: Request<WorkerDoneRequest>,
    ) -> Result<Response<WorkerDoneResponse>, Status> {
        let reply = WorkerDoneResponse { success: true };
        Ok(Response::new(reply))
    }
}

