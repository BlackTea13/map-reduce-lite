use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

pub use coordinator::coordinator_server::{Coordinator, CoordinatorServer};
use coordinator::{
    JobsRequest, JobsResponse, WorkerJoinRequest, WorkerJoinResponse, WorkerLeaveRequest,
    WorkerLeaveResponse, WorkerTaskRequest, WorkerTaskResponse,
};
pub mod coordinator {
    tonic::include_proto!("coordinator");
}

pub mod worker {
    tonic::include_proto!("worker");
}
pub use worker::{worker_client::WorkerClient, AckRequest};

use crate::{
    jobs,
    worker_info::{Worker, WorkerIDVendor, WorkerState},
    worker_registry::WorkerRegistry,
};

use std::{collections::VecDeque, net::SocketAddr, sync::Arc};

use crate::worker_info::{WorkerID, WorkerInfo};

#[derive(Debug, Default)]
pub struct MRCoordinator {
    jobs: VecDeque<jobs::Job>,
    worker_registry: Arc<Mutex<WorkerRegistry>>,
}

impl MRCoordinator {
    async fn get_registry(&self) -> tokio::sync::MutexGuard<'_, WorkerRegistry> {
        self.worker_registry.lock().await
    }

    async fn add_free_worker(&self, worker_id: WorkerID) {
        let mut registry = self.get_registry().await;
        registry.set_worker_state(worker_id, WorkerState::Free);
    }
}

#[tonic::async_trait]
impl Coordinator for MRCoordinator {
    async fn jobs(&self, request: Request<JobsRequest>) -> Result<Response<JobsResponse>, Status> {
        println!("Got a request from {:?}", request.remote_addr());

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
            registry.register_worker(addr)
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
            println!("Worker joined (ID={})", Worker::get_worker_index(worker_id));
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

        println!(
            "Worker exitted (ID={})",
            Worker::get_worker_index(worker_id)
        );

        let reply = WorkerLeaveResponse {};
        Ok(Response::new(reply))
    }

    async fn worker_task(
        &self,
        request: Request<WorkerTaskRequest>,
    ) -> Result<Response<WorkerTaskResponse>, Status> {
        let worker_id = request.into_inner().worker_id;

        let _ = self.add_free_worker(worker_id).await;

        // println!("Got a request from {:?}", request.remote_addr());

        let reply = WorkerTaskResponse { success: true };
        Ok(Response::new(reply))
    }
}
