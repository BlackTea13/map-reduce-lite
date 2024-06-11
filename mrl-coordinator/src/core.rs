use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

pub use coordinator::coordinator_server::{Coordinator, CoordinatorServer};
use coordinator::{JobsRequest, JobsResponse, WorkerJoinRequest, WorkerJoinResponse};
pub mod coordinator {
    tonic::include_proto!("coordinator");
}

pub mod worker {
    tonic::include_proto!("worker");
}
pub use worker::{worker_client::WorkerClient, AckRequest};

use crate::jobs;

use std::{collections::VecDeque, net::SocketAddr, sync::Arc};

use crate::worker_info::WorkerInfo;

#[derive(Debug, Default)]
pub struct MRCoordinator {
    jobs: VecDeque<jobs::Job>,
    workers: Arc<Mutex<Vec<WorkerInfo>>>,
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
        let worker_ip = request.remote_addr().unwrap().ip();
        let addr = SocketAddr::new(worker_ip, request.into_inner().port as u16);

        {
            let mut workers = self.workers.lock().await;
            workers.push(WorkerInfo::new(addr));
        }

        // Server ack.
        let mut client = WorkerClient::connect(format!("http://{}", addr).to_string())
            .await
            .map_err(|_| Status::unknown("Unable to connect to client"))?;
        let request = tonic::Request::new(AckRequest {});
        let resp = client.ack(request).await;
        if resp.is_err() {
            return Err(Status::unknown("Worker did not acknowledge connection."));
        } else {
            println!("Worker joined.");
        }

        let reply = WorkerJoinResponse { success: true };
        Ok(Response::new(reply))
    }
}
