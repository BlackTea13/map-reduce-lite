use tonic::{transport::Server, Request, Response, Status};

pub use coordinator::coordinator_server::{Coordinator, CoordinatorServer};
use coordinator::{JobsRequest, JobsResponse, WorkerJoinRequest, WorkerJoinResponse};
pub mod coordinator {
    tonic::include_proto!("coordinator");
}

use crate::jobs;

use std::collections::VecDeque;

#[derive(Debug, Default)]
pub struct MRCoordinator {
    jobs: VecDeque<jobs::Job>,
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

    async fn worker_join(&self, request: Request<WorkerJoinRequest>) -> Result<Response<WorkerJoinResponse>, Status> {
        println!("Got a request from {:?}", request.remote_addr());

        let reply = WorkerJoinResponse {
            success: true,
        };
        Ok(Response::new(reply))
    }
}

