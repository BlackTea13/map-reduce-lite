use tonic::{transport::Server, Request, Response, Status};

use coordinator::coordinator_server::{Coordinator, CoordinatorServer};
use coordinator::{JobsRequest, JobsResponse};

use mrlite::dist::job::Job;

use std::collections::VecDeque;

pub mod coordinator {
    tonic::include_proto!("coordinator");
}

#[derive(Debug, Default)]
pub struct MRCoordinator {
    jobs: VecDeque<Job>,
}

#[tonic::async_trait]
impl Coordinator for MRCoordinator {
    async fn jobs(
        &self,
        request: Request<JobsRequest>,
    ) -> Result<Response<JobsResponse>, Status> {
        println!("Got a request from {:?}", request.remote_addr());

        let reply = JobsResponse {
            job_count: self.jobs.len() as u32,
        };
        Ok(Response::new(reply))
    }
}

// Note: Move this out later.
const PORT: u16 = 8030;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("[::1]:{}", PORT).parse().unwrap();
    let greeter = MRCoordinator::default();

    println!("CoordinatorServer listening on {}", addr);

    Server::builder()
        .add_service(CoordinatorServer::new(greeter))
        .serve(addr)
        .await?;

    Ok(())
}
