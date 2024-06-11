//
// Import gRPC stubs/definitions.
//
pub use coordinator::{coordinator_client::CoordinatorClient, WorkerJoinRequest};
pub mod coordinator {
    tonic::include_proto!("coordinator");
}

pub use worker::worker_server::{Worker, WorkerServer};
pub use worker::{WorkRequest, WorkResponse};
pub mod worker {
    tonic::include_proto!("worker");
}

use tonic::{Request, Response, Status};

#[derive(Debug, Default)]
pub struct MRWorker {}


#[tonic::async_trait]
impl Worker for MRWorker {
    async fn request_work(&self, request: Request<WorkRequest>) -> Result<Response<WorkResponse>, Status> {
        println!("Got a work request {:?}", request.into_inner());

        let reply = WorkResponse {
            success: true,
        };
        Ok(Response::new(reply))
    }
}
