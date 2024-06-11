use tonic::{transport::Server, Request, Response, Status};

use clap::Parser;

//
// Import gRPC stubs/definitions.
//
use coordinator::{coordinator_client::CoordinatorClient, WorkerJoinRequest};
pub mod coordinator {
    tonic::include_proto!("coordinator");
}

use worker::worker_server::{Worker, WorkerServer};
use worker::{WorkRequest, WorkResponse};
pub mod worker {
    tonic::include_proto!("worker");
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// The address of the coordinator server
    #[arg(short = 'j', long = "join", default_value = "http://[::1]:8030")]
    address: String,

    /// The port to run the worker on.
    #[arg(short, long)]
    port: u16,

}

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let mut client = CoordinatorClient::connect(args.address).await?;
    let request = tonic::Request::new(WorkerJoinRequest {});
    let response = client.worker_join(request).await?;

    let success = response.into_inner().success;

    if success {
        // Start gRPC server, accepting request from server.
        println!("Worker registered... spinning server...");

        let worker = MRWorker::default();
        let addr = format!("[::1]:{}", args.port).parse().unwrap();

        Server::builder()
            .add_service(WorkerServer::new(worker))
            .serve(addr)
            .await?;

        println!("Server exited...");

        Ok(())
    } else {
        Err("Failed to register worker".into())
    }
}
