use tonic::{transport::Server};
use clap::Parser;

mod core;
use core::{MRWorker, CoordinatorClient, WorkerJoinRequest, WorkerServer};

mod args;
use args::Args;

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
