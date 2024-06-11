use clap::Parser;
use tokio::{signal, sync::mpsc};
use tonic::transport::Server;

mod core;
use core::{CoordinatorClient, MRWorker, WorkerJoinRequest, WorkerServer};

mod args;
use args::Args;

async fn start_server(port: u16) {
    tokio::task::spawn(async move {
        let worker = MRWorker::default();
        let addr = format!("[::1]:{}", port).parse().unwrap();
        println!("Worker server listening on {}", addr);

        let _ = Server::builder()
            .add_service(WorkerServer::new(worker))
            .serve(addr)
            .await;
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Start server as background task.
    start_server(args.port).await;

    let mut client = CoordinatorClient::connect(args.address).await?;
    let request = tonic::Request::new(WorkerJoinRequest {
        port: args.port as u32,
    });
    let response = client.worker_join(request).await?;

    let success = response.into_inner().success;

    if success {
        // Start gRPC server, accepting request from server.
        println!("Worker registered...");
    }

    match signal::ctrl_c().await {
        Ok(()) => {
            println!("Server exited...");
            Ok(())
        }
        Err(err) => {
            // we also shut down in case of error
            Err(format!("Unable to listen for shutdown signal: {}", err).into())
        }
    }
}
