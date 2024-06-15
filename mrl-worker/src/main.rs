use clap::Parser;
use log::{info, Level};
use tokio::signal;
use tonic::transport::Server;
use tracing::error;

mod core;
use core::{CoordinatorClient, MRWorker, WorkerJoinRequest, WorkerLeaveRequest, WorkerServer};

mod args;
use args::Args;

mod workload;

async fn start_server(port: u16) {
    tokio::task::spawn(async move {
        let worker = MRWorker::default();
        let addr = format!("[::1]:{}", port).parse().unwrap();
        info!("Worker server listening on {}", addr);

        let _ = Server::builder()
            .add_service(WorkerServer::new(worker))
            .serve(addr)
            .await;
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    // Start server as background task.
    start_server(args.port).await;

    let mut client = CoordinatorClient::connect(args.address).await?;
    let request = tonic::Request::new(WorkerJoinRequest {
        port: args.port as u32,
    });
    let response = client.worker_join(request).await?;

    let worker_id = response.into_inner().worker_id;

    info!("Worker registered (ID={})", worker_id & 0xFFFF);

    match signal::ctrl_c().await {
        Ok(()) => {
            info!("Worker server exited...");
            let leave_request = tonic::Request::new(WorkerLeaveRequest { worker_id });
            client.worker_leave(leave_request).await?;
            Ok(())
        }
        Err(err) => {
            error!("Fatal error encountered {}", err);
            // we also shut down in case of error
            Err(format!("Unable to listen for shutdown signal: {}", err).into())
        }
    }
}
