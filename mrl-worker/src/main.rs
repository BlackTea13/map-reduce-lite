use clap::Parser;
use tokio::signal;
use tokio::sync::mpsc;
use tonic::transport::Server;
use tracing::{error, info};
use walkdir::WalkDir;

use args::Args;
use common::minio::ClientConfig;
use core::{CoordinatorClient, MRWorker, WorkerJoinRequest, WorkerLeaveRequest, WorkerServer};

use crate::core::{WORKING_DIR_MAP, WORKING_DIR_REDUCE};

mod core;

mod args;

mod map;

mod reduce;

async fn start_server(
    port: u16,
    address: String,
    client_config: ClientConfig,
    sender: mpsc::Sender<()>,
) {
    tokio::task::spawn(async move {
        let addr = format!("0.0.0.0:{}", port).parse().unwrap();
        info!("Worker server listening on {}", addr);

        let worker = MRWorker::new(address, client_config, sender);

        for entry in WalkDir::new(WORKING_DIR_MAP).into_iter().flatten() {
            let _ = tokio::fs::remove_dir_all(entry.path()).await;
        }

        for entry in WalkDir::new(WORKING_DIR_REDUCE).into_iter().flatten() {
            let _ = tokio::fs::remove_dir_all(entry.path()).await;
        }

        let svc = WorkerServer::new(worker);

        Server::builder()
            .add_service(svc) // Add the service to the server
            .serve(addr)
            .await
            .unwrap();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let address_clone = args.address.clone();

    let mut client = CoordinatorClient::connect(args.address.clone()).await?;
    let request = tonic::Request::new(WorkerJoinRequest {
        port: args.port as u32,
    });
    let (shutdown_sender, mut shutdown_receiver) = mpsc::channel(32);

    // Start server as background task.
    let minio_client_config = ClientConfig {
        access_key_id: args.access_key_id,
        secret_access_key: args.secret_access_key,
        region: args.region,
        url: args.minio_url,
    };

    start_server(
        args.port,
        address_clone,
        minio_client_config,
        shutdown_sender,
    )
    .await;

    let response = client.worker_join(request).await?;

    let worker_id = response.into_inner().worker_id;

    info!("Worker registered (ID={})", worker_id & 0xFFFF);

    // Await the shutdown signal
    tokio::select! {
        result = signal::ctrl_c() => match result {
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
        },
        _ = shutdown_receiver.recv() => {
            info!("Shutdown signal received...");
            let leave_request = tonic::Request::new(WorkerLeaveRequest { worker_id });
            client.worker_leave(leave_request).await?;
            Ok(())
        }
    }
}
