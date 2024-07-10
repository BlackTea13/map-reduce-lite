use clap::Parser;
use tonic::transport::Server;
use tracing::info;

use args::Args;
use common::minio::{self, Client};
use core::{CoordinatorServer, MRCoordinator};
use job_queue::process_job_queue;

mod args;

mod core;

mod jobs;

mod job_queue;
mod worker_info;
mod worker_registry;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Retrieve server configuration from command line.
    // Note: There are default values for EACH argument.
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    // Configure address.
    let addr = format!("0.0.0.0:{}", args.port).parse().unwrap();
    info!("CoordinatorServer listening on {}", addr);

    // Create minio client config from cli arguments and retrieve minio client.
    let minio_client_config = minio::ClientConfig {
        access_key_id: args.access_key_id,
        secret_access_key: args.secret_access_key,
        region: args.region,
        url: args.minio_url,
    };

    let coordinator = MRCoordinator::new(minio_client_config.clone());

    // Set up for processing job queue.
    let client = Client::from_conf(minio_client_config);
    let job_queue = coordinator.clone_job_queue();
    let job_queue_notifier = coordinator.clone_job_queue_notifier();
    let registry = coordinator.clone_registry();

    tokio::task::spawn(async move {
        loop {
            info!(" - Waiting for job");
            // A job has been pushed.
            job_queue_notifier.notified().await;

            let result =
                process_job_queue(client.clone(), job_queue.clone(), registry.clone()).await;

            match result {
                Ok(_) => info!(" - Task handled"),
                Err(e) => info!(" - {}", e),
            }
        }
    });

    Server::builder()
        .add_service(CoordinatorServer::new(coordinator))
        .serve(addr)
        .await?;

    Ok(())
}
