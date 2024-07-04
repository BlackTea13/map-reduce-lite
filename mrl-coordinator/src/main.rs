use aws_sdk_s3 as s3;
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

async fn show_buckets(client: &s3::Client) -> Result<(), Box<dyn std::error::Error>> {
    let resp = client.list_buckets().send().await?;
    let buckets = resp.buckets();
    let num_buckets = buckets.len();

    let mut bucket_names = "".to_owned();
    for bucket in buckets {
        let bucket_name = bucket.name().unwrap_or_default().to_owned();
        bucket_names.push_str(&bucket_name);
        bucket_names.push_str(", ");
    }

    info!("Found {} buckets in all regions.", num_buckets);
    info!("Bucket names: {}", bucket_names);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Retrieve server configuration from command line.
    // Note: There are default values for EACH argument.
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    // Configure address.
    let addr = format!("[::1]:{}", args.port).parse().unwrap();
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
