mod args;

use args::Args;

mod core;

use core::{CoordinatorServer, MRCoordinator};

mod jobs;

mod worker_info;
mod worker_registry;

use aws_sdk_s3 as s3;
use clap::Parser;
use tonic::transport::Server;
use common::minio;
use tracing::{debug, info, warn};

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

    let coordinator = MRCoordinator::new(minio_client_config);

    // Remove me. This is just for ensuring that the connection works.
    if let Err(e) = show_buckets(&coordinator.s3_client.client).await {
        dbg!(e);
    }

    Server::builder()
        .add_service(CoordinatorServer::new(coordinator))
        .serve(addr)
        .await?;

    Ok(())
}
