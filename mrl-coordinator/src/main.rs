mod args;
use args::Args;

mod core;
use core::{MRCoordinator, CoordinatorServer};

mod minio;
mod jobs;

use aws_sdk_s3 as s3;
use clap::Parser;
use tonic::{transport::Server};

async fn show_buckets(client: &s3::Client) -> Result<(), Box<dyn std::error::Error>> {
    let resp = client.list_buckets().send().await?;
    let buckets = resp.buckets();
    let num_buckets = buckets.len();

    println!("Bucket names:");
    for bucket in buckets {
        println!(" {}", bucket.name().unwrap_or_default());
    }

    println!("Found {} buckets in all regions.", num_buckets);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Retrieve server configuration from command line.
    // Note: There are default values for EACH argument.
    let args = Args::parse();

    // Configure address.
    let addr = format!("[::1]:{}", args.port).parse().unwrap();
    println!("CoordinatorServer listening on {}", addr);

    let coordinator = MRCoordinator::default();

    // Create minio client config from cli arguments and retrieve minio client.
    let minio_client_config = minio::ClientConfig {
        access_key_id: args.access_key_id,
        secret_access_key: args.secret_access_key,
        region: args.region,
        url: args.minio_url,
    };
    let client = minio::Client::from_conf(minio_client_config);

    // Remove me. This is just for ensuring that the connection works.
    if let Err(e) = show_buckets(&client.client).await {
        dbg!(e);
    }

    Server::builder()
        .add_service(CoordinatorServer::new(coordinator))
        .serve(addr)
        .await?;

    Ok(())
}
