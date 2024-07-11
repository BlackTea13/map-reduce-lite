use clap::Parser;
use tonic::transport::Server;
use tonic::Request;
use tracing::info;

use args::Args;
use common::minio::{self, Client};
use core::{CoordinatorServer, MRCoordinator};
use job_queue::process_job_queue;

use crate::core::worker::KillWorkerRequest;
use crate::job_queue::update_job_state;
use crate::jobs::JobState;

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

    let job_queue_registry = coordinator.clone_registry();
    let shut_down_registry = coordinator.clone_registry();

    tokio::task::spawn(async move {
        loop {
            // A job has been pushed.
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

            if job_queue.lock().await.is_empty() {
                continue;
            }

            let result = process_job_queue(
                client.clone(),
                job_queue.clone(),
                job_queue_registry.clone(),
            )
            .await;

            match result {
                Ok(_) => info!(" - Task handled"),
                Err(e) => {
                    update_job_state(job_queue.clone(), JobState::Failed).await;
                    info!(" - {}", e)
                }
            }
        }
    });

    let (shut_down_sender, shut_down_receiver) = tokio::sync::oneshot::channel::<()>();

    tokio::task::spawn(async move {
        tokio::select! {
            result = tokio::signal::ctrl_c() => match result {
                Ok(()) => {
                    let worker_registry = shut_down_registry.lock().await;
                    let workers = worker_registry.get_workers();
                    for worker in workers {
                        let request = Request::new(KillWorkerRequest {});
                        let mut worker_client = worker.client.clone();
                        let _ = worker_client.kill_worker(request).await;
                    }
                    let _ = shut_down_sender.send(());
                }
                Err(_err) => {
                    // Err(format!("Unable to listen for shutdown signal: {}", err).into())
                }
            },
        }
    });

    let run_server = Server::builder()
        .add_service(CoordinatorServer::new(coordinator))
        .serve_with_shutdown(addr, async {
            shut_down_receiver.await.ok();
        });

    run_server.await?;

    Ok(())
}
