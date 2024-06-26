// The function which deals with the job queue will just be a single function,
// if you wish to change this and refactor it into a struct, feel free to do so.
// - Appy

use std::sync::Arc;

use tokio::sync::Mutex;
use tracing::info;

use crate::core::worker::received_work_request::JobMessage;
use crate::core::worker::MapJobRequest;
use crate::core::ReceivedWorkRequest;
use common::minio::Client;

use crate::{
    jobs::{Job, JobQueue},
    worker_info::WorkerState,
    worker_registry::WorkerRegistry,
};
use tonic::Request;

pub async fn process_job_queue(
    client: Client,
    job_queue: Arc<Mutex<JobQueue>>,
    registry: Arc<Mutex<WorkerRegistry>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut job = {
        let mut job_queue = job_queue.lock().await;
        job_queue.pop_job().unwrap()
    };

    let result = _process_job_queue(&mut job, client, registry.clone()).await;

    // Ensure worker states are resetted.
    set_job_worker_state(registry, &mut job, WorkerState::Free).await?;

    result
}

/// Pop a job from the queue and perform it.
async fn _process_job_queue(
    job: &mut Job,
    client: Client,
    registry: Arc<Mutex<WorkerRegistry>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Collect workers and assign them to the job.
    // NOTE: Right now, workers can't join while job is inflight.
    //
    //       If we want workers to be able to join in between work
    //       phases, we have to clear job worker list and call this
    //       again. We can't just call this again because we might
    //       add duplicates into the job's worker ID list.

    assign_workers_to_job(registry.clone(), job).await?;

    // Handle the job in stages.

    // 1. Mapping stage.
    process_map_job(&client, registry.clone(), job).await?;

    monitor_workers(registry.clone(), job).await;

    set_job_worker_state(registry, job, WorkerState::Free).await?;

    Ok(())
}

async fn assign_workers_to_job(
    registry: Arc<Mutex<WorkerRegistry>>,
    job: &mut Job,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let workers = { registry.lock().await.get_free_workers() };

    if workers.is_empty() {
        return Err("Failed to assign workers - None available".into());
    }

    job.add_workers(workers);

    Ok(())
}

/// Process map job.
async fn process_map_job(
    client: &Client,
    registry: Arc<Mutex<WorkerRegistry>>,
    job: &mut Job,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let input_path = job.get_input_path().clone();
    info!("Input path for map {}", input_path);

    let input_files = client
        .list_objects_in_dir("robert", input_path.as_str())
        .await?;

    let free_workers = registry.lock().await.get_free_workers();
    for (i, input) in input_files.chunks(free_workers.len()).enumerate() {
        let worker_lock = registry.lock().await;
        let worker = worker_lock.get_worker(free_workers[i].clone()).unwrap();
        let mut worker_client = worker.client.clone();

        let map_message = MapJobRequest {
            input_keys: input.to_vec(),
            workload: job.get_workload().clone(),
            aux: job.get_args().clone(),
        };

        let request = ReceivedWorkRequest {
            job_message: Some(JobMessage::MapMessage(map_message)),
        };

        let request = Request::new(request);

        worker_client.received_work(request).await?;
    }

    // Set all the workers' state.
    set_job_worker_state(registry.clone(), job, WorkerState::Mapping).await?;

    Ok(())
}

/// Set the state of all workers assigned to a given job.
pub async fn set_job_worker_state(
    registry: Arc<Mutex<WorkerRegistry>>,
    job: &mut Job,
    state: WorkerState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let workers = job.get_workers();
    {
        let mut registry = registry.lock().await;
        workers
            .iter()
            .for_each(|&worker_id| registry.set_worker_state(worker_id, state.clone()));
    }

    Ok(())
}

async fn monitor_workers(registry: Arc<Mutex<WorkerRegistry>>, job: &mut Job) {
    let timeout = job.get_timeout().clone();
    tokio::select! {
        _ = wait_workers_free(registry.clone(), job) => {
            info!("All workers are free, proceed to next stage");
        },
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(timeout as u64)) => {
            info!("Stragglers detected");
        }
    }
}
/// Wait until all the workers are free
async fn wait_workers_free(
    registry: Arc<Mutex<WorkerRegistry>>,
    job: &mut Job,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut workers = job.get_workers().clone();
    let mut workers_working = job.get_workers().len();

    while workers_working > 0 {
        for worker_id in workers.iter_mut() {
            if *worker_id != -1 {
                let registry = registry.lock().await;
                if matches!(
                    registry.get_worker_state(*worker_id),
                    Some(WorkerState::Free)
                ) {
                    *worker_id = -1;
                    workers_working -= 1;
                }
            }
        }
    }

    Ok(())
}
