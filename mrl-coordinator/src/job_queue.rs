// The function which deals with the job queue will just be a single function,
// if you wish to change this and refactor it into a struct, feel free to do so.
// - Appy

use std::sync::Arc;

use tokio::sync::Mutex;
use tonic::{Request};
use tracing::info;

use common::minio::{Client, path_to_bucket_key};

use crate::{
    jobs::{Job, JobQueue},
    worker_info::WorkerState,
    worker_registry::WorkerRegistry,
};
use crate::core::ReceivedWorkRequest;
use crate::core::worker::{received_work_request::JobMessage, ReduceJobRequest};
use crate::core::worker::MapJobRequest;

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

    // TODO: Wait for workers to be complete.

    // 2. Reduce stage.
    process_reduce_job(&client, registry.clone(), job).await?;

    // TODO: Wait for workers to be complete.

    // TODO: This can be removed. Once we actually reset the state of workers.
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
    // Set all the workers' state.
    set_job_worker_state(registry.clone(), job, WorkerState::Mapping).await?;

    let input_path = job.get_input_path().clone();
    let output_path = job.get_output_path().clone();
    info!("Input path for map {}", input_path);

    let input = path_to_bucket_key(&input_path)?;
    let (bucket_in, key_in) = (input.bucket, input.key);

    let output = path_to_bucket_key(&output_path)?;
    let (bucket_out, path_out) = (output.bucket, output.key);

    let input_files = client.list_objects_in_dir(&bucket_in, &key_in).await?;

    let workers = job.get_workers();
    let chunk_size = f32::ceil((input_files.len() as f32) / (workers.len() as f32)) as usize;
    let jobs = input_files.chunks(chunk_size).zip(workers.iter());

    for (input, worker) in jobs {
        let registry = registry.lock().await;
        let worker = registry.get_worker(*worker).unwrap();
        let mut worker_client = worker.client.clone();

        let map_message = MapJobRequest {
            bucket_in: bucket_in.clone(),
            input_keys: input.to_vec(),
            bucket_out: bucket_out.clone(),
            output_path: path_out.clone(),
            workload: job.get_workload().clone(),
            aux: job.get_args().clone(),
        };

        let request = ReceivedWorkRequest {
            num_workers: workers.len() as u32,
            job_message: Some(JobMessage::MapMessage(map_message)),
        };

        let request = Request::new(request);

        worker_client.received_work(request).await?;
    }

    Ok(())
}

/// Process reduce job.
async fn process_reduce_job(
    client: &Client,
    registry: Arc<Mutex<WorkerRegistry>>,
    job: &mut Job,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let workers = job.get_workers();
    let output_path = job.get_output_path().clone();
    let temp_output_path = format!("{output_path}/temp");

    let bucket_key = path_to_bucket_key(&output_path)?;
    let (bucket, key) = (bucket_key.bucket, bucket_key.key);

    let temp_output_files = client
        .list_objects_in_dir(&bucket, &key)
        .await?;

    info!("Input path for reduce {output_path}");

    let jobs = temp_output_files.into_iter().zip(workers.iter());

    for (input, worker) in jobs {
        let registry = registry.lock().await;
        let worker = registry.get_worker(*worker).unwrap();
        let mut worker_client = worker.client.clone();

        let reduce_message = ReduceJobRequest {
            input_key: input.clone(),
            workload: job.get_workload().clone(),
            aux: job.get_args().clone(),
        };

        let request = ReceivedWorkRequest {
            num_workers: workers.len() as u32,
            job_message: Some(JobMessage::ReduceMessage(reduce_message)),
        };

        let request = Request::new(request);

        worker_client.received_work(request).await?;
    }

    set_job_worker_state(registry.clone(), job, WorkerState::Reducing).await?;

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
