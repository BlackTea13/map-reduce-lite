// The function which deals with the job queue will just be a single function,
// if you wish to change this and refactor it into a struct, feel free to do so.
// - Appy

use std::sync::Arc;

use tokio::sync::Mutex;
use tonic::codegen::Body;
use tonic::{Request, Response, Status};
use tracing::info;
use url::Url;

use common::minio::{path_to_bucket_key, Client};

use crate::core::worker::MapJobRequest;
use crate::core::worker::{received_work_request::JobMessage, ReduceJobRequest};
use crate::core::ReceivedWorkRequest;
use crate::{
    jobs::{Job, JobQueue},
    worker_info::WorkerState,
    worker_registry::WorkerRegistry,
};

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

    // Wait for workers to be complete.
    monitor_workers(registry.clone(), job).await;

    // 2. Reduce stage.
    process_reduce_job(&client, registry.clone(), job).await?;

    // Wait for workers to be complete.
    monitor_workers(registry.clone(), job).await;

    // TODO: Wait for workers to be complete.
    monitor_workers(registry.clone(), job).await;

    // 2. Reduce stage.
    process_reduce_job(&client, registry.clone(), job).await?;

    // TODO: Wait for workers to be complete.
    monitor_workers(registry.clone(), job).await;

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
    let (bucket_out, key_out) = (output.bucket, output.key);

    let input_files = client.list_objects_in_dir(&bucket_in, &key_in).await?;

    let job_clone = job.clone();
    let workers = job_clone.get_workers();
    let chunk_size = f32::ceil((input_files.len() as f32) / (workers.len() as f32)) as usize;
    let jobs = input_files.chunks(chunk_size).zip(workers.iter());

    for (input, worker) in jobs {
        let registry = registry.lock().await;
        let worker = registry.get_worker(*worker).unwrap();
        let mut worker_client = worker.client.clone();
        let inputs = input.to_vec();

        let map_message = MapJobRequest {
            bucket_in: bucket_in.clone(),
            input_keys: input.to_vec(),
            bucket_out: bucket_out.clone(),
            output_key: key_out.clone(),
            workload: job.get_workload().clone(),
            aux: job.get_args().clone(),
        };

        job.set_worker_files(worker.id, inputs.clone());

        let request = ReceivedWorkRequest {
            num_workers: workers.len() as u32,
            job_message: Some(JobMessage::MapMessage(map_message)),
        };

        let request = Request::new(request);

        worker_client.received_work(request).await?;
    }

    // Set all the workers' state.
    set_job_worker_state(registry.clone(), job, WorkerState::Mapping).await?;
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

    let temp_output_files = client.list_objects_in_dir(&bucket, &key).await?;

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

/// Monitor for stragglers and handle them
async fn monitor_workers(
    registry: Arc<Mutex<WorkerRegistry>>,
    job: &mut Job,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let timeout = job.get_timeout().clone();
    let workload = job.get_workload().clone();
    let aux = job.get_args().clone();

    let output_path = job.get_output_path().clone();

    let output = path_to_bucket_key(&output_path)?;
    let (bucket_out, key_out) = (output.bucket, output.key);

    tokio::select! {
        _ = wait_workers_free(registry.clone(), job) => {
            info!("All workers are free, proceed to next stage");
            Ok(())
        },
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(timeout as u64)) => {
            let registry = registry.lock().await;
            let job_clone = job.clone();
            let stragglers: Vec<&i32> = job_clone.get_workers().iter()
                .filter(|&worker_id| matches!(registry.get_worker_state(*worker_id), Some(WorkerState::Mapping)))
                .collect();
            let mut free_workers: Vec<&i32> = job_clone.get_workers().iter()
                .filter(|&worker_id| matches!(registry.get_worker_state(*worker_id), Some(WorkerState::Free)))
                .collect();


            for straggler_id in stragglers {
                if let Some(free_worker_id) = free_workers.pop() {
                    let worker = registry.get_worker(*free_worker_id).unwrap();
                    let mut worker_client = worker.client.clone();
                    let straggler_input = job.get_worker_files(&straggler_id).unwrap();

                    let map_message = MapJobRequest {
                        bucket_in: bucket_in.clone(),
                        input_keys: straggler_input.to_vec(),
                        bucket_out: bucket_out.clone(),
                        output_key: format!("{}_straggler_copy", key_out.clone()),
                        workload: workload,
                        aux: aux,
                    };

                    job.set_worker_files(*free_worker_id, straggler_input.clone());

                    let request = ReceivedWorkRequest {
                        job_message: Some(JobMessage::MapMessage(map_message)),
                    };

                    let request = Request::new(request);

                    job.set_worker_files(*free_worker_id, job.get_worker_files(&straggler_id).unwrap().clone());

                    worker_client.received_work(request).await?;
                }
            }
            info!("Stragglers detected");
            Ok(())
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
