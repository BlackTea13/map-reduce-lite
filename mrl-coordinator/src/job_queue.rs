// The function which deals with the job queue will just be a single function,
// if you wish to change this and refactor it into a struct, feel free to do so.
// - Appy

use std::io::{Error, ErrorKind};
use std::sync::Arc;
use anyhow::anyhow;

use tokio::select;
use tokio::sync::Mutex;
use tonic::Request;
use tracing::{debug, info};

use common::minio::{path_to_bucket_key, Client};

use crate::core::worker::{KillWorkerRequest, MapJobRequest};
use crate::core::worker::{received_work_request::JobMessage, ReduceJobRequest};
use crate::core::ReceivedWorkRequest;
use crate::worker_info::WorkerID;
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
    monitor_workers(&client, registry.clone(), job, WorkerState::Mapping).await?;
    //
    // // 2. Reduce stage.
    process_reduce_job(&client, registry.clone(), job).await?;

    // Wait for workers to be complete.
    monitor_workers(&client, registry.clone(), job, WorkerState::Reducing).await?;

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
            output_path: path_out.clone(),
            workload: job.get_workload().clone(),
            aux: job.get_args().clone(),
            worker_id: worker.id.clone(),
        };

        info!("Saving files for worker {}", worker.id.clone());
        job.set_worker_map_files(worker.id, inputs.clone());

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

async fn get_reduce_input_files(
    client: &Client,
    bucket: &str,
    key: &str,
    index: usize,
) -> Result<Vec<String>, anyhow::Error> {
    let dir = format!("{}/temp/mr-in-{}", &key, index);

    client.list_objects_in_dir(&bucket, &dir[1..]).await
}

/// Process reduce job.
async fn process_reduce_job(
    client: &Client,
    registry: Arc<Mutex<WorkerRegistry>>,
    job: &mut Job,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let job_clone = job.clone();
    set_job_worker_state(registry.clone(), job, WorkerState::Reducing).await?;

    // Where output files are written to. (dir)
    let output_root = job_clone.get_output_path();

    let bucket_key = path_to_bucket_key(&output_root)?;
    let (bucket, output_key) = (bucket_key.bucket, bucket_key.key);

    let workers = job_clone.get_workers();

    for (index, worker) in workers.iter().enumerate() {
        let registry = registry.lock().await;
        let worker = registry.get_worker(*worker).unwrap();
        let mut worker_client = worker.client.clone();

        let inputs = get_reduce_input_files(client, &bucket, &output_key, index).await?;
        let index = index as u32;

        let reduce_message = ReduceJobRequest {
            bucket: bucket.clone(),
            inputs: inputs.clone(),
            output: output_key.clone(),
            aux: job_clone.get_args().clone(),
            workload: job_clone.get_workload().clone(),
            reduce_id: index,
        };

        info!("Saving files for worker {}", worker.id.clone());
        job.set_worker_reduce_files(worker.id, index, inputs.clone());

        let request = ReceivedWorkRequest {
            num_workers: workers.len() as u32,
            job_message: Some(JobMessage::ReduceMessage(reduce_message)),
        };

        let request = Request::new(request);

        worker_client.received_work(request).await?;
    }

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

/// Monitor for potential stragglers and handle them.
async fn monitor_workers(
    client: &Client,
    registry: Arc<Mutex<WorkerRegistry>>,
    job: &mut Job,
    current_state: WorkerState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let timeout = job.get_timeout().clone();

    select! {
        _ = wait_workers_free(registry.clone(), job) => {
            info!("All workers are free, proceed to next stage");
            Ok(())
        },
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(timeout as u64)) => {
            info!("Stragglers detected...");
            let _ = handling_stragglers(&client,registry.clone(), job, current_state).await;

            Ok(())
        }
    }
}

async fn handling_stragglers(
    client: &Client,
    registry: Arc<Mutex<WorkerRegistry>>,
    job: &mut Job,
    current_state: WorkerState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let job_clone = job.clone();
    let registry_lock = registry.lock().await;

    let stragglers: Vec<&WorkerID> = job_clone
        .get_workers()
        .iter()
        .filter(|&worker_id| {
            registry_lock.get_worker_state(*worker_id).unwrap() == current_state
        })
        .collect();

    let mut free_workers: Vec<&WorkerID> = job_clone
        .get_workers()
        .iter()
        .filter(|&worker_id| {
            matches!(
                registry_lock.get_worker_state(*worker_id),
                Some(WorkerState::Free)
            )
        })
        .collect();

    for straggler_id in stragglers {
        if let Some(free_worker_id) = free_workers.pop() {

            let worker = registry_lock.get_worker(*free_worker_id).unwrap();
            let mut worker_client = worker.client.clone();

            let request =
                match current_state {
                    WorkerState::Mapping => {
                        let straggler_input = job_clone.get_worker_map_files(&straggler_id).unwrap();
                        job.set_worker_map_files(
                            *free_worker_id,
                           straggler_input.clone(),
                        );
                        create_straggler_request(job, current_state, straggler_id.clone(), &straggler_input, None).await
                    },
                    _ => {
                        let (index, straggler_input) = job_clone.get_worker_reduce_files(&straggler_id).unwrap();
                        job.set_worker_reduce_files(
                            *free_worker_id,
                            *index,
                            straggler_input.clone(),
                        );
                        create_straggler_request(job, current_state, straggler_id.clone(),&straggler_input, Some(*index)).await
                    }
                }?;

            worker_client.received_work(request).await?;

            let client_clone = client.clone();
            let free_worker_id = free_worker_id.clone();
            let straggler_id = straggler_id.clone();
            let registry = registry.clone();
            let mut job = job.clone();

            info!(
                "Commencing a race between free {} and straggler {}",
                free_worker_id.clone(),
                straggler_id.clone()
            );

            tokio::spawn(async move {
                straggler_vs_free_worker(
                    &client_clone,
                    straggler_id,
                    free_worker_id,
                    registry.clone(),
                    &mut job,
                    current_state
                )
                .await
            });
        }
    }

    Ok(())
}

async fn create_straggler_request(
    job: &mut Job,
    current_state: WorkerState,
    worker_id: WorkerID,
    straggler_input: &Vec<String>,
    index: Option<u32>
) -> Result<Request<ReceivedWorkRequest>, Box<dyn std::error::Error + Send + Sync>> {
    let workload = job.get_workload().clone();
    let aux = job.get_args().clone();

    let input_path = job.get_input_path().clone();
    let output_path = job.get_output_path().clone();

    let input = path_to_bucket_key(&input_path)?;
    let (bucket_in, _) = (input.bucket, input.key);

    let output = path_to_bucket_key(&output_path)?;
    let (bucket_out, key_out) = (output.bucket, output.key);

    let workers = job.get_workers();

    let message = if matches!(current_state, WorkerState::Mapping) {
        JobMessage::MapMessage(MapJobRequest {
            bucket_in,
            input_keys: straggler_input.to_vec(),
            bucket_out,
            output_path: format!("{}/temp/straggler_copy", key_out),
            workload,
            aux,
            worker_id,
        })
    } else {
        JobMessage::ReduceMessage(ReduceJobRequest {
            bucket: bucket_in,
            inputs: straggler_input.clone(),
            output: key_out,
            aux,
            workload,
            reduce_id: index.unwrap() as u32,
        })
    };

    let request = ReceivedWorkRequest {
        num_workers: workers.len() as u32,
        job_message: Some(message),
    };

    Ok(Request::new(request))
}

async fn straggler_vs_free_worker(
    client: &Client,
    straggler_id: WorkerID,
    free_worker_id: WorkerID,
    registry: Arc<Mutex<WorkerRegistry>>,
    job: &mut Job,
    current_state: WorkerState
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let output_path = job.get_output_path().clone();
    let output = path_to_bucket_key(&output_path)?;
    let (bucket_out, _) = (output.bucket, output.key);

    let kill_message = KillWorkerRequest {};
    let request = KillWorkerRequest {};
    let request = Request::new(request);

    let key_prefix = "temp/straggler_copy/temp";


    select! {
        _ = wait_for_worker_to_become_free(registry.clone(), free_worker_id) => {
            let registry_lock = registry.lock().await;

            info!("Free worker {} is done", free_worker_id);

            let worker = registry_lock.get_worker(straggler_id).ok_or(anyhow!("Failed to find worker"))?;

            let mut worker_client = worker.client.clone();

            worker_client.kill_worker(request).await?;

            if matches!(WorkerState::Mapping, current_state) {

                // Wait for object to exist because of S3's upload latency
                // Ref: https://stackoverflow.com/questions/8856316/amazon-s3-how-to-deal-with-the-delay-from-upload-to-object-availability
                while client.list_objects_in_dir(&bucket_out, &key_prefix).await?.is_empty() {
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }

                client.move_objects(&bucket_out,"temp/straggler_copy/temp","temp").await?;

            };

        },
        _ = wait_for_worker_to_become_free(registry.clone(), straggler_id) => {

            let registry_lock = registry.lock().await;

            info!("Straggler worker {} is done", straggler_id);

            if matches!(WorkerState::Mapping, current_state){
                // Wait for object to exist because of S3's upload latency
                // Ref: https://stackoverflow.com/questions/8856316/amazon-s3-how-to-deal-with-the-delay-from-upload-to-object-availability
                while client.list_objects_in_dir(&bucket_out, &key_prefix).await?.is_empty() {
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }

                let source_objects = client.list_objects_in_dir(&bucket_out, key_prefix).await?;

                for source_object in source_objects {
                    client.delete_object(&bucket_out, &source_object).await?;
                }

            }


            let worker = registry_lock.get_worker(free_worker_id).ok_or(anyhow!("Failed to find worker"))?;

            let mut worker_client = worker.client.clone();

            worker_client.kill_worker(request).await?;
        },
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {
            info!("Timeout waiting for workers to become free");
        },
    }

    Ok(())
}

async fn wait_for_worker_to_become_free(registry: Arc<Mutex<WorkerRegistry>>, worker_id: WorkerID) {
    loop {
        let registry = registry.lock().await;
        if matches!(
            registry.get_worker_state(worker_id),
            Some(WorkerState::Free)
        ) {
            break;
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
