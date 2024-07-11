use std::fs;
use std::sync::Arc;

use anyhow::anyhow;
use tokio::select;
use tokio::sync::{mpsc, Mutex};
use tonic::{Request, Response, Status};
use tracing::{error, info};
use walkdir::WalkDir;

use common::minio::{Client, ClientConfig};
//
// Import gRPC stubs/definitions.
//
pub use coordinator::{
    coordinator_client::CoordinatorClient, WorkerJoinRequest, WorkerLeaveRequest,
};
pub use worker::{
    AckRequest, AckResponse, InterruptWorkerRequest, InterruptWorkerResponse, KillWorkerRequest,
    KillWorkerResponse, MapJobRequest, ReceivedWorkRequest, ReceivedWorkResponse,
};
pub use worker::worker_server::{Worker, WorkerServer};

use crate::core::coordinator::WorkerDoneRequest;
use crate::core::worker::received_work_request::JobMessage::{MapMessage, ReduceMessage};
use crate::map;
use crate::reduce;

pub const WORKING_DIR_MAP: &str = "/var/tmp/map/";
pub const WORKING_DIR_REDUCE: &str = "/var/tmp/reduce/";

pub mod coordinator {
    tonic::include_proto!("coordinator");
}

pub mod worker {
    tonic::include_proto!("worker");
}

#[derive(Debug, Default)]
enum WorkerState {
    #[default]
    Idle,

    #[allow(dead_code)]
    InProgress,
}

#[derive(Debug)]
pub struct MRWorker {
    state: WorkerState,
    client: Client,
    id: Arc<Mutex<Option<u32>>>,
    address: String,
    sender: mpsc::Sender<()>,
    interrupt_sender: Arc<Mutex<Option<mpsc::Sender<()>>>>,
}

impl MRWorker {
    pub fn new(address: String, client_config: ClientConfig, sender: mpsc::Sender<()>) -> MRWorker {
        MRWorker {
            state: WorkerState::Idle,
            id: Arc::new(Mutex::new(None)),
            address,
            client: Client::from_conf(client_config),
            sender,
            interrupt_sender: Arc::new(Mutex::new(None)),
        }
    }
}

#[tonic::async_trait]
impl Worker for MRWorker {
    async fn received_work(
        &self,
        request: Request<ReceivedWorkRequest>,
    ) -> Result<Response<ReceivedWorkResponse>, Status> {
        let address = self.address.clone();
        let id = self.id.clone();
        let client = self.client.clone();
        let (interrupt_sender, mut interrupt_receiver) = mpsc::channel(32);

        {
            let mut sender = self.interrupt_sender.lock().await;
            *sender = Some(interrupt_sender);
        }

        {
            // Accept the work only if we are free
            match self.state {
                WorkerState::Idle => {}
                WorkerState::InProgress => {
                    return Ok(Response::new(ReceivedWorkResponse { success: false }))
                }
            }
        }

        let work_request = request.into_inner();

        tokio::spawn(async move {
            select! {
                _ = async {
                    let id = (*id.lock().await).unwrap();

                    let result = match work_request.job_message.unwrap() {
                        MapMessage(msg) => {
                            // Test for straggler: map
                            // info!("Sleeping");
                            //tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
                            // if id & 0xFFFF == 2 {
                            //     tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
                            // }

                            map::perform_map(msg, work_request.num_workers, &client).await
                        },
                        ReduceMessage(msg) => {
                            // Test for straggler: map
                            // if id == 1 {
                            //     tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
                            // }
                            reduce::perform_reduce(msg, &client, id).await
                        },
                    };

                    if let Err(e) = result {
                        error!("{}", anyhow!(e));
                    }

                    let coordinator_connect = CoordinatorClient::connect(address.clone()).await;

                    if let Ok(mut client) = coordinator_connect {
                        let request = Request::new(WorkerDoneRequest {
                            worker_id: id as i32,
                        });

                        if client.worker_done(request).await.is_err() {
                            error!("Worker (ID={}) failed to finish job", id & 0xFFFF);
                        } else {
                            info!("Worker (ID={}) done with job", id & 0xFFFF);
                        }
                    }
                } => {},
                _ = interrupt_receiver.recv() => {
                    let id = (*id.lock().await).unwrap();

                    let coordinator_connect = CoordinatorClient::connect(address.clone()).await;

                    if let Ok(mut client) = coordinator_connect {
                        let request = Request::new(WorkerDoneRequest {
                            worker_id: id as i32,
                        });

                        if client.worker_done(request).await.is_err() {
                            error!("Worker (ID={}) failed to finish job", id & 0xFFFF);
                        } else {
                            info!("Worker (ID={}) halted with job", id & 0xFFFF);
                        }
                    }
                }
            }
        });

        let reply = ReceivedWorkResponse { success: true };
        Ok(Response::new(reply))
    }

    async fn ack(&self, request: Request<AckRequest>) -> Result<Response<AckResponse>, Status> {
        let work_request = request.into_inner();
        info!(work_request.worker_id);
        {
            let mut id = self.id.lock().await;
            *id = Some(work_request.worker_id);
        }
        let reply = AckResponse {};
        Ok(Response::new(reply))
    }

    async fn kill_worker(
        &self,
        _: Request<KillWorkerRequest>,
    ) -> Result<Response<KillWorkerResponse>, Status> {
        info!("Kill signal received");
        if self.sender.clone().send(()).await.is_err() {
            return Err(Status::internal("Failed to send shutdown signal"));
        }
        let id = self.id.clone();
        let id = (*id.lock().await).unwrap() & 0xFFFF;

        // clean up locally cc: @Appy
        //
        tokio::task::spawn(async move {
            for entry in WalkDir::new(WORKING_DIR_MAP).into_iter().flatten() {
                if entry.path().is_dir()
                    && entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with(&format!("mrl-{}", id))
                {
                    let _ = fs::remove_dir_all(entry.path());
                }
            }

            for entry in WalkDir::new(WORKING_DIR_REDUCE).into_iter().flatten() {
                if entry.path().is_dir()
                    && entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with(&format!("mrl-{}", id))
                {
                    let _ = fs::remove_dir_all(entry.path());
                }
            }
        });

        let reply = KillWorkerResponse { success: true };
        Ok(Response::new(reply))
    }

    async fn interrupt_worker(
        &self,
        _: Request<InterruptWorkerRequest>,
    ) -> Result<Response<InterruptWorkerResponse>, Status> {
        info!("Interrupt signal received");

        {
            let mut interrupt_sender_lock = self.interrupt_sender.lock().await;
            if let Some(interrupt_sender) = interrupt_sender_lock.take() {
                interrupt_sender
                    .send(())
                    .await
                    .map_err(|_| Status::unknown("Failed to send interrupt"))?;
            }
        }

        let reply = InterruptWorkerResponse { success: true };
        Ok(Response::new(reply))
    }
}
