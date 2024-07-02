use std::sync::Arc;

use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tracing::{error, info};

use common::minio::{Client, ClientConfig};
//
// Import gRPC stubs/definitions.
//
pub use coordinator::{
    coordinator_client::CoordinatorClient, WorkerJoinRequest, WorkerLeaveRequest,
};
pub use worker::worker_server::{Worker, WorkerServer};
pub use worker::{
    AckRequest, AckResponse, MapJobRequest, ReceivedWorkRequest, ReceivedWorkResponse,
};

use crate::core::coordinator::WorkerDoneRequest;
use crate::core::worker::received_work_request::JobMessage::{MapMessage, ReduceMessage};
use crate::map;
use crate::reduce;

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
    InProgress,
}

#[derive(Debug)]
pub struct MRWorker {
    state: WorkerState,
    client: Client,
    id: Arc<Mutex<Option<u32>>>,
    address: String,
}

impl MRWorker {
    pub fn new(address: String, client_config: ClientConfig) -> MRWorker {
        MRWorker {
            state: WorkerState::Idle,
            id: Arc::new(Mutex::new(None)),
            address,
            client: Client::from_conf(client_config),
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

        tokio::task::spawn(async move {
            let id = id.clone().lock().await.unwrap();

            let _ = match work_request.job_message.unwrap() {
                MapMessage(msg) => {
                    map::perform_map(msg, &id, work_request.num_workers, &client).await
                }
                ReduceMessage(msg) => reduce::perform_reduce(msg, &id, &client).await,
            };

            let coordinator_connect = CoordinatorClient::connect(address.clone()).await;

            if let Ok(mut client) = coordinator_connect {
                let request = Request::new(WorkerDoneRequest {
                    worker_id: id as i32,
                });

                if let Err(_) = client.worker_done(request).await {
                    error!("Worker (ID={}) failed to finish job", id & 0xFFFF);
                } else {
                    info!("Worker (ID={}) done with job", id & 0xFFFF);
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
}
