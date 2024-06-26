use std::sync::Arc;

use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tonic::transport::Channel;
use tracing::{debug, error, info};

use common::minio::{Client, ClientConfig};
//
// Import gRPC stubs/definitions.
//
pub use coordinator::{
    coordinator_client::CoordinatorClient, WorkerJoinRequest, WorkerLeaveRequest,
};
pub use worker::{AckRequest, AckResponse, MapJobRequest, ReceivedWorkRequest, ReceivedWorkResponse};
pub use worker::worker_server::{Worker, WorkerServer};

use crate::core::coordinator::WorkerDoneRequest;
use crate::core::worker::received_work_request::JobMessage::{MapMessage, ReduceMessage};
use crate::core::worker::worker_client::WorkerClient;
use crate::map;

pub mod coordinator {
    tonic::include_proto!("coordinator");
}

pub mod worker {
    tonic::include_proto!("worker");
}


#[derive(Debug, Default)]
enum WorkerState {
    #[default] Idle,
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
        MRWorker { state: WorkerState::Idle, id: Arc::new(Mutex::new(None)), address, client: Client::from_conf(client_config) }
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
            let worker_id = id.lock().await;
            info!("Worker (ID={:?}) Received a work request", worker_id);

            // Accept the work only if we are free
            match self.state {
                WorkerState::Idle => {}
                WorkerState::InProgress => return Ok(Response::new(ReceivedWorkResponse { success: false })),
            }
        }

        let work_request = request.into_inner();

        tokio::task::spawn(async move {
            let result = match work_request.job_message.unwrap() {
                MapMessage(msg) => map::perform_map(msg, &client).await,
                ReduceMessage(msg) => todo!(),
            };

            let mut coordinator_connect = CoordinatorClient::connect(address.clone()).await;
            let worker_id = id.lock().await.unwrap() as i32;

            if let Ok(mut client) = coordinator_connect {
                let request = Request::new(WorkerDoneRequest { worker_id });

                if let Err(_) = client.worker_done(request).await {
                    error!("Worker (ID={}) failed to finish job", worker_id);
                } else {
                    info!("Worker (ID={}) done with job", worker_id);
                }
            }
        });

        let reply = ReceivedWorkResponse { success: true };
        Ok(Response::new(reply))
    }

    async fn ack(&self, request: Request<AckRequest>) -> Result<Response<AckResponse>, Status> {
        let work_request = request.into_inner();
        {
            let mut id = self.id.lock().await;
            *id = Some(work_request.worker_id);
        }
        let reply = AckResponse {};
        Ok(Response::new(reply))
    }
}

