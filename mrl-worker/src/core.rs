use std::sync::Arc;
use tokio::sync::Mutex;
//
// Import gRPC stubs/definitions.
//
pub use coordinator::{
    coordinator_client::CoordinatorClient, WorkerJoinRequest, WorkerLeaveRequest,
};

pub mod coordinator {
    tonic::include_proto!("coordinator");
}

pub use worker::worker_server::{Worker, WorkerServer};
pub use worker::{AckRequest, AckResponse, ReceivedWorkRequest, MapJobRequest, ReceivedWorkResponse};

pub mod worker {
    tonic::include_proto!("worker");
}

use tonic::{Request, Response, Status};
use tonic::transport::Channel;


use tracing::{debug, error, info};
use crate::core::coordinator::WorkerDoneRequest;
use crate::core::worker::received_work_request::JobMessage::{MapMessage, ReduceMessage};
use crate::core::worker::worker_client::WorkerClient;

use crate::map;

#[derive(Debug, Default)]
enum WorkerState {
    #[default] Idle,
    InProgress,
}

#[derive(Debug, Default)]
pub struct MRWorker {
    state: WorkerState,
    id: Arc<Mutex<Option<u32>>>,
    address: String,
}

impl MRWorker {
    pub fn new(address: String) -> MRWorker {
        MRWorker { state: WorkerState::Idle, id: Arc::new(Mutex::new(None)), address  }
    }
}

#[tonic::async_trait]
impl Worker for MRWorker {
    async fn received_work(
        &self,
        request: Request<ReceivedWorkRequest>,
    ) -> Result<Response<ReceivedWorkResponse>, Status> {

        debug!("Received a work request");

        // we accept the work only if we are free
        match self.state {
            WorkerState::Idle => {}
            WorkerState::InProgress => return Ok(Response::new(ReceivedWorkResponse { success: false })),
        };

        let address = self.address.clone();
        let id = self.id.clone();

        let work_request = request.into_inner();
        tokio::task::spawn(async move {
            let _ = match work_request.job_message.unwrap() {
                MapMessage(msg) => map::perform_map(msg).await,
                ReduceMessage(msg) => {
                    todo!()
                },

            };

            let mut coordinator_connect = CoordinatorClient::connect(address).await;


            if let Ok(mut client) = coordinator_connect {
                let request = Request::new(WorkerDoneRequest { worker_id: (*id.lock().await).unwrap() as i32 });

                let resp = client.worker_done(request).await;
                if resp.is_err() {
                    error!("Failed to finish job");
                } else {
                    info!("Worker done!")
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
