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
pub use worker::{AckRequest, AckResponse, ReceivedWorkRequest, MapJobRequest, ReduceJobRequest, ReceivedWorkResponse};
pub mod worker {
    tonic::include_proto!("worker");
}

use tonic::{Request, Response, Status};

use workload::{vertex_degree, wc, grep};
use common::{KeyValue, Workload};

use bytes::Bytes;
use tracing::debug;
use common::job::JobState;
use crate::core::worker::received_work_request::JobMessage::{MapMessage, ReduceMessage};

use crate::map;

#[derive(Debug)]
enum WorkerState {
    Idle,
    InProgress
}

#[derive(Debug)]
pub struct MRWorker {
    state: WorkerState,
}

impl MRWorker {
    fn new() -> MRWorker {
        MRWorker {state: WorkerState::Idle}
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
            WorkerState::Idle => {},
            WorkerState::InProgress => return Ok(Response::new(ReceivedWorkResponse{ success: false})),
        };

        let work_request = request.into_inner();
        match work_request.job_message.unwrap() {
            MapMessage(msg) => map::perform_map(msg),
            ReduceMessage(msg) => todo!(),
        }.await;

        let reply = ReceivedWorkResponse { success: true };
        Ok(Response::new(reply))
    }

    // Just for debugging. This can be removed.
    async fn ack(&self, _: Request<AckRequest>) -> Result<Response<AckResponse>, Status> {
        let reply = AckResponse {};
        Ok(Response::new(reply))
    }
}
