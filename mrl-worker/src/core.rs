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
pub use worker::{AckRequest, AckResponse, ReceivedWorkRequest, ReceivedWorkResponse, WorkType};
pub mod worker {
    tonic::include_proto!("worker");
}

use tonic::{Request, Response, Status};

use crate::workload::{map, reduce, KeyValue};

use bytes::Bytes;

#[derive(Debug, Default)]
pub struct MRWorker {}

#[tonic::async_trait]
impl Worker for MRWorker {
    async fn received_work(
        &self,
        request: Request<ReceivedWorkRequest>,
    ) -> Result<Response<ReceivedWorkResponse>, Status> {
        // println!("Got a work request {:?}", request.into_inner());

        let work_request = request.into_inner();

        let buf = Bytes::from("appy");
        let key = Bytes::from("filename");
        let input_kv = KeyValue {
            key: key.clone(),
            value: buf,
        };

        match WorkType::from_i32(work_request.work_type) {
            Some(WorkType::Map) => {
                dbg!("Performing map");
                map(input_kv, Bytes::from(work_request.workload))
            }
            Some(WorkType::Reduce) => {
                dbg!("Performing reduce");
                reduce(key, Bytes::from(work_request.workload))
            }
            _ => unimplemented!(),
        };

        let reply = ReceivedWorkResponse { success: true };
        Ok(Response::new(reply))
    }

    // Just for debugging. This can be removed.
    async fn ack(&self, _: Request<AckRequest>) -> Result<Response<AckResponse>, Status> {
        let reply = AckResponse {};
        Ok(Response::new(reply))
    }
}
