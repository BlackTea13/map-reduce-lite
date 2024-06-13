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
pub use worker::{AckRequest, AckResponse, WorkRequest, WorkResponse};
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
    async fn request_work(
        &self,
        request: Request<WorkRequest>,
    ) -> Result<Response<WorkResponse>, Status> {
        // println!("Got a work request {:?}", request.into_inner());

        let work_request = request.into_inner();

        let buf = Bytes::from("appy");
        let key = Bytes::from("filename");
        let input_kv = KeyValue {
            key: key.clone(),
            value: buf,
        };

        match work_request.work_type.as_bytes() {
            b"map" => map(input_kv, Bytes::from(work_request.workload)),
            b"reduce" => reduce(key, Bytes::from(work_request.workload)),
            _ => unimplemented!(),
        };

        let reply = WorkResponse { success: true };
        Ok(Response::new(reply))
    }

    // Just for debugging. This can be removed.
    async fn ack(&self, _: Request<AckRequest>) -> Result<Response<AckResponse>, Status> {
        let reply = AckResponse {};
        Ok(Response::new(reply))
    }
}
