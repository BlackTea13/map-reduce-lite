use tonic::{transport::Server, Request, Response, Status};

use coordinator::coordinator_server::{Coordinator, CoordinatorServer};
use coordinator::{MessageRequest, MessageResponse};

pub mod coordinator {
    tonic::include_proto!("coordinator");
}

#[derive(Debug, Default)]
pub struct MyCoordinator {}

#[tonic::async_trait]
impl Coordinator for MyCoordinator {
    async fn echo(
        &self,
        request: Request<MessageRequest>,
    ) -> Result<Response<MessageResponse>, Status> {
        println!("Got a request from {:?}", request.remote_addr());

        let reply = MessageResponse {
            message: format!("Hello {}!", request.into_inner().message),
        };
        Ok(Response::new(reply))
    }
}

// Note: Move this out later.
const PORT: u16 = 8030;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("[::1]:{}", PORT).parse().unwrap();
    let greeter = MyCoordinator::default();

    println!("CoordinatorServer listening on {}", addr);

    Server::builder()
        .add_service(CoordinatorServer::new(greeter))
        .serve(addr)
        .await?;

    Ok(())
}
