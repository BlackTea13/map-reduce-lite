//
// Import gRPC stubs/definitions.
//
use coordinator::{coordinator_client::CoordinatorClient, JobsRequest};
pub mod coordinator {
    tonic::include_proto!("coordinator");
}

const PORT: u16 = 8030;

// Tasks
pub async fn jobs() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = CoordinatorClient::connect(format!("http://[::1]:{}", PORT)).await?;
    let request = tonic::Request::new(JobsRequest {});
    let response = client.jobs(request).await?;
    dbg!(response.into_inner());

    Ok(())
}

