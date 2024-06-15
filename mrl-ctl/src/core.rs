use clap::Command;
use common::Workload;
//
// Import gRPC stubs/definitions.
//
use crate::args::Commands;
use crate::args::Commands::Submit;
use coordinator::{coordinator_client::CoordinatorClient, JobsRequest, StartTaskRequest};

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

pub async fn submit(
    input: String,
    output: String,
    workload: String,
    aux: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = CoordinatorClient::connect(format!("http://[::1]:{}", PORT)).await?;
    let request = tonic::Request::new(StartTaskRequest {
        input_files: input,
        output_files: output,
        workload,
        aux,
    });
    let response = client.start_task(request).await?;
    dbg!(response.into_inner());

    Ok(())
}
