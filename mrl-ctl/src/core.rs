use coordinator::{AddJobRequest, coordinator_client::CoordinatorClient, JobsRequest};

//
// Import gRPC stubs/definitions.
//
use crate::core::coordinator::StatusRequest;

pub mod coordinator {
    tonic::include_proto!("coordinator");
}

const PORT: u16 = 8030;

// Tasks
pub async fn jobs() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = CoordinatorClient::connect(format!("http://[::1]:{}", PORT)).await?;
    let request = tonic::Request::new(JobsRequest {});
    let response = client.jobs(request).await?;

    let data = response.into_inner().data;

    println!("[Jobs]");
    println!("{}", data[0]);
    println!("{}", data[1]);

    if data.len() > 2 {
        println!();
        for s in &data[2..] {
            println!("{}", s);
        }
    }

    Ok(())
}

pub async fn submit(
    input: String,
    output: String,
    workload: String,
    aux: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = CoordinatorClient::connect(format!("http://[::1]:{}", PORT)).await?;
    let request = tonic::Request::new(AddJobRequest {
        input_files: input,
        output_files: output,
        workload,
        aux,
    });
    let response = client.add_job(request).await?;
    dbg!(response.into_inner());

    Ok(())
}

pub async fn status() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = CoordinatorClient::connect(format!("http://[::1]:{}", PORT)).await?;
    let request = tonic::Request::new(StatusRequest {});
    let response = client.status(request).await?;

    let data = response.into_inner().data;

    println!("[Status]");
    for s in data {
        println!("{}", s);
    }

    Ok(())
}
