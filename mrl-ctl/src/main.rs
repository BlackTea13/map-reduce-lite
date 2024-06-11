use clap::{Parser};
use coordinator::{coordinator_client::CoordinatorClient, JobsRequest};

pub mod coordinator {
    tonic::include_proto!("coordinator");
}

use common::{Args, Commands};

/// Parse and user command.
/// Can be 1 of 3:
/// - submit
/// - status
/// - jobs
fn parse_args() -> Commands {
    Args::parse().command
}

const PORT: u16 = 8030;

/// Tasks
async fn jobs() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = CoordinatorClient::connect(format!("http://[::1]:{}", PORT)).await?;
    let request = tonic::Request::new(JobsRequest{});
    let response = client.jobs(request).await?;
    dbg!(response.into_inner());

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let command = parse_args();

    match command {
        Commands::Jobs => {
            jobs().await?;
        }
        Commands::Status => todo!(),
        Commands::Submit {
            input,
            workload,
            output,
            args,
        } => todo!(),
    }

    Ok(())
}
