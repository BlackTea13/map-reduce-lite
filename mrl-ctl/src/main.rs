use clap::{command, Parser, Subcommand};

//
// Import gRPC stubs/definitions.
//
use coordinator::{coordinator_client::CoordinatorClient, JobsRequest};
pub mod coordinator {
    tonic::include_proto!("coordinator");
}

//
// For parsing user specified command.
//
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[clap(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// List all tasks which have been submitted to the system and their statuses.
    Jobs,
    /// Display the health status of the system, showing how many workers are registered,
    /// what the coordinator is doing and what the workers are doing.
    Status,
    /// Submit a job to the cluster
    Submit {
        /// Glob spec for the input files
        #[arg(short, long)]
        input: String,

        // Name of the workload
        #[arg(short, long)]
        workload: String,

        /// Output directory
        #[arg(short, long)]
        output: String,

        /// Auxiliary arguments to pass to the MapReduce application.
        #[clap(value_parser, last = true)]
        args: Vec<String>,
    },
}

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
    let request = tonic::Request::new(JobsRequest {});
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
