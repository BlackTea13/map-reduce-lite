use clap::{command, Parser, Subcommand};

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
        // IP Address of Coordinator
        #[arg(short, long)]
        address: String,

        // Glob spec for the input files
        #[arg(short, long)]
        input: String,

        // Name of the workload
        #[arg(short, long)]
        workload: String,

        /// Output directory
        #[arg(short, long)]
        output: String,

        /// Timeout for worker executing a job
        #[clap(short, long)]
        timeout: Option<u32>,

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
pub fn parse_args() -> Commands {
    Args::parse().command
}
