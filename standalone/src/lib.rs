use clap::{Parser, Subcommand};

mod engine;
pub mod standalone;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[clap(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
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

pub struct Job {
    pub input: String,
    pub workload: String,
    pub output: String,
    pub args: Vec<String>,
}