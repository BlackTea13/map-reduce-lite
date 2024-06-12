use anyhow::*;
use bytes::Bytes;
use clap::Parser;
use common::Workload;
use standalone::{Args, Commands};
use standalone::{
    engine::{perform_map, perform_reduce},
    Job,
};

fn parse_args() -> Job {
    let args = Args::parse();
    match args.command {
        Commands::Submit {
            input,
            workload,
            output,
            args,
        } => Job {
            input,
            workload,
            output,
            args,
        },
    }
}
fn run_standalone_mr_job(job: Job, engine: Workload) -> Result<()> {
    let serialized_args = Bytes::from(serde_json::to_string(&job.args)?);
    let n_reduce = 11;
    /*  The map logic carries out mapping and also shuffle. This makes sense in
     *  the case of a standalone system.
     */
    let buckets = perform_map(&job, &engine, &serialized_args, n_reduce)?;
    // reduce phase
    perform_reduce(&job, &engine, &serialized_args, n_reduce, buckets)
}

fn main() -> Result<()> {
    let job = parse_args();
    let engine = workload::try_named(&job.workload).expect("what?");

    run_standalone_mr_job(job, engine)
}
