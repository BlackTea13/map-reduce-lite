use args::{Commands, parse_args};

mod args;

mod core;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let command = parse_args();

    match command {
        Commands::Jobs { address } => {
            core::jobs(address).await?;
        }
        Commands::Status { address } => core::status(address).await?,
        Commands::Submit {
            address,
            input,
            workload,
            output,
            args,
            timeout,
        } => core::submit(address, input, output, workload, args, timeout).await?,
    }

    Ok(())
}
