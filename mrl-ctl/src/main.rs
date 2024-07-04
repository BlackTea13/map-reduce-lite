use args::{Commands, parse_args};

mod args;

mod core;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let command = parse_args();

    match command {
        Commands::Jobs => {
            core::jobs().await?;
        }
        Commands::Status => core::status().await?,
        Commands::Submit {
            input,
            workload,
            output,
            args,
            timeout,
        } => core::submit(input, output, workload, args, timeout).await?,
    }

    Ok(())
}
