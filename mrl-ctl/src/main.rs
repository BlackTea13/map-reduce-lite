mod args;
use args::{parse_args, Commands};

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
        } => core::submit(input, output, workload, args).await?,
    }

    Ok(())
}
