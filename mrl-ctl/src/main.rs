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
        Commands::Status => todo!(),
        Commands::Submit {
            input,
            workload,
            output,
            args,
        } => core::submit(input, workload, output, args).await?,
    }

    Ok(())
}
