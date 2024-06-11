use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// The address of the coordinator server
    #[arg(short = 'j', long = "join", default_value = "http://[::1]:8030")]
    pub address: String,

    /// The port to run the worker on.
    #[arg(short, long)]
    pub port: u16,
}
