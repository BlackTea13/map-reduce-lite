use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// The port for the server to run on.
    #[arg(short, long, default_value = "8030")]
    pub port: u16,

    /// Minio access key / user ID.
    #[arg(short, long, default_value = "robert")]
    pub access_key_id: String,

    /// Minio secret key / password.
    #[arg(short, long, default_value = "robertisawesome")]
    pub secret_access_key: String,

    /// Minio region.
    #[arg(short, long, default_value = "us-east-1")]
    pub region: String,

    #[arg(short, long, default_value = "http://127.0.0.1:9000")]
    pub minio_url: String,
}
