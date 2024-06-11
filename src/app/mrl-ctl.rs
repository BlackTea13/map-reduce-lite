use std::io::stdin;

use coordinator::{coordinator_client::CoordinatorClient, MessageRequest};

pub mod coordinator {
  tonic::include_proto!("coordinator");
}

const PORT: u16 = 8030;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  let mut client = CoordinatorClient::connect(format!("http://[::1]:{}", PORT)).await?;
  loop {
    println!("\nInput message");
    let mut message: String = String::new();
    stdin().read_line(&mut message).unwrap();
    let message = message.trim();
    let request = tonic::Request::new(MessageRequest {
      message: String::from(message),
    });
    let response = client.echo(request).await?;
    println!("Got: '{}' from service", response.into_inner().message);
  }
  Ok(())
}
