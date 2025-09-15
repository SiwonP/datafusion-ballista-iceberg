mod client;
mod error;
mod models;

use client::NessieClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let nessie = NessieClient::new("http://localhost:19120/api/v2/")?;

    let refresponse = nessie.get_reference("main".to_string()).await?;

    print!("{:?}", refresponse);

    Ok(())
}
