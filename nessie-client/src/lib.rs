mod client;
mod error;
mod models;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_main_reference() {
        let nessie = client::NessieClient::new("http://localhost:19120/api/v2/").unwrap();
        let refresponse = nessie.get_reference("main".to_string()).await.unwrap();

        println!("{:?}", refresponse);

        // Optionally add assertions, e.g.,
        assert_eq!(refresponse.reference.name, "main");

    }
}
