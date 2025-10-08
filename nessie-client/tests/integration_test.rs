use testcontainers::{core::WaitFor, runners::AsyncRunner, GenericImage, ImageExt};

use nessie_client::{client::NessieClient, models::{Reference, ReferenceType}};

#[tokio::test]
async fn test_nessie_references() {
    let host_port = 19120;
    let _container = GenericImage::new("ghcr.io/projectnessie/nessie", "latest")
        .with_wait_for(WaitFor::message_on_stdout("Installed features: [agroal, amazon-sdk-dynamodb, amazon-sdk-secretsmanager, azure-keyvault-secret, cassandra-client, cdi, google-cloud-bigtable, google-cloud-secret-manager, hibernate-validator, jdbc-h2, jdbc-mariadb, jdbc-postgresql, logging-sentry, micrometer, mongodb-client, narayana-jta, oidc, opentelemetry, reactive-routes, rest, rest-jackson, security, security-properties-file, smallrye-context-propagation, smallrye-health, smallrye-openapi, vault, vertx]"))
        .with_mapped_port(19120, testcontainers::core::ContainerPort::Tcp(19120))
        .start()
        .await
        .unwrap();

    let base_url = format!("http://localhost:{host_port}/api/v2/");

    let nc = NessieClient::new(base_url.as_str()).expect("error in creating the nessie client");

    let refs = nc.list_references().await.expect("references error");
    assert_eq!(refs.len(), 1);

    let ref_name = refs[0].name.clone();

    let ref_main = nc.get_reference(ref_name.clone())
        .await
        .expect("reference error");

    assert_eq!(ref_main.reference.name, ref_name);

    let new_ref = Reference {
        name: "test".to_string(),
        ref_type: ReferenceType::Branch,
        hash: None,
    };

    let a = nc.create_reference("main".to_string(),"BRANCH".to_string(), new_ref)
        .await
        .expect("error in post reference");

    let refs = nc.list_references().await.expect("references error");
    assert_eq!(refs.len(), 2);
}