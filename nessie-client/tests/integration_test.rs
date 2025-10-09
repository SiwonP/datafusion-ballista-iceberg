use testcontainers::{core::WaitFor, runners::AsyncRunner, GenericImage, ImageExt};

use nessie_client::{
    client::NessieClient,
    models::{CommitMeta, Content, ContentKey, Operation, Operations},
};

#[tokio::test]
async fn test_nessie_references() {
    let host_port = 29120;
    let _container = GenericImage::new("ghcr.io/projectnessie/nessie", "latest")
        .with_wait_for(WaitFor::message_on_stdout("Installed features: [agroal, amazon-sdk-dynamodb, amazon-sdk-secretsmanager, azure-keyvault-secret, cassandra-client, cdi, google-cloud-bigtable, google-cloud-secret-manager, hibernate-validator, jdbc-h2, jdbc-mariadb, jdbc-postgresql, logging-sentry, micrometer, mongodb-client, narayana-jta, oidc, opentelemetry, reactive-routes, rest, rest-jackson, security, security-properties-file, smallrye-context-propagation, smallrye-health, smallrye-openapi, vault, vertx]"))
        .with_mapped_port(host_port, testcontainers::core::ContainerPort::Tcp(19120))
        .start()
        .await
        .unwrap();

    let base_url = format!("http://localhost:{host_port}/api/v2/");

    let nc = NessieClient::new(base_url.as_str()).expect("error in creating the nessie client");

    let refs = nc.list_references().await.expect("references error");
    assert_eq!(refs.len(), 1);

    let ref_name = refs[0].name.clone();

    let ref_main = nc
        .get_reference(ref_name.clone())
        .await
        .expect("reference error");

    assert_eq!(ref_main.reference.name, ref_name);

    let new_ref_response = nc
        .create_reference("test".to_string(), "BRANCH".to_string(), refs[0].clone())
        .await
        .expect("error in post reference");

    let refs = nc.list_references().await.expect("references error");
    assert_eq!(refs.len(), 2);

    let _ = nc
        .delete_reference(new_ref_response.reference)
        .await
        .expect("error deleting");

    let refs = nc.list_references().await.expect("references error");
    assert_eq!(refs.len(), 1);
}

#[tokio::test]
async fn test_nessie_entries() {
    let host_port = 29121;
    let _container = GenericImage::new("ghcr.io/projectnessie/nessie", "latest")
        .with_wait_for(WaitFor::message_on_stdout("Installed features: [agroal, amazon-sdk-dynamodb, amazon-sdk-secretsmanager, azure-keyvault-secret, cassandra-client, cdi, google-cloud-bigtable, google-cloud-secret-manager, hibernate-validator, jdbc-h2, jdbc-mariadb, jdbc-postgresql, logging-sentry, micrometer, mongodb-client, narayana-jta, oidc, opentelemetry, reactive-routes, rest, rest-jackson, security, security-properties-file, smallrye-context-propagation, smallrye-health, smallrye-openapi, vault, vertx]"))
        .with_mapped_port(host_port, testcontainers::core::ContainerPort::Tcp(19120))
        .start()
        .await
        .unwrap();

    let base_url = format!("http://localhost:{host_port}/api/v2/");

    let nc = NessieClient::new(base_url.as_str()).expect("error in creating the nessie client");

    let refs = nc
        .list_references()
        .await
        .expect("error in listing references");

    let main_entries = nc
        .list_entries(refs[0].clone())
        .await
        .expect("error in listing main entries");

    assert_eq!(main_entries.len(), 0);

    let operations = Operations {
        commit_meta: CommitMeta {
            author: "test_author".to_string(),
            author_time: chrono::prelude::Utc::now().to_rfc3339(),
            message: "test commit message".to_string(),
            signed_off_by: None,
            properties: todo!(),
        },
        operations: vec![Operation::Put {
            key: ContentKey { elements: todo!() },
            content: Content {
                content_type: "ICEBERG_TABLE".to_string(),
                metadata_location: todo!(),
                snapshot_id: todo!(),
                schema_id: todo!(),
                spec_id: todo!(),
                sort_order_id: todo!(),
            },
        }],
    };

    let new_entry_response = nc
        .commit_entry(refs[0].clone(), operations)
        .await
        .expect("error in commiting entry");
}
