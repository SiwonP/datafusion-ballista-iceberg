mod client;
mod error;
mod models;

use std::collections::HashMap;

use client::NessieClient;
use iceberg::{
    spec::{NestedField, NestedFieldRef, PrimitiveType, Schema, Type},
    Catalog, NamespaceIdent, TableCreation,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let nessie = NessieClient::new("http://localhost:19120/api/v2/")?;

    let ns1 = NamespaceIdent::from_vec(vec!["test".to_string()])?;

    let new_namespace = nessie.create_namespace(&ns1, HashMap::new()).await?;

    let ns2 = NamespaceIdent::from_vec(vec!["test".to_string(), "db".to_string()])?;

    let new_namespace = nessie.create_namespace(&ns2, HashMap::new()).await?;

    let table_ident = iceberg::TableIdent::new(ns2.clone(), "my_table".into());

    let fields: Vec<NestedFieldRef> = vec![
        NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
        NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
    ];

    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(fields)
        .build()?;

    let mut props = HashMap::new();
    props.insert("owner".to_string(), "admin".to_string());

    let creation = TableCreation::builder()
        .schema(schema)
        .properties(props)
        .name(table_ident.name().into())
        .build();

    let ns = nessie.create_table(&ns2, creation).await?;

    let ns_ident = nessie.list_namespaces(None).await?;

    for ns_id in ns_ident {
        println!("{:?}", ns_id);
    }

    Ok(())
}
