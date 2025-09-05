use std::collections::HashMap;
use std::vec;

use crate::error::NessieError;
use crate::models::{
    CommitResponse, Content, ContentKey, EntriesResponse, Entry, Reference, ReferenceResponse,
    ReferencesResponse,
};
use iceberg::io::{FileIO, FileIOBuilder};
use iceberg::table::Table;
use reqwest::Client;
use url::Url;

use iceberg::{Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent};

use async_trait::async_trait;

#[derive(Debug)]
pub struct NessieClient {
    base_url: Url,
    client: Client,
    file_io: iceberg::io::FileIO,
}

impl NessieClient {
    pub fn new(base_url: &str) -> Result<Self, NessieError> {
        let file_io = FileIOBuilder::new("s3")
            .with_prop("s3.endpoint", "https://s3.gra.io.cloud.ovh.net/")
            .with_prop("s3.accessKey", "52b575304c24477a9cdeddbfcb504756")
            .with_prop("s3.secretKey", "cb51da2806ad49c3952777bc4458d3e0").build()?;

        Ok(Self {
            base_url: base_url.parse()?,
            client: Client::new(),
            file_io: file_io,
        })
    }

    pub async fn list_references(&self) -> Result<Vec<Reference>, NessieError> {
        let url = self.base_url.join("trees")?;
        // let response = self.client.get(url).send().await?.json::<ReferencesResponse>()?;
        let response = self
            .client
            .get(url)
            .send()
            .await?
            .json::<ReferencesResponse>()
            .await?;
        Ok(response.references)
    }

    pub async fn get_reference(
        &self,
        reference_name: String,
    ) -> Result<ReferenceResponse, NessieError> {
        let url = self.base_url.join(&format!("trees/{}", reference_name))?;
        let response = self
            .client
            .get(url)
            .send()
            .await?
            .json::<ReferenceResponse>()
            .await?;
        Ok(response)
    }

    pub async fn create_reference(
        &self,
        reference: Reference,
    ) -> Result<ReferenceResponse, NessieError> {
        let url = self.base_url.join("trees")?;
        let response = self
            .client
            .post(url)
            .json(&reference)
            .send()
            .await?
            .json::<ReferenceResponse>()
            .await?;
        Ok(response)
    }

    pub async fn delete_reference(
        &self,
        reference: Reference,
    ) -> Result<ReferenceResponse, NessieError> {
        let url = self.base_url.join(&format!("trees/{}", reference.name))?;
        let response = self
            .client
            .delete(url)
            .send()
            .await?
            .json::<ReferenceResponse>()
            .await?;
        Ok(response)
    }

    pub async fn list_entries(&self, reference: &str) -> Result<Vec<String>, NessieError> {
        let url = self
            .base_url
            .join(&format!("trees/{}/entries", reference))?;
        let response = self.client.get(url).send().await?;

        // if !response.status().is_success() {
        //     let status = response.status();
        //     let body = response.text().unwrap_or_default();
        //     return Err(NessieError::InvalidResponse(format!(
        //         "Status: {}, Body: {}",
        //         status, body
        //     )));
        // }

        let parsed: EntriesResponse = response.json().await?;

        let entries = parsed
            .entries
            .into_iter()
            .map(|e| e.name.to_string())
            .collect();

        Ok(entries)
    }

    pub async fn commit_entry(
        &self,
        branch: &str,
        operations: crate::models::Operations,
    ) -> Result<crate::models::CommitResponse, NessieError> {
        let url = self
            .base_url
            .join(&format!("trees/{}/history/commit", branch))?;
        let response = self.client.post(url).json(&operations).send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<failed to read body>".to_string());
            println!("{}", status);
            println!("{}", body);
            return Err(NessieError::InvalidResponse(format!(
                "HTTP {}: {}",
                status, body
            )));
        }
        Ok(response.json::<CommitResponse>().await?)
    }
}

#[async_trait]
impl Catalog for NessieClient {
    #[doc = " List namespaces inside the catalog."]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>, iceberg::Error> {
        // Get all entries from Nessie (main branch, or configurable)
        let entries = self.list_entries("main").await?; // or self.reference()

        let mut namespaces = std::collections::HashSet::new();

        for entry in entries {
            let parts: Vec<&str> = entry.split('.').collect();
            if parts.len() > 1 {
                let ns_parts = parts[..parts.len() - 1]
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<String>>();
                let ns = NamespaceIdent::from_vec(ns_parts)?;
                namespaces.insert(ns);
            }
        }

        let ns = namespaces.into_iter().collect();
        Ok(ns)
    }

    #[doc = " Create a new namespace inside the catalog."]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace, iceberg::Error> {
        let ns_key = ContentKey {
            elements: namespace.clone().inner(),
        };

        let content = crate::models::Content {
            content_type: "NAMESPACE".to_string(),
            metadata_location: "".to_string(), // not used for namespaces
            snapshot_id: None,
            schema_id: None,
            spec_id: None,
            sort_order_id: None,
        };

        let operation = crate::models::Operation::Put {
            key: ns_key,
            content,
        };
        let properties2 = properties.clone();

        // let author_time = OffsetDateTime::now_utc().format(&Rfc3339)?;
        let author_time = "2025-06-12T10:00:00Z".to_string();

        let commit_meta = crate::models::CommitMeta {
            author: "iceberg-rust-client <unknown>".into(),
            author_time: author_time,
            message: format!("Create namespace {:?}", namespace),
            signed_off_by: None,
            properties: properties, // Optionally pass `properties` here
        };

        let operations = crate::models::Operations {
            commit_meta: commit_meta,
            operations: vec![operation],
        };

        let commit_response = self
            .commit_entry(
                "main@2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d",
                operations,
            )
            .await?;

        Ok(iceberg::Namespace::with_properties(
            namespace.clone(),
            properties2,
        ))
    }

    #[doc = " Get a namespace information from the catalog."]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace, iceberg::Error> {
        let entries = self.list_entries("main").await?;
        let ns_key = namespace.clone().inner();

        for entry in entries {
            let parts: Vec<&str> = entry.split('.').collect();
            let ns_parts: Vec<String> = parts.iter().map(|s| s.to_string()).collect();

            if ns_parts == *ns_key {
                return Ok(Namespace::new(namespace.clone()));
            }
        }

        Err(iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            format!("Namespace {:?} not found", namespace),
        ))
    }

    #[doc = " Check if namespace exists in catalog."]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool, iceberg::Error> {
        let entries = self.list_entries("main").await?;

        let ns_key = namespace.clone().inner();

        Ok(entries.iter().any(|entry| {
            let parts: Vec<&str> = entry.split('.').collect();
            let ns_parts: Vec<String> = parts.iter().map(|s| s.to_string()).collect();
            ns_parts == *ns_key
        }))
    }

    #[doc = " Update a namespace inside the catalog."]
    #[doc = ""]
    #[doc = " # Behavior"]
    #[doc = ""]
    #[doc = " The properties must be the full set of namespace."]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<(), iceberg::Error> {
        Ok(())
    }

    #[doc = " Drop a namespace from the catalog."]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<(), iceberg::Error> {
        todo!()
    }

    #[doc = " List tables from namespace."]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    async fn list_tables(
        &self,
        namespace: &NamespaceIdent,
    ) -> Result<Vec<TableIdent>, iceberg::Error> {
        todo!()
    }

    #[doc = " Create a new table inside the namespace."]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table, iceberg::Error> {
        let table_name = creation.name;

        let mut key_parts = namespace.clone().inner(); // Vec<String>
        key_parts.push(table_name.clone());
        let content_key = ContentKey {
            elements: key_parts,
        };

        let metadata_location = format!("s3://warehouse/{}/metadata.json", table_name);

        let content = Content {
            content_type: "ICEBERG_TABLE".into(),
            metadata_location,
            snapshot_id: Some(0),
            schema_id: Some(creation.schema.schema_id()),
            spec_id: Some(0),
            sort_order_id: Some(0),
        };

        let operation = crate::models::Operation::Put {
            key: content_key,
            content,
        };

        let commit_meta = crate::models::CommitMeta {
            author: "iceberg-rust <client@rust>".into(),
            author_time: "2025-01-06T10:00:00Z".into(),
            message: "Create table".into(),
            signed_off_by: None,
            properties: Default::default(),
        };

        let ops = crate::models::Operations {
            commit_meta,
            operations: vec![operation],
        };

        let branch = "main";
        let reference = self.get_reference("main".to_string()).await?;
        let hash = reference.reference.hash.unwrap();

        self.commit_entry(format!("{}@{}", branch, hash).as_str(), ops)
            .await?;

        // let file_io: Arc<dyn FileIO> = Arc::new(LocalFileIO::new());

        // Ok(Table::from_creation(creation))
        let builder = Table::builder().file_io(self.file_io.clone());
        let table = builder.build()?;
        Ok(table)
    }

    #[doc = " Load table from the catalog."]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    async fn load_table(&self, table: &TableIdent) -> Result<Table, iceberg::Error> {
        todo!()
    }

    #[doc = " Drop a table from the catalog."]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    async fn drop_table(&self, table: &TableIdent) -> Result<(), iceberg::Error> {
        let ns = table.namespace().clone();
        let key = ContentKey {
            elements: ns
                .inner()
                .into_iter()
                .chain(std::iter::once(table.name().to_string()))
                .collect(),
        };

        let branch = "main";
        let reference = self.get_reference("main".to_string()).await?;
        let hash = reference.reference.hash.ok_or(iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "Cannot get hash",
        ))?;

        let commit_meta = crate::models::CommitMeta {
            author: "iceberg-rust <client@rust>".into(),
            author_time: "2025-01-06T10:00:00Z".into(),
            message: "Delete table".into(),
            signed_off_by: None,
            properties: Default::default(),
        };

        let op = crate::models::Operation::Delete { key: key };

        let ops = crate::models::Operations {
            commit_meta: commit_meta,
            operations: vec![op],
        };

        self.commit_entry(format!("{}@{}", branch, hash).as_str(), ops)
            .await?;

        Ok(())
    }

    #[doc = " Check if a table exists in the catalog."]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    async fn table_exists(&self, table: &TableIdent) -> Result<bool, iceberg::Error> {
        todo!()
    }

    #[doc = " Rename a table in the catalog."]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    async fn rename_table(
        &self,
        src: &TableIdent,
        dest: &TableIdent,
    ) -> Result<(), iceberg::Error> {
        todo!()
    }

    #[doc = " Update a table to the catalog."]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    async fn update_table(&self, commit: TableCommit) -> Result<Table, iceberg::Error> {
        todo!()
    }
}
