use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ReferenceType {
    Branch,
    Tag,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Reference {
    pub name: String,
    #[serde(rename = "type")]
    pub ref_type: ReferenceType,
    pub hash: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ReferencesResponse {
    pub references: Vec<Reference>,
}

#[derive(Debug, Deserialize)]
pub struct ReferenceResponse {
    pub reference: Reference,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "UPPERCASE")]
pub enum ContentEntry {
    IcebergTable {
        id: Option<String>,
        metadata_location: String,
        name: String,
        schema_id: Option<i32>,
        spec_id: Option<i32>,
        snapshot_id: Option<i64>,
        sort_order_id: Option<i32>,
    },
    DeltaLakeTable {
        id: String,
        metadata_location_history: Vec<String>,
        checkpoint_location_history: Vec<String>,
        last_checkpoint: String,
    },
    View {
        name: String,
    },
    Namespace {
        name: String,
    },
    Unknown,
}

#[derive(Debug, Deserialize)]
pub struct EntriesResponse {
    pub entries: Vec<Entry>,
}

#[derive(Debug, Deserialize)]
pub struct EntryResponse {
    pub entry: Entry,
}

#[derive(Debug, Deserialize)]
pub struct Entry {
    pub name: ContentKey,
    #[serde(rename = "type")]
    pub content_type: String, // e.g., "ICEBERG_TABLE"
}

#[derive(Debug, Deserialize)]
pub struct CommitResponse {}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommitMeta {
    pub author: String,
    pub author_time: String, // Consider `chrono::DateTime<Utc>` for strict typing
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signed_off_by: Option<String>,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Operations {
    pub commit_meta: CommitMeta,
    pub operations: Vec<Operation>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "UPPERCASE")]
pub enum Operation {
    Put { key: ContentKey, content: Content },
    Delete { key: ContentKey },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ContentKey {
    pub elements: Vec<String>, // ["db", "table"]
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Content {
    #[serde(rename = "type")]
    pub content_type: String, // e.g. "ICEBERG_TABLE"
    pub metadata_location: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_order_id: Option<i32>,
}

impl std::fmt::Display for ContentKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.elements.join("."))
    }
}
