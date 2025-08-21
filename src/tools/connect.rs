use schemars;

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ConnectRequest {
    #[schemars(description = "Path to the SQLite database file")]
    pub path: String,
    
    #[schemars(description = "Create the database if it doesn't exist")]
    #[serde(default)]
    pub create_if_missing: bool,
    
    #[schemars(description = "Open in read-only mode")]
    #[serde(default)]
    pub readonly: bool,
}

#[derive(Debug, serde::Serialize)]
pub struct ConnectResult {
    pub success: bool,
    pub path: String,
    pub size_bytes: Option<u64>,
    pub page_count: Option<i64>,
    pub page_size: Option<i64>,
    pub journal_mode: Option<String>,
    pub version: Option<String>,
}