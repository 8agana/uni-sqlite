use thiserror::Error;

#[derive(Error, Debug)]
pub enum UniSqliteError {
    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("CSV error: {0}")]
    Csv(#[from] csv::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Invalid database path: {0}")]
    InvalidPath(String),

    #[error("Database not found: {0}")]
    DatabaseNotFound(String),

    #[error("Query failed: {0}")]
    QueryFailed(String),

    #[error("Export failed: {0}")]
    ExportFailed(String),

    #[error("Import failed: {0}")]
    ImportFailed(String),

    #[error("{0}")]
    Other(String),
}

impl From<UniSqliteError> for rmcp::ErrorData {
    fn from(err: UniSqliteError) -> Self {
        rmcp::ErrorData::internal_error(err.to_string(), None)
    }
}
