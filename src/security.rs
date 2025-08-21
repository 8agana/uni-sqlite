//! Security utilities module for comprehensive input validation and sanitization

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use thiserror::Error;

/// Error type for security utilities module
#[derive(Error, Debug)]
pub enum SecurityError {
    #[error("Invalid input")]
    InvalidInput,
    #[error("Invalid path")]
    InvalidPath(String),
    #[error("Sanitization failed")]
    SanitizationFailed(String),
}

/// Validate and sanitize input data
pub fn validate_and_sanitize_input<T: Deserialize<'static>>(
    input: &str,
) -> Result<T, SecurityError> {
    let input_map: HashMap<String, String> = serde_json::from_str(input)?;
    let input_value = serde_json::from_value(
        input_map
            .remove("value")
            .ok_or_else(|| SecurityError::InvalidInput)?,
    );
    match input_value {
        Ok(value) => Ok(value),
        Err(_) => Err(SecurityError::InvalidInput),
    }
}

/// Validate and sanitize path
pub fn validate_and_sanitize_path(path: &str) -> Result<PathBuf, SecurityError> {
    let path = Path::new(path)
        .canonicalize()
        .map_err(|_| SecurityError::InvalidPath("Cannot canonicalize path".to_string()))?;
    if !path.is_absolute() {
        return Err(SecurityError::InvalidPath(
            "Path is not absolute".to_string(),
        ));
    }
    Ok(path)
}

/// Sanitize SQL query
pub fn sanitize_sql_query(sql: &str) -> Result<String, SecurityError> {
    // Implement SQL query sanitization logic here
    // For now, just return the original SQL query
    Ok(sql.to_string())
}

/// Validate and sanitize database file path
pub fn validate_and_sanitize_db_path(path: &str) -> Result<PathBuf, SecurityError> {
    let path = validate_and_sanitize_path(path)?;
    if !path.extension().map_or(false, |ext| {
        ext == "db" || ext == "sqlite" || ext == "sqlite3"
    }) {
        return Err(SecurityError::InvalidPath(
            "Invalid database file extension".to_string(),
        ));
    }
    Ok(path)
}
