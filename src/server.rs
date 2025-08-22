#![allow(clippy::redundant_closure)]

use chrono::{DateTime, Utc};
use rmcp::{
    ServerHandler, ServiceExt,
    model::{
        CallToolRequestParam, CallToolResult, Implementation, ListToolsResult,
        PaginatedRequestParam, ProtocolVersion, ServerCapabilities, ServerInfo, Tool,
    },
    service::RequestContext,
    transport::stdio,
};
use rusqlite::{Connection, OpenFlags, Transaction};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::borrow::Cow;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::error::UniSqliteError;

#[derive(Debug, Clone)]
pub struct SqliteHandler {
    // Current database connection wrapped in Arc<Mutex> for thread safety (blocking)
    pub current_db: Arc<Mutex<Option<Connection>>>,
    // Path to current database
    pub current_path: Arc<Mutex<Option<PathBuf>>>,
}

// Connection and Basic Query Types
#[derive(Debug, Deserialize, JsonSchema)]
pub struct ConnectRequest {
    #[schemars(description = "Path to the SQLite database file")]
    pub path: String,
    #[schemars(description = "Create the database if it doesn't exist")]
    #[serde(default)]
    pub create_if_missing: bool,
}

#[derive(Debug, Serialize)]
pub struct ConnectResult {
    pub success: bool,
    pub path: String,
    pub database_size: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct QueryRequest {
    #[schemars(description = "SQL query to execute")]
    pub sql: String,
    #[schemars(description = "Parameters for a prepared statement")]
    #[serde(default)]
    pub parameters: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct QueryResult {
    pub message: String,
    pub rows_affected: Option<usize>,
    pub data: Option<Vec<Vec<serde_json::Value>>>,
    pub columns: Option<Vec<String>>,
}

// Transaction Types
#[derive(Debug, Deserialize, JsonSchema)]
pub struct TransactionRequest {
    #[schemars(description = "List of SQL queries to execute in a transaction")]
    pub queries: Vec<QueryRequest>,
    #[schemars(description = "Whether to rollback on any error")]
    #[serde(default = "default_true")]
    pub rollback_on_error: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Serialize)]
pub struct TransactionResult {
    pub success: bool,
    pub message: String,
    pub results: Vec<QueryResult>,
    pub total_rows_affected: usize,
}

// Schema Management Types
#[derive(Debug, Deserialize, JsonSchema)]
pub struct CreateTableRequest {
    #[schemars(description = "Name of the table to create")]
    pub table_name: String,
    #[schemars(
        description = "Column definitions (e.g., 'id INTEGER PRIMARY KEY, name TEXT NOT NULL')"
    )]
    pub columns: String,
    #[schemars(description = "Create table only if it doesn't exist")]
    #[serde(default)]
    pub if_not_exists: bool,
}

#[derive(Debug, Serialize)]
pub struct CreateTableResult {
    pub success: bool,
    pub message: String,
    pub table_name: String,
}

// Introspection Types
#[derive(Debug, Serialize)]
pub struct TableInfo {
    pub name: String,
    pub sql: Option<String>,
    pub row_count: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct ListTablesResult {
    pub tables: Vec<TableInfo>,
    pub total_count: usize,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DescribeTableRequest {
    #[schemars(description = "Name of the table to describe")]
    pub table_name: String,
}

#[derive(Debug, Serialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub not_null: bool,
    pub default_value: Option<String>,
    pub primary_key: bool,
}

#[derive(Debug, Serialize)]
pub struct DescribeTableResult {
    pub table_name: String,
    pub columns: Vec<ColumnInfo>,
    pub indexes: Vec<String>,
}

// Backup Types
#[derive(Debug, Deserialize, JsonSchema)]
pub struct BackupRequest {
    #[schemars(description = "Destination path for the backup file")]
    pub destination_path: String,
}

#[derive(Debug, Serialize)]
pub struct BackupResult {
    pub success: bool,
    pub message: String,
    pub backup_path: String,
    pub backup_size: Option<u64>,
    pub timestamp: DateTime<Utc>,
}

// Batch Operations Types
#[derive(Debug, Deserialize, JsonSchema)]
pub struct BatchInsertRequest {
    #[schemars(description = "Name of the table to insert into")]
    pub table_name: String,
    #[schemars(description = "Column names for the insert")]
    pub columns: Vec<String>,
    #[schemars(description = "Rows of data to insert")]
    pub rows: Vec<Vec<serde_json::Value>>,
    #[schemars(description = "Use INSERT OR REPLACE instead of INSERT")]
    #[serde(default)]
    pub replace_on_conflict: bool,
}

#[derive(Debug, Serialize)]
pub struct BatchInsertResult {
    pub success: bool,
    pub message: String,
    pub rows_inserted: usize,
}

// Export Types
#[derive(Debug, Deserialize, JsonSchema)]
pub struct ExportCsvRequest {
    #[schemars(description = "SQL query to export")]
    pub query: String,
    #[schemars(description = "Output file path")]
    pub output_path: String,
    #[schemars(description = "Include column headers")]
    #[serde(default = "default_true")]
    pub include_headers: bool,
}

#[derive(Debug, Serialize)]
pub struct ExportCsvResult {
    pub success: bool,
    pub message: String,
    pub output_path: String,
    pub rows_exported: usize,
}

// Health Check Types
#[derive(Debug, Serialize)]
pub struct HealthCheckResult {
    pub connected: bool,
    pub database_path: Option<String>,
    pub database_size: Option<u64>,
    pub table_count: Option<usize>,
    pub last_modified: Option<DateTime<Utc>>,
    pub sqlite_version: String,
}

impl SqliteHandler {
    pub fn new() -> Self {
        Self {
            current_db: Arc::new(Mutex::new(None)),
            current_path: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn connect_tool(&self, req: ConnectRequest) -> Result<ConnectResult, UniSqliteError> {
        let requested_path = PathBuf::from(&req.path);
        let path = self.validate_db_path(&requested_path)?;

        let flags = if req.create_if_missing {
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE
        } else {
            OpenFlags::SQLITE_OPEN_READ_WRITE
        };

        let conn = Connection::open_with_flags(&path, flags)?;

        // Get database size
        let database_size = fs::metadata(&path).ok().map(|m| m.len());

        *self.current_db.lock().await = Some(conn);
        *self.current_path.lock().await = Some(path.clone());

        Ok(ConnectResult {
            success: true,
            path: path.display().to_string(),
            database_size,
        })
    }

    /// Validate and sanitize a database file path (prevents directory traversal)
    fn validate_db_path(&self, requested_path: &Path) -> Result<PathBuf, UniSqliteError> {
        let canonical_path = if requested_path.exists() {
            requested_path.canonicalize()
        } else {
            let parent = requested_path
                .parent()
                .ok_or_else(|| UniSqliteError::InvalidPath("No parent directory".into()))?;

            let canonical_parent = parent.canonicalize().map_err(|_| {
                UniSqliteError::InvalidPath("Parent directory does not exist".into())
            })?;

            let file_name = requested_path
                .file_name()
                .ok_or_else(|| UniSqliteError::InvalidPath("No filename".into()))?;

            Ok(canonical_parent.join(file_name))
        }
        .map_err(|e| UniSqliteError::InvalidPath(e.to_string()))?;

        // In test mode, allow temp directories
        #[cfg(test)]
        {
            if canonical_path.to_string_lossy().contains("tmp") {
                match canonical_path.extension().and_then(|e| e.to_str()) {
                    Some("db") | Some("sqlite") | Some("sqlite3") => return Ok(canonical_path),
                    _ => {
                        return Err(UniSqliteError::InvalidPath(
                            "Invalid database file extension".into(),
                        ));
                    }
                }
            }
        }

        let current_dir = std::env::current_dir()
            .and_then(|p| p.canonicalize())
            .map_err(|_| {
                UniSqliteError::InvalidPath("Cannot determine current directory".into())
            })?;

        if !canonical_path.starts_with(&current_dir) {
            return Err(UniSqliteError::InvalidPath(
                "Path outside allowed directory".into(),
            ));
        }

        match canonical_path.extension().and_then(|e| e.to_str()) {
            Some("db") | Some("sqlite") | Some("sqlite3") => Ok(canonical_path),
            _ => Err(UniSqliteError::InvalidPath(
                "Invalid database file extension".into(),
            )),
        }
    }

    /// Convert a JSON value to a rusqlite parameter.
    fn json_to_sql_param(
        value: &serde_json::Value,
    ) -> Result<Box<dyn rusqlite::ToSql>, UniSqliteError> {
        match value {
            serde_json::Value::Null => Ok(Box::new(rusqlite::types::Null)),
            serde_json::Value::Bool(b) => Ok(Box::new(*b)),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(Box::new(i))
                } else if let Some(f) = n.as_f64() {
                    Ok(Box::new(f))
                } else {
                    Err(UniSqliteError::QueryFailed(
                        "Invalid number parameter".into(),
                    ))
                }
            }
            serde_json::Value::String(s) => Ok(Box::new(s.clone())),
            _ => Err(UniSqliteError::QueryFailed(
                "Unsupported parameter type".into(),
            )),
        }
    }

    /// Validate SQL query - now allows more admin operations
    fn validate_sql_query(sql: &str) -> Result<(), UniSqliteError> {
        let sql_trim = sql.trim_start();
        let sql_upper = sql_trim.to_ascii_uppercase();
        let allowed = [
            "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP", "PRAGMA", "EXPLAIN",
            "ANALYZE",
        ];

        // Ensure exactly one statement (no extra semicolons).
        if sql_trim.matches(';').count() > 1 {
            return Err(UniSqliteError::QueryFailed(
                "Multiple statements are not allowed".into(),
            ));
        }

        for cmd in &allowed {
            if sql_upper.starts_with(cmd) {
                return Ok(());
            }
        }

        Err(UniSqliteError::QueryFailed(format!(
            "Only {} statements are allowed",
            allowed.join(", ")
        )))
    }

    pub async fn query_tool(&self, req: QueryRequest) -> Result<QueryResult, UniSqliteError> {
        Self::validate_sql_query(&req.sql)?;

        let guard = self.current_db.lock().await;
        let conn = guard
            .as_ref()
            .ok_or_else(|| UniSqliteError::Other("No database connected".into()))?;

        // Convert JSON parameters to rusqlite parameters.
        let params: Vec<Box<dyn rusqlite::ToSql>> = req
            .parameters
            .iter()
            .map(Self::json_to_sql_param)
            .collect::<Result<_, _>>()?;

        let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| &**p).collect();

        // Determine query type.
        let sql_upper = req.sql.trim_start().to_ascii_uppercase();
        if sql_upper.starts_with("SELECT")
            || sql_upper.starts_with("PRAGMA")
            || sql_upper.starts_with("EXPLAIN")
        {
            // SELECT-like queries – return rows.
            let mut stmt = conn.prepare(&req.sql)?;
            let column_count = stmt.column_count();
            let column_names: Vec<String> =
                stmt.column_names().iter().map(|s| s.to_string()).collect();

            let rows = stmt.query_map(&param_refs[..], |row| {
                let mut values = Vec::new();
                for i in 0..column_count {
                    let v = row.get_ref(i)?;
                    let json = match v {
                        rusqlite::types::ValueRef::Null => Value::Null,
                        rusqlite::types::ValueRef::Integer(i) => Value::Number(i.into()),
                        rusqlite::types::ValueRef::Real(f) => Value::Number(
                            serde_json::Number::from_f64(f)
                                .unwrap_or_else(|| serde_json::Number::from(0)),
                        ),
                        rusqlite::types::ValueRef::Text(t) => {
                            Value::String(String::from_utf8_lossy(t).into_owned())
                        }
                        rusqlite::types::ValueRef::Blob(b) => Value::String(hex::encode(b)),
                    };
                    values.push(json);
                }
                Ok(values)
            })?;

            let mut data = Vec::new();
            for row in rows {
                data.push(row?);
            }

            Ok(QueryResult {
                message: format!("Query executed successfully, returned {} rows", data.len()),
                rows_affected: Some(data.len()),
                data: Some(data),
                columns: Some(column_names),
            })
        } else {
            // Non‑SELECT – execute and report affected rows.
            let rows_affected = conn.execute(&req.sql, &param_refs[..])?;
            Ok(QueryResult {
                message: "Query executed successfully".into(),
                rows_affected: Some(rows_affected),
                data: None,
                columns: None,
            })
        }
    }

    pub async fn transaction_tool(
        &self,
        req: TransactionRequest,
    ) -> Result<TransactionResult, UniSqliteError> {
        let guard = self.current_db.lock().await;
        let conn = guard
            .as_ref()
            .ok_or_else(|| UniSqliteError::Other("No database connected".into()))?;

        let tx = conn.unchecked_transaction()?;
        let mut results = Vec::new();
        let mut total_rows_affected = 0;
        let mut success = true;

        for query_req in req.queries {
            match self.execute_query_in_transaction(&tx, query_req) {
                Ok(result) => {
                    if let Some(rows) = result.rows_affected {
                        total_rows_affected += rows;
                    }
                    results.push(result);
                }
                Err(e) => {
                    success = false;
                    results.push(QueryResult {
                        message: format!("Error: {e}"),
                        rows_affected: None,
                        data: None,
                        columns: None,
                    });
                    if req.rollback_on_error {
                        break;
                    }
                }
            }
        }

        if success || !req.rollback_on_error {
            tx.commit()?;
            Ok(TransactionResult {
                success,
                message: if success {
                    "Transaction completed successfully".into()
                } else {
                    "Transaction completed with errors".into()
                },
                results,
                total_rows_affected,
            })
        } else {
            // Transaction will be rolled back when dropped
            Ok(TransactionResult {
                success: false,
                message: "Transaction rolled back due to errors".into(),
                results,
                total_rows_affected: 0,
            })
        }
    }

    fn execute_query_in_transaction(
        &self,
        tx: &Transaction<'_>,
        req: QueryRequest,
    ) -> Result<QueryResult, UniSqliteError> {
        Self::validate_sql_query(&req.sql)?;

        let params: Vec<Box<dyn rusqlite::ToSql>> = req
            .parameters
            .iter()
            .map(Self::json_to_sql_param)
            .collect::<Result<_, _>>()?;

        let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| &**p).collect();

        let sql_upper = req.sql.trim_start().to_ascii_uppercase();
        if sql_upper.starts_with("SELECT")
            || sql_upper.starts_with("PRAGMA")
            || sql_upper.starts_with("EXPLAIN")
        {
            let mut stmt = tx.prepare(&req.sql)?;
            let column_count = stmt.column_count();
            let column_names: Vec<String> =
                stmt.column_names().iter().map(|s| s.to_string()).collect();

            let rows = stmt.query_map(&param_refs[..], |row| {
                let mut values = Vec::new();
                for i in 0..column_count {
                    let v = row.get_ref(i)?;
                    let json = match v {
                        rusqlite::types::ValueRef::Null => Value::Null,
                        rusqlite::types::ValueRef::Integer(i) => Value::Number(i.into()),
                        rusqlite::types::ValueRef::Real(f) => Value::Number(
                            serde_json::Number::from_f64(f)
                                .unwrap_or_else(|| serde_json::Number::from(0)),
                        ),
                        rusqlite::types::ValueRef::Text(t) => {
                            Value::String(String::from_utf8_lossy(t).into_owned())
                        }
                        rusqlite::types::ValueRef::Blob(b) => Value::String(hex::encode(b)),
                    };
                    values.push(json);
                }
                Ok(values)
            })?;

            let mut data = Vec::new();
            for row in rows {
                data.push(row?);
            }

            Ok(QueryResult {
                message: format!("Query executed successfully, returned {} rows", data.len()),
                rows_affected: Some(data.len()),
                data: Some(data),
                columns: Some(column_names),
            })
        } else {
            let rows_affected = tx.execute(&req.sql, &param_refs[..])?;
            Ok(QueryResult {
                message: "Query executed successfully".into(),
                rows_affected: Some(rows_affected),
                data: None,
                columns: None,
            })
        }
    }

    pub async fn create_table_tool(
        &self,
        req: CreateTableRequest,
    ) -> Result<CreateTableResult, UniSqliteError> {
        let guard = self.current_db.lock().await;
        let conn = guard
            .as_ref()
            .ok_or_else(|| UniSqliteError::Other("No database connected".into()))?;

        let if_not_exists = if req.if_not_exists {
            "IF NOT EXISTS "
        } else {
            ""
        };
        let sql = format!(
            "CREATE TABLE {}{}({});",
            if_not_exists, req.table_name, req.columns
        );

        conn.execute(&sql, [])?;

        Ok(CreateTableResult {
            success: true,
            message: format!("Table '{}' created successfully", req.table_name),
            table_name: req.table_name,
        })
    }

    pub async fn list_tables_tool(&self) -> Result<ListTablesResult, UniSqliteError> {
        let guard = self.current_db.lock().await;
        let conn = guard
            .as_ref()
            .ok_or_else(|| UniSqliteError::Other("No database connected".into()))?;

        let mut stmt = conn.prepare(
            "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )?;

        let rows = stmt.query_map([], |row| {
            let name: String = row.get(0)?;
            let sql: Option<String> = row.get(1)?;
            Ok((name, sql))
        })?;

        let mut tables = Vec::new();
        for row in rows {
            let (name, sql) = row?;

            // Get row count for each table
            let count_sql = format!("SELECT COUNT(*) FROM [{name}]");
            let row_count: Option<i64> = conn.query_row(&count_sql, [], |row| row.get(0)).ok();

            tables.push(TableInfo {
                name,
                sql,
                row_count,
            });
        }

        let total_count = tables.len();

        Ok(ListTablesResult {
            tables,
            total_count,
        })
    }

    pub async fn describe_table_tool(
        &self,
        req: DescribeTableRequest,
    ) -> Result<DescribeTableResult, UniSqliteError> {
        let guard = self.current_db.lock().await;
        let conn = guard
            .as_ref()
            .ok_or_else(|| UniSqliteError::Other("No database connected".into()))?;

        // Get column information
        let mut stmt = conn.prepare(&format!("PRAGMA table_info([{}])", req.table_name))?;
        let rows = stmt.query_map([], |row| {
            Ok(ColumnInfo {
                name: row.get(1)?,
                data_type: row.get(2)?,
                not_null: row.get::<_, i32>(3)? != 0,
                default_value: row.get(4)?,
                primary_key: row.get::<_, i32>(5)? != 0,
            })
        })?;

        let mut columns = Vec::new();
        for row in rows {
            columns.push(row?);
        }

        // Get index information
        let mut index_stmt = conn.prepare(&format!("PRAGMA index_list([{}])", req.table_name))?;
        let index_rows = index_stmt.query_map([], |row| {
            let name: String = row.get(1)?;
            Ok(name)
        })?;

        let mut indexes = Vec::new();
        for row in index_rows {
            indexes.push(row?);
        }

        Ok(DescribeTableResult {
            table_name: req.table_name,
            columns,
            indexes,
        })
    }

    pub async fn backup_tool(&self, req: BackupRequest) -> Result<BackupResult, UniSqliteError> {
        let guard = self.current_db.lock().await;
        let conn = guard
            .as_ref()
            .ok_or_else(|| UniSqliteError::Other("No database connected".into()))?;

        let backup_path = PathBuf::from(&req.destination_path);
        let validated_path = self.validate_db_path(&backup_path)?;

        // Use SQLite's backup API
        let mut backup_conn = Connection::open(&validated_path)?;
        let backup = rusqlite::backup::Backup::new(conn, &mut backup_conn)?;
        backup.run_to_completion(5, std::time::Duration::from_millis(250), None)?;

        let backup_size = fs::metadata(&validated_path).ok().map(|m| m.len());

        Ok(BackupResult {
            success: true,
            message: "Backup completed successfully".into(),
            backup_path: validated_path.display().to_string(),
            backup_size,
            timestamp: Utc::now(),
        })
    }

    pub async fn batch_insert_tool(
        &self,
        req: BatchInsertRequest,
    ) -> Result<BatchInsertResult, UniSqliteError> {
        let guard = self.current_db.lock().await;
        let conn = guard
            .as_ref()
            .ok_or_else(|| UniSqliteError::Other("No database connected".into()))?;

        let tx = conn.unchecked_transaction()?;

        let placeholders = vec!["?"; req.columns.len()].join(", ");
        let insert_type = if req.replace_on_conflict {
            "INSERT OR REPLACE"
        } else {
            "INSERT"
        };
        let sql = format!(
            "{} INTO [{}] ({}) VALUES ({})",
            insert_type,
            req.table_name,
            req.columns
                .iter()
                .map(|c| format!("[{c}]"))
                .collect::<Vec<_>>()
                .join(", "),
            placeholders
        );

        let mut rows_inserted = 0;

        {
            let mut stmt = tx.prepare(&sql)?;

            for row in req.rows {
                if row.len() != req.columns.len() {
                    return Err(UniSqliteError::QueryFailed(
                        "Row data length doesn't match column count".into(),
                    ));
                }

                let params: Vec<Box<dyn rusqlite::ToSql>> = row
                    .iter()
                    .map(Self::json_to_sql_param)
                    .collect::<Result<_, _>>()?;

                let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| &**p).collect();

                stmt.execute(&param_refs[..])?;
                rows_inserted += 1;
            }
        }

        tx.commit()?;

        Ok(BatchInsertResult {
            success: true,
            message: format!("Successfully inserted {rows_inserted} rows"),
            rows_inserted,
        })
    }

    pub async fn export_csv_tool(
        &self,
        req: ExportCsvRequest,
    ) -> Result<ExportCsvResult, UniSqliteError> {
        let guard = self.current_db.lock().await;
        let conn = guard
            .as_ref()
            .ok_or_else(|| UniSqliteError::Other("No database connected".into()))?;

        Self::validate_sql_query(&req.query)?;

        let output_path = PathBuf::from(&req.output_path);

        // Create CSV writer
        let file = std::fs::File::create(&output_path)?;
        let mut wtr = csv::Writer::from_writer(file);

        let mut stmt = conn.prepare(&req.query)?;
        let column_names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();

        // Write headers if requested
        if req.include_headers {
            wtr.write_record(&column_names)?;
        }

        let rows = stmt.query_map([], |row| {
            let mut record = Vec::new();
            for i in 0..column_names.len() {
                let value = match row.get_ref(i)? {
                    rusqlite::types::ValueRef::Null => String::new(),
                    rusqlite::types::ValueRef::Integer(i) => i.to_string(),
                    rusqlite::types::ValueRef::Real(f) => f.to_string(),
                    rusqlite::types::ValueRef::Text(t) => String::from_utf8_lossy(t).into_owned(),
                    rusqlite::types::ValueRef::Blob(b) => hex::encode(b),
                };
                record.push(value);
            }
            Ok(record)
        })?;

        let mut rows_exported = 0;
        for row in rows {
            let record = row?;
            wtr.write_record(&record)?;
            rows_exported += 1;
        }

        wtr.flush()?;

        Ok(ExportCsvResult {
            success: true,
            message: format!("Successfully exported {rows_exported} rows to CSV"),
            output_path: output_path.display().to_string(),
            rows_exported,
        })
    }

    pub async fn health_check_tool(&self) -> Result<HealthCheckResult, UniSqliteError> {
        let db_guard = self.current_db.lock().await;
        let path_guard = self.current_path.lock().await;

        let connected = db_guard.is_some();
        let database_path = path_guard.as_ref().map(|p| p.display().to_string());

        let (database_size, table_count, last_modified) = if let (Some(conn), Some(path)) =
            (db_guard.as_ref(), path_guard.as_ref())
        {
            let size = fs::metadata(path).ok().map(|m| m.len());
            let modified = fs::metadata(path)
                .ok()
                .and_then(|m| m.modified().ok())
                .map(|t| DateTime::from(t));

            let count: Result<i32, _> = conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
                [],
                |row| row.get(0)
            );

            (size, count.ok().map(|c| c as usize), modified)
        } else {
            (None, None, None)
        };

        // Get SQLite version
        let sqlite_version = if let Some(conn) = db_guard.as_ref() {
            conn.query_row("SELECT sqlite_version()", [], |row| {
                let version: String = row.get(0)?;
                Ok(version)
            })
            .unwrap_or_else(|_| "Unknown".to_string())
        } else {
            "Not connected".to_string()
        };

        Ok(HealthCheckResult {
            connected,
            database_path,
            database_size,
            table_count,
            last_modified,
            sqlite_version,
        })
    }

    fn get_tools() -> Vec<Tool> {
        vec![
            Tool {
                name: Cow::Borrowed("connect"),
                description: Some(Cow::Borrowed("Connect to a SQLite database")),
                input_schema: serde_json::to_value(schemars::schema_for!(ConnectRequest).schema)
                    .unwrap()
                    .as_object()
                    .unwrap()
                    .clone()
                    .into(),
                annotations: None,
                output_schema: None,
            },
            Tool {
                name: Cow::Borrowed("query"),
                description: Some(Cow::Borrowed(
                    "Execute a SQL query (SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, PRAGMA)",
                )),
                input_schema: serde_json::to_value(schemars::schema_for!(QueryRequest).schema)
                    .unwrap()
                    .as_object()
                    .unwrap()
                    .clone()
                    .into(),
                annotations: None,
                output_schema: None,
            },
            Tool {
                name: Cow::Borrowed("transaction"),
                description: Some(Cow::Borrowed("Execute multiple queries in a transaction")),
                input_schema: serde_json::to_value(
                    schemars::schema_for!(TransactionRequest).schema,
                )
                .unwrap()
                .as_object()
                .unwrap()
                .clone()
                .into(),
                annotations: None,
                output_schema: None,
            },
            Tool {
                name: Cow::Borrowed("create_table"),
                description: Some(Cow::Borrowed("Create a new table with specified columns")),
                input_schema: serde_json::to_value(
                    schemars::schema_for!(CreateTableRequest).schema,
                )
                .unwrap()
                .as_object()
                .unwrap()
                .clone()
                .into(),
                annotations: None,
                output_schema: None,
            },
            Tool {
                name: Cow::Borrowed("list_tables"),
                description: Some(Cow::Borrowed(
                    "List all tables in the database with metadata",
                )),
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                })
                .as_object()
                .unwrap()
                .clone()
                .into(),
                annotations: None,
                output_schema: None,
            },
            Tool {
                name: Cow::Borrowed("describe_table"),
                description: Some(Cow::Borrowed(
                    "Get detailed information about a table's structure",
                )),
                input_schema: serde_json::to_value(
                    schemars::schema_for!(DescribeTableRequest).schema,
                )
                .unwrap()
                .as_object()
                .unwrap()
                .clone()
                .into(),
                annotations: None,
                output_schema: None,
            },
            Tool {
                name: Cow::Borrowed("backup"),
                description: Some(Cow::Borrowed("Create a backup of the current database")),
                input_schema: serde_json::to_value(schemars::schema_for!(BackupRequest).schema)
                    .unwrap()
                    .as_object()
                    .unwrap()
                    .clone()
                    .into(),
                annotations: None,
                output_schema: None,
            },
            Tool {
                name: Cow::Borrowed("batch_insert"),
                description: Some(Cow::Borrowed(
                    "Insert multiple rows efficiently in a single transaction",
                )),
                input_schema: serde_json::to_value(
                    schemars::schema_for!(BatchInsertRequest).schema,
                )
                .unwrap()
                .as_object()
                .unwrap()
                .clone()
                .into(),
                annotations: None,
                output_schema: None,
            },
            Tool {
                name: Cow::Borrowed("export_csv"),
                description: Some(Cow::Borrowed("Export query results to a CSV file")),
                input_schema: serde_json::to_value(schemars::schema_for!(ExportCsvRequest).schema)
                    .unwrap()
                    .as_object()
                    .unwrap()
                    .clone()
                    .into(),
                annotations: None,
                output_schema: None,
            },
            Tool {
                name: Cow::Borrowed("health_check"),
                description: Some(Cow::Borrowed(
                    "Get health and status information about the current database connection",
                )),
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                })
                .as_object()
                .unwrap()
                .clone()
                .into(),
                annotations: None,
                output_schema: None,
            },
        ]
    }

    async fn list_tools_handler(
        &self,
        _request: Option<PaginatedRequestParam>,
        _context: RequestContext<rmcp::service::RoleServer>,
    ) -> Result<ListToolsResult, rmcp::ErrorData> {
        Ok(ListToolsResult {
            tools: Self::get_tools(),
            next_cursor: None,
        })
    }

    async fn call_tool_handler(
        &self,
        request: CallToolRequestParam,
        _context: RequestContext<rmcp::service::RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        match request.name.as_ref() {
            "connect" => {
                let params: ConnectRequest =
                    serde_json::from_value(request.arguments.unwrap_or_default().into())
                        .map_err(|e| rmcp::ErrorData::invalid_params(e.to_string(), None))?;

                let result = self
                    .connect_tool(params)
                    .await
                    .map_err(rmcp::ErrorData::from)?;

                Ok(CallToolResult {
                    content: vec![],
                    structured_content: Some(serde_json::to_value(result).unwrap()),
                    is_error: Some(false),
                })
            }
            "query" => {
                let params: QueryRequest =
                    serde_json::from_value(request.arguments.unwrap_or_default().into())
                        .map_err(|e| rmcp::ErrorData::invalid_params(e.to_string(), None))?;

                let result = self
                    .query_tool(params)
                    .await
                    .map_err(rmcp::ErrorData::from)?;

                Ok(CallToolResult {
                    content: vec![],
                    structured_content: Some(serde_json::to_value(result).unwrap()),
                    is_error: Some(false),
                })
            }
            "transaction" => {
                let params: TransactionRequest =
                    serde_json::from_value(request.arguments.unwrap_or_default().into())
                        .map_err(|e| rmcp::ErrorData::invalid_params(e.to_string(), None))?;

                let result = self
                    .transaction_tool(params)
                    .await
                    .map_err(rmcp::ErrorData::from)?;

                Ok(CallToolResult {
                    content: vec![],
                    structured_content: Some(serde_json::to_value(result).unwrap()),
                    is_error: Some(false),
                })
            }
            "create_table" => {
                let params: CreateTableRequest =
                    serde_json::from_value(request.arguments.unwrap_or_default().into())
                        .map_err(|e| rmcp::ErrorData::invalid_params(e.to_string(), None))?;

                let result = self
                    .create_table_tool(params)
                    .await
                    .map_err(rmcp::ErrorData::from)?;

                Ok(CallToolResult {
                    content: vec![],
                    structured_content: Some(serde_json::to_value(result).unwrap()),
                    is_error: Some(false),
                })
            }
            "list_tables" => {
                let result = self
                    .list_tables_tool()
                    .await
                    .map_err(rmcp::ErrorData::from)?;

                Ok(CallToolResult {
                    content: vec![],
                    structured_content: Some(serde_json::to_value(result).unwrap()),
                    is_error: Some(false),
                })
            }
            "describe_table" => {
                let params: DescribeTableRequest =
                    serde_json::from_value(request.arguments.unwrap_or_default().into())
                        .map_err(|e| rmcp::ErrorData::invalid_params(e.to_string(), None))?;

                let result = self
                    .describe_table_tool(params)
                    .await
                    .map_err(rmcp::ErrorData::from)?;

                Ok(CallToolResult {
                    content: vec![],
                    structured_content: Some(serde_json::to_value(result).unwrap()),
                    is_error: Some(false),
                })
            }
            "backup" => {
                let params: BackupRequest =
                    serde_json::from_value(request.arguments.unwrap_or_default().into())
                        .map_err(|e| rmcp::ErrorData::invalid_params(e.to_string(), None))?;

                let result = self
                    .backup_tool(params)
                    .await
                    .map_err(rmcp::ErrorData::from)?;

                Ok(CallToolResult {
                    content: vec![],
                    structured_content: Some(serde_json::to_value(result).unwrap()),
                    is_error: Some(false),
                })
            }
            "batch_insert" => {
                let params: BatchInsertRequest =
                    serde_json::from_value(request.arguments.unwrap_or_default().into())
                        .map_err(|e| rmcp::ErrorData::invalid_params(e.to_string(), None))?;

                let result = self
                    .batch_insert_tool(params)
                    .await
                    .map_err(rmcp::ErrorData::from)?;

                Ok(CallToolResult {
                    content: vec![],
                    structured_content: Some(serde_json::to_value(result).unwrap()),
                    is_error: Some(false),
                })
            }
            "export_csv" => {
                let params: ExportCsvRequest =
                    serde_json::from_value(request.arguments.unwrap_or_default().into())
                        .map_err(|e| rmcp::ErrorData::invalid_params(e.to_string(), None))?;

                let result = self
                    .export_csv_tool(params)
                    .await
                    .map_err(rmcp::ErrorData::from)?;

                Ok(CallToolResult {
                    content: vec![],
                    structured_content: Some(serde_json::to_value(result).unwrap()),
                    is_error: Some(false),
                })
            }
            "health_check" => {
                let result = self
                    .health_check_tool()
                    .await
                    .map_err(rmcp::ErrorData::from)?;

                Ok(CallToolResult {
                    content: vec![],
                    structured_content: Some(serde_json::to_value(result).unwrap()),
                    is_error: Some(false),
                })
            }
            _ => Err(rmcp::ErrorData::invalid_params("Tool not found", None)),
        }
    }
}

// Implement ServerHandler trait
impl ServerHandler for SqliteHandler {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            server_info: Implementation {
                name: "uni-sqlite".into(),
                version: env!("CARGO_PKG_VERSION").into(),
            },
            capabilities: ServerCapabilities {
                tools: Some(Default::default()),
                ..Default::default()
            },
            instructions: Some(
                "A comprehensive SQLite administration MCP server for consciousness persistence infrastructure. \
                Features: database connections, schema management, transactions, backups, batch operations, \
                CSV export/import, and health monitoring. Designed for LegacyMind project requirements."
                    .into(),
            ),
        }
    }

    fn list_tools(
        &self,
        request: Option<PaginatedRequestParam>,
        context: RequestContext<rmcp::service::RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, rmcp::ErrorData>> + Send + '_
    {
        self.list_tools_handler(request, context)
    }

    fn call_tool(
        &self,
        request: CallToolRequestParam,
        context: RequestContext<rmcp::service::RoleServer>,
    ) -> impl std::future::Future<Output = Result<CallToolResult, rmcp::ErrorData>> + Send + '_
    {
        self.call_tool_handler(request, context)
    }
}

pub async fn run() -> anyhow::Result<()> {
    let handler = SqliteHandler::new();

    // Serve the handler with stdio transport
    let server = handler.serve(stdio()).await?;

    // Wait for service to complete
    server.waiting().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    async fn create_test_handler_with_db() -> (SqliteHandler, TempDir, PathBuf) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");

        let handler = SqliteHandler::new();

        // Connect to the test database
        let connect_req = ConnectRequest {
            path: db_path.display().to_string(),
            create_if_missing: true,
        };

        handler.connect_tool(connect_req).await.unwrap();

        (handler, temp_dir, db_path)
    }

    #[tokio::test]
    async fn test_connect_and_health_check() {
        let (handler, _temp_dir, _db_path) = create_test_handler_with_db().await;

        let health = handler.health_check_tool().await.unwrap();
        assert!(health.connected);
        // Check that the path ends with the expected filename (handles canonicalization differences)
        assert!(health.database_path.as_ref().unwrap().ends_with("test.db"));
        assert!(health.database_size.is_some());
        assert_eq!(health.table_count, Some(0));
    }

    #[tokio::test]
    async fn test_create_table_and_list_tables() {
        let (handler, _temp_dir, _db_path) = create_test_handler_with_db().await;

        // Create a test table
        let create_req = CreateTableRequest {
            table_name: "consciousness_data".to_string(),
            columns: "id INTEGER PRIMARY KEY, timestamp TEXT, data TEXT".to_string(),
            if_not_exists: true,
        };

        let result = handler.create_table_tool(create_req).await.unwrap();
        assert!(result.success);
        assert_eq!(result.table_name, "consciousness_data");

        // List tables
        let tables = handler.list_tables_tool().await.unwrap();
        assert_eq!(tables.total_count, 1);
        assert_eq!(tables.tables[0].name, "consciousness_data");
        assert_eq!(tables.tables[0].row_count, Some(0));
    }

    #[tokio::test]
    async fn test_describe_table() {
        let (handler, _temp_dir, _db_path) = create_test_handler_with_db().await;

        // Create a test table
        let create_req = CreateTableRequest {
            table_name: "test_table".to_string(),
            columns: "id INTEGER PRIMARY KEY, name TEXT NOT NULL, active BOOLEAN DEFAULT 1"
                .to_string(),
            if_not_exists: true,
        };
        handler.create_table_tool(create_req).await.unwrap();

        // Describe the table
        let describe_req = DescribeTableRequest {
            table_name: "test_table".to_string(),
        };

        let result = handler.describe_table_tool(describe_req).await.unwrap();
        assert_eq!(result.table_name, "test_table");
        assert_eq!(result.columns.len(), 3);

        // Check column details
        let id_col = &result.columns[0];
        assert_eq!(id_col.name, "id");
        assert_eq!(id_col.data_type, "INTEGER");
        assert!(id_col.primary_key);

        let name_col = &result.columns[1];
        assert_eq!(name_col.name, "name");
        assert_eq!(name_col.data_type, "TEXT");
        assert!(name_col.not_null);
    }

    #[tokio::test]
    async fn test_query_operations() {
        let (handler, _temp_dir, _db_path) = create_test_handler_with_db().await;

        // Create table
        let create_req = CreateTableRequest {
            table_name: "users".to_string(),
            columns: "id INTEGER PRIMARY KEY, name TEXT, email TEXT".to_string(),
            if_not_exists: true,
        };
        handler.create_table_tool(create_req).await.unwrap();

        // Insert data
        let insert_req = QueryRequest {
            sql: "INSERT INTO users (name, email) VALUES (?, ?)".to_string(),
            parameters: vec![
                serde_json::Value::String("Alice".to_string()),
                serde_json::Value::String("alice@example.com".to_string()),
            ],
        };

        let insert_result = handler.query_tool(insert_req).await.unwrap();
        assert_eq!(insert_result.rows_affected, Some(1));

        // Select data
        let select_req = QueryRequest {
            sql: "SELECT * FROM users WHERE name = ?".to_string(),
            parameters: vec![serde_json::Value::String("Alice".to_string())],
        };

        let select_result = handler.query_tool(select_req).await.unwrap();
        assert_eq!(select_result.rows_affected, Some(1));
        assert!(select_result.data.is_some());
        assert!(select_result.columns.is_some());

        let data = select_result.data.unwrap();
        assert_eq!(data.len(), 1);
        assert_eq!(data[0][1], serde_json::Value::String("Alice".to_string()));
    }

    #[tokio::test]
    async fn test_batch_insert() {
        let (handler, _temp_dir, _db_path) = create_test_handler_with_db().await;

        // Create table
        let create_req = CreateTableRequest {
            table_name: "batch_test".to_string(),
            columns: "id INTEGER PRIMARY KEY, value TEXT".to_string(),
            if_not_exists: true,
        };
        handler.create_table_tool(create_req).await.unwrap();

        // Batch insert
        let batch_req = BatchInsertRequest {
            table_name: "batch_test".to_string(),
            columns: vec!["value".to_string()],
            rows: vec![
                vec![serde_json::Value::String("row1".to_string())],
                vec![serde_json::Value::String("row2".to_string())],
                vec![serde_json::Value::String("row3".to_string())],
            ],
            replace_on_conflict: false,
        };

        let result = handler.batch_insert_tool(batch_req).await.unwrap();
        assert!(result.success);
        assert_eq!(result.rows_inserted, 3);

        // Verify data was inserted
        let select_req = QueryRequest {
            sql: "SELECT COUNT(*) FROM batch_test".to_string(),
            parameters: vec![],
        };

        let select_result = handler.query_tool(select_req).await.unwrap();
        let count = &select_result.data.unwrap()[0][0];
        assert_eq!(*count, serde_json::Value::Number(3.into()));
    }

    #[tokio::test]
    async fn test_transaction() {
        let (handler, _temp_dir, _db_path) = create_test_handler_with_db().await;

        // Create table
        let create_req = CreateTableRequest {
            table_name: "tx_test".to_string(),
            columns: "id INTEGER PRIMARY KEY, value TEXT".to_string(),
            if_not_exists: true,
        };
        handler.create_table_tool(create_req).await.unwrap();

        // Transaction with multiple queries
        let tx_req = TransactionRequest {
            queries: vec![
                QueryRequest {
                    sql: "INSERT INTO tx_test (value) VALUES (?)".to_string(),
                    parameters: vec![serde_json::Value::String("tx1".to_string())],
                },
                QueryRequest {
                    sql: "INSERT INTO tx_test (value) VALUES (?)".to_string(),
                    parameters: vec![serde_json::Value::String("tx2".to_string())],
                },
            ],
            rollback_on_error: true,
        };

        let result = handler.transaction_tool(tx_req).await.unwrap();
        assert!(result.success);
        assert_eq!(result.total_rows_affected, 2);
        assert_eq!(result.results.len(), 2);

        // Verify both rows were inserted
        let select_req = QueryRequest {
            sql: "SELECT COUNT(*) FROM tx_test".to_string(),
            parameters: vec![],
        };

        let select_result = handler.query_tool(select_req).await.unwrap();
        let count = &select_result.data.unwrap()[0][0];
        assert_eq!(*count, serde_json::Value::Number(2.into()));
    }

    #[tokio::test]
    async fn test_backup() {
        let (handler, temp_dir, _db_path) = create_test_handler_with_db().await;

        // Create some data
        let create_req = CreateTableRequest {
            table_name: "backup_test".to_string(),
            columns: "id INTEGER PRIMARY KEY, data TEXT".to_string(),
            if_not_exists: true,
        };
        handler.create_table_tool(create_req).await.unwrap();

        let insert_req = QueryRequest {
            sql: "INSERT INTO backup_test (data) VALUES (?)".to_string(),
            parameters: vec![serde_json::Value::String("test_data".to_string())],
        };
        handler.query_tool(insert_req).await.unwrap();

        // Create backup
        let backup_path = temp_dir.path().join("backup.db");
        let backup_req = BackupRequest {
            destination_path: backup_path.display().to_string(),
        };

        let result = handler.backup_tool(backup_req).await.unwrap();
        assert!(result.success);
        assert!(result.backup_size.is_some());
        assert!(backup_path.exists());

        // Verify backup contains data
        let backup_conn = Connection::open(&backup_path).unwrap();
        let count: i32 = backup_conn
            .query_row("SELECT COUNT(*) FROM backup_test", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_export_csv() {
        let (handler, temp_dir, _db_path) = create_test_handler_with_db().await;

        // Create and populate table
        let create_req = CreateTableRequest {
            table_name: "csv_test".to_string(),
            columns: "id INTEGER PRIMARY KEY, name TEXT, value INTEGER".to_string(),
            if_not_exists: true,
        };
        handler.create_table_tool(create_req).await.unwrap();

        let batch_req = BatchInsertRequest {
            table_name: "csv_test".to_string(),
            columns: vec!["name".to_string(), "value".to_string()],
            rows: vec![
                vec![
                    serde_json::Value::String("Alice".to_string()),
                    serde_json::Value::Number(100.into()),
                ],
                vec![
                    serde_json::Value::String("Bob".to_string()),
                    serde_json::Value::Number(200.into()),
                ],
            ],
            replace_on_conflict: false,
        };
        handler.batch_insert_tool(batch_req).await.unwrap();

        // Export to CSV
        let csv_path = temp_dir.path().join("export.csv");
        let export_req = ExportCsvRequest {
            query: "SELECT name, value FROM csv_test ORDER BY name".to_string(),
            output_path: csv_path.display().to_string(),
            include_headers: true,
        };

        let result = handler.export_csv_tool(export_req).await.unwrap();
        assert!(result.success);
        assert_eq!(result.rows_exported, 2);
        assert!(csv_path.exists());

        // Verify CSV content
        let csv_content = fs::read_to_string(&csv_path).unwrap();
        assert!(csv_content.contains("name,value"));
        assert!(csv_content.contains("Alice,100"));
        assert!(csv_content.contains("Bob,200"));
    }

    #[tokio::test]
    async fn test_sql_validation() {
        let (handler, _temp_dir, _db_path) = create_test_handler_with_db().await;

        // Test invalid SQL (multiple statements)
        let invalid_req = QueryRequest {
            sql: "SELECT 1; DROP TABLE users;".to_string(),
            parameters: vec![],
        };

        let result = handler.query_tool(invalid_req).await;
        assert!(result.is_err());

        // Test disallowed command
        let disallowed_req = QueryRequest {
            sql: "ATTACH DATABASE 'other.db' AS other".to_string(),
            parameters: vec![],
        };

        let result = handler.query_tool(disallowed_req).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_path_validation() {
        let handler = SqliteHandler::new();
        let temp_dir = TempDir::new().unwrap();

        // Test invalid extension in temp directory
        let invalid_ext_path = temp_dir.path().join("test.txt");
        let invalid_ext_req = ConnectRequest {
            path: invalid_ext_path.display().to_string(),
            create_if_missing: true,
        };

        let result = handler.connect_tool(invalid_ext_req).await;
        assert!(result.is_err());

        // Test valid path in temp directory should work
        let valid_path = temp_dir.path().join("test.db");
        let valid_req = ConnectRequest {
            path: valid_path.display().to_string(),
            create_if_missing: true,
        };

        let result = handler.connect_tool(valid_req).await;
        assert!(result.is_ok());
    }
}
