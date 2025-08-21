use rmcp::{
    ServerHandler, ServiceExt,
    model::{
        CallToolRequestParam, CallToolResult, Implementation, ListToolsResult,
        PaginatedRequestParam, ProtocolVersion, ServerCapabilities, ServerInfo, Tool,
    },
    service::RequestContext,
    transport::stdio,
};
use rusqlite::{Connection, OpenFlags};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::error::UniSqliteError;

#[derive(Debug, Clone)]
pub struct SqliteHandler {
    // Current database connection wrapped in Arc<Mutex> for thread safety (blocking)
    pub current_db: Arc<Mutex<Option<Connection>>>,
    // Path to current database
    pub current_path: Arc<Mutex<Option<PathBuf>>>,
}

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
        *self.current_db.lock().unwrap() = Some(conn);
        *self.current_path.lock().unwrap() = Some(path.clone());

        Ok(ConnectResult {
            success: true,
            path: path.display().to_string(),
        })
    }

    /// Validate and sanitize a database file path (prevents directory traversal)
    fn validate_db_path(&self, requested_path: &Path) -> Result<PathBuf, UniSqliteError> {
        let canonical = requested_path.canonicalize().map_err(|_| {
            UniSqliteError::InvalidPath(format!("Invalid path: {}", requested_path.display()))
        })?;

        let current_dir = std::env::current_dir()
            .and_then(|p| p.canonicalize())
            .map_err(|_| {
                UniSqliteError::InvalidPath("Cannot determine current directory".into())
            })?;

        if !canonical.starts_with(&current_dir) {
            return Err(UniSqliteError::InvalidPath(
                "Path outside allowed directory".into(),
            ));
        }

        match canonical.extension().and_then(|e| e.to_str()) {
            Some("db") | Some("sqlite") | Some("sqlite3") => Ok(canonical),
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

    /// Simple blacklist to block clearly dangerous statements.
    fn validate_sql_query(sql: &str) -> Result<(), UniSqliteError> {
        let sql_upper = sql.trim().to_uppercase();
        let dangerous = [
            "ATTACH",
            "DETACH",
            "LOAD_EXTENSION",
            "PRAGMA",
            "VACUUM INTO",
        ];
        for kw in &dangerous {
            if sql_upper.contains(kw) {
                return Err(UniSqliteError::QueryFailed(format!(
                    "Operation '{}' is not allowed for security reasons",
                    kw
                )));
            }
        }
        Ok(())
    }

    pub async fn query_tool(&self, req: QueryRequest) -> Result<QueryResult, UniSqliteError> {
        // Reject multiple statements for safety.
        if req.sql.matches(';').count() > 1 {
            return Err(UniSqliteError::QueryFailed(
                "Multiple statements are not allowed".into(),
            ));
        }

        // Acquire the current connection.
        let guard = self.current_db.lock().unwrap();
        let conn = guard
            .as_ref()
            .ok_or_else(|| UniSqliteError::Other("No database connected".into()))?;

        // Validate the SQL statement.
        Self::validate_sql_query(&req.sql)?;

        // Convert JSON parameters to rusqlite parameters.
        let params: Vec<Box<dyn rusqlite::ToSql>> = req
            .parameters
            .iter()
            .map(Self::json_to_sql_param)
            .collect::<Result<_, _>>()?;

        // Convert to slice of trait objects for rusqlite
        let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| &**p).collect();

        // Determine query type.
        let sql_upper = req.sql.trim_start().to_ascii_uppercase();
        if sql_upper.starts_with("SELECT") {
            // SELECT – return rows.
            let mut stmt = conn.prepare(&req.sql)?;
            let column_count = stmt.column_count();
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
            })
        } else {
            // Non‑SELECT – execute and report affected rows.
            let rows_affected = conn.execute(&req.sql, &param_refs[..])?;
            Ok(QueryResult {
                message: "Query executed successfully".into(),
                rows_affected: Some(rows_affected),
                data: None,
            })
        }
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
                description: Some(Cow::Borrowed("Execute a SQL query (SELECT or DML)")),
                input_schema: serde_json::to_value(schemars::schema_for!(QueryRequest).schema)
                    .unwrap()
                    .as_object()
                    .unwrap()
                    .clone()
                    .into(),
                annotations: None,
                output_schema: None,
            },
        ]
    }

    fn list_tools_handler(
        &self,
        _request: Option<PaginatedRequestParam>,
        _context: RequestContext<rmcp::service::RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, rmcp::ErrorData>> + Send + '_
    {
        async move {
            Ok(ListToolsResult {
                tools: Self::get_tools(),
                next_cursor: None,
            })
        }
    }

    fn call_tool_handler(
        &self,
        request: CallToolRequestParam,
        _context: RequestContext<rmcp::service::RoleServer>,
    ) -> impl std::future::Future<Output = Result<CallToolResult, rmcp::ErrorData>> + Send + '_
    {
        async move {
            match request.name.as_ref() {
                "connect" => {
                    let params: ConnectRequest =
                        serde_json::from_value(request.arguments.unwrap_or_default().into())
                            .map_err(|e| rmcp::ErrorData::invalid_params(e.to_string(), None))?;

                    let result = self
                        .connect_tool(params)
                        .await
                        .map_err(|e| rmcp::ErrorData::from(e))?;

                    Ok(CallToolResult {
                        content: vec![],
                        structured_content: Some(serde_json::json!({
                            "success": result.success,
                            "path": result.path
                        })),
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
                        .map_err(|e| rmcp::ErrorData::from(e))?;

                    Ok(CallToolResult {
                        content: vec![],
                        structured_content: Some(serde_json::json!({
                            "message": result.message,
                            "rows_affected": result.rows_affected,
                            "data": result.data
                        })),
                        is_error: Some(false),
                    })
                }
                _ => Err(rmcp::ErrorData::invalid_params("Tool not found", None)),
            }
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
                "A focused SQLite administration MCP server. \
                Connect to databases, execute queries, manage schema, \
                perform maintenance operations, and handle import/export."
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
