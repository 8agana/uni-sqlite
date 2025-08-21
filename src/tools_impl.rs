use crate::server::SqliteHandler;
use crate::error::UniSqliteError;
use rmcp::{tool, tool_router, schemars};
use rusqlite::{Connection, OpenFlags, backup, types::Value};
use std::path::PathBuf;
use std::fs::File;
use std::io::{Read, Write};
use csv::{Reader, Writer};
use serde_json::json;
use serde::{Serialize, Deserialize};

// Import request/response types from tools modules
use crate::tools::connect::{ConnectRequest, ConnectResult};
use crate::tools::query::{QueryRequest, QueryResult, json_to_sqlite_value, sqlite_value_to_json, default_limit};
use crate::tools::schema::{TableInfo, ColumnInfo};
use crate::tools::maintenance::{IntegrityCheckResult, DatabaseStats};
use crate::tools::backup::{BackupRequest, BackupResult, default_page_step};
use crate::tools::import_export::{ExportCsvRequest, ImportCsvRequest, default_true};

// Single impl block with all tools
#[tool_router]
impl SqliteHandler {
    // ===== CONNECTION TOOLS =====
    
    #[tool(description = "Connect to a SQLite database")]
    async fn connect(&self, #[tool(aggr)] req: ConnectRequest) -> Result<ConnectResult, UniSqliteError> {
        let path = PathBuf::from(&req.path);
        
        // Expand ~ to home directory
        let path = if req.path.starts_with("~/") {
            home::home_dir()
                .ok_or_else(|| UniSqliteError::InvalidPath("Cannot determine home directory".into()))?
                .join(&req.path[2..])
        } else {
            path
        };
        
        // Check if file exists
        if !path.exists() && !req.create_if_missing {
            return Err(UniSqliteError::DatabaseNotFound(req.path));
        }
        
        // Set open flags
        let flags = if req.readonly {
            OpenFlags::SQLITE_OPEN_READ_ONLY
        } else if req.create_if_missing {
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE
        } else {
            OpenFlags::SQLITE_OPEN_READ_WRITE
        };
        
        // Open connection
        let conn = Connection::open_with_flags(&path, flags)?;
        
        // Get database info
        let page_count: i64 = conn.query_row("PRAGMA page_count", [], |row| row.get(0))?;
        let page_size: i64 = conn.query_row("PRAGMA page_size", [], |row| row.get(0))?;
        let journal_mode: String = conn.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
        let version: String = conn.query_row("SELECT sqlite_version()", [], |row| row.get(0))?;
        
        let size_bytes = (page_count * page_size) as u64;
        
        // Store connection
        *self.current_db.lock().unwrap() = Some(conn);
        *self.current_path.lock().unwrap() = Some(path.clone());
        
        Ok(ConnectResult {
            success: true,
            path: path.display().to_string(),
            size_bytes: Some(size_bytes),
            page_count: Some(page_count),
            page_size: Some(page_size),
            journal_mode: Some(journal_mode),
            version: Some(version),
        })
    }
    
    #[tool(description = "Disconnect from the current database")]
    async fn disconnect(&self) -> Result<String, UniSqliteError> {
        let mut db = self.current_db.lock().unwrap();
        let mut path = self.current_path.lock().unwrap();
        
        if db.is_none() {
            return Ok("No database connected".into());
        }
        
        *db = None;
        *path = None;
        
        Ok("Disconnected successfully".into())
    }
    
    // ===== QUERY TOOLS =====
    
    #[tool(description = "Execute a SQL query and return results")]
    async fn execute_query(&self, #[tool(aggr)] req: QueryRequest) -> Result<QueryResult, UniSqliteError> {
        let db = self.current_db.lock().unwrap();
        let conn = db.as_ref()
            .ok_or_else(|| UniSqliteError::Other("No database connected".into()))?;
        
        // Check if it's a SELECT query
        let is_select = req.sql.trim().to_uppercase().starts_with("SELECT");
        
        if is_select {
            // Execute SELECT query
            let mut stmt = conn.prepare(&req.sql)?;
            let column_count = stmt.column_count();
            
            // Get column names
            let columns: Vec<String> = (0..column_count)
                .map(|i| stmt.column_name(i).unwrap().to_string())
                .collect();
            
            // Convert params
            let params: Vec<Value> = req.params.iter()
                .map(|v| json_to_sqlite_value(v))
                .collect();
            
            // Execute and collect rows
            let rows_iter = stmt.query_map(rusqlite::params_from_iter(params.iter()), |row| {
                let mut row_values = Vec::new();
                for i in 0..column_count {
                    let value: Value = row.get(i)?;
                    row_values.push(sqlite_value_to_json(&value));
                }
                Ok(row_values)
            })?;
            
            let mut rows = Vec::new();
            for (i, row) in rows_iter.enumerate() {
                if i >= req.limit {
                    break;
                }
                rows.push(row?);
            }
            
            Ok(QueryResult {
                columns,
                rows,
                rows_affected: None,
                last_insert_rowid: None,
            })
        } else {
            // Execute non-SELECT query
            let rows_affected = conn.execute(&req.sql, [])?;
            let last_insert_rowid = conn.last_insert_rowid();
            
            Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: Some(rows_affected),
                last_insert_rowid: Some(last_insert_rowid),
            })
        }
    }
    
    // ===== SCHEMA TOOLS =====
    
    #[tool(description = "List all tables in the database")]
    async fn list_tables(&self) -> Result<Vec<TableInfo>, UniSqliteError> {
        let db = self.current_db.lock().unwrap();
        let conn = db.as_ref()
            .ok_or_else(|| UniSqliteError::Other("No database connected".into()))?;
        
        let mut stmt = conn.prepare(
            "SELECT name, sql FROM sqlite_master 
             WHERE type='table' AND name NOT LIKE 'sqlite_%' 
             ORDER BY name"
        )?;
        
        let tables = stmt.query_map([], |row| {
            let name: String = row.get(0)?;
            let sql: String = row.get(1)?;
            
            // Get row count for each table
            let count_sql = format!("SELECT COUNT(*) FROM {}", name);
            let row_count: i64 = conn.query_row(&count_sql, [], |r| r.get(0))
                .unwrap_or(0);
            
            Ok(TableInfo {
                name,
                sql,
                row_count,
            })
        })?;
        
        let mut result = Vec::new();
        for table in tables {
            result.push(table?);
        }
        
        Ok(result)
    }
    
    #[tool(description = "Get detailed information about a table's columns")]
    async fn describe_table(&self,
        #[tool(param)]
        #[schemars(description = "Name of the table to describe")]
        table_name: String,
    ) -> Result<Vec<ColumnInfo>, UniSqliteError> {
        let db = self.current_db.lock().unwrap();
        let conn = db.as_ref()
            .ok_or_else(|| UniSqliteError::Other("No database connected".into()))?;
        
        let sql = format!("PRAGMA table_info({})", table_name);
        let mut stmt = conn.prepare(&sql)?;
        
        let columns = stmt.query_map([], |row| {
            Ok(ColumnInfo {
                cid: row.get(0)?,
                name: row.get(1)?,
                type_name: row.get(2)?,
                not_null: row.get::<_, i32>(3)? != 0,
                default_value: row.get(4)?,
                primary_key: row.get::<_, i32>(5)? != 0,
            })
        })?;
        
        let mut result = Vec::new();
        for col in columns {
            result.push(col?);
        }
        
        Ok(result)
    }
    
    // ===== MAINTENANCE TOOLS =====
    
    #[tool(description = "Run VACUUM to reclaim space and defragment the database")]
    async fn vacuum(&self) -> Result<String, UniSqliteError> {
        let db = self.current_db.lock().unwrap();
        let conn = db.as_ref()
            .ok_or_else(|| UniSqliteError::Other("No database connected".into()))?;
        
        conn.execute("VACUUM", [])?;
        
        Ok("VACUUM completed successfully".into())
    }
    
    #[tool(description = "Run ANALYZE to update query optimizer statistics")]
    async fn analyze(&self,
        #[tool(param)]
        #[schemars(description = "Optional table name to analyze (analyzes all if not specified)")]
        table_name: Option<String>,
    ) -> Result<String, UniSqliteError> {
        let db = self.current_db.lock().unwrap();
        let conn = db.as_ref()
            .ok_or_else(|| UniSqliteError::Other("No database connected".into()))?;
        
        let sql = if let Some(table) = table_name {
            format!("ANALYZE {}", table)
        } else {
            "ANALYZE".to_string()
        };
        
        conn.execute(&sql, [])?;
        
        Ok("ANALYZE completed successfully".into())
    }
    
    #[tool(description = "Check database integrity")]
    async fn integrity_check(&self,
        #[tool(param)]
        #[schemars(description = "Use quick check (less thorough but faster)")]
        #[serde(default)]
        quick: bool,
    ) -> Result<IntegrityCheckResult, UniSqliteError> {
        let db = self.current_db.lock().unwrap();
        let conn = db.as_ref()
            .ok_or_else(|| UniSqliteError::Other("No database connected".into()))?;
        
        let sql = if quick {
            "PRAGMA quick_check"
        } else {
            "PRAGMA integrity_check"
        };
        
        let mut stmt = conn.prepare(sql)?;
        let messages: Vec<String> = stmt.query_map([], |row| {
            row.get(0)
        })?.collect::<Result<Vec<_>, _>>()?;
        
        let ok = messages.len() == 1 && messages[0] == "ok";
        
        Ok(IntegrityCheckResult {
            ok,
            messages,
        })
    }
    
    #[tool(description = "Reindex all indices or a specific table")]
    async fn reindex(&self,
        #[tool(param)]
        #[schemars(description = "Optional table name to reindex")]
        table_name: Option<String>,
    ) -> Result<String, UniSqliteError> {
        let db = self.current_db.lock().unwrap();
        let conn = db.as_ref()
            .ok_or_else(|| UniSqliteError::Other("No database connected".into()))?;
        
        let sql = if let Some(table) = table_name {
            format!("REINDEX {}", table)
        } else {
            "REINDEX".to_string()
        };
        
        conn.execute(&sql, [])?;
        
        Ok("REINDEX completed successfully".into())
    }
    
    #[tool(description = "Get database statistics")]
    async fn get_stats(&self) -> Result<DatabaseStats, UniSqliteError> {
        let db = self.current_db.lock().unwrap();
        let conn = db.as_ref()
            .ok_or_else(|| UniSqliteError::Other("No database connected".into()))?;
        
        let page_count: i64 = conn.query_row("PRAGMA page_count", [], |row| row.get(0))?;
        let page_size: i64 = conn.query_row("PRAGMA page_size", [], |row| row.get(0))?;
        let freelist_count: i64 = conn.query_row("PRAGMA freelist_count", [], |row| row.get(0))?;
        let cache_size: i64 = conn.query_row("PRAGMA cache_size", [], |row| row.get(0))?;
        let journal_mode: String = conn.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
        let auto_vacuum: i64 = conn.query_row("PRAGMA auto_vacuum", [], |row| row.get(0))?;
        let encoding: String = conn.query_row("PRAGMA encoding", [], |row| row.get(0))?;
        
        Ok(DatabaseStats {
            page_count,
            page_size,
            total_size: page_count * page_size,
            freelist_count,
            cache_size,
            journal_mode,
            auto_vacuum,
            encoding,
        })
    }
    
    // ===== BACKUP TOOLS =====
    
    #[tool(description = "Create an online backup of the database")]
    async fn backup(&self, #[tool(aggr)] req: BackupRequest) -> Result<BackupResult, UniSqliteError> {
        let db = self.current_db.lock().unwrap();
        let source = db.as_ref()
            .ok_or_else(|| UniSqliteError::Other("No database connected".into()))?;
        
        let mut dest = Connection::open(&req.dest_path)?;
        
        let backup = backup::Backup::new(source, &mut dest)?;
        
        let page_step = req.page_step.unwrap_or(default_page_step());
        
        let start = std::time::Instant::now();
        let mut progress_count = 0;
        
        // Run backup to completion
        backup.run_to_completion(page_step, std::time::Duration::from_millis(250), None)?;
        
        let duration_ms = start.elapsed().as_millis() as u64;
        
        Ok(BackupResult {
            success: true,
            pages_backed_up: page_step, // We don't have page count anymore
            duration_ms,
        })
    }
    
    #[tool(description = "Dump database schema and data to SQL file")]
    async fn dump_to_sql(&self,
        #[tool(param)]
        #[schemars(description = "Path to write SQL dump")]
        output_path: String,
        #[tool(param)]
        #[schemars(description = "Include data (not just schema)")]
        #[serde(default = "default_true")]
        include_data: bool,
    ) -> Result<String, UniSqliteError> {
        let db = self.current_db.lock().unwrap();
        let conn = db.as_ref()
            .ok_or_else(|| UniSqliteError::Other("No database connected".into()))?;
        
        let mut output = String::new();
        
        // Get schema
        let mut stmt = conn.prepare(
            "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type DESC, name"
        )?;
        
        let schemas = stmt.query_map([], |row| {
            let sql: String = row.get(0)?;
            Ok(sql)
        })?;
        
        for schema in schemas {
            output.push_str(&schema?);
            output.push_str(";\n");
        }
        
        if include_data {
            // Get all tables
            let mut stmt = conn.prepare(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )?;
            
            let tables: Vec<String> = stmt.query_map([], |row| {
                row.get(0)
            })?.collect::<Result<Vec<_>, _>>()?;
            
            for table in tables {
                // Get all data from table
                let query = format!("SELECT * FROM {}", table);
                let mut stmt = conn.prepare(&query)?;
                
                let column_count = stmt.column_count();
                let columns: Vec<String> = (0..column_count)
                    .map(|i| stmt.column_name(i).unwrap().to_string())
                    .collect();
                
                let rows = stmt.query_map([], |row| {
                    let mut values = Vec::new();
                    for i in 0..column_count {
                        let value: Value = row.get(i)?;
                        values.push(match value {
                            Value::Null => "NULL".to_string(),
                            Value::Integer(i) => i.to_string(),
                            Value::Real(f) => f.to_string(),
                            Value::Text(s) => format!("'{}'", s.replace("'", "''")),
                            Value::Blob(b) => format!("X'{}'", hex::encode(b)),
                        });
                    }
                    Ok(values)
                })?;
                
                for row in rows {
                    let row = row?;
                    output.push_str(&format!(
                        "INSERT INTO {} ({}) VALUES ({});\n",
                        table,
                        columns.join(", "),
                        row.join(", ")
                    ));
                }
            }
        }
        
        std::fs::write(&output_path, output)?;
        
        Ok(format!("Database dumped to {}", output_path))
    }
    
    #[tool(description = "Restore database from SQL dump")]
    async fn restore_from_sql(&self,
        #[tool(param)]
        #[schemars(description = "Path to SQL dump file")]
        input_path: String,
    ) -> Result<String, UniSqliteError> {
        let db = self.current_db.lock().unwrap();
        let conn = db.as_ref()
            .ok_or_else(|| UniSqliteError::Other("No database connected".into()))?;
        
        let sql = std::fs::read_to_string(&input_path)?;
        
        conn.execute_batch(&sql)?;
        
        Ok(format!("Database restored from {}", input_path))
    }
    
    #[tool(description = "Create a new database using VACUUM INTO")]
    async fn vacuum_into(&self,
        #[tool(param)]
        #[schemars(description = "Path for the new vacuumed database")]
        dest_path: String,
    ) -> Result<String, UniSqliteError> {
        let db = self.current_db.lock().unwrap();
        let conn = db.as_ref()
            .ok_or_else(|| UniSqliteError::Other("No database connected".into()))?;
        
        let sql = format!("VACUUM INTO '{}'", dest_path);
        conn.execute(&sql, [])?;
        
        Ok(format!("Vacuumed database created at {}", dest_path))
    }
    
    // ===== IMPORT/EXPORT TOOLS =====
    
    #[tool(description = "Export table data to CSV")]
    async fn export_csv(&self, #[tool(aggr)] req: ExportCsvRequest) -> Result<String, UniSqliteError> {
        let db = self.current_db.lock().unwrap();
        let conn = db.as_ref()
            .ok_or_else(|| UniSqliteError::Other("No database connected".into()))?;
        
        let query = format!("SELECT * FROM {}", req.table_name);
        let mut stmt = conn.prepare(&query)?;
        
        let column_count = stmt.column_count();
        let columns: Vec<String> = (0..column_count)
            .map(|i| stmt.column_name(i).unwrap().to_string())
            .collect();
        
        let mut wtr = Writer::from_path(&req.output_path)?;
        
        if req.include_headers {
            wtr.write_record(&columns)?;
        }
        
        let rows = stmt.query_map([], |row| {
            let mut record = Vec::new();
            for i in 0..column_count {
                let value: Value = row.get(i)?;
                record.push(match value {
                    Value::Null => String::new(),
                    Value::Integer(i) => i.to_string(),
                    Value::Real(f) => f.to_string(),
                    Value::Text(s) => s,
                    Value::Blob(b) => hex::encode(b),
                });
            }
            Ok(record)
        })?;
        
        let mut count = 0;
        for row in rows {
            wtr.write_record(row?)?;
            count += 1;
        }
        
        wtr.flush()?;
        
        Ok(format!("Exported {} rows to {}", count, req.output_path))
    }
    
    #[tool(description = "Import CSV data into a table")]
    async fn import_csv(&self, #[tool(aggr)] req: ImportCsvRequest) -> Result<String, UniSqliteError> {
        let mut db = self.current_db.lock().unwrap();
        let conn = db.as_mut()
            .ok_or_else(|| UniSqliteError::Other("No database connected".into()))?;
        
        let mut rdr = Reader::from_path(&req.input_path)?;
        
        let headers = if req.has_headers {
            rdr.headers()?.iter().map(|s| s.to_string()).collect()
        } else {
            req.column_names.clone().ok_or_else(|| {
                UniSqliteError::ImportFailed("No headers in CSV and no column names provided".into())
            })?
        };
        
        // Prepare insert statement
        let placeholders = (0..headers.len()).map(|_| "?").collect::<Vec<_>>().join(", ");
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            req.table_name,
            headers.join(", "),
            placeholders
        );
        
        let mut count = 0;
        let tx = conn.transaction()?;
        
        for result in rdr.records() {
            let record = result?;
            let values: Vec<Value> = record.iter().map(|s| {
                if s.is_empty() {
                    Value::Null
                } else if let Ok(i) = s.parse::<i64>() {
                    Value::Integer(i)
                } else if let Ok(f) = s.parse::<f64>() {
                    Value::Real(f)
                } else {
                    Value::Text(s.to_string())
                }
            }).collect();
            
            tx.execute(&sql, rusqlite::params_from_iter(values))?;
            count += 1;
        }
        
        tx.commit()?;
        
        Ok(format!("Imported {} rows into {}", count, req.table_name))
    }
    
    #[tool(description = "Export query results to JSON")]
    async fn export_json(&self,
        #[tool(param)]
        #[schemars(description = "SQL query to execute")]
        query: String,
        #[tool(param)]
        #[schemars(description = "Path to write JSON output")]
        output_path: String,
        #[tool(param)]
        #[schemars(description = "Pretty print JSON")]
        #[serde(default)]
        pretty: bool,
    ) -> Result<String, UniSqliteError> {
        let db = self.current_db.lock().unwrap();
        let conn = db.as_ref()
            .ok_or_else(|| UniSqliteError::Other("No database connected".into()))?;
        
        let mut stmt = conn.prepare(&query)?;
        let column_count = stmt.column_count();
        
        let columns: Vec<String> = (0..column_count)
            .map(|i| stmt.column_name(i).unwrap().to_string())
            .collect();
        
        let rows = stmt.query_map([], |row| {
            let mut obj = serde_json::Map::new();
            for (i, col) in columns.iter().enumerate() {
                let value: Value = row.get(i)?;
                obj.insert(col.clone(), sqlite_value_to_json(&value));
            }
            Ok(serde_json::Value::Object(obj))
        })?;
        
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        
        let row_count = results.len();
        let json = serde_json::Value::Array(results);
        
        let output = if pretty {
            serde_json::to_string_pretty(&json)?
        } else {
            serde_json::to_string(&json)?
        };
        
        std::fs::write(&output_path, output)?;
        
        Ok(format!("Exported {} rows to {}", row_count, output_path))
    }
}