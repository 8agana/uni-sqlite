use rusqlite::types::Value;
use schemars;
use serde_json::json;

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct QueryRequest {
    #[schemars(description = "SQL query to execute")]
    pub sql: String,
    
    #[schemars(description = "Parameters for the query")]
    #[serde(default)]
    pub params: Vec<serde_json::Value>,
    
    #[schemars(description = "Maximum number of rows to return")]
    #[serde(default = "default_limit")]
    pub limit: usize,
}

pub fn default_limit() -> usize {
    1000
}

#[derive(Debug, serde::Serialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub rows_affected: Option<usize>,
    pub last_insert_rowid: Option<i64>,
}

pub fn json_to_sqlite_value(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Integer(*b as i64),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                Value::Real(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::Text(s.clone()),
        _ => Value::Text(v.to_string()),
    }
}

pub fn sqlite_value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Integer(i) => json!(i),
        Value::Real(f) => json!(f),
        Value::Text(s) => json!(s),
        Value::Blob(b) => json!(format!("<blob: {} bytes>", b.len())),
    }
}