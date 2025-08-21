#[derive(Debug, serde::Serialize)]
pub struct TableInfo {
    pub name: String,
    pub sql: String,
    pub row_count: i64,
}

#[derive(Debug, serde::Serialize)]
pub struct ColumnInfo {
    pub cid: i32,
    pub name: String,
    pub type_name: String,
    pub not_null: bool,
    pub default_value: Option<String>,
    pub primary_key: bool,
}
