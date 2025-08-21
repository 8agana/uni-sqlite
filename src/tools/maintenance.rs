#[derive(Debug, serde::Serialize)]
pub struct IntegrityCheckResult {
    pub ok: bool,
    pub messages: Vec<String>,
}

#[derive(Debug, serde::Serialize)]
pub struct DatabaseStats {
    pub page_count: i64,
    pub page_size: i64,
    pub total_size: i64,
    pub freelist_count: i64,
    pub cache_size: i64,
    pub journal_mode: String,
    pub auto_vacuum: i64,
    pub encoding: String,
}
