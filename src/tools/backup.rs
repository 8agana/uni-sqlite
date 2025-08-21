use schemars;

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct BackupRequest {
    #[schemars(description = "Path for the backup file")]
    pub dest_path: String,

    #[schemars(description = "Number of pages to copy per step (for progress reporting)")]
    pub page_step: Option<i32>,

    #[schemars(description = "Verbose progress reporting")]
    #[serde(default)]
    pub verbose: bool,
}

pub fn default_page_step() -> i32 {
    100
}

#[derive(Debug, serde::Serialize)]
pub struct BackupResult {
    pub success: bool,
    pub pages_backed_up: i32,
    pub duration_ms: u64,
}
