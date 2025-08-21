#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ExportCsvRequest {
    #[schemars(description = "Table name to export")]
    pub table_name: String,

    #[schemars(description = "Path for the output CSV file")]
    pub output_path: String,

    #[schemars(description = "Include column headers")]
    #[serde(default = "default_true")]
    pub include_headers: bool,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ImportCsvRequest {
    #[schemars(description = "Path to the CSV file to import")]
    pub input_path: String,

    #[schemars(description = "Target table name")]
    pub table_name: String,

    #[schemars(description = "First row contains headers")]
    #[serde(default = "default_true")]
    pub has_headers: bool,

    #[schemars(description = "Column names if no headers in CSV")]
    pub column_names: Option<Vec<String>>,
}

pub fn default_true() -> bool {
    true
}
