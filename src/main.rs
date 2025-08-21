mod error;
mod server;
mod tools;

// mod tools_impl;  // Full version for later

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new("uni_sqlite=info,rmcp=info")
            }),
        )
        .init();

    tracing::info!("Starting uni-sqlite MCP server");

    // Run the server
    server::run().await
}
