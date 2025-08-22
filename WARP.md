# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Common commands (Rust/Cargo)
- Build (debug): cargo build
- Build (release): cargo build --release
- Run: cargo run
- Run with debug logs: RUST_LOG=uni_sqlite=debug,rmcp=info cargo run
- Lint: cargo clippy --all-targets --all-features -- -D warnings
- Format: cargo fmt --all
- Test all: cargo test
- Run a single test by name: cargo test <pattern>
- Show test output: cargo test -- --nocapture

Notes
- This crate is a binary MCP server speaking over stdio; cargo run starts it on stdio for an MCP client.
- RUST_LOG is respected via tracing_subscriber EnvFilter; default is uni_sqlite=info,rmcp=info when RUST_LOG is unset.

## Architecture overview

Big picture
- Purpose: A comprehensive SQLite administration MCP server for consciousness persistence infrastructure using rmcp 0.6 over stdio.
- Runtime: tokio async with tracing + env filter.
- Transport: rmcp stdio service; suitable for MCP clients that speak over stdio.
- Target: LegacyMind project consciousness persistence layer.

Key components
- main.rs
  - Initializes tracing_subscriber with EnvFilter (defaults to `uni_sqlite=info,rmcp=info`).
  - Entrypoint calls server::run().
- server.rs
  - SqliteHandler holds:
    - current_db: Arc<Mutex<Option<rusqlite::Connection>>>
    - current_path: Arc<Mutex<Option<PathBuf>>>
  - Comprehensive MCP tools for database administration:
    - **Connection Management**:
      - connect: open a SQLite DB at a validated path; optionally create if missing.
      - health_check: get database status, size, table count, and SQLite version.
    - **Query Operations**:
      - query: execute single-statement SQL (SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, PRAGMA, EXPLAIN, ANALYZE) with JSON parameters.
      - transaction: execute multiple queries atomically with rollback support.
    - **Schema Management**:
      - create_table: create new tables with column definitions.
      - list_tables: enumerate all tables with metadata and row counts.
      - describe_table: get detailed table structure (columns, types, constraints, indexes).
    - **Data Operations**:
      - batch_insert: efficiently insert multiple rows in a single transaction.
      - export_csv: export query results to CSV files with optional headers.
    - **Maintenance**:
      - backup: create database backups using SQLite's backup API.
  - Safety/validation:
    - Path canonicalization; restricts DB files to repo cwd subtree (or temp dirs in tests); only .db/.sqlite/.sqlite3 allowed.
    - SQL validation: only one statement; expanded allowlist for admin operations.
    - Parameters converted from serde_json::Value into rusqlite ToSql values.
  - Query flow:
    - SELECT/PRAGMA/EXPLAIN → prepares, maps rows into JSON (Null/Integer/Real/Text/Blob→hex), returns column names.
    - DML/DDL → executes and returns rows_affected.
  - RMCP integration:
    - Implements ServerHandler with protocol version 2024-11-05; declares 9 comprehensive tools.
    - serve(stdio) to run; server.waiting().await to block until completion.
- error.rs
  - UniSqliteError wraps rusqlite/IO/CSV/JSON errors + domain errors; From<UniSqliteError>→rmcp::ErrorData::internal_error.

## Tool Reference

### Connection Tools
- **connect**: Connect to SQLite database with optional creation
- **health_check**: Get connection status, database metrics, and system info

### Query Tools  
- **query**: Execute single SQL statements (SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, PRAGMA, EXPLAIN, ANALYZE)
- **transaction**: Execute multiple queries atomically with rollback support

### Schema Tools
- **create_table**: Create new tables with column specifications
- **list_tables**: List all tables with metadata and row counts
- **describe_table**: Get detailed table structure information

### Data Tools
- **batch_insert**: Efficiently insert multiple rows with transaction support
- **export_csv**: Export query results to CSV files

### Maintenance Tools
- **backup**: Create database backups using SQLite's native backup API

## Operational notes
- The service maintains a single mutable Connection guarded by an async Mutex; calls assume one active DB per process.
- All file paths are resolved relative to the current working directory; ensure your MCP client starts in the repo (or adjust cwd) when connecting to DB files.
- Binary BLOBs are hex-encoded in SELECT results.
- Transaction support ensures ACID properties for multi-query operations.
- Backup operations use SQLite's online backup API for consistency.
- CSV export handles all SQLite data types with proper encoding.
- Comprehensive test suite covers all major functionality with 10 test cases.

## Testing
- Full test coverage with tempfile-based isolated testing
- Tests cover: connection management, schema operations, queries, transactions, batch operations, backups, CSV export, security validation
- Run tests with: `cargo test`
- Individual test patterns: `cargo test test_backup`

## Extending tools
- To add a new tool: define request/response types with schemars derive, implement handler fn on SqliteHandler, add entry in get_tools(), and wire in call_tool_handler branch.
- Maintain validation patterns (path/SQL/params) when expanding capabilities.
- Follow the established async/await patterns for database operations.
- Add corresponding tests for new functionality.

## Security considerations
- Path validation prevents directory traversal attacks
- SQL injection protection via prepared statements
- Command allowlist prevents dangerous operations
- Single-statement enforcement prevents SQL injection chains
- File extension validation ensures only database files are accessed