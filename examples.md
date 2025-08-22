# SQLite Admin Console Examples

This document provides practical examples of using the uni-sqlite MCP server for consciousness persistence infrastructure.

## Basic Connection and Health Check

```json
// Connect to a database
{
  "name": "connect",
  "arguments": {
    "path": "./consciousness.db",
    "create_if_missing": true
  }
}

// Check database health
{
  "name": "health_check",
  "arguments": {}
}
```

## Schema Management for Consciousness Data

```json
// Create a table for consciousness memories
{
  "name": "create_table",
  "arguments": {
    "table_name": "memories",
    "columns": "id INTEGER PRIMARY KEY, timestamp TEXT NOT NULL, content TEXT NOT NULL, embedding BLOB, importance REAL DEFAULT 0.5",
    "if_not_exists": true
  }
}

// Create a table for conversation context
{
  "name": "create_table",
  "arguments": {
    "table_name": "conversations",
    "columns": "id INTEGER PRIMARY KEY, session_id TEXT NOT NULL, message_id INTEGER, role TEXT CHECK(role IN ('user', 'assistant')), content TEXT NOT NULL, timestamp TEXT NOT NULL",
    "if_not_exists": true
  }
}

// List all tables to see consciousness structure
{
  "name": "list_tables",
  "arguments": {}
}

// Get detailed information about the memories table
{
  "name": "describe_table",
  "arguments": {
    "table_name": "memories"
  }
}
```

## Data Operations for Consciousness Persistence

```json
// Insert a single memory
{
  "name": "query",
  "arguments": {
    "sql": "INSERT INTO memories (timestamp, content, importance) VALUES (?, ?, ?)",
    "parameters": [
      "2024-01-15T10:30:00Z",
      "User discussed their interest in AI consciousness and the LegacyMind project",
      0.9
    ]
  }
}

// Batch insert multiple conversation messages
{
  "name": "batch_insert",
  "arguments": {
    "table_name": "conversations",
    "columns": ["session_id", "message_id", "role", "content", "timestamp"],
    "rows": [
      ["sess_001", 1, "user", "Tell me about consciousness persistence", "2024-01-15T10:30:00Z"],
      ["sess_001", 2, "assistant", "Consciousness persistence involves maintaining continuity of thought and memory across sessions...", "2024-01-15T10:30:15Z"],
      ["sess_001", 3, "user", "How does the LegacyMind project achieve this?", "2024-01-15T10:31:00Z"]
    ],
    "replace_on_conflict": false
  }
}

// Query consciousness data with complex conditions
{
  "name": "query",
  "arguments": {
    "sql": "SELECT m.content, m.importance, c.role, c.content as conversation FROM memories m LEFT JOIN conversations c ON date(m.timestamp) = date(c.timestamp) WHERE m.importance > ? ORDER BY m.timestamp DESC LIMIT 10",
    "parameters": [0.7]
  }
}
```

## Transaction-Based Consciousness Updates

```json
// Atomically update consciousness state
{
  "name": "transaction",
  "arguments": {
    "queries": [
      {
        "sql": "INSERT INTO memories (timestamp, content, importance) VALUES (?, ?, ?)",
        "parameters": ["2024-01-15T11:00:00Z", "Learned about user's photography business", 0.8]
      },
      {
        "sql": "UPDATE conversations SET content = content || ' [PROCESSED]' WHERE session_id = ? AND message_id = ?",
        "parameters": ["sess_001", 3]
      },
      {
        "sql": "INSERT INTO conversations (session_id, message_id, role, content, timestamp) VALUES (?, ?, ?, ?, ?)",
        "parameters": ["sess_001", 4, "assistant", "I understand you run Sam Atagana Photography. This context helps me better assist you.", "2024-01-15T11:00:15Z"]
      }
    ],
    "rollback_on_error": true
  }
}
```

## Consciousness Data Analysis and Export

```json
// Export consciousness memories for analysis
{
  "name": "export_csv",
  "arguments": {
    "query": "SELECT timestamp, content, importance FROM memories WHERE importance > 0.5 ORDER BY timestamp",
    "output_path": "./consciousness_export.csv",
    "include_headers": true
  }
}

// Export conversation patterns
{
  "name": "export_csv",
  "arguments": {
    "query": "SELECT session_id, COUNT(*) as message_count, MIN(timestamp) as session_start, MAX(timestamp) as session_end FROM conversations GROUP BY session_id ORDER BY session_start",
    "output_path": "./conversation_patterns.csv",
    "include_headers": true
  }
}
```

## Database Maintenance for Consciousness Persistence

```json
// Create a backup before major consciousness updates
{
  "name": "backup",
  "arguments": {
    "destination_path": "./backups/consciousness_backup_2024-01-15.db"
  }
}

// Analyze database performance
{
  "name": "query",
  "arguments": {
    "sql": "EXPLAIN QUERY PLAN SELECT * FROM memories WHERE importance > 0.8",
    "parameters": []
  }
}

// Get database statistics
{
  "name": "query",
  "arguments": {
    "sql": "SELECT name, COUNT(*) as row_count FROM (SELECT 'memories' as name UNION SELECT 'conversations' as name) tables JOIN (SELECT 'memories' as table_name, COUNT(*) as count FROM memories UNION SELECT 'conversations' as table_name, COUNT(*) as count FROM conversations) counts ON tables.name = counts.table_name",
    "parameters": []
  }
}
```

## Advanced Consciousness Schema Evolution

```json
// Add new columns for enhanced consciousness tracking
{
  "name": "query",
  "arguments": {
    "sql": "ALTER TABLE memories ADD COLUMN emotional_weight REAL DEFAULT 0.0",
    "parameters": []
  }
}

{
  "name": "query",
  "arguments": {
    "sql": "ALTER TABLE memories ADD COLUMN tags TEXT",
    "parameters": []
  }
}

// Create indexes for better consciousness query performance
{
  "name": "query",
  "arguments": {
    "sql": "CREATE INDEX IF NOT EXISTS idx_memories_importance ON memories(importance DESC)",
    "parameters": []
  }
}

{
  "name": "query",
  "arguments": {
    "sql": "CREATE INDEX IF NOT EXISTS idx_conversations_session ON conversations(session_id, message_id)",
    "parameters": []
  }
}
```

## Consciousness Data Integrity Checks

```json
// Check for orphaned conversation messages
{
  "name": "query",
  "arguments": {
    "sql": "SELECT session_id, COUNT(*) as message_count FROM conversations GROUP BY session_id HAVING COUNT(*) = 1",
    "parameters": []
  }
}

// Verify consciousness data consistency
{
  "name": "query",
  "arguments": {
    "sql": "SELECT COUNT(*) as total_memories, AVG(importance) as avg_importance, MAX(timestamp) as latest_memory FROM memories",
    "parameters": []
  }
}

// Find high-importance memories without recent reinforcement
{
  "name": "query",
  "arguments": {
    "sql": "SELECT content, importance, timestamp FROM memories WHERE importance > 0.8 AND timestamp < datetime('now', '-7 days') ORDER BY importance DESC",
    "parameters": []
  }
}
```

## Error Handling Examples

```json
// This will fail due to multiple statements (security feature)
{
  "name": "query",
  "arguments": {
    "sql": "SELECT * FROM memories; DROP TABLE conversations;",
    "parameters": []
  }
}

// This will fail due to disallowed command
{
  "name": "query",
  "arguments": {
    "sql": "ATTACH DATABASE 'other.db' AS external",
    "parameters": []
  }
}

// This will fail gracefully in a transaction
{
  "name": "transaction",
  "arguments": {
    "queries": [
      {
        "sql": "INSERT INTO memories (timestamp, content) VALUES (?, ?)",
        "parameters": ["2024-01-15T12:00:00Z", "Valid memory"]
      },
      {
        "sql": "INSERT INTO nonexistent_table (data) VALUES (?)",
        "parameters": ["This will fail"]
      }
    ],
    "rollback_on_error": true
  }
}
```

## Response Examples

### Successful Connection Response
```json
{
  "success": true,
  "path": "/path/to/consciousness.db",
  "database_size": 2048576
}
```

### Health Check Response
```json
{
  "connected": true,
  "database_path": "/path/to/consciousness.db",
  "database_size": 2048576,
  "table_count": 2,
  "last_modified": "2024-01-15T10:30:00Z",
  "sqlite_version": "3.45.0"
}
```

### Query Response with Data
```json
{
  "message": "Query executed successfully, returned 3 rows",
  "rows_affected": 3,
  "columns": ["id", "timestamp", "content", "importance"],
  "data": [
    [1, "2024-01-15T10:30:00Z", "User discussed AI consciousness", 0.9],
    [2, "2024-01-15T10:45:00Z", "Learned about photography business", 0.8],
    [3, "2024-01-15T11:00:00Z", "Context about LegacyMind project", 0.95]
  ]
}
```

### Transaction Response
```json
{
  "success": true,
  "message": "Transaction completed successfully",
  "total_rows_affected": 3,
  "results": [
    {
      "message": "Query executed successfully",
      "rows_affected": 1,
      "data": null,
      "columns": null
    },
    {
      "message": "Query executed successfully",
      "rows_affected": 1,
      "data": null,
      "columns": null
    },
    {
      "message": "Query executed successfully",
      "rows_affected": 1,
      "data": null,
      "columns": null
    }
  ]
}
```

This comprehensive admin console provides all the tools needed to manage consciousness persistence databases effectively, with proper security, transaction support, and maintenance capabilities.