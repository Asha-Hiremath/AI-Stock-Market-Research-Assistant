# Database Setup Guide

## Lakebase Postgres with pgvector

### Prerequisites
- Lakebase Postgres endpoint provisioned
- pgvector extension enabled
- OAuth authentication configured

### Setup Steps

#### 1. Create Watchlist Table

Run the provided schema:

```sql
-- File: schema_watchlist.sql
CREATE TABLE IF NOT EXISTS watchlist (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    latest_price DECIMAL(10, 2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(email, symbol)
);

CREATE INDEX idx_watchlist_email ON watchlist(email);
CREATE INDEX idx_watchlist_symbol ON watchlist(symbol);
```

#### 2. Enable pgvector Extension

```sql
CREATE EXTENSION IF NOT EXISTS vector;
```

#### 3. Create Ticker News Table (for vector search)

```sql
CREATE TABLE IF NOT EXISTS ticker_news (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    headline TEXT NOT NULL,
    summary TEXT,
    published_at TIMESTAMP,
    embedding vector(384),  -- all-MiniLM-L6-v2 produces 384-dim vectors
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_ticker_news_ticker ON ticker_news(ticker);
CREATE INDEX idx_ticker_news_embedding ON ticker_news USING ivfflat (embedding vector_cosine_ops);
```

### OAuth Authentication

The app uses OAuth tokens instead of passwords:

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
token = w.postgres.generate_database_credential(endpoint=endpoint_name).token

# Use token in connection string
conn = psycopg2.connect(
    host=host,
    port=5432,
    dbname=database,
    user=user,
    password=token  # OAuth token, not a password!
)
```

### Testing the Connection

```python
# Run from notebook
from app.lakebase import get_connection

with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM watchlist")
        count = cur.fetchone()[0]
        print(f"Watchlist has {count} entries")
```

### Common Issues

**Issue**: `password authentication failed`
**Solution**: Ensure OAuth token generation is working (requires `databricks-sdk>=0.118.0`)

**Issue**: `relation "watchlist" does not exist`
**Solution**: Run `schema_watchlist.sql` to create the table

**Issue**: `extension "vector" does not exist`
**Solution**: Enable pgvector extension in Lakebase console

---

**Note**: All database operations in the MCP server use OAuth authentication automatically.
