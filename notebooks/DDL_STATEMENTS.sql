-- Delta Lake Table DDL Statements
-- Run these in Databricks SQL or via Spark SQL

-- ============================================
-- 1. CREATE SCHEMA
-- ============================================

CREATE SCHEMA IF NOT EXISTS main.stock_news
  COMMENT 'Stock market news data with embeddings for semantic search';

-- ============================================
-- 2. RAW NEWS TABLE (with CDF)
-- ============================================

CREATE TABLE IF NOT EXISTS main.stock_news.ticker_news_raw (
  id STRING NOT NULL,
  ticker STRING NOT NULL,
  headline STRING NOT NULL,
  summary STRING,
  author STRING,
  url STRING,
  symbols STRING,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP,
  source STRING,
  raw_json STRING,
  ingested_at TIMESTAMP NOT NULL,
  ingestion_date DATE NOT NULL,
  text_length INT
)
USING DELTA
PARTITIONED BY (ingestion_date)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Raw news articles from Alpaca API with Change Data Feed enabled';

-- ============================================
-- 3. DOCUMENTS TABLE
-- ============================================

CREATE TABLE IF NOT EXISTS main.stock_news.ticker_news_documents (
  id STRING NOT NULL,
  ticker STRING NOT NULL,
  headline STRING NOT NULL,
  summary STRING,
  url STRING,
  full_text STRING NOT NULL,
  text_length INT NOT NULL,
  created_at TIMESTAMP NOT NULL,
  processed_at TIMESTAMP NOT NULL,
  has_chunks BOOLEAN NOT NULL
)
USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'News document metadata';

-- ============================================
-- 4. EMBEDDINGS TABLE (384-dimensional vectors)
-- ============================================

CREATE TABLE IF NOT EXISTS main.stock_news.ticker_news_embeddings (
  id STRING NOT NULL,
  ticker STRING NOT NULL,
  headline STRING NOT NULL,
  summary STRING,
  url STRING,
  created_at TIMESTAMP NOT NULL,
  full_text STRING NOT NULL,
  embedding ARRAY<FLOAT> NOT NULL,
  processed_at TIMESTAMP NOT NULL
)
USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true'
)
COMMENT 'News article embeddings (384-dim vectors from all-MiniLM-L6-v2)';

-- ============================================
-- 5. CHUNK EMBEDDINGS TABLE
-- ============================================

CREATE TABLE IF NOT EXISTS main.stock_news.ticker_news_chunk_embeddings (
  chunk_id STRING NOT NULL,
  article_id STRING NOT NULL,
  ticker STRING NOT NULL,
  chunk_index INT NOT NULL,
  chunk_text STRING NOT NULL,
  embedding ARRAY<FLOAT> NOT NULL,
  article_headline STRING NOT NULL,
  article_url STRING,
  created_at TIMESTAMP NOT NULL,
  processed_at TIMESTAMP NOT NULL
)
USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Chunk-level embeddings for long news articles';

-- ============================================
-- 6. CDF ANALYTICS - INGESTION METRICS
-- ============================================

CREATE TABLE IF NOT EXISTS main.stock_news.news_ingestion_metrics (
  metric_date DATE NOT NULL,
  ticker STRING NOT NULL,
  articles_added INT NOT NULL,
  articles_updated INT NOT NULL,
  articles_deleted INT NOT NULL,
  total_articles INT NOT NULL,
  avg_text_length DOUBLE,
  unique_sources INT,
  calculated_at TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (metric_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true'
)
COMMENT 'Daily metrics tracking news article changes by ticker';

-- ============================================
-- 7. CDF ANALYTICS - CHANGE LOG
-- ============================================

CREATE TABLE IF NOT EXISTS main.stock_news.news_change_log (
  change_id STRING NOT NULL,
  article_id STRING NOT NULL,
  ticker STRING NOT NULL,
  change_type STRING NOT NULL,
  commit_version BIGINT NOT NULL,
  commit_timestamp TIMESTAMP NOT NULL,
  headline STRING,
  previous_headline STRING,
  change_date DATE NOT NULL
)
USING DELTA
PARTITIONED BY (change_date)
COMMENT 'Audit trail of all news article changes captured via CDF';

-- ============================================
-- 8. VIEWS FOR BI
-- ============================================

CREATE OR REPLACE VIEW main.stock_news.recent_news AS
SELECT 
  id,
  ticker,
  headline,
  summary,
  author,
  url,
  symbols,
  created_at,
  source,
  text_length,
  ingested_at
FROM main.stock_news.ticker_news_raw
WHERE ingestion_date >= CURRENT_DATE - INTERVAL '7' DAY
ORDER BY created_at DESC;

CREATE OR REPLACE VIEW main.stock_news.news_metrics_dashboard AS
SELECT 
  m.metric_date,
  m.ticker,
  m.articles_added,
  m.articles_updated,
  m.total_articles,
  m.avg_text_length,
  m.unique_sources,
  c.change_count,
  m.calculated_at
FROM main.stock_news.news_ingestion_metrics m
LEFT JOIN (
  SELECT 
    change_date,
    ticker,
    COUNT(*) as change_count
  FROM main.stock_news.news_change_log
  GROUP BY change_date, ticker
) c ON m.metric_date = c.change_date AND m.ticker = c.ticker
ORDER BY m.metric_date DESC;

-- ============================================
-- 9. VERIFY CDF IS ENABLED
-- ============================================

-- Check table properties
DESCRIBE DETAIL main.stock_news.ticker_news_raw;

-- Read CDF from table
SELECT * FROM table_changes('main.stock_news.ticker_news_raw', 0);

