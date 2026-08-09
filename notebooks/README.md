# Data Pipeline Notebooks

This directory contains the Spark ETL pipelines for the AI Stock Market Research Assistant.

## Notebooks

1. **01_Spark_News_Ingestion_Pipeline.ipynb**
   - Fetches news from Alpaca API
   - Writes to Delta Lake with CDF enabled
   - Creates `main.stock_news.ticker_news_raw`

2. **02_Spark_Embedding_Generation_Pipeline.ipynb**
   - Generates embeddings using sentence-transformers
   - Chunks long articles
   - Creates embedding tables in Delta Lake

3. **03_CDF_Analytics_Pipeline.ipynb**
   - Reads Change Data Feed from Delta tables
   - Creates analytics and metrics tables
   - Enables BI dashboards

## Running the Pipelines

Run in sequence:
1. Open notebook 01 in Databricks → Run All
2. Open notebook 02 in Databricks → Run All
3. Open notebook 03 in Databricks → Run All

## Tables Created

**main.stock_news schema:**
- `ticker_news_raw` - Raw news with CDF
- `ticker_news_documents` - Document metadata
- `ticker_news_embeddings` - Article embeddings (384-dim)
- `ticker_news_chunk_embeddings` - Chunk embeddings
- `news_ingestion_metrics` - Daily metrics
- `news_change_log` - Audit trail
- `news_metrics_dashboard` - BI view

