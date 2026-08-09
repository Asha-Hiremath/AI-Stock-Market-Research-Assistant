-- Lakebase Postgres DDL with pgvector Extension
-- Run this SQL against your Lakebase Postgres database to create vector search tables

-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- ============================================
-- 1. NEWS DOCUMENTS TABLE
-- ============================================

CREATE TABLE IF NOT EXISTS ticker_news_documents (
    id TEXT PRIMARY KEY,
    ticker TEXT NOT NULL,
    headline TEXT NOT NULL,
    summary TEXT,
    url TEXT,
    full_text TEXT NOT NULL,
    text_length INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL,
    processed_at TIMESTAMP NOT NULL,
    has_chunks BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS ticker_news_documents_ticker_idx ON ticker_news_documents(ticker);
CREATE INDEX IF NOT EXISTS ticker_news_documents_created_at_idx ON ticker_news_documents(created_at DESC);

COMMENT ON TABLE ticker_news_documents IS 'News document metadata for embedding generation';

-- ============================================
-- 2. NEWS EMBEDDINGS TABLE (384-dimensional vectors)
-- ============================================

CREATE TABLE IF NOT EXISTS ticker_news_embeddings (
    id TEXT PRIMARY KEY,
    ticker TEXT NOT NULL,
    headline TEXT NOT NULL,
    summary TEXT,
    url TEXT,
    created_at TIMESTAMP NOT NULL,
    full_text TEXT NOT NULL,
    embedding vector(384) NOT NULL,
    processed_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS ticker_news_embeddings_ticker_idx ON ticker_news_embeddings(ticker);
CREATE INDEX IF NOT EXISTS ticker_news_embeddings_created_at_idx ON ticker_news_embeddings(created_at DESC);

-- Vector index for fast similarity search (cosine distance)
CREATE INDEX IF NOT EXISTS ticker_news_embeddings_embedding_idx 
    ON ticker_news_embeddings 
    USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);

COMMENT ON TABLE ticker_news_embeddings IS 'News article embeddings (384-dim vectors from all-MiniLM-L6-v2)';

-- ============================================
-- 3. CHUNK EMBEDDINGS TABLE
-- ============================================

CREATE TABLE IF NOT EXISTS ticker_news_chunk_embeddings (
    chunk_id TEXT PRIMARY KEY,
    article_id TEXT NOT NULL,
    ticker TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    chunk_text TEXT NOT NULL,
    embedding vector(384) NOT NULL,
    article_headline TEXT NOT NULL,
    article_url TEXT,
    created_at TIMESTAMP NOT NULL,
    processed_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS ticker_news_chunk_embeddings_article_idx ON ticker_news_chunk_embeddings(article_id);
CREATE INDEX IF NOT EXISTS ticker_news_chunk_embeddings_ticker_idx ON ticker_news_chunk_embeddings(ticker);

-- Vector index for chunk-level similarity search
CREATE INDEX IF NOT EXISTS ticker_news_chunk_embeddings_embedding_idx 
    ON ticker_news_chunk_embeddings 
    USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);

COMMENT ON TABLE ticker_news_chunk_embeddings IS 'Chunk-level embeddings for long news articles';

-- ============================================
-- VERIFICATION QUERIES
-- ============================================

-- Check row counts
-- SELECT 'ticker_news_documents' as table_name, COUNT(*) as row_count FROM ticker_news_documents
-- UNION ALL
-- SELECT 'ticker_news_embeddings', COUNT(*) FROM ticker_news_embeddings
-- UNION ALL
-- SELECT 'ticker_news_chunk_embeddings', COUNT(*) FROM ticker_news_chunk_embeddings;

-- Sample vector search (cosine similarity)
-- Replace [YOUR_QUERY_VECTOR] with actual 384-dim array
-- SELECT 
--     id, 
--     ticker, 
--     headline,
--     1 - (embedding <=> '[0.1, 0.2, ...]'::vector) as similarity
-- FROM ticker_news_embeddings
-- ORDER BY embedding <=> '[0.1, 0.2, ...]'::vector
-- LIMIT 10;

-- Check vector index
-- SELECT indexname, indexdef 
-- FROM pg_indexes 
-- WHERE tablename IN ('ticker_news_embeddings', 'ticker_news_chunk_embeddings');
