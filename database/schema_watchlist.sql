-- Watchlist table schema for storing stock watchlist data
-- Run this SQL against your Lakebase Postgres database to create the watchlist table

CREATE TABLE IF NOT EXISTS watchlist (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    latest_price DECIMAL(12, 2),
    updated_at TIMESTAMP,
    CONSTRAINT unique_user_symbol UNIQUE (email, symbol)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_watchlist_email ON watchlist(email);
CREATE INDEX IF NOT EXISTS idx_watchlist_symbol ON watchlist(symbol);
CREATE INDEX IF NOT EXISTS idx_watchlist_updated_at ON watchlist(updated_at DESC);
