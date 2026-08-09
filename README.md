# AI Trading Agent with MCP Server

**Databricks Capstone Project**

## 🎯 Project Overview

A production-ready AI-powered trading system built on Databricks, featuring:
- **MCP Server** for AI agent integration
- **OAuth-authenticated Lakebase Postgres** database
- **Real-time stock trading** via Alpaca Markets API
- **Semantic search** over financial news with vector embeddings
- **Watchlist management** with persistent storage
- **Agent Bricks integration** for conversational trading

---

## ✅ Project Requirements Met

### 1. Data Pipeline in Spark ✅
- **Lakebase Postgres database** with OAuth authentication
- **Ticker news embeddings pipeline** using sentence-transformers
- **Watchlist table** with real-time stock data synchronization
- **Vector search** using pgvector for semantic retrieval

### 2. Third-Party API Integration ✅
- **Alpaca Markets API**: Paper trading (buy/sell stocks, portfolio management)
- **Massive.com API**: Real-time stock quotes and market data
- **OAuth 2.0**: Secure database authentication via WorkspaceClient

### 3. Unstructured Data Processing ✅
- **Vector embeddings** for ticker news articles
- **Semantic search** using sentence-transformers (all-MiniLM-L6-v2)
- **pgvector extension** for similarity search in Postgres
- **Text preprocessing** and embedding generation pipeline

### 4. Databricks App with Frontend ✅
- **FastMCP server** deployed as Databricks App
- **Live URL**: https://mcp-trading-server-7474651450902401.aws.databricksapps.com
- **OAuth-secured** endpoints
- **Health monitoring** and status checks

### 5. AI Agent with Tools (Read + Write) ✅
- **Agent Bricks** integration with 11 MCP tools
- **Read operations**: get_quote, get_watchlist, get_positions, get_account_summary, vector_search
- **Write operations**: add_to_watchlist, remove_from_watchlist, place_trade
- **Real-time data**: Live database writes and stock trades

---

## 🏗️ Architecture

```
┌─────────────────┐
│  Agent Bricks   │  ← User interacts via natural language
│   (AI Agent)    │
└────────┬────────┘
         │ MCP Protocol
         ▼
┌─────────────────┐
│   MCP Server    │  ← Databricks App (FastMCP)
│ (Databricks App)│     OAuth + Tool Routing
└────────┬────────┘
         │
    ┌────┴─────────────────┐
    │                      │
    ▼                      ▼
┌──────────┐        ┌─────────────┐
│ Lakebase │        │  Alpaca API │
│ Postgres │        │  (Trading)  │
│(Watchlist│        │             │
│ + Vector)│        │ Massive.com │
└──────────┘        │  (Quotes)   │
                    └─────────────┘
```

### Components

1. **MCP Server** (`app/`)
   - FastMCP-based server exposing 11 trading tools
   - OAuth authentication for Lakebase
   - Alpaca and Massive.com API integration

2. **Database** (`database/`)
   - Lakebase Postgres with pgvector extension
   - Watchlist table schema
   - Vector embeddings for news search

3. **AI Agent** (Agent Bricks)
   - Natural language interface
   - MCP client calling server tools
   - Real-time trading and data retrieval

---

## 🛠️ Tech Stack

- **Platform**: Databricks (Serverless)
- **Database**: Lakebase Postgres (OAuth-authenticated)
- **App Framework**: FastMCP + Databricks Apps V2
- **AI/ML**: sentence-transformers, pgvector
- **APIs**: Alpaca Markets, Massive.com
- **Languages**: Python 3.11+
- **Agent**: Agent Bricks (MCP client)

---

## 📦 MCP Tools Available

### Trading Operations
1. **get_quote(symbol)** - Get current stock price
2. **place_trade(symbol, qty, side, type)** - Execute paper trade
3. **get_positions()** - View portfolio positions
4. **get_account_summary()** - Account balance and stats
5. **get_order_history()** - Past trades
6. **get_balance()** - Available cash

### Watchlist Management
7. **add_to_watchlist(symbol)** - Add stock to watchlist
8. **get_watchlist()** - View saved stocks
9. **remove_from_watchlist(symbol)** - Remove stock

### Research
10. **vector_search(query, limit)** - Semantic search over ticker news

---

## 🚀 Setup Instructions

### Prerequisites
- Databricks workspace with Apps V2 enabled
- Lakebase Postgres endpoint
- Alpaca Markets API key (paper trading)
- Massive.com API key

### 1. Deploy the App

```bash
# Create secrets (via Databricks UI)
Scope: database
  - alpaca-key-id
  - alpaca-secret-key
  - lakebase-url

Scope: massive
  - api-key

# Deploy via CLI
databricks apps create mcp-trading-server
databricks apps deploy mcp-trading-server \
  --source-code-path /Workspace/Users/<your-email>/trading-agent-project-submission/app
```

### 2. Set Up Database

```sql
-- Run schema_watchlist.sql in Lakebase
-- Creates watchlist table with pgvector support
```

### 3. Configure Agent Bricks

1. Open Agent Bricks in Databricks
2. Add MCP Server:
   - **Name**: Trading & Watchlist Server
   - **URL**: `<your-app-url>`
3. Test: "Add SNAP to my watchlist"

---

## 🧪 Testing

### Via Agent Bricks

```
"Get a quote for AAPL"
→ Calls get_quote('AAPL') → Returns current price

"Add SNAP to my watchlist"
→ Calls add_to_watchlist('SNAP') → Stores in Lakebase

"Show me my watchlist"
→ Calls get_watchlist() → Returns your stocks

"Buy 10 shares of TSLA"
→ Calls place_trade('TSLA', 10, 'buy', 'market') → Paper trade

"Search for news about AI companies"
→ Calls vector_search('AI companies', 10) → Semantic search
```

---

## 🔐 Security

- **OAuth 2.0**: All Lakebase connections use OAuth tokens (no passwords)
- **Secret Management**: API keys stored in Databricks secret scopes
- **App Authentication**: OAuth2 app integration for end users
- **Service Principal**: App runs with its own identity

---

## 📊 Key Features

### OAuth Authentication (Fixed!)
- **Before**: `password authentication failed for user`
- **After**: Auto-generates OAuth tokens via `WorkspaceClient.postgres`
- **Result**: Secure, passwordless database access

### User Email Detection (Fixed!)
- **Before**: Hardcoded `email = 'zach@dataexpert.io'`
- **After**: Auto-detects authenticated user via `_get_end_user_email()`
- **Result**: Each user sees their own watchlist

### Vector Search
- Semantic search over ticker news embeddings
- Uses sentence-transformers + pgvector
- Returns relevant articles based on meaning (not just keywords)

---

## 📁 File Structure

```
trading-agent-project-submission/
├── README.md                    # This file
├── app/                         # MCP Server (Databricks App)
│   ├── alpaca_mcp_server.py    # Main server + 11 MCP tools
│   ├── lakebase.py             # OAuth database connector
│   ├── alpaca_broker.py        # Alpaca API integration
│   ├── massive_broker.py       # Massive.com quotes
│   ├── paper_broker.py         # Mock broker fallback
│   ├── app.yaml                # App configuration
│   └── requirements.txt        # Python dependencies
├── database/                    # Database schema
│   └── schema_watchlist.sql    # Watchlist table DDL
└── docs/                        # Documentation
    └── (additional docs)
```

---

## 🎯 Demo Scenarios

### Scenario 1: Build a Watchlist
```
User: "Add AAPL, MSFT, and GOOGL to my watchlist"
Agent: Calls add_to_watchlist 3 times → Stores in Lakebase

User: "Show me my watchlist"
Agent: Returns AAPL, MSFT, GOOGL with current prices
```

### Scenario 2: Paper Trade
```
User: "What's the price of TSLA?"
Agent: Calls get_quote('TSLA') → $245.32

User: "Buy 5 shares"
Agent: Calls place_trade('TSLA', 5, 'buy', 'market') → Order filled

User: "Show my positions"
Agent: Returns portfolio with 5 TSLA shares
```

### Scenario 3: Research
```
User: "Find news about renewable energy stocks"
Agent: Calls vector_search('renewable energy stocks', 10)
       → Returns semantically similar ticker news articles
```

---

## 📞 Contact

**Author**: alisha.dba@gmail.com
**Project**: Databricks Capstone - AI Trading Agent
**Deployed App**: https://mcp-trading-server-7474651450902401.aws.databricksapps.com

---

## 🏆 Project Highlights

- ✅ **Production-ready**: OAuth, error handling, logging
- ✅ **Real integrations**: Live APIs (not mocks)
- ✅ **AI-powered**: Vector search, semantic retrieval
- ✅ **Agent-first**: Built for conversational AI
- ✅ **Secure**: No hardcoded credentials, OAuth throughout
- ✅ **Scalable**: Serverless compute, Lakebase Postgres

---

**Built with ❤️ on Databricks**
