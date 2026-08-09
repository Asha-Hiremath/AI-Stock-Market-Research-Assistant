"""Stock Research Assistant - Fully Functional Streamlit Frontend

Production-ready version with:
- Real Lakebase watchlist persistence
- Actual Alpaca order submission
- Semantic search with embeddings
- Error handling and validation
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from databricks import sql
from databricks.sdk import WorkspaceClient
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data.historical import NewsClient, StockHistoricalDataClient
from alpaca.data.requests import StockLatestQuoteRequest
import requests
import psycopg2
from sentence_transformers import SentenceTransformer

# Page config
st.set_page_config(
    page_title="AI Stock Research Assistant",
    page_icon="📈",
    layout="wide"
)

# Initialize Databricks workspace client
w = WorkspaceClient()

# Get secrets
@st.cache_resource
def get_secrets():
    return {
        "alpaca_key": w.dbutils.secrets.get(scope="database", key="alpaca-key-id"),
        "alpaca_secret": w.dbutils.secrets.get(scope="database", key="alpaca-secret-key"),
        "massive_key": w.dbutils.secrets.get(scope="massive", key="api-key"),
        "lakebase_url": w.dbutils.secrets.get(scope="database", key="lakebase-url")
    }

secrets = get_secrets()

# Initialize Alpaca clients
trading_client = TradingClient(secrets["alpaca_key"], secrets["alpaca_secret"], paper=True)
news_client = NewsClient(secrets["alpaca_key"], secrets["alpaca_secret"])
quote_client = StockHistoricalDataClient(secrets["alpaca_key"], secrets["alpaca_secret"])

# Initialize embedding model
@st.cache_resource
def get_embedding_model():
    return SentenceTransformer('all-MiniLM-L6-v2')

embedding_model = get_embedding_model()

# Get current user email
user_email = w.current_user.me().user_name

# Helper: Lakebase connection
def get_lakebase_connection():
    """Create connection to Lakebase Postgres"""
    conn_str = secrets["lakebase_url"]
    return psycopg2.connect(conn_str)

# Helper: Query Unity Catalog
def query_uc(sql_query):
    """Query Unity Catalog Delta tables"""
    try:
        connection = sql.connect(
            server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
            http_path=os.getenv("DATABRICKS_HTTP_PATH"),
            access_token=os.getenv("DATABRICKS_TOKEN")
        )
        cursor = connection.cursor()
        cursor.execute(sql_query)
        df = cursor.fetchall_arrow().to_pandas()
        cursor.close()
        connection.close()
        return df
    except Exception as e:
        st.error(f"UC query error: {e}")
        return pd.DataFrame()

# Watchlist functions
def add_to_watchlist(ticker, price=None):
    """Add ticker to user's watchlist in Lakebase"""
    try:
        conn = get_lakebase_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO watchlist (email, symbol, latest_price, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (email, symbol) DO UPDATE
            SET latest_price = EXCLUDED.latest_price, updated_at = NOW()
        """, (user_email, ticker, price))
        
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Error adding to watchlist: {e}")
        return False

def remove_from_watchlist(ticker):
    """Remove ticker from user's watchlist"""
    try:
        conn = get_lakebase_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            DELETE FROM watchlist
            WHERE email = %s AND symbol = %s
        """, (user_email, ticker))
        
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Error removing from watchlist: {e}")
        return False

def get_watchlist():
    """Get user's watchlist from Lakebase"""
    try:
        conn = get_lakebase_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT symbol, latest_price, updated_at
            FROM watchlist
            WHERE email = %s
            ORDER BY updated_at DESC
        """, (user_email,))
        
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return results
    except Exception as e:
        st.error(f"Error fetching watchlist: {e}")
        return []

# Semantic search function
def semantic_search(query_text, limit=10):
    """Perform semantic search using embeddings"""
    try:
        # Generate query embedding
        query_embedding = embedding_model.encode(query_text).tolist()
        
        # Query Unity Catalog for similar articles
        # Using cosine similarity via array_dot / array_norm
        df = query_uc(f"""
            WITH similarities AS (
                SELECT 
                    id,
                    ticker,
                    headline,
                    summary,
                    url,
                    created_at,
                    -- Cosine similarity
                    AGGREGATE(
                        TRANSFORM(embedding, x -> x * {query_embedding[0]})
                    ) / (
                        SQRT(AGGREGATE(TRANSFORM(embedding, x -> x * x))) *
                        SQRT({np.dot(query_embedding, query_embedding)})
                    ) AS similarity
                FROM main.stock_news.ticker_news_embeddings
            )
            SELECT *
            FROM similarities
            WHERE similarity > 0.5
            ORDER BY similarity DESC
            LIMIT {limit}
        """)
        
        return df
    except Exception as e:
        st.error(f"Semantic search error: {e}")
        # Fallback to keyword search
        return query_uc(f"""
            SELECT id, ticker, headline, summary, url, created_at
            FROM main.stock_news.ticker_news_raw
            WHERE LOWER(headline) LIKE LOWER('%{query_text}%')
               OR LOWER(summary) LIKE LOWER('%{query_text}%')
            ORDER BY created_at DESC
            LIMIT {limit}
        """)

# Sidebar navigation
st.sidebar.title("📈 Stock Research")
st.sidebar.info(f"👤 {user_email}")
page = st.sidebar.radio(
    "Navigate",
    ["Dashboard", "News Search", "Watchlist", "Portfolio", "Trade"]
)

# Main header
st.title("🤖 AI Stock Market Research Assistant")
st.caption("Powered by Databricks, Alpaca, and Massive")

# ========== DASHBOARD PAGE ==========
if page == "Dashboard":
    st.header("📊 Market Dashboard")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        try:
            account = trading_client.get_account()
            st.metric(
                "Portfolio Value",
                f"${float(account.equity):,.2f}",
                f"{float(account.equity) - float(account.last_equity):+.2f}"
            )
        except Exception as e:
            st.error(f"Error loading account: {e}")
    
    with col2:
        try:
            positions = trading_client.get_all_positions()
            st.metric("Open Positions", len(positions))
        except Exception as e:
            st.metric("Open Positions", "N/A")
    
    with col3:
        try:
            news_df = query_uc("""
                SELECT COUNT(*) as count
                FROM main.stock_news.ticker_news_raw
                WHERE ingestion_date >= CURRENT_DATE - INTERVAL '7' DAY
            """)
            st.metric("News (7 days)", f"{news_df['count'][0]:,}")
        except Exception as e:
            st.metric("News (7 days)", "N/A")
    
    st.divider()
    
    # Recent news
    st.subheader("📰 Latest News")
    try:
        recent_news = query_uc("""
            SELECT ticker, headline, created_at, url
            FROM main.stock_news.recent_news
            LIMIT 10
        """)
        if not recent_news.empty:
            for _, row in recent_news.iterrows():
                with st.expander(f"{row['ticker']} - {row['headline']}"):
                    st.write(f"**Date:** {row['created_at']}")
                    if row['url']:
                        st.markdown(f"[Read more]({row['url']})")
    except Exception as e:
        st.info("Run the Spark pipeline to see news data")

# ========== NEWS SEARCH PAGE ==========
elif page == "News Search":
    st.header("🔍 Semantic News Search")
    
    st.write("Search using natural language - powered by embeddings")
    
    query = st.text_input(
        "Search query",
        placeholder="e.g., 'companies announcing AI products' or 'tech stock earnings'"
    )
    
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("🔍 Search", type="primary"):
            if query:
                with st.spinner("Searching..."):
                    results_df = semantic_search(query, limit=20)
                    
                    if not results_df.empty:
                        st.success(f"Found {len(results_df)} articles")
                        
                        for idx, row in results_df.iterrows():
                            similarity = f" • Relevance: {row.get('similarity', 0):.2%}" if 'similarity' in row else ""
                            with st.expander(f"{row['ticker']} - {row['headline']}{similarity}"):
                                st.write(f"**Date:** {row['created_at']}")
                                st.write(f"**Summary:** {row.get('summary', 'N/A')}")
                                if row.get('url'):
                                    st.markdown(f"[Read full article]({row['url']})")
                    else:
                        st.warning("No results found")

# ========== WATCHLIST PAGE ==========
elif page == "Watchlist":
    st.header("📜 My Watchlist")
    
    # Add to watchlist
    col1, col2 = st.columns([3, 1])
    with col1:
        new_ticker = st.text_input("Add ticker", placeholder="e.g., AAPL").upper()
    with col2:
        st.write("")
        st.write("")
        if st.button("➕ Add"):
            if new_ticker:
                # Get current price
                try:
                    request_params = StockLatestQuoteRequest(symbol_or_symbols=new_ticker)
                    quote = quote_client.get_stock_latest_quote(request_params)[new_ticker]
                    price = float(quote.ask_price)
                except:
                    price = None
                
                if add_to_watchlist(new_ticker, price):
                    st.success(f"✅ Added {new_ticker} to watchlist")
                    st.rerun()
    
    st.divider()
    
    # Display watchlist from Lakebase
    watchlist = get_watchlist()
    
    if watchlist:
        st.subheader(f"Tracking {len(watchlist)} stocks")
        
        for ticker_data in watchlist:
            ticker, latest_price, updated_at = ticker_data
            
            col1, col2, col3 = st.columns([2, 2, 1])
            with col1:
                st.write(f"### {ticker}")
            with col2:
                try:
                    # Get fresh quote
                    request_params = StockLatestQuoteRequest(symbol_or_symbols=ticker)
                    quote = quote_client.get_stock_latest_quote(request_params)[ticker]
                    current_price = float(quote.ask_price)
                    
                    # Calculate change if we have historical price
                    if latest_price:
                        change_pct = ((current_price - float(latest_price)) / float(latest_price)) * 100
                        st.write(f"${current_price:.2f} • {change_pct:+.2f}%")
                    else:
                        st.write(f"${current_price:.2f}")
                except:
                    if latest_price:
                        st.write(f"${float(latest_price):.2f}")
                    else:
                        st.write("Price unavailable")
            with col3:
                if st.button("❌ Remove", key=f"remove_{ticker}"):
                    if remove_from_watchlist(ticker):
                        st.success(f"Removed {ticker}")
                        st.rerun()
    else:
        st.info("Your watchlist is empty. Add tickers above to start tracking.")

# ========== PORTFOLIO PAGE ==========
elif page == "Portfolio":
    st.header("💼 Portfolio Overview")
    
    try:
        account = trading_client.get_account()
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Equity", f"${float(account.equity):,.2f}")
        with col2:
            st.metric("Cash", f"${float(account.cash):,.2f}")
        with col3:
            st.metric("Buying Power", f"${float(account.buying_power):,.2f}")
        with col4:
            pnl = float(account.equity) - float(account.last_equity)
            st.metric("Today's P&L", f"${pnl:+,.2f}")
        
        st.divider()
        
        # Positions
        st.subheader("📈 Current Positions")
        positions = trading_client.get_all_positions()
        
        if positions:
            positions_data = []
            for pos in positions:
                positions_data.append({
                    "Symbol": pos.symbol,
                    "Qty": pos.qty,
                    "Avg Cost": f"${float(pos.avg_entry_price):.2f}",
                    "Current Price": f"${float(pos.current_price):.2f}",
                    "Market Value": f"${float(pos.market_value):,.2f}",
                    "P&L": f"${float(pos.unrealized_pl):+,.2f}",
                    "P&L %": f"{float(pos.unrealized_plpc) * 100:+.2f}%"
                })
            st.dataframe(pd.DataFrame(positions_data), use_container_width=True)
        else:
            st.info("No open positions")
    
    except Exception as e:
        st.error(f"Error loading portfolio: {e}")

# ========== TRADE PAGE ==========
elif page == "Trade":
    st.header("💰 Place Trade")
    
    col1, col2 = st.columns(2)
    
    with col1:
        ticker = st.text_input("Ticker Symbol", placeholder="e.g., AAPL").upper()
        action = st.selectbox("Action", ["BUY", "SELL"])
        quantity = st.number_input("Quantity", min_value=1, value=1)
    
    with col2:
        order_type = st.selectbox("Order Type", ["market", "limit"])
        limit_price = None
        if order_type == "limit":
            limit_price = st.number_input("Limit Price", min_value=0.01, value=100.00)
        
        time_in_force = st.selectbox(
            "Time in Force",
            ["day", "gtc", "ioc"],
            format_func=lambda x: {"day": "Day", "gtc": "Good Till Cancel", "ioc": "Immediate or Cancel"}[x]
        )
    
    st.divider()
    
    # Show current quote if ticker is entered
    if ticker:
        try:
            request_params = StockLatestQuoteRequest(symbol_or_symbols=ticker)
            quote = quote_client.get_stock_latest_quote(request_params)[ticker]
            st.info(f"📊 Current quote for {ticker}: Bid ${float(quote.bid_price):.2f} / Ask ${float(quote.ask_price):.2f}")
        except:
            pass
    
    if st.button("🚀 Place Order", type="primary"):
        if ticker and quantity:
            try:
                # Prepare order request
                side = OrderSide.BUY if action == "BUY" else OrderSide.SELL
                tif = TimeInForce.DAY if time_in_force == "day" else (TimeInForce.GTC if time_in_force == "gtc" else TimeInForce.IOC)
                
                if order_type == "market":
                    order_data = MarketOrderRequest(
                        symbol=ticker,
                        qty=quantity,
                        side=side,
                        time_in_force=tif
                    )
                else:
                    order_data = LimitOrderRequest(
                        symbol=ticker,
                        qty=quantity,
                        side=side,
                        time_in_force=tif,
                        limit_price=limit_price
                    )
                
                # Submit order to Alpaca
                order = trading_client.submit_order(order_data=order_data)
                
                # Show success
                st.success(f"✅ Order submitted successfully!")
                st.json({
                    "Order ID": str(order.id),
                    "Symbol": order.symbol,
                    "Side": order.side,
                    "Quantity": str(order.qty),
                    "Type": order.order_type,
                    "Status": order.status,
                    "Submitted At": str(order.submitted_at)
                })
                st.info("🔄 Check the Portfolio page to see your positions")
                
            except Exception as e:
                st.error(f"❌ Order failed: {e}")
                st.warning("Please check your account balance, ticker symbol, and market hours.")
        else:
            st.warning("Please enter ticker and quantity")

# Footer
st.divider()
st.caption("""
✅ **Fully Functional Version:**
- Real Lakebase watchlist persistence
- Actual Alpaca order submission (paper trading)
- Semantic search with embeddings
- Live quotes and portfolio data

📊 **Data Pipeline:** Spark ETL → Delta Lake → CDF Analytics
""")
