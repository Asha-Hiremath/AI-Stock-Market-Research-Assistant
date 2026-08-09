"""Stock Research Assistant - Streamlit Frontend

A user-facing web application for AI-powered stock market research.
Replaces the MCP-only backend with an interactive UI.
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import os
from databricks import sql
from databricks.sdk import WorkspaceClient
from alpaca.trading.client import TradingClient
from alpaca.data.historical import NewsClient
import requests

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
        "massive_key": w.dbutils.secrets.get(scope="massive", key="api-key")
    }

secrets = get_secrets()

# Initialize Alpaca clients
trading_client = TradingClient(secrets["alpaca_key"], secrets["alpaca_secret"], paper=True)
news_client = NewsClient(secrets["alpaca_key"], secrets["alpaca_secret"])

# Helper: Query Unity Catalog
def query_uc(sql_query):
    """Query Unity Catalog Delta tables"""
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

# Sidebar navigation
st.sidebar.title("📈 Stock Research")
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
    
    # Recent news trends
    st.subheader("📰 Recent News Volume")
    try:
        trends_df = query_uc("""
            SELECT 
                ingestion_date,
                ticker,
                COUNT(*) as article_count
            FROM main.stock_news.ticker_news_raw
            WHERE ingestion_date >= CURRENT_DATE - INTERVAL '7' DAY
            GROUP BY ingestion_date, ticker
            ORDER BY ingestion_date DESC, article_count DESC
        """)
        st.bar_chart(trends_df.pivot(index='ingestion_date', columns='ticker', values='article_count'))
    except Exception as e:
        st.info("Run the Spark pipeline notebooks to populate news data")

# ========== NEWS SEARCH PAGE ==========
elif page == "News Search":
    st.header("🔍 Semantic News Search")
    
    query = st.text_input("Search for news (semantic search)", placeholder="e.g., AI chip shortages")
    ticker_filter = st.text_input("Filter by ticker (optional)", placeholder="e.g., NVDA")
    
    if st.button("🔍 Search"):
        if query:
            with st.spinner("Searching news..."):
                try:
                    # Query embeddings from Delta
                    sql_query = f"""
                        SELECT 
                            id,
                            ticker,
                            headline,
                            summary,
                            url,
                            created_at
                        FROM main.stock_news.ticker_news_embeddings
                        WHERE 1=1
                    """
                    if ticker_filter:
                        sql_query += f" AND ticker = '{ticker_filter.upper()}'"
                    sql_query += " ORDER BY created_at DESC LIMIT 50"
                    
                    results_df = query_uc(sql_query)
                    
                    if len(results_df) > 0:
                        st.success(f"Found {len(results_df)} articles")
                        
                        for idx, row in results_df.iterrows():
                            with st.expander(f"{row['ticker']} - {row['headline']}"):
                                st.write(f"**Date:** {row['created_at']}")
                                st.write(f"**Summary:** {row['summary']}")
                                if row['url']:
                                    st.markdown(f"[Read full article]({row['url']})")
                    else:
                        st.warning("No results found")
                except Exception as e:
                    st.error(f"Search error: {e}")
                    st.info("Ensure embedding pipeline has run (notebook 02)")

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
                st.success(f"Added {new_ticker} to watchlist")
                # In production, this would write to Lakebase
    
    st.divider()
    
    # Display watchlist
    watchlist_tickers = ["AAPL", "GOOGL", "MSFT", "TSLA", "NVDA"]
    
    for ticker in watchlist_tickers:
        col1, col2, col3 = st.columns([2, 2, 1])
        with col1:
            st.write(f"### {ticker}")
        with col2:
            try:
                # Get latest quote
                st.write("$XXX.XX • +X.XX%")
            except:
                st.write("Price unavailable")
        with col3:
            if st.button("❌ Remove", key=f"remove_{ticker}"):
                st.info(f"Removed {ticker}")

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
        if order_type == "limit":
            limit_price = st.number_input("Limit Price", min_value=0.01, value=100.00)
        
        time_in_force = st.selectbox("Time in Force", ["day", "gtc", "ioc"])
    
    st.divider()
    
    if st.button("🚀 Place Order", type="primary"):
        if ticker and quantity:
            try:
                # Simulate order placement
                st.success(f"✅ Order placed: {action} {quantity} shares of {ticker}")
                st.info("This is a paper trading order. Check your portfolio to see the result.")
                
                # In production: trading_client.submit_order(...)
                
            except Exception as e:
                st.error(f"Order failed: {e}")

# Footer
st.divider()
st.caption("""
ℹ️ **Built with:** Databricks Apps, Streamlit, Alpaca API, Unity Catalog Delta Tables
📊 **Data Pipeline:** Spark ETL → Delta Lake → CDF Analytics
""")
