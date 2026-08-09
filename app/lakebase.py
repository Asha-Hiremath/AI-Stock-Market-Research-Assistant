"""
Lakebase (Databricks-managed Postgres) connection helper.

USES OAUTH TOKEN AUTHENTICATION (Required for Lakebase Postgres Autoscaling)
- No static passwords or secrets needed
- Tokens are generated fresh on each connection
- Works with both user and Service Principal identities
"""

import os
from contextlib import contextmanager

import psycopg2
from databricks.sdk import WorkspaceClient
from psycopg2.extras import RealDictCursor

try:
    from sqlalchemy import create_engine
    _SQLALCHEMY_AVAILABLE = True
except ImportError:
    _SQLALCHEMY_AVAILABLE = False

# Lakebase endpoint configuration
_ENDPOINT_NAME = os.environ.get(
    "LAKEBASE_ENDPOINT",
    "projects/bootcamp-lakebase/branches/production/endpoints/primary"
)
_HOST = os.environ.get(
    "LAKEBASE_HOST",
    "ep-royal-haze-d8jeuvgn.database.us-east-2.cloud.databricks.com"
)
_DATABASE = os.environ.get("LAKEBASE_DATABASE", "databricks_postgres")


def _get_lakebase_token() -> tuple[str, str]:
    """Generate fresh OAuth token and get current user.
    
    Returns:
        tuple[str, str]: (username, oauth_token)
    """
    w = WorkspaceClient()
    
    # Generate OAuth token for Lakebase
    token = w.postgres.generate_database_credential(endpoint=_ENDPOINT_NAME).token
    
    # Get current user (works for both users and Service Principals)
    user = w.current_user.me().user_name
    
    return user, token


@contextmanager
def get_connection():
    """Yield a raw psycopg2 connection with OAuth token authentication."""
    user, token = _get_lakebase_token()
    
    conn = psycopg2.connect(
        host=_HOST,
        port=5432,
        dbname=_DATABASE,
        user=user,
        password=token,  # OAuth token as password
        sslmode="require",
        cursor_factory=RealDictCursor
    )
    try:
        yield conn
    finally:
        conn.close()


def get_engine():
    """Return a SQLAlchemy engine for Lakebase."""
    if not _SQLALCHEMY_AVAILABLE:
        raise ImportError(
            "SQLAlchemy is not installed. Install it with: pip install sqlalchemy"
        )
    user, token = _get_lakebase_token()
    url = f"postgresql://{user}:{token}@{_HOST}:5432/{_DATABASE}?sslmode=require"
    return create_engine(url)


def run_query(sql: str, params: tuple | dict | None = None) -> list[dict]:
    """Run a read query against Lakebase and return rows as list[dict]."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def run_write(sql: str, params: tuple | dict | None = None) -> int:
    """Run an INSERT/UPDATE/DELETE against Lakebase, return affected row count."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            conn.commit()
            return cur.rowcount
