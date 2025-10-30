"""Database access helpers for the Flask layer."""
from __future__ import annotations

from typing import Any, Dict, List

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from etl.utils import get_database_url, get_logger, load_settings

_engine: Engine | None = None


def get_engine() -> Engine:
    """Return a singleton SQLAlchemy engine based on configuration."""

    global _engine
    if _engine is None:
        settings = load_settings()
        database_url = get_database_url(settings)
        _engine = create_engine(database_url)
        get_logger().info("Conexión creada a %s", database_url)
    return _engine


def fetch_all(query: str, params: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    """Execute a parametrised SQL query and return all rows as dictionaries."""

    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        columns = result.keys()
        return [dict(zip(columns, row)) for row in result.fetchall()]
