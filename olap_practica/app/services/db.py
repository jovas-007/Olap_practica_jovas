"""Database access helpers for the Flask layer."""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal
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


def _serialise_value(value: Any) -> Any:
    """Convert database values to JSON-serialisable representations."""

    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, time):
        return value.strftime("%H:%M:%S")
    if isinstance(value, timedelta):
        total_seconds = int(value.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    if isinstance(value, Decimal):
        return float(value)
    return value


def fetch_all(query: str, params: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    """Execute a parametrised SQL query and return all rows as dictionaries."""

    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        columns = result.keys()
        rows: List[Dict[str, Any]] = []
        for db_row in result.fetchall():
            row_dict: Dict[str, Any] = {}
            for column, value in zip(columns, db_row):
                row_dict[column] = _serialise_value(value)
            rows.append(row_dict)
        return rows
