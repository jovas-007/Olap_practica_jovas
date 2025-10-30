"""Utility helpers for the OLAP práctica ETL workflows."""
from __future__ import annotations

import logging
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel
from slugify import slugify
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


class DayInfo(BaseModel):
    """Represents the metadata for a day of the week."""

    nombre: str
    orden: int


class AppSettings(BaseModel):
    """Application configuration loaded from :mod:`config/settings.yaml`."""

    periodo: str
    plan: str
    programas: Dict[str, str]
    day_map: Dict[str, DayInfo]
    salon_regex: str
    db: Dict[str, str]
    app: Dict[str, object]


@lru_cache(maxsize=1)
def get_logger(name: str = "olap_practica") -> logging.Logger:
    """Return a configured :class:`logging.Logger` instance.

    Parameters
    ----------
    name:
        Name of the logger.

    Returns
    -------
    logging.Logger
        Logger with a stream handler and basic formatter configured at INFO level.
    """

    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


def collapse_spaces(value: str) -> str:
    """Collapse multiple whitespaces and trim the supplied string.

    Parameters
    ----------
    value:
        Input string that may contain inconsistent spacing.

    Returns
    -------
    str
        Sanitized string with internal consecutive whitespaces replaced by a single
        space and trimmed.
    """

    return re.sub(r"\s+", " ", value.strip()) if value else ""


def safe_title(value: str) -> str:
    """Return a title-cased string preserving accents and abbreviations.

    The implementation uses :func:`python_slugify.slugify` to detect all-uppercase
    tokens and only applies title casing when the token is not an acronym.

    Examples
    --------
    >>> safe_title("juan perez")
    'Juan Perez'
    >>> safe_title("MARTÍN lópez")
    'Martín López'
    """

    value = collapse_spaces(value)
    tokens = value.split(" ")
    normalized = []
    for token in tokens:
        if not token:
            continue
        slug = slugify(token, separator="")
        if slug and token.isupper() and len(token) <= 4:
            normalized.append(token)
        else:
            normalized.append(token.capitalize())
    return " ".join(normalized)


def load_settings(path: str | Path = "config/settings.yaml") -> AppSettings:
    """Load the YAML settings file into an :class:`AppSettings` instance."""

    with open(path, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return AppSettings.parse_obj(data)


def get_database_url(settings: Optional[AppSettings] = None) -> str:
    """Resolve the database URL from environment variables or settings.

    Parameters
    ----------
    settings:
        Optional settings object. When provided, its ``db.url`` acts as fallback.

    Returns
    -------
    str
        Database URL suitable for SQLAlchemy connections.
    """

    load_dotenv()
    env_url = os.environ.get("DATABASE_URL")
    if env_url:
        return env_url
    if settings is None:
        settings = load_settings()
    return settings.db["url"]


def ensure_directory(path: str | Path) -> Path:
    """Create the directory if missing and return the :class:`Path` instance."""

    directory = Path(path)
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def execute_sql_file(path: str | Path, settings: Optional[AppSettings] = None) -> None:
    """Execute the SQL statements contained in ``path`` against the warehouse."""

    if settings is None:
        settings = load_settings()
    sql_text = Path(path).read_text(encoding="utf-8")
    database_url = get_database_url(settings)
    engine = create_engine(database_url)
    statements = [segment.strip() for segment in sql_text.split(";") if segment.strip()]
    logger = get_logger()
    with engine.begin() as conn:
        for statement in statements:
            try:
                conn.execute(text(statement))
            except SQLAlchemyError as exc:
                message = str(exc).lower()
                if "already exists" in message or "duplicate" in message:
                    logger.warning("Sentencia omitida por duplicado: %s", statement)
                else:
                    raise
    logger.info("Ejecución SQL completada desde %s", path)


def validate_salon(value: str, pattern: str) -> bool:
    """Validate a classroom string against the expected regular expression."""

    return bool(re.match(pattern, value))


__all__ = [
    "AppSettings",
    "DayInfo",
    "collapse_spaces",
    "ensure_directory",
    "get_database_url",
    "get_logger",
    "load_settings",
    "execute_sql_file",
    "safe_title",
    "validate_salon",
]
