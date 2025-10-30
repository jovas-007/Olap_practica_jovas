"""ETL package for the OLAP práctica project."""

from .extract_pdf import extract_all
from .transform import transform
from .load import load

__all__ = ["extract_all", "transform", "load"]
