"""ETL package for the OLAP práctica project."""

from .extract_pdf import extract_all
from .load import load
from . import transform as transform_module

# Re-export the transform module so ``from etl import transform`` behaves as expected
transform = transform_module

__all__ = ["extract_all", "transform", "load"]
