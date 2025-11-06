"""Service layer for database access and domain queries."""

from .db import fetch_all, get_engine
from .queries import (
    docentes_en_edificio_a_hora,
    docentes_por_materia,
    horario_docente,
    preview_dataset,
)

__all__ = [
    "fetch_all",
    "get_engine",
    "docentes_en_edificio_a_hora",
    "docentes_por_materia",
    "horario_docente",
    "preview_dataset",
]
