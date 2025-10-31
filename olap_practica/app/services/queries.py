"""High level query helpers executed by the Flask routes."""
from __future__ import annotations

import csv
from itertools import islice
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.exc import ProgrammingError

from etl.utils import load_settings

from .db import fetch_all

SETTINGS = load_settings()

QUERY_HORARIO_DOCENTE = """
SELECT d.nombre_completo, t.dia_codigo, f.inicio, f.fin, a.clave, a.nombre AS materia, e.edificio, e.salon
FROM fact_clase f
JOIN dim_docente d ON f.fk_docente = d.id
JOIN dim_asignatura a ON f.fk_asignatura = a.id
JOIN dim_tiempo t ON f.fk_tiempo = t.id
JOIN dim_espacio e ON f.fk_espacio = e.id
WHERE f.periodo = :periodo AND f.plan = :plan AND LOWER(d.nombre_completo) LIKE LOWER(:docente)
ORDER BY d.nombre_completo, t.dia_codigo, f.inicio;
"""

QUERY_DOCENTES_MATERIA = """
SELECT DISTINCT d.nombre_completo
FROM fact_clase f
JOIN dim_docente d ON f.fk_docente = d.id
JOIN dim_asignatura a ON f.fk_asignatura = a.id
WHERE f.periodo = :periodo AND f.plan = :plan
  AND (
        (:clave IS NOT NULL AND UPPER(a.clave) LIKE CONCAT(UPPER(:clave), '%'))
        OR (:clave IS NULL AND :texto IS NOT NULL AND LOWER(a.nombre) LIKE CONCAT('%', LOWER(:texto), '%'))
      );
"""

QUERY_DOCENTES_EDIFICIO_BASE = """
SELECT DISTINCT d.nombre_completo
FROM fact_clase f
JOIN dim_docente d ON f.fk_docente = d.id
JOIN dim_espacio e ON f.fk_espacio = e.id
WHERE f.periodo = :periodo AND f.plan = :plan
  AND (
        LOWER(e.edificio) = LOWER(:edificio)
        OR (:salon IS NOT NULL AND LOWER(CONCAT(e.edificio, '/', e.salon)) = LOWER(:salon))
      )
  AND f.inicio <= CAST(:hora AS TIME)
  AND f.fin > CAST(:hora AS TIME);
"""

QUERY_DOCENTES_EDIFICIO_SLOTS = """
SELECT DISTINCT d.nombre_completo
FROM fact_clase_slot s
JOIN dim_docente d ON s.fk_docente = d.id
JOIN dim_espacio e ON s.fk_espacio = e.id
WHERE LOWER(e.edificio) = LOWER(:edificio)
  AND (:salon IS NULL OR LOWER(CONCAT(e.edificio, '/', e.salon)) = LOWER(:salon))
  AND s.slot_inicio <= CAST(:hora AS TIME)
  AND s.slot_fin > CAST(:hora AS TIME);
"""

PREVIEW_TABLE_QUERIES: Dict[str, Tuple[str, List[str]]] = {
    "fact_clase": (
        """
        SELECT f.id, d.nombre_completo, a.clave, t.dia_codigo, f.inicio, f.fin, e.edificio, e.salon, f.periodo, f.plan
        FROM fact_clase f
        JOIN dim_docente d ON f.fk_docente = d.id
        JOIN dim_asignatura a ON f.fk_asignatura = a.id
        JOIN dim_tiempo t ON f.fk_tiempo = t.id
        JOIN dim_espacio e ON f.fk_espacio = e.id
        ORDER BY f.id
        LIMIT :limit
        """,
        [
            "id",
            "nombre_completo",
            "clave",
            "dia_codigo",
            "inicio",
            "fin",
            "edificio",
            "salon",
            "periodo",
            "plan",
        ],
    ),
    "dim_docente": ("SELECT id, nombre_completo FROM dim_docente ORDER BY nombre_completo LIMIT :limit", ["id", "nombre_completo"]),
    "dim_asignatura": (
        "SELECT id, clave, nombre, programa FROM dim_asignatura ORDER BY clave LIMIT :limit",
        ["id", "clave", "nombre", "programa"],
    ),
    "dim_grupo": (
        "SELECT id, nrc, seccion, cruzada FROM dim_grupo ORDER BY nrc LIMIT :limit",
        ["id", "nrc", "seccion", "cruzada"],
    ),
    "dim_tiempo": (
        "SELECT id, dia_codigo, dia_semana FROM dim_tiempo ORDER BY dia_semana LIMIT :limit",
        ["id", "dia_codigo", "dia_semana"],
    ),
    "dim_espacio": (
        "SELECT id, edificio, salon FROM dim_espacio ORDER BY edificio, salon LIMIT :limit",
        ["id", "edificio", "salon"],
    ),
    "fact_clase_slot": (
        "SELECT fk_docente, fk_espacio, slot_inicio, slot_fin FROM fact_clase_slot ORDER BY slot_inicio LIMIT :limit",
        ["fk_docente", "fk_espacio", "slot_inicio", "slot_fin"],
    ),
}

CSV_PREVIEWS: Dict[str, Tuple[Path, str]] = {
    "staging": (Path("data/staging/staging.csv"), "Vista previa staging.csv"),
    "fact_ready": (Path("data/staging/fact_ready.csv"), "Vista previa fact_ready.csv"),
}


def horario_docente(nombre_o_patron: str) -> List[Dict[str, Any]]:
    """Return the weekly timetable for a given teacher name pattern."""

    pattern = f"%{nombre_o_patron.strip()}%"
    return fetch_all(
        QUERY_HORARIO_DOCENTE,
        {
            "periodo": SETTINGS.periodo,
            "plan": SETTINGS.plan,
            "docente": pattern,
        },
    )


def docentes_por_materia(clave: Optional[str] = None, texto: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return distinct teachers matching a course key or name text."""

    clave_param = clave.strip().upper() if clave and clave.strip() else None
    texto_param = texto.strip() if texto and texto.strip() else None
    if clave_param:
        texto_param = None
    if not clave_param and not texto_param:
        raise ValueError("Debe proporcionar una clave o un texto")
    params = {
        "periodo": SETTINGS.periodo,
        "plan": SETTINGS.plan,
        "clave": clave_param,
        "texto": texto_param,
    }
    return fetch_all(QUERY_DOCENTES_MATERIA, params)


def docentes_en_edificio_a_hora(edificio: str, hora_hhmm: str, usar_slots: bool = False) -> List[Dict[str, Any]]:
    """Return teachers teaching inside a building at a given time."""

    hora = hora_hhmm if ":" in hora_hhmm else f"{hora_hhmm[:2]}:{hora_hhmm[2:]}"
    edificio_clean = edificio.strip()
    salon_param: Optional[str] = None
    if "/" in edificio_clean:
        edificio_part, salon_part = edificio_clean.split("/", 1)
        edificio_clean = edificio_part.strip()
        salon_param = f"{edificio_clean}/{salon_part.strip()}"

    if not edificio_clean:
        raise ValueError("Debe indicar un edificio")

    query = QUERY_DOCENTES_EDIFICIO_SLOTS if usar_slots else QUERY_DOCENTES_EDIFICIO_BASE
    params = {
        "periodo": SETTINGS.periodo,
        "plan": SETTINGS.plan,
        "edificio": edificio_clean,
        "salon": salon_param,
        "hora": hora,
    }
    return fetch_all(query, params)


def _read_csv_preview(path: Path, limit: int = 100) -> Tuple[List[str], List[Dict[str, Any]]]:
    if not path.exists():
        raise FileNotFoundError(f"No se encontró el archivo {path}")
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"El archivo {path} no contiene encabezados")
        rows = list(islice(reader, limit))
        return reader.fieldnames, rows


def preview_dataset(target: str, limit: int = 100) -> Tuple[str, List[str], List[Dict[str, Any]]]:
    """Return headers and rows for a dataset preview requested from the UI."""

    normalized = target.lower()
    if normalized in PREVIEW_TABLE_QUERIES:
        sql, headers = PREVIEW_TABLE_QUERIES[normalized]
        try:
            rows = fetch_all(sql, {"limit": limit})
        except ProgrammingError as exc:
            if normalized == "fact_clase_slot":
                raise ValueError(
                    "La vista fact_clase_slot no está disponible. Ejecuta data/dw/views.sql para generarla."
                ) from exc
            raise
        return f"Vista previa de {normalized}", headers, rows

    if normalized in CSV_PREVIEWS:
        path, title = CSV_PREVIEWS[normalized]
        headers, rows = _read_csv_preview(path, limit)
        return title, headers, rows

    raise ValueError("Destino de vista previa no reconocido")
