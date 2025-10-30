"""High level query helpers executed by the Flask routes."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

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
WHERE (:clave IS NOT NULL AND a.clave = :clave)
   OR (:texto IS NOT NULL AND LOWER(a.nombre) LIKE LOWER(CONCAT('%', :texto, '%')));
"""

QUERY_DOCENTES_EDIFICIO = """
SELECT DISTINCT d.nombre_completo
FROM fact_clase f
JOIN dim_docente d ON f.fk_docente = d.id
JOIN dim_espacio e ON f.fk_espacio = e.id
WHERE e.edificio = :edificio AND f.inicio <= CAST(:hora AS TIME) AND f.fin > CAST(:hora AS TIME);
"""

QUERY_DOCENTES_EDIFICIO_SLOTS = """
SELECT DISTINCT d.nombre_completo
FROM fact_clase_slot s
JOIN dim_docente d ON s.fk_docente = d.id
JOIN dim_espacio e ON s.fk_espacio = e.id
WHERE e.edificio = :edificio AND s.slot_inicio <= CAST(:hora AS TIME) AND s.slot_fin > CAST(:hora AS TIME);
"""


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

    if not clave and not texto:
        raise ValueError("Debe proporcionar una clave o un texto")
    params = {
        "clave": clave,
        "texto": texto,
    }
    return fetch_all(QUERY_DOCENTES_MATERIA, params)


def docentes_en_edificio_a_hora(edificio: str, hora_hhmm: str, usar_slots: bool = False) -> List[Dict[str, Any]]:
    """Return teachers teaching inside a building at a given time."""

    hora = hora_hhmm if ":" in hora_hhmm else f"{hora_hhmm[:2]}:{hora_hhmm[2:]}"
    query = QUERY_DOCENTES_EDIFICIO_SLOTS if usar_slots else QUERY_DOCENTES_EDIFICIO
    return fetch_all(query, {"edificio": edificio, "hora": hora})
