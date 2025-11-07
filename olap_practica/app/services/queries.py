"""High level query helpers executed by the Flask routes."""
from __future__ import annotations

import csv
from itertools import islice
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

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
SELECT DISTINCT d.nombre_completo, t.dia_codigo
FROM fact_clase f
JOIN dim_docente d ON f.fk_docente = d.id
JOIN dim_espacio e ON f.fk_espacio = e.id
JOIN dim_tiempo t ON f.fk_tiempo = t.id
WHERE f.periodo = :periodo AND f.plan = :plan
  AND (
        LOWER(e.edificio) = LOWER(:edificio)
        OR (:salon IS NOT NULL AND LOWER(CONCAT(e.edificio, '/', e.salon)) = LOWER(:salon))
      )
  AND f.inicio <= CAST(:hora AS TIME)
  AND f.fin > CAST(:hora AS TIME)
ORDER BY d.nombre_completo, t.dia_codigo;
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

QUERY_LIST_DOCENTES = """
SELECT DISTINCT d.nombre_completo
FROM fact_clase f
JOIN dim_docente d ON f.fk_docente = d.id
WHERE f.periodo = :periodo AND f.plan = :plan
ORDER BY d.nombre_completo;
"""

QUERY_LIST_MATERIAS = """
SELECT DISTINCT a.clave, a.nombre
FROM fact_clase f
JOIN dim_asignatura a ON f.fk_asignatura = a.id
WHERE f.periodo = :periodo AND f.plan = :plan
ORDER BY a.clave, a.nombre;
"""

QUERY_LIST_ESPACIOS = """
SELECT DISTINCT e.edificio, e.salon
FROM fact_clase f
JOIN dim_espacio e ON f.fk_espacio = e.id
WHERE f.periodo = :periodo AND f.plan = :plan
ORDER BY e.edificio, e.salon;
"""

QUERY_LIST_HORAS = """
SELECT DISTINCT DATE_FORMAT(f.inicio, '%H:%i') AS hora
FROM fact_clase f
WHERE f.periodo = :periodo AND f.plan = :plan
ORDER BY hora;
"""

PreviewDefinition = Tuple[str, str, List[str], Callable[[int], Dict[str, Any]]]


def _limit_only(limit: int) -> Dict[str, Any]:
    """Return a dictionary with the provided limit."""

    return {"limit": limit}


def _limit_with_context(limit: int) -> Dict[str, Any]:
    """Return parameters that include periodo y plan para vistas agregadas."""

    return {
        "limit": limit,
        "periodo": SETTINGS.periodo,
        "plan": SETTINGS.plan,
    }


PREVIEW_QUERIES: Dict[str, PreviewDefinition] = {
    "fact_clase": (
        "Vista previa de fact_clase",
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
        _limit_only,
    ),
    "dim_docente": (
        "Vista previa de dim_docente",
        "SELECT id, nombre_completo FROM dim_docente ORDER BY nombre_completo LIMIT :limit",
        ["id", "nombre_completo"],
        _limit_only,
    ),
    "dim_asignatura": (
        "Vista previa de dim_asignatura",
        "SELECT id, clave, nombre, programa FROM dim_asignatura ORDER BY clave LIMIT :limit",
        ["id", "clave", "nombre", "programa"],
        _limit_only,
    ),
    "dim_grupo": (
        "Vista previa de dim_grupo",
        "SELECT id, nrc, seccion, cruzada FROM dim_grupo ORDER BY nrc LIMIT :limit",
        ["id", "nrc", "seccion", "cruzada"],
        _limit_only,
    ),
    "dim_tiempo": (
        "Vista previa de dim_tiempo",
        "SELECT id, dia_codigo, dia_semana FROM dim_tiempo ORDER BY dia_semana LIMIT :limit",
        ["id", "dia_codigo", "dia_semana"],
        _limit_only,
    ),
    "dim_espacio": (
        "Vista previa de dim_espacio",
        "SELECT id, edificio, salon FROM dim_espacio ORDER BY edificio, salon LIMIT :limit",
        ["id", "edificio", "salon"],
        _limit_only,
    ),
    "fact_clase_slot": (
        "Vista previa de fact_clase_slot",
        "SELECT fk_docente, fk_espacio, slot_inicio, slot_fin FROM fact_clase_slot ORDER BY slot_inicio LIMIT :limit",
        ["fk_docente", "fk_espacio", "slot_inicio", "slot_fin"],
        _limit_only,
    ),
    "vista_docentes_por_materia": (
        "Docentes por materia (vista)",
        """
        SELECT a.clave, a.nombre AS materia, COUNT(DISTINCT d.id) AS docentes
        FROM fact_clase f
        JOIN dim_asignatura a ON f.fk_asignatura = a.id
        JOIN dim_docente d ON f.fk_docente = d.id
        WHERE f.periodo = :periodo AND f.plan = :plan
        GROUP BY a.clave, a.nombre
        ORDER BY a.clave
        LIMIT :limit
        """,
        ["clave", "materia", "docentes"],
        _limit_with_context,
    ),
    "vista_ocupacion_edificio": (
        "Ocupación por edificio y día",
        """
        SELECT e.edificio, t.dia_codigo, COUNT(*) AS sesiones
        FROM fact_clase f
        JOIN dim_espacio e ON f.fk_espacio = e.id
        JOIN dim_tiempo t ON f.fk_tiempo = t.id
        WHERE f.periodo = :periodo AND f.plan = :plan
        GROUP BY e.edificio, t.dia_codigo
        ORDER BY e.edificio, t.dia_codigo
        LIMIT :limit
        """,
        ["edificio", "dia_codigo", "sesiones"],
        _limit_with_context,
    ),
    "vista_carga_docente": (
        "Carga de clases por docente",
        """
        SELECT d.nombre_completo, COUNT(*) AS clases, MIN(f.inicio) AS primer_inicio, MAX(f.fin) AS ultimo_fin
        FROM fact_clase f
        JOIN dim_docente d ON f.fk_docente = d.id
        WHERE f.periodo = :periodo AND f.plan = :plan
        GROUP BY d.nombre_completo
        ORDER BY clases DESC, d.nombre_completo
        LIMIT :limit
        """,
        ["nombre_completo", "clases", "primer_inicio", "ultimo_fin"],
        _limit_with_context,
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

    params = {
        "periodo": SETTINGS.periodo,
        "plan": SETTINGS.plan,
        "edificio": edificio_clean,
        "salon": salon_param,
        "hora": hora,
    }
    base_rows = fetch_all(QUERY_DOCENTES_EDIFICIO_BASE, params)

    if usar_slots:
        slot_rows = fetch_all(QUERY_DOCENTES_EDIFICIO_SLOTS, params)
        allowed = {row["nombre_completo"] for row in slot_rows}
        base_rows = [row for row in base_rows if row["nombre_completo"] in allowed]

    formatted_rows: List[Dict[str, str]] = []
    for row in base_rows:
        dia_codigo = row.get("dia_codigo", "")
        dia_info = SETTINGS.day_map.get(dia_codigo)
        dia_nombre = dia_info.nombre if dia_info else row.get("dia", dia_codigo)
        formatted_rows.append(
            {
                "docente": row["nombre_completo"],
                "dia": dia_nombre,
            }
        )

    return formatted_rows


def list_docentes() -> List[str]:
    """Return the full list of docentes disponibles para selección."""

    rows = fetch_all(
        QUERY_LIST_DOCENTES,
        {"periodo": SETTINGS.periodo, "plan": SETTINGS.plan},
    )
    return [row["nombre_completo"] for row in rows]


def list_materias() -> List[Dict[str, str]]:
    """Return distinct course keys and names used in the current periodo/plan."""

    rows = fetch_all(
        QUERY_LIST_MATERIAS,
        {"periodo": SETTINGS.periodo, "plan": SETTINGS.plan},
    )
    return [{"clave": row["clave"], "nombre": row["nombre"]} for row in rows]


def list_espacios() -> Dict[str, List[str]]:
    """Return edificios y salones asociados a clases del periodo/plan."""

    rows = fetch_all(
        QUERY_LIST_ESPACIOS,
        {"periodo": SETTINGS.periodo, "plan": SETTINGS.plan},
    )
    edificios: List[str] = []
    salones: List[str] = []
    seen_edificios: set[str] = set()
    for row in rows:
        edificio = row["edificio"]
        if edificio not in seen_edificios:
            edificios.append(edificio)
            seen_edificios.add(edificio)
        salones.append(f"{edificio}/{row['salon']}")
    return {"edificios": edificios, "salones": salones}


def list_horas_disponibles() -> List[str]:
    """Return sorted class start times (HH:MM) for the active periodo/plan."""

    rows = fetch_all(
        QUERY_LIST_HORAS,
        {"periodo": SETTINGS.periodo, "plan": SETTINGS.plan},
    )
    return [row["hora"] for row in rows]


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
    if normalized in PREVIEW_QUERIES:
        title, sql, headers, param_builder = PREVIEW_QUERIES[normalized]
        try:
            rows = fetch_all(sql, param_builder(limit))
        except ProgrammingError as exc:
            if normalized == "fact_clase_slot":
                raise ValueError(
                    "La vista fact_clase_slot no está disponible. Ejecuta data/dw/views.sql para generarla."
                ) from exc
            raise
        return title, headers, rows

    if normalized in CSV_PREVIEWS:
        path, title = CSV_PREVIEWS[normalized]
        headers, rows = _read_csv_preview(path, limit)
        return title, headers, rows

    raise ValueError("Destino de vista previa no reconocido")
