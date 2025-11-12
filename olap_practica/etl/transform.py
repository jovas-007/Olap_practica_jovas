"""Transformation routines to normalise staging data before loading."""
from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Dict, Iterable

import pandas as pd

try:  # pragma: no cover - allows ``python etl/transform.py``
    from .utils import AppSettings, collapse_spaces, get_logger, load_settings
except ImportError:  # pragma: no cover - fallback when executed as a script
    import sys

    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from etl.utils import (  # type: ignore  # pylint: disable=import-error
        AppSettings,
        collapse_spaces,
        get_logger,
        load_settings,
    )

OUTPUT_COLUMNS = [
    "nrc",
    "clave",
    "materia",
    "seccion",
    "profesor",
    "programa",
    "edificio",
    "aula",
    "dia_codigo",
    "dia_orden",
    "inicio",
    "fin",
    "minutos",
    "cruzada",
]


def _to_time(value: str) -> dt.time:
    value = value.strip()
    return dt.datetime.strptime(value, "%H%M").time()


def _split_hour_range(value: str) -> tuple[dt.time, dt.time]:
    cleaned = value.replace(" ", "")
    start, end = cleaned.split("-")
    return _to_time(start), _to_time(end)


def _normalise_seccion(value: str) -> str:
    return collapse_spaces(value.upper())


def _normalise_clave(value: str) -> str:
    return collapse_spaces(value.upper())


def transform(staging_csv: str, settings: Dict | AppSettings | None = None) -> Path:

    logger = get_logger()
    if settings is None:
        settings = load_settings()
    elif isinstance(settings, dict):
        settings = AppSettings.parse_obj(settings)

    df = pd.read_csv(staging_csv, dtype=str).fillna("")
    if df.empty:
        raise ValueError("El archivo de staging está vacío")

    records = []
    for _, row in df.iterrows():
        try:
            start_time, end_time = _split_hour_range(row["hora"])
            minutos = int(
                (dt.datetime.combine(dt.date.today(), end_time)
                - dt.datetime.combine(dt.date.today(), start_time)).total_seconds()
                // 60
            )
            dias = [char for char in row["dias"] if char]
            if not dias:
                dias = ["?"]
            cruzada = "CRUZADA" in row["materia"].upper()
            edificio, aula = (row["salon"].split("/") + [""])[:2]
            for dia in dias:
                info = settings.day_map.get(dia)
                if not info:
                    logger.error("Código de día desconocido %s", dia)
                    continue
                records.append(
                    {
                        "nrc": int(row["nrc"]),
                        "clave": _normalise_clave(row["clave"]),
                        "materia": collapse_spaces(row["materia"]),
                        "seccion": _normalise_seccion(row["seccion"]),
                        "profesor": collapse_spaces(row["profesor"]),
                        "programa": row["programa"],
                        "edificio": collapse_spaces(edificio),
                        "aula": collapse_spaces(aula),
                        "dia_codigo": dia,
                        "dia_orden": info.orden,
                        "inicio": start_time.strftime("%H:%M:%S"),
                        "fin": end_time.strftime("%H:%M:%S"),
                        "minutos": minutos,
                        "cruzada": cruzada,
                    }
                )
        except Exception as exc:
            logger.error("Error al transformar fila %s: %s", row.to_dict(), exc)
    if not records:
        raise ValueError("No se generaron registros transformados")

    output_path = Path(staging_csv).parent / "fact_ready.csv"
    output_df = pd.DataFrame(records, columns=OUTPUT_COLUMNS)
    output_df.to_csv(output_path, index=False)
    logger.info("Archivo fact_ready.csv generado en %s", output_path)
    return output_path


if __name__ == "__main__":
    transform("data/staging/staging.csv")
