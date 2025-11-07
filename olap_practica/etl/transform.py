"""Transformation routines to normalise staging data before loading."""
from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Dict

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
    """Transform the staging CSV into a fact-ready dataset.

    Parameters
    ----------
    staging_csv:
        Path to the CSV created by :mod:`etl.extract_pdf`.
    settings:
        Optional settings object or dictionary. When ``None`` it is loaded from
        ``config/settings.yaml``.

    Returns
    -------
    Path
        Path to the generated ``fact_ready.csv``.
    """

    logger = get_logger()
    if settings is None:
        settings = load_settings()
    elif isinstance(settings, dict):
        settings = AppSettings.parse_obj(settings)

    df = pd.read_csv(staging_csv, dtype=str).fillna("")
    if df.empty:
        raise ValueError("El archivo de staging está vacío")

    aggregated: Dict[
        tuple[int, str, str, str, str, str, str, str, int, str, str, int, bool],
        Dict[str, object],
    ] = {}
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
            programa = collapse_spaces(str(row["programa"]).upper())
            materia = collapse_spaces(row["materia"])
            profesor = collapse_spaces(row["profesor"])
            clave = _normalise_clave(row["clave"])
            seccion = _normalise_seccion(row["seccion"])
            inicio_str = start_time.strftime("%H:%M:%S")
            fin_str = end_time.strftime("%H:%M:%S")
            edificio_norm = collapse_spaces(edificio)
            aula_norm = collapse_spaces(aula)
            for dia in dias:
                info = settings.day_map.get(dia)
                if not info:
                    logger.error("Código de día desconocido %s", dia)
                    continue
                key = (
                    int(row["nrc"]),
                    clave,
                    materia,
                    seccion,
                    profesor,
                    edificio_norm,
                    aula_norm,
                    dia,
                    info.orden,
                    inicio_str,
                    fin_str,
                    minutos,
                    cruzada,
                )
                entry = aggregated.get(key)
                if entry is None:
                    aggregated[key] = {
                        "nrc": int(row["nrc"]),
                        "clave": clave,
                        "materia": materia,
                        "seccion": seccion,
                        "profesor": profesor,
                        "edificio": edificio_norm,
                        "aula": aula_norm,
                        "dia_codigo": dia,
                        "dia_orden": info.orden,
                        "inicio": inicio_str,
                        "fin": fin_str,
                        "minutos": minutos,
                        "cruzada": cruzada,
                        "_programas": {programa},
                    }
                else:
                    entry["_programas"].add(programa)  # type: ignore[index]
        except Exception as exc:
            logger.error("Error al transformar fila %s: %s", row.to_dict(), exc)
    if not aggregated:
        raise ValueError("No se generaron registros transformados")

    output_path = Path(staging_csv).parent / "fact_ready.csv"
    records = []
    for record in aggregated.values():
        programas = sorted(str(p) for p in record.pop("_programas"))
        record["programa"] = "/".join(programas)
        records.append(record)

    output_df = pd.DataFrame(records, columns=OUTPUT_COLUMNS)
    output_df.to_csv(output_path, index=False)
    logger.info("Archivo fact_ready.csv generado en %s", output_path)
    return output_path


if __name__ == "__main__":
    transform("data/staging/staging.csv")
