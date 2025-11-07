"""Load transformed data into the dimensional warehouse."""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Tuple

import pandas as pd
from sqlalchemy import MetaData, Table, and_, delete, insert, select
from sqlalchemy.engine import Engine, Connection

try:  # pragma: no cover - allows script-style execution
    from .utils import (
        AppSettings,
        assert_database_connection,
        get_logger,
        load_settings,
    )
except ImportError:  # pragma: no cover - fallback for ``python etl/load.py``
    import sys

    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from etl.utils import (  # type: ignore  # pylint: disable=import-error
        AppSettings,
        assert_database_connection,
        get_logger,
        load_settings,
    )


class DimensionCache:
    """Helper that caches lookups to dimension IDs."""

    def __init__(self, table: Table, unique_fields: Tuple[str, ...]):
        self.table = table
        self.unique_fields = unique_fields
        self.cache: Dict[Tuple, int] = {}

    def get_or_create(self, conn: Connection, values: Dict) -> int:
        key = tuple(values[field] for field in self.unique_fields)
        if key in self.cache:
            return self.cache[key]
        filters = [self.table.c[field] == values[field] for field in self.unique_fields]
        stmt = select(self.table.c.id).where(and_(*filters))
        existing = conn.execute(stmt).scalar_one_or_none()
        if existing is not None:
            self.cache[key] = existing
            return existing
        result = conn.execute(insert(self.table).values(**values))
        inserted_id = result.inserted_primary_key[0]
        self.cache[key] = inserted_id
        return inserted_id


def _ensure_dim_tiempo(engine: Engine, settings: AppSettings, dim_tiempo: Table) -> None:
    logger = get_logger()
    with engine.begin() as conn:
        for codigo, info in settings.day_map.items():
            stmt = select(dim_tiempo.c.id).where(dim_tiempo.c.dia_codigo == codigo)
            if conn.execute(stmt).scalar_one_or_none() is None:
                conn.execute(
                    insert(dim_tiempo).values(
                        dia_codigo=codigo,
                        dia_semana=info.orden,
                    )
                )
                logger.info("Inserción dim_tiempo: %s", codigo)


def load(fact_csv: str = "data/staging/fact_ready.csv", settings: AppSettings | None = None) -> None:
    """Load the transformed CSV into the data warehouse."""

    logger = get_logger()
    if settings is None:
        settings = load_settings()

    csv_path = Path(fact_csv)
    if not csv_path.exists():
        raise FileNotFoundError(f"No se encontró el archivo {fact_csv}")

    df = pd.read_csv(csv_path)
    if df.empty:
        raise ValueError("El archivo transformado está vacío")

    engine = assert_database_connection(settings)
    metadata = MetaData()
    metadata.reflect(bind=engine)

    tables = {name: metadata.tables[name] for name in (
        "dim_docente",
        "dim_asignatura",
        "dim_grupo",
        "dim_tiempo",
        "dim_espacio",
        "fact_clase",
    )}

    _ensure_dim_tiempo(engine, settings, tables["dim_tiempo"])

    docente_cache = DimensionCache(tables["dim_docente"], ("nombre_completo",))
    asignatura_cache = DimensionCache(
        tables["dim_asignatura"], ("clave", "nombre", "programa")
    )
    grupo_cache = DimensionCache(tables["dim_grupo"], ("nrc", "seccion", "cruzada"))
    espacio_cache = DimensionCache(tables["dim_espacio"], ("edificio", "salon"))

    inserted = 0
    with engine.begin() as conn:
        purge_stmt = (
            delete(tables["fact_clase"])
            .where(tables["fact_clase"].c.periodo == settings.periodo)
            .where(tables["fact_clase"].c.plan == settings.plan)
        )
        deleted = conn.execute(purge_stmt).rowcount or 0
        if deleted:
            logger.info(
                "Filas previas eliminadas en fact_clase para %s/%s: %s",
                settings.periodo,
                settings.plan,
                deleted,
            )
        for record in df.to_dict(orient="records"):
            docente_id = docente_cache.get_or_create(
                conn,
                {"nombre_completo": record["profesor"]},
            )
            asignatura_id = asignatura_cache.get_or_create(
                conn,
                {
                    "clave": record["clave"],
                    "nombre": record["materia"],
                    "programa": record["programa"],
                },
            )
            grupo_id = grupo_cache.get_or_create(
                conn,
                {
                    "nrc": int(record["nrc"]),
                    "seccion": record["seccion"],
                    "cruzada": str(record["cruzada"]).lower() == "true",
                },
            )
            espacio_id = espacio_cache.get_or_create(
                conn,
                {
                    "edificio": record["edificio"],
                    "salon": record["aula"],
                },
            )
            tiempo_stmt = select(tables["dim_tiempo"].c.id).where(
                tables["dim_tiempo"].c.dia_codigo == record["dia_codigo"]
            )
            tiempo_id = conn.execute(tiempo_stmt).scalar_one()
            conn.execute(
                insert(tables["fact_clase"]).values(
                    fk_docente=docente_id,
                    fk_asignatura=asignatura_id,
                    fk_grupo=grupo_id,
                    fk_tiempo=tiempo_id,
                    fk_espacio=espacio_id,
                    periodo=settings.periodo,
                    plan=settings.plan,
                    inicio=record["inicio"],
                    fin=record["fin"],
                    minutos=int(record["minutos"]),
                )
            )
            inserted += 1
    logger.info("Inserciones en fact_clase: %s", inserted)


if __name__ == "__main__":
    load()
