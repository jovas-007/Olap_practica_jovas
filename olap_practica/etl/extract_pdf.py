"""PDF extraction routines for the OLAP práctica project."""
from __future__ import annotations

import csv
import os
import shutil
from pathlib import Path
from typing import Iterable, List

import pdfplumber

try:  # pragma: no cover - executed only when running as a script
    from .utils import (
        collapse_spaces,
        ensure_directory,
        get_logger,
        load_settings,
        safe_title,
        validate_salon,
    )
except ImportError:  # pragma: no cover - fallback for ``python etl/extract_pdf.py``
    import sys

    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from etl.utils import (  # type: ignore  # pylint: disable=import-error
        collapse_spaces,
        ensure_directory,
        get_logger,
        load_settings,
        safe_title,
        validate_salon,
    )

EXPECTED_COLUMNS = [
    "nrc",
    "clave",
    "materia",
    "seccion",
    "dias",
    "hora",
    "profesor",
    "salon",
    "programa",
]


def _detect_program(file_name: str) -> str:
    """Infer the academic program from the PDF filename."""

    file_name = file_name.upper()
    for code in ("ITI", "ICC", "LCC"):
        if f"_{code}.PDF" in file_name:
            return code
    raise ValueError(f"No se pudo inferir el programa desde el archivo {file_name}")


def _move_source_pdfs(base_dir: Path, raw_dir: Path) -> List[Path]:
    """Move candidate PDFs located at the repository root to ``raw_dir``."""

    pattern = "PA_OTOÑO_2025_SEMESTRAL_*.pdf"
    moved_files: List[Path] = []
    for pdf in base_dir.glob(pattern):
        destination = raw_dir / pdf.name
        if destination.exists():
            pdf.unlink()
        else:
            shutil.move(str(pdf), destination)
            moved_files.append(destination)
    return moved_files


def _collect_raw_pdfs(raw_dir: Path) -> List[Path]:
    """Return the list of PDF files ready for extraction."""

    return sorted(raw_dir.glob("*.pdf"))


def _normalise_row(row: Iterable[str]) -> List[str]:
    return [collapse_spaces(cell or "") for cell in row]


def _parse_table_rows(table: Iterable[Iterable[str]], programa: str, logger) -> List[dict]:
    records: List[dict] = []
    for raw_row in table:
        row = _normalise_row(raw_row)
        if not any(row):
            continue
        if row[0].isdigit() and len(row) >= 8:
            nrc, clave, materia, seccion, dias, hora, profesor, salon = row[:8]
            records.append(
                {
                    "nrc": nrc,
                    "clave": clave,
                    "materia": materia,
                    "seccion": seccion,
                    "dias": dias.replace("/", ""),
                    "hora": hora.replace(":", ""),
                    "profesor": safe_title(profesor),
                    "salon": salon,
                    "programa": programa,
                }
            )
        elif not row[0] and len(row) >= 7:
            extra_prof = row[6]
            if records:
                current = records[-1]
                merged = collapse_spaces(f"{current['profesor']} {extra_prof}")
                current["profesor"] = safe_title(merged)
            else:
                logger.error("Fila de profesor sin contexto: %s", row)
        else:
            logger.debug("Fila omitida por formato inesperado: %s", row)
    return records


def extract_all(output_path: str = "data/staging/staging.csv") -> Path:
    """Run the PDF extraction workflow and generate the staging CSV file.

    Parameters
    ----------
    output_path:
        Destination CSV path. Parent directories are created automatically.

    Returns
    -------
    Path
        Path to the generated CSV file.
    """

    settings = load_settings()
    logger = get_logger()
    base_path = os.environ.get("OLAP_BASE_DIR")
    base_dir = Path(base_path) if base_path else Path(__file__).resolve().parents[1]
    repo_root = base_dir.parent

    raw_dir = ensure_directory(base_dir / "data" / "raw")
    ensure_directory(base_dir / "data" / "staging")

    _move_source_pdfs(repo_root, raw_dir)
    pdf_files = _collect_raw_pdfs(raw_dir)
    if not pdf_files:
        logger.error("No se encontraron archivos PDF en %s", raw_dir)
        raise FileNotFoundError("No hay PDFs para procesar")

    records: List[dict] = []
    for pdf_path in pdf_files:
        programa = _detect_program(pdf_path.name)
        logger.info("Procesando %s", pdf_path)
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page_number, page in enumerate(pdf.pages, start=1):
                    try:
                        tables = page.extract_tables() or []
                        for table in tables:
                            rows = _parse_table_rows(table, programa, logger)
                            for row in rows:
                                salon = row.get("salon", "")
                                if salon and not validate_salon(salon, settings.salon_regex):
                                    logger.info(
                                        "Fila omitida por salón inválido %s en %s",
                                        salon,
                                        pdf_path,
                                    )
                                    continue
                                records.append(row)
                    except Exception as exc:  # pragma: no cover - logging path
                        logger.error(
                            "Error al procesar tabla en %s página %s: %s",
                            pdf_path,
                            page_number,
                            exc,
                        )
        except Exception as exc:  # pragma: no cover - logging path
            logger.error("No se pudo abrir %s: %s", pdf_path, exc)

    if not records:
        logger.error("No se generaron registros desde los PDFs")
        raise ValueError("Extracción sin resultados")

    output_file = base_dir / output_path if not output_path.startswith(str(base_dir)) else Path(output_path)
    ensure_directory(output_file.parent)
    with open(output_file, "w", encoding="utf-8", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=EXPECTED_COLUMNS)
        writer.writeheader()
        for record in records:
            writer.writerow({col: record.get(col, "") for col in EXPECTED_COLUMNS})

    logger.info("Archivo de staging generado: %s (%s filas)", output_file, len(records))
    return output_file


if __name__ == "__main__":
    extract_all()
