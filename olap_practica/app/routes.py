"""Flask routes for the OLAP práctica web interface."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List

from flask import Blueprint, flash, redirect, render_template, request, url_for

from etl.utils import get_logger, load_settings

from .services import queries

bp = Blueprint("web", __name__)
settings = load_settings()
LOGGER = get_logger()


@bp.get("/")
def index():
    """Render the main form allowing the user to choose a query."""

    docentes = queries.list_docentes()
    materias = queries.list_materias()
    espacios = queries.list_espacios()
    horas = queries.list_horas_disponibles()
    return render_template(
        "index.html",
        settings=settings,
        docentes=docentes,
        materias=materias,
        espacios=espacios,
        horas=horas,
    )


@bp.post("/run")
def run_query():
    """Dispatch the selected operation and render the results table."""

    operation = request.form.get("operation")
    LOGGER.info("Operación recibida: %s", operation)
    try:
        if operation == "horario_docente":
            docente = request.form.get("docente", "")
            if not docente.strip():
                raise ValueError("Debe seleccionar un docente")
            data = queries.horario_docente(docente)
            LOGGER.info("Consulta horario_docente para patrón: %s", docente)
            headers = [
                "nombre_completo",
                "dia_codigo",
                "inicio",
                "fin",
                "clave",
                "materia",
                "edificio",
                "salon",
            ]
            display_name = data[0]["nombre_completo"] if data else docente
            title = f"Horario semanal de {display_name}"
            schedule_days, schedule_rows = _build_schedule_view(data)
        elif operation == "docentes_por_materia":
            clave = request.form.get("clave") or None
            texto = request.form.get("texto") or None
            data = queries.docentes_por_materia(clave=clave, texto=texto)
            LOGGER.info(
                "Consulta docentes_por_materia - clave: %s texto: %s",
                clave,
                texto,
            )
            headers = ["nombre_completo"]
            title = "Docentes para la materia seleccionada"
            schedule_days = []
            schedule_rows = []
        elif operation == "docentes_en_edificio":
            edificio = request.form.get("edificio", "").strip()
            hora = request.form.get("hora", "").replace(":", "")
            usar_slots = request.form.get("usar_slots") == "on"
            if not edificio:
                raise ValueError("Debe indicar un edificio")
            if len(hora) != 4:
                raise ValueError("La hora debe tener formato HH:MM")
            data = queries.docentes_en_edificio_a_hora(edificio, hora, usar_slots)
            LOGGER.info(
                "Consulta docentes_en_edificio - edificio: %s hora: %s slots: %s",
                edificio,
                hora,
                usar_slots,
            )
            headers = ["docente", "dia"]
            title = f"Docentes en {edificio} a las {hora[:2]}:{hora[2:]}"
            schedule_days = []
            schedule_rows = []
        else:
            raise ValueError("Operación no válida")
    except Exception as exc:
        flash(str(exc), "error")
        LOGGER.error("Error al ejecutar operación %s: %s", operation, exc)
        return redirect(url_for("web.index"))

    if not data:
        flash("Sin resultados para los parámetros proporcionados", "warning")
        LOGGER.warning("Operación %s sin resultados", operation)
        return redirect(url_for("web.index"))

    return render_template(
        "results.html",
        headers=headers,
        rows=data,
        title=title,
        schedule_days=schedule_days,
        schedule_rows=schedule_rows,
    )


@bp.get("/preview/<target>")
def preview(target: str):
    """Display a dataset preview such as fact tables, dimensions or staging files."""

    try:
        title, headers, rows = queries.preview_dataset(target)
    except Exception as exc:  # pragma: no cover - handled via flash in UI
        flash(str(exc), "error")
        return redirect(url_for("web.index"))

    if not rows:
        flash("No hay registros para mostrar en la vista previa", "warning")
        return redirect(url_for("web.index"))

    return render_template(
        "results.html",
        headers=headers,
        rows=rows,
        title=title,
        schedule_days=[],
        schedule_rows=[],
        back_text="Volver al inicio",
    )


def _build_schedule_view(data: List[Dict[str, Any]]) -> tuple[List[Dict[str, str]], List[Dict[str, Any]]]:
    """Return metadata and rows to render a horario estilo calendario."""

    if not data:
        return [], []

    ordered_days = sorted(
        settings.day_map.items(),
        key=lambda item: item[1].orden,
    )
    schedule_days = [
        {"codigo": codigo, "nombre": info.nombre}
        for codigo, info in ordered_days
    ]

    def _parse_time(value: str) -> datetime:
        fmt = "%H:%M:%S" if len(value) > 5 else "%H:%M"
        return datetime.strptime(value, fmt)

    events_by_day: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    earliest: datetime | None = None
    latest: datetime | None = None

    for row in data:
        inicio_dt = _parse_time(row["inicio"])
        fin_dt = _parse_time(row["fin"])
        if fin_dt <= inicio_dt:
            fin_dt = inicio_dt + timedelta(minutes=row.get("minutos", 60))

        event = {
            "materia": row["materia"],
            "clave": row["clave"],
            "espacio": f"{row['edificio']}/{row['salon']}",
            "inicio": inicio_dt,
            "fin": fin_dt,
        }
        events_by_day[row["dia_codigo"]].append(event)

        earliest = inicio_dt if earliest is None else min(earliest, inicio_dt)
        latest = fin_dt if latest is None else max(latest, fin_dt)

    if earliest is None or latest is None:
        return schedule_days, []

    # Ajustar a bloques de 60 minutos
    start_cursor = earliest.replace(minute=0, second=0)
    end_cursor = latest.replace(minute=0, second=0)
    if latest.minute or latest.second:
        end_cursor += timedelta(hours=1)

    schedule_rows: List[Dict[str, Any]] = []
    current = start_cursor
    while current < end_cursor:
        slot_end = current + timedelta(hours=1)
        label = f"{current.strftime('%H:%M')} - {(slot_end - timedelta(minutes=1)).strftime('%H:%M')}"
        dia_map: Dict[str, List[Dict[str, str]]] = {}
        for day in schedule_days:
            codigo = day["codigo"]
            day_events = events_by_day.get(codigo, [])
            cell_events: List[Dict[str, str]] = []
            for event in day_events:
                if event["fin"] <= current or event["inicio"] >= slot_end:
                    continue
                cell_events.append(
                    {
                        "materia": event["materia"],
                        "clave": event["clave"],
                        "espacio": event["espacio"],
                        "inicio": event["inicio"].strftime("%H:%M"),
                        "fin": event["fin"].strftime("%H:%M"),
                    }
                )
            dia_map[codigo] = cell_events
        schedule_rows.append({"hora": label, "dias": dia_map})
        current = slot_end

    return schedule_days, schedule_rows
