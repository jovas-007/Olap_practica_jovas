"""Flask routes for the OLAP práctica web interface."""
from __future__ import annotations

from flask import Blueprint, flash, redirect, render_template, request, url_for

from etl.utils import get_logger, load_settings

from .services import queries

bp = Blueprint("web", __name__)
settings = load_settings()
LOGGER = get_logger()


@bp.get("/")
def index():
    """Render the main form allowing the user to choose a query."""

    return render_template("index.html", settings=settings)


@bp.post("/run")
def run_query():
    """Dispatch the selected operation and render the results table."""

    operation = request.form.get("operation")
    LOGGER.info("Operación recibida: %s", operation)
    try:
        if operation == "horario_docente":
            docente = request.form.get("docente", "")
            if not docente.strip():
                raise ValueError("Debe indicar un nombre de docente")
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
            title = f"Horario semanal para {docente}"
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
            headers = ["nombre_completo"]
            title = f"Docentes en {edificio} a las {hora[:2]}:{hora[2:]}"
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

    return render_template("results.html", headers=headers, rows=data, title=title)


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
        back_text="Volver al inicio",
    )
