from pathlib import Path

import pytest

from app.main import create_app


@pytest.fixture
def app_client(monkeypatch):
    project_root = Path(__file__).resolve().parents[1]
    monkeypatch.chdir(project_root)

    app = create_app()
    app.config.update(TESTING=True, SECRET_KEY="testing")

    with app.test_client() as client:
        yield client


def test_index_page_loads(app_client):
    response = app_client.get("/")
    assert response.status_code == 200
    assert b"Consultas sobre horarios" in response.data


def test_run_horario_docente(monkeypatch, app_client):
    fake_rows = [
        {
            "nombre_completo": "Ana López",
            "dia_codigo": "L",
            "inicio": "09:00:00",
            "fin": "10:00:00",
            "clave": "MAT101",
            "materia": "Cálculo",
            "edificio": "1CCO4",
            "salon": "307",
        }
    ]
    monkeypatch.setattr("app.routes.queries.horario_docente", lambda _: fake_rows)
    response = app_client.post(
        "/run",
        data={"operation": "horario_docente", "docente": "Ana"},
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert b"Horario semanal" in response.data


def test_run_docentes_por_materia(monkeypatch, app_client):
    monkeypatch.setattr(
        "app.routes.queries.docentes_por_materia",
        lambda **_: [{"nombre_completo": "Luis Perez"}],
    )
    response = app_client.post(
        "/run",
        data={"operation": "docentes_por_materia", "texto": "Calculo"},
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert b"Luis Perez" in response.data


def test_run_docentes_en_edificio(monkeypatch, app_client):
    monkeypatch.setattr(
        "app.routes.queries.docentes_en_edificio_a_hora",
        lambda *args, **kwargs: [{"nombre_completo": "Maria"}],
    )
    response = app_client.post(
        "/run",
        data={
            "operation": "docentes_en_edificio",
            "edificio": "1CCO4",
            "hora": "09:00",
        },
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert b"Maria" in response.data
