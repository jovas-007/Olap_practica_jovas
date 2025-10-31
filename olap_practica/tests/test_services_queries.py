import pytest

from app.services import queries


def test_docentes_por_materia_prefers_clave(monkeypatch):
    captured = {}

    def fake_fetch_all(query, params):
        captured["query"] = query
        captured["params"] = params
        return []

    monkeypatch.setattr("app.services.queries.fetch_all", fake_fetch_all)
    queries.docentes_por_materia(clave=" mat101 ", texto="Calculo")
    assert captured["params"]["clave"] == "MAT101"
    assert captured["params"]["texto"] is None


def test_docentes_por_materia_uses_text_when_no_clave(monkeypatch):
    captured = {}

    def fake_fetch_all(query, params):
        captured["params"] = params
        return []

    monkeypatch.setattr("app.services.queries.fetch_all", fake_fetch_all)
    queries.docentes_por_materia(texto="  bases de datos  ")
    assert captured["params"]["clave"] is None
    assert captured["params"]["texto"] == "bases de datos"


def test_docentes_en_edificio_normalizes_inputs(monkeypatch):
    captured = {}

    def fake_fetch_all(query, params):
        captured["params"] = params
        return []

    monkeypatch.setattr("app.services.queries.fetch_all", fake_fetch_all)
    queries.docentes_en_edificio_a_hora(" 1CCO4 ", "0900")
    assert captured["params"]["edificio"] == "1CCO4"
    assert captured["params"]["hora"] == "09:00"
