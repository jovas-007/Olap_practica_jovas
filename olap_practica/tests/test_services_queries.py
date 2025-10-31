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
    assert captured["params"]["periodo"]
    assert captured["params"]["plan"]


def test_docentes_por_materia_uses_text_when_no_clave(monkeypatch):
    captured = {}

    def fake_fetch_all(query, params):
        captured["params"] = params
        return []

    monkeypatch.setattr("app.services.queries.fetch_all", fake_fetch_all)
    queries.docentes_por_materia(texto="  bases de datos  ")
    assert captured["params"]["clave"] is None
    assert captured["params"]["texto"] == "bases de datos"
    assert captured["params"]["periodo"]
    assert captured["params"]["plan"]


def test_docentes_en_edificio_normalizes_inputs(monkeypatch):
    captured = {}

    def fake_fetch_all(query, params):
        captured["params"] = params
        return []

    monkeypatch.setattr("app.services.queries.fetch_all", fake_fetch_all)
    queries.docentes_en_edificio_a_hora(" 1CCO4 ", "0900")
    assert captured["params"]["edificio"] == "1CCO4"
    assert captured["params"]["hora"] == "09:00"
    assert captured["params"]["salon"] is None
    assert captured["params"]["periodo"]
    assert captured["params"]["plan"]


def test_docentes_en_edificio_accepts_salon(monkeypatch):
    captured = {}

    def fake_fetch_all(query, params):
        captured["params"] = params
        return []

    monkeypatch.setattr("app.services.queries.fetch_all", fake_fetch_all)
    queries.docentes_en_edificio_a_hora("1CCO4/307", "0930")
    assert captured["params"]["edificio"] == "1CCO4"
    assert captured["params"]["salon"] == "1CCO4/307"
    assert captured["params"]["hora"] == "09:30"


def test_preview_dataset_for_table(monkeypatch, tmp_path):
    expected_rows = [{"id": 1, "nombre_completo": "Ana", "clave": "MAT101"}]

    def fake_fetch_all(query, params):
        assert "LIMIT" in query.upper()
        assert params["limit"] == 100
        return expected_rows

    monkeypatch.setattr("app.services.queries.fetch_all", fake_fetch_all)
    title, headers, rows = queries.preview_dataset("fact_clase")
    assert "fact_clase" in title
    assert "nombre_completo" in headers
    assert rows == expected_rows


def test_preview_dataset_reads_csv(monkeypatch, tmp_path):
    csv_path = tmp_path / "fact_ready.csv"
    csv_path.write_text("col1,col2\nA,B\n", encoding="utf-8")
    monkeypatch.setattr("app.services.queries.CSV_PREVIEWS", {"fact_ready": (csv_path, "Test")})
    title, headers, rows = queries.preview_dataset("fact_ready")
    assert title == "Test"
    assert headers == ["col1", "col2"]
    assert rows[0]["col1"] == "A"


def test_preview_dataset_missing_target():
    with pytest.raises(ValueError):
        queries.preview_dataset("desconocido")
