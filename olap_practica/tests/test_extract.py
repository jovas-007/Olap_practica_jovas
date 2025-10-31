import shutil
from pathlib import Path

import pandas as pd
import pytest

from etl import extract_pdf


class DummyPage:
    def __init__(self, tables):
        self._tables = tables

    def extract_tables(self):
        return self._tables


class DummyPDF:
    def __init__(self, tables):
        self.pages = [DummyPage(tables)]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


@pytest.fixture
def project_copy(tmp_path):
    repo_root = Path(__file__).resolve().parents[2]
    project_src = repo_root / "olap_practica"
    project_dst = tmp_path / "olap_practica"
    shutil.copytree(project_src / "config", project_dst / "config")
    (project_dst / "data" / "raw").mkdir(parents=True)
    (project_dst / "data" / "staging").mkdir(parents=True)
    return project_dst


def test_extract_all_creates_staging_csv(monkeypatch, project_copy):
    dummy_table = [
        ["12345", "MAT101", "CÁLCULO DIFERENCIAL", "01", "LMV", "0900-1059", "JUAN PEREZ", "1CCO4/307"],
        ["", "", "", "", "", "", "GARCIA LOPEZ", ""],
    ]

    def fake_open(_path):
        return DummyPDF([dummy_table])

    monkeypatch.setenv("OLAP_BASE_DIR", str(project_copy))
    monkeypatch.setattr(extract_pdf, "pdfplumber", type("Stub", (), {"open": staticmethod(fake_open)}))
    monkeypatch.chdir(project_copy)

    pdf_source = project_copy.parent / "PA_OTOÑO_2025_SEMESTRAL_ITI.pdf"
    pdf_source.write_bytes(b"PDF")

    output_path = extract_pdf.extract_all()
    assert output_path.exists()

    df = pd.read_csv(output_path)
    assert list(df.columns) == extract_pdf.EXPECTED_COLUMNS
    assert "Juan Perez Garcia Lopez" in df["profesor"].iloc[0]
    assert "  " not in df["profesor"].iloc[0]

    pdf_dest = project_copy / "data" / "raw" / pdf_source.name
    assert pdf_dest.exists()
    assert not pdf_source.exists()
