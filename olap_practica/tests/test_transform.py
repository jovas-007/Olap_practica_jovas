import shutil
from pathlib import Path

import pandas as pd

from etl import transform


def prepare_project(tmp_path) -> Path:
    repo_root = Path(__file__).resolve().parents[2]
    project_src = repo_root / "olap_practica"
    project_dst = tmp_path / "olap_practica"
    shutil.copytree(project_src / "config", project_dst / "config")
    (project_dst / "data" / "staging").mkdir(parents=True)
    return project_dst


def test_transform_splits_hours_and_days(tmp_path, monkeypatch):
    project = prepare_project(tmp_path)
    monkeypatch.chdir(project)
    staging_path = project / "data" / "staging" / "staging.csv"
    df = pd.DataFrame(
        [
            {
                "nrc": "12345",
                "clave": "MAT101",
                "materia": "Cálculo Cruzada",
                "seccion": "01",
                "dias": "AJ",
                "hora": "0900-1059",
                "profesor": "Juan Perez",
                "salon": "1CCO4/307",
                "programa": "ITI",
            }
        ]
    )
    df.to_csv(staging_path, index=False)

    output = transform.transform(str(staging_path))
    result = pd.read_csv(output)

    assert set(result["dia_codigo"]) == {"A", "J"}
    assert result.loc[result["dia_codigo"] == "A", "dia_orden"].iloc[0] == 2
    assert (result["inicio"] == "09:00:00").all()
    assert (result["fin"] == "10:59:00").all()
    assert (result["minutos"] > 0).all()
    assert (result["edificio"] == "1CCO4").all()
    assert (result["aula"].astype(str) == "307").all()
    assert bool(result["cruzada"].iloc[0]) is True


def test_transform_merges_duplicate_programs(tmp_path, monkeypatch):
    project = prepare_project(tmp_path)
    monkeypatch.chdir(project)
    staging_path = project / "data" / "staging" / "staging.csv"
    df = pd.DataFrame(
        [
            {
                "nrc": "99999",
                "clave": "MAT202",
                "materia": "Física II",
                "seccion": "02",
                "dias": "L",
                "hora": "1100-1259",
                "profesor": "Ana Ruiz",
                "salon": "1CCO4/205",
                "programa": "ICC",
            },
            {
                "nrc": "99999",
                "clave": "MAT202",
                "materia": "Física II",
                "seccion": "02",
                "dias": "L",
                "hora": "1100-1259",
                "profesor": "Ana Ruiz",
                "salon": "1CCO4/205",
                "programa": "ITI",
            },
        ]
    )
    df.to_csv(staging_path, index=False)

    output = transform.transform(str(staging_path))
    result = pd.read_csv(output)

    assert len(result) == 1
    assert result.loc[0, "programa"] == "ICC/ITI"
