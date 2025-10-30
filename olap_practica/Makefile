PYTHON?=python3
PIP?=pip3
DATABASE_URL?=$(shell python3 -c "import yaml; from pathlib import Path; \
    data=yaml.safe_load(Path('config/settings.yaml').read_text(encoding='utf-8')); \
    print(data['db']['url'])" 2>/dev/null)

PSQL_URL?=$(shell python3 -c "import os, yaml; from pathlib import Path; \
    data=yaml.safe_load(Path('config/settings.yaml').read_text(encoding='utf-8')); \
    url=os.environ.get('DATABASE_URL', data['db']['url']); \
    print(url.replace('postgresql+psycopg2', 'postgresql'))" 2>/dev/null)

export DATABASE_URL

install:
	$(PIP) install -r requirements.txt
extract:
	$(PYTHON) etl/extract_pdf.py
transform:
	$(PYTHON) -c "from etl.transform import transform; import yaml; s=yaml.safe_load(open('config/settings.yaml')); transform('data/staging/staging.csv', s)"
ddl:
	psql $$PSQL_URL -f data/dw/schema_postgres.sql
indexes:
	psql $$PSQL_URL -f data/dw/indexes.sql
views:
	psql $$PSQL_URL -f data/dw/views.sql
load:
	$(PYTHON) etl/load.py
etl: extract transform ddl indexes load
web:
	FLASK_APP=app/main.py flask run --host=0.0.0.0 --port=8080
all: install extract transform ddl indexes load
.PHONY: install extract transform ddl indexes views load etl web all
