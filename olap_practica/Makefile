PYTHON?=python
PIP?=pip
DATABASE_URL?=$(shell $(PYTHON) -c "import yaml; from pathlib import Path; \
    data=yaml.safe_load(Path('config/settings.yaml').read_text(encoding='utf-8')); \
    print(data['db']['url'])" 2>/dev/null)

export DATABASE_URL

install:
	$(PIP) install -r requirements.txt

extract:
	$(PYTHON) etl/extract_pdf.py

transform:
	$(PYTHON) -c "from etl.transform import transform; import yaml; s=yaml.safe_load(open('config/settings.yaml')); transform('data/staging/staging.csv', s)"

ddl:
	$(PYTHON) -c "from etl.utils import execute_sql_file, load_settings; execute_sql_file('data/dw/schema_mysql.sql', load_settings())"

indexes:
	$(PYTHON) -c "from etl.utils import execute_sql_file, load_settings; execute_sql_file('data/dw/indexes.sql', load_settings())"

load:
	$(PYTHON) etl/load.py

etl: extract transform ddl indexes load

web:
	FLASK_APP=app/main.py flask run --host=0.0.0.0 --port=8080

all: install extract transform ddl indexes load

.PHONY: install extract transform ddl indexes load etl web all
