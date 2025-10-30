# OLAP práctica de horarios BUAP

Proyecto integral que automatiza la extracción de horarios desde archivos PDF oficiales, los
normaliza para cargar un esquema dimensional (modelo estrella) en **MySQL** (por ejemplo el motor
incluido en XAMPP) y expone una interfaz web Flask para ejecutar consultas analíticas.

## Arquitectura

1. **Extract**: `pdfplumber` procesa los PDFs `PA_OTOÑO_2025_SEMESTRAL_*.pdf`, normalizando
   columnas y generando `data/staging/staging.csv`.
2. **Transform**: `pandas` divide horarios, explota días de clase y calcula métricas antes de
   producir `data/staging/fact_ready.csv`.
3. **Load**: `SQLAlchemy` inserta dimensiones y hechos en MySQL siguiendo el modelo estrella
   definido en `data/dw/schema_mysql.sql`.
4. **Web**: Flask + Jinja2 ofrecen un formulario para seleccionar la consulta OLAP y visualizar
   resultados tabulares en tiempo real.

```
PDFs -> ETL (extract/transform/load) -> Data Warehouse (modelo estrella) -> Flask Web UI
```

## Requisitos

- Python 3.10+
- MySQL 8 (o MariaDB compatible, como el que instala XAMPP).
- Dependencias Python listadas en `requirements.txt`:
  `pdfplumber`, `pandas`, `SQLAlchemy`, `PyMySQL`, `pydantic`, `python-slugify`,
  `python-dateutil`, `Flask`, `Jinja2`, `PyYAML`, `python-dotenv`, `pytest`.

## Configuración

1. Clona el repositorio y posiciona los PDFs `PA_OTOÑO_2025_SEMESTRAL_*.pdf` en la raíz del repo.
2. Copia `.env.example` a `.env` y ajusta `DATABASE_URL` para apuntar a tu instancia de MySQL.
3. Revisa `config/settings.yaml` para validar periodo, plan, regex de salón y credenciales de MySQL.

## Ejecución local

```bash
cd olap_practica
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
make all  # install + extract + transform + ddl + indexes + load
python app/main.py
```

> En Windows puedes ejecutar los pasos manualmente si no tienes `make` instalado. Ejecuta, en
> este orden: `python etl/extract_pdf.py`, `python -c "from etl.transform import transform; import
> yaml; settings=yaml.safe_load(open('config/settings.yaml')); transform('data/staging/staging.csv',
> settings)"`, `python -c "from etl.utils import execute_sql_file; execute_sql_file('data/dw/schema_mysql.sql')"`,
> `python -c "from etl.utils import execute_sql_file; execute_sql_file('data/dw/indexes.sql')"` y,
> finalmente, `python etl/load.py`.

Abre <http://localhost:8080> y selecciona la consulta:

- **Horario semanal de un docente**: busca por coincidencias en el nombre (sin distinguir mayúsculas/minúsculas).
- **Docentes que imparten una materia**: usa la clave exacta o texto parcial en el nombre.
- **Docentes en un edificio a una hora**: muestra docentes activos en el edificio y hora
  especificada. Activa “usar slots” únicamente si creaste manualmente la tabla `fact_clase_slot`.

> _Vista de referencia_: la interfaz muestra un formulario responsive sin frameworks CSS pesados,
> con un resumen de resultados en tabla y enlace para lanzar una nueva consulta.

## Comandos Make principales

| Comando       | Descripción                                                                 |
|---------------|------------------------------------------------------------------------------|
| `make install`| Instala dependencias de `requirements.txt`.                                  |
| `make extract`| Ejecuta `etl/extract_pdf.py`. Mueve PDFs y genera `staging.csv`.              |
| `make transform`| Normaliza `staging.csv` hacia `fact_ready.csv`.                           |
| `make ddl`    | Crea el esquema estrella en MySQL (`schema_mysql.sql`).                      |
| `make indexes`| Aplica índices definidos en `data/dw/indexes.sql`.                           |
| `make load`   | Inserta dimensiones y hechos en la base.                                     |
| `make etl`    | Encadena extract, transform, ddl, indexes y load.                            |
| `make web`    | Inicia la aplicación Flask en modo desarrollo.                               |
| `make all`    | `install` + pipeline ETL completo (extract, transform, ddl, indexes, load).  |

## SQL de referencia

Las consultas expuestas en la web están documentadas en `data/dw/queries.sql` e incluyen:

1. Horario semanal por docente (comparación case-insensitive).
2. Docentes que imparten una materia por clave o texto.
3. Docentes presentes en un edificio a una hora dada (con y sin slots de 60 minutos).

## Pruebas automatizadas

```bash
cd olap_practica
pytest
```

Los tests cubren:

- Extracción: columnas esperadas, normalización de docentes y movimiento de PDFs a `data/raw/`.
- Transformación: división de horas, explosión de días y partición de salón en edificio/aula.
- Rutas Flask: carga del formulario principal y ejecución de las tres operaciones con mocks.

## Solución de problemas

- **No se encuentran PDFs**: confirma que los archivos `PA_OTOÑO_2025_SEMESTRAL_*.pdf` están en la
  raíz del repo o en `data/raw/`. El extractor registrará en logs los archivos procesados.
- **Regex de salón fallida**: ajusta `salon_regex` en `config/settings.yaml` o revisa los PDF por
  celdas mal formateadas.
- **Conexión a MySQL**: confirma que el servicio se encuentre activo y que puedas conectarte con
  las credenciales definidas en `.env` (por ejemplo usando `mysql -h localhost -P 3307 -u root`).
- **Consultas sin resultados**: asegúrate de haber ejecutado `make load` y de usar parámetros que
  coincidan con los datos cargados.

## Backlog y mejoras futuras

- Alimentar automáticamente la vista de slots tras `make load` para habilitar consultas por bloques.
- Cachear resultados frecuentemente solicitados usando Redis o materialized views adicionales.
- Añadir autenticación simple para restringir acceso a la interfaz web.
