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
   - Con XAMPP la cuenta `root` suele no tener contraseña, por eso la URL por defecto es
     `mysql+pymysql://root@localhost:3307/horarios?charset=utf8mb4`. Si configuraste clave,
     agrégala en el formato `root:clave` antes de `@`.
3. Revisa `config/settings.yaml` para validar periodo, plan, regex de salón y credenciales de MySQL.

## Ejecución local

```bash
cd olap_practica
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# Inicia MySQL (por ejemplo, desde el panel de XAMPP) y verifica que la base "horarios" exista.
make all  # install + extract + transform + ddl + indexes + load
python app/main.py
```

> En Windows puedes ejecutar los pasos manualmente si no tienes `make` instalado. Antes de correr
> los comandos, enciende MySQL y crea la base `horarios` (puedes hacerlo con phpMyAdmin o la
> consola de MySQL). Ejecuta, en este orden: `python etl/extract_pdf.py`, `python -c "from
> etl.transform import transform; import yaml; settings=yaml.safe_load(open('config/settings.yaml'));
> transform('data/staging/staging.csv', settings)"`, `python -c "from etl.utils import execute_sql_file;
> execute_sql_file('data/dw/schema_mysql.sql')"`, `python -c "from etl.utils import execute_sql_file;
> execute_sql_file('data/dw/indexes.sql')"` y, finalmente, `python etl/load.py`.

Abre <http://localhost:8080> y utiliza los selectores de cada panel:

- **Horario semanal de un docente**: despliega todos los nombres disponibles en `dim_docente`.
  El resultado incluye una vista tipo calendario con columnas por día (Lunes-Sábado) y filas por
  hora, resaltando materia, clave y salón en cada celda, además de la tabla tabular tradicional.
- **Docentes que imparten una materia**: ofrece dos menús desplegables, uno por clave oficial y otro por
  nombre. Si eliges ambos, la clave tiene prioridad. La consulta opera en mayúsculas/minúsculas de manera
  transparente.
- **Docentes en un edificio a una hora**: el selector combina edificios y salones (`edificio/salón`) y solo
  muestra horas registradas en `fact_clase`. El checkbox “usar slots” sigue disponible si aplicaste
  `data/dw/views.sql`.

> _Vista de referencia_: la interfaz sigue siendo responsive y ahora añade accesos directos a cada cara del
> cubo, conservando la tabla de resultados y el enlace para lanzar una nueva consulta.

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

1. Horario semanal por docente (comparación case-insensitive sobre `dim_docente`).
2. Docentes que imparten una materia por clave parcial (prefijo) o texto case-insensitive.
3. Docentes presentes en un edificio/aula a una hora dada (con y sin slots de 60 minutos),
   devolviendo el día de la semana correspondiente para cada coincidencia.

## Exploración rápida del cubo

En la parte inferior de la página principal encontrarás botones para navegar por cada cara del cubo:

- **Cubo completo**: vista previa de `fact_clase` con joins a las dimensiones principales.
- **Vistas OLAP**: acceso directo a `fact_clase_slot` (si fue creada).
- **Dimensiones**: botones independientes para `dim_docente`, `dim_asignatura`, `dim_grupo`, `dim_tiempo` y `dim_espacio`.
- **Datos crudos**: lectura rápida de `data/staging/staging.csv` y `data/staging/fact_ready.csv`.

Cada botón abre una tabla en la propia aplicación sin necesidad de ejecutar SQL manualmente; si una vista no existe (por ejemplo `fact_clase_slot`), la interfaz mostrará un mensaje orientativo para generarla.

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
  Si el servicio está apagado o las credenciales son erróneas, los comandos del Makefile detendrán
  la ejecución con un mensaje "Conexión a MySQL rechazada" para evitar trazas extensas.
- **Consultas sin resultados**: asegúrate de haber ejecutado `make load` y de usar parámetros que
  coincidan con los datos cargados.

## Backlog y mejoras futuras

- Alimentar automáticamente la vista de slots tras `make load` para habilitar consultas por bloques.
- Cachear resultados frecuentemente solicitados usando Redis o materialized views adicionales.
- Añadir autenticación simple para restringir acceso a la interfaz web.
