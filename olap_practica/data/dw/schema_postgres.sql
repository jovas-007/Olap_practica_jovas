CREATE TABLE IF NOT EXISTS dim_docente (
    id SERIAL PRIMARY KEY,
    nombre_completo TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_asignatura (
    id SERIAL PRIMARY KEY,
    clave TEXT NOT NULL,
    nombre TEXT NOT NULL,
    programa TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_grupo (
    id SERIAL PRIMARY KEY,
    nrc INT NOT NULL UNIQUE,
    seccion TEXT NOT NULL,
    cruzada BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dim_tiempo (
    id SERIAL PRIMARY KEY,
    dia_codigo CHAR(1) NOT NULL CHECK (dia_codigo IN ('L','A','M','J','V','S')),
    dia_semana INT NOT NULL CHECK (dia_semana BETWEEN 1 AND 6)
);

CREATE TABLE IF NOT EXISTS dim_espacio (
    id SERIAL PRIMARY KEY,
    edificio TEXT NOT NULL,
    salon TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_clase (
    id SERIAL PRIMARY KEY,
    fk_docente INT NOT NULL REFERENCES dim_docente(id),
    fk_asignatura INT NOT NULL REFERENCES dim_asignatura(id),
    fk_grupo INT NOT NULL REFERENCES dim_grupo(id),
    fk_tiempo INT NOT NULL REFERENCES dim_tiempo(id),
    fk_espacio INT NOT NULL REFERENCES dim_espacio(id),
    periodo TEXT NOT NULL,
    plan TEXT NOT NULL,
    inicio TIME NOT NULL,
    fin TIME NOT NULL,
    minutos INT NOT NULL
);
