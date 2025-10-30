CREATE TABLE IF NOT EXISTS dim_docente (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre_completo TEXT NOT NULL,
    UNIQUE KEY uq_docente_nombre (nombre_completo(255))
);

CREATE TABLE IF NOT EXISTS dim_asignatura (
    id INT AUTO_INCREMENT PRIMARY KEY,
    clave VARCHAR(50) NOT NULL,
    nombre TEXT NOT NULL,
    programa VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_grupo (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nrc INT NOT NULL UNIQUE,
    seccion VARCHAR(50) NOT NULL,
    cruzada TINYINT(1) DEFAULT 0
);

CREATE TABLE IF NOT EXISTS dim_tiempo (
    id INT AUTO_INCREMENT PRIMARY KEY,
    dia_codigo CHAR(1) NOT NULL,
    dia_semana INT NOT NULL,
    CONSTRAINT chk_dia_codigo CHECK (dia_codigo IN ('L','A','M','J','V','S')),
    CONSTRAINT chk_dia_semana CHECK (dia_semana BETWEEN 1 AND 6)
);

CREATE TABLE IF NOT EXISTS dim_espacio (
    id INT AUTO_INCREMENT PRIMARY KEY,
    edificio VARCHAR(100) NOT NULL,
    salon VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_clase (
    id INT AUTO_INCREMENT PRIMARY KEY,
    fk_docente INT NOT NULL,
    fk_asignatura INT NOT NULL,
    fk_grupo INT NOT NULL,
    fk_tiempo INT NOT NULL,
    fk_espacio INT NOT NULL,
    periodo VARCHAR(100) NOT NULL,
    plan VARCHAR(100) NOT NULL,
    inicio TIME NOT NULL,
    fin TIME NOT NULL,
    minutos INT NOT NULL,
    FOREIGN KEY (fk_docente) REFERENCES dim_docente(id),
    FOREIGN KEY (fk_asignatura) REFERENCES dim_asignatura(id),
    FOREIGN KEY (fk_grupo) REFERENCES dim_grupo(id),
    FOREIGN KEY (fk_tiempo) REFERENCES dim_tiempo(id),
    FOREIGN KEY (fk_espacio) REFERENCES dim_espacio(id)
);
