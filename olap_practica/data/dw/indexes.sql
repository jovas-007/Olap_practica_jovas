CREATE INDEX idx_fact_docente_dia ON fact_clase (fk_docente, fk_tiempo, inicio, fin);
CREATE INDEX idx_fact_espacio_hora ON fact_clase (fk_espacio, inicio, fin);
CREATE INDEX idx_asignatura_clave ON dim_asignatura (clave);
CREATE INDEX idx_tiempo_codigo ON dim_tiempo (dia_codigo);
