-- 1) Horario semanal de un docente
SELECT d.nombre_completo, t.dia_codigo, f.inicio, f.fin, a.clave, a.nombre AS materia, e.edificio, e.salon
FROM fact_clase f
JOIN dim_docente d ON f.fk_docente = d.id
JOIN dim_asignatura a ON f.fk_asignatura = a.id
JOIN dim_tiempo t ON f.fk_tiempo = t.id
JOIN dim_espacio e ON f.fk_espacio = e.id
WHERE f.periodo = 'Otoño 2025' AND f.plan = 'Semestral' AND d.nombre_completo ILIKE :docente
ORDER BY d.nombre_completo, t.dia_codigo, f.inicio;

-- 2) Docentes que dan una materia específica
SELECT DISTINCT d.nombre_completo
FROM fact_clase f
JOIN dim_docente d ON f.fk_docente = d.id
JOIN dim_asignatura a ON f.fk_asignatura = a.id
WHERE a.clave = :clave OR a.nombre ILIKE '%' || :texto || '%';

-- 3) Docentes en el mismo edificio a una hora dada
SELECT DISTINCT d.nombre_completo
FROM fact_clase f
JOIN dim_docente d ON f.fk_docente = d.id
JOIN dim_espacio e ON f.fk_espacio = e.id
WHERE e.edificio = :edificio AND f.inicio <= :hora::time AND f.fin > :hora::time;

-- 3b) Versión con slots
SELECT DISTINCT d.nombre_completo
FROM fact_clase_slot s
JOIN dim_docente d ON s.fk_docente = d.id
JOIN dim_espacio e ON s.fk_espacio = e.id
WHERE e.edificio = :edificio AND s.slot_inicio <= :hora::time AND s.slot_fin > :hora::time;
