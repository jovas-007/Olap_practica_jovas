-- 1) Horario semanal de un docente
SELECT d.nombre_completo, t.dia_codigo, f.inicio, f.fin, a.clave, a.nombre AS materia, e.edificio, e.salon
FROM fact_clase f
JOIN dim_docente d ON f.fk_docente = d.id
JOIN dim_asignatura a ON f.fk_asignatura = a.id
JOIN dim_tiempo t ON f.fk_tiempo = t.id
JOIN dim_espacio e ON f.fk_espacio = e.id
WHERE f.periodo = 'Otoño 2025' AND f.plan = 'Semestral' AND LOWER(d.nombre_completo) LIKE LOWER(:docente)
ORDER BY d.nombre_completo, t.dia_codigo, f.inicio;

-- 2) Docentes que dan una materia específica
SELECT DISTINCT d.nombre_completo
FROM fact_clase f
JOIN dim_docente d ON f.fk_docente = d.id
JOIN dim_asignatura a ON f.fk_asignatura = a.id
WHERE f.periodo = 'Otoño 2025' AND f.plan = 'Semestral'
  AND ((:clave IS NOT NULL AND UPPER(a.clave) LIKE CONCAT(UPPER(:clave), '%'))
    OR (:clave IS NULL AND :texto IS NOT NULL AND LOWER(a.nombre) LIKE CONCAT('%', LOWER(:texto), '%')));

-- 3) Docentes en el mismo edificio a una hora dada
SELECT DISTINCT d.nombre_completo, t.dia_codigo
FROM fact_clase f
JOIN dim_docente d ON f.fk_docente = d.id
JOIN dim_espacio e ON f.fk_espacio = e.id
JOIN dim_tiempo t ON f.fk_tiempo = t.id
WHERE f.periodo = 'Otoño 2025' AND f.plan = 'Semestral'
  AND (LOWER(e.edificio) = LOWER(:edificio)
       OR (:salon IS NOT NULL AND LOWER(CONCAT(e.edificio, '/', e.salon)) = LOWER(:salon)))
  AND f.inicio <= CAST(:hora AS TIME)
  AND f.fin > CAST(:hora AS TIME)
ORDER BY d.nombre_completo, t.dia_codigo;
