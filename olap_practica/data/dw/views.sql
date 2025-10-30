DROP MATERIALIZED VIEW IF EXISTS fact_clase_slot;

CREATE MATERIALIZED VIEW fact_clase_slot AS
SELECT
    f.id AS fact_id,
    f.fk_docente,
    f.fk_asignatura,
    f.fk_grupo,
    f.fk_tiempo,
    f.fk_espacio,
    f.periodo,
    f.plan,
    slot_time AS slot_inicio,
    LEAST(f.fin, slot_time + INTERVAL '60 minutes') AS slot_fin
FROM fact_clase f
CROSS JOIN LATERAL generate_series(f.inicio, f.fin - INTERVAL '1 minute', INTERVAL '60 minutes') AS slot_time;

CREATE INDEX IF NOT EXISTS idx_fact_slot_espacio ON fact_clase_slot (fk_espacio);
CREATE INDEX IF NOT EXISTS idx_fact_slot_inicio ON fact_clase_slot (slot_inicio);
CREATE INDEX IF NOT EXISTS idx_fact_slot_fin ON fact_clase_slot (slot_fin);
