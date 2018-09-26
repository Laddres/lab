CREATE OR REPLACE VIEW
  view_vigieaqui_politico_processado
AS
  SELECT
    politico.candidato_id as id_candidato,
    politico.processado
  FROM
    politico_vigieaqui politico
  ORDER BY
    politico.candidato_id ASC;

CREATE TABLE hot_vigieaqui_politico_processado AS SELECT * FROM view_vigieaqui_politico_processado;

CREATE INDEX id_candidato_index ON hot_vigieaqui_politico_processado (id_candidato);

DROP VIEW view_vigieaqui_politico_processado;
