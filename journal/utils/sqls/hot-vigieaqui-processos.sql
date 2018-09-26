CREATE OR REPLACE VIEW
  view_vigieaqui_processos
AS
  SELECT
    politico.candidato_id as id_candidato,
    processo.id as id_processo,
    processo.numero,
    processo.tipo,
    processo.tribunal,
    processo.descricao,
    processo.link
  FROM
    politico_vigieaqui politico
    INNER JOIN processo_judicial processo on politico.id = processo.politico_vigieaqui_id
  ORDER BY
    politico.candidato_id ASC;

CREATE TABLE hot_vigieaqui_processos AS SELECT * FROM view_vigieaqui_processos;

CREATE INDEX id_candidato_index ON hot_vigieaqui_processos (id_candidato);

DROP VIEW view_vigieaqui_processos;
