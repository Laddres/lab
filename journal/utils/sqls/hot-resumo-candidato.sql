CREATE OR REPLACE VIEW
  view_resumo_candidaturas
AS
  SELECT
    candidatura.candidato_id as id_candidato,
    COUNT(DISTINCT candidatura.partido_id) as numero_partidos,
    -- GROUP_CONCAT(DISTINCT partido.sigla SEPARATOR ' - ') as partidos_anteriores,
    (
      SELECT
        GROUP_CONCAT(DISTINCT p.sigla SEPARATOR ' - ') as partidos
      FROM
        candidatura c
        INNER JOIN partido p on c.partido_id = p.id
      WHERE
        c.candidato_id = candidatura.candidato_id
    ) as partidos_anteriores,
    COUNT(IF(
      (candidatura.turno = 1)
      AND (candidatura.situacao_candidatura_id != 6)
      AND (candidatura.eleicao_id != 202 AND candidatura.eleicao_id != 203)
      , 1
      , NULL
    )) as numero_candidaturas,
    COUNT(IF(candidatura.resultado_candidatura_id BETWEEN 1 AND 3, 1, NULL)) as numero_mandatos,
    COUNT(IF(
      candidatura.resultado_candidatura_id IS NULL AND
      (candidatura.eleicao_id != 202 AND candidatura.eleicao_id != 203)
      , 1
      , NULL
    )) as numero_resultados_nulos
  FROM
    candidatura
    INNER JOIN hot_dados_candidato on candidatura.candidato_id = hot_dados_candidato.id
    INNER JOIN partido on candidatura.partido_id = partido.id
  GROUP BY
    candidatura.candidato_id;

CREATE OR REPLACE VIEW
  view_resumo_atuacao_parlamentar
AS
  SELECT
    deputado.candidato_id as id_candidato,
    COUNT(IF(cp.ano > 2002 , 1, NULL)) as numero_proposicoes,
    COUNT(IF(cp.tipo_sigla IN ('PEC', 'PFC', 'PL', 'PLP', 'PDC', 'MPV') AND cp.ano > 2002, 1, NULL)) as numero_projetos
  FROM
    deputado
    INNER JOIN hot_dados_candidato on deputado.candidato_id = hot_dados_candidato.id
    INNER JOIN camara_proposicao_deputado cpd on deputado.id = cpd.deputado_id
    INNER JOIN camara_proposicao cp on cpd.camara_proposicao_id = cp.id
  GROUP BY
    deputado.candidato_id;

CREATE OR REPLACE VIEW
  view_resumo_processos
AS
  SELECT
    politico.candidato_id as id_candidato,
    COUNT(DISTINCT processo.id) as numero_processos
  FROM
    politico_vigieaqui politico
    LEFT JOIN processo_judicial processo on politico.id = processo.politico_vigieaqui_id
  GROUP BY
    politico.candidato_id
  ORDER BY
    politico.candidato_id ASC;

CREATE TABLE
  hot_resumo_candidato
AS
  SELECT
    vrc.id_candidato,
    vrc.numero_partidos,
    vrc.partidos_anteriores,
    vrc.numero_candidaturas,
    vrc.numero_mandatos,
    vrc.numero_resultados_nulos,
    vrap.numero_proposicoes,
    vrap.numero_projetos,
    vrp.numero_processos
  FROM
    view_resumo_candidaturas vrc
    LEFT JOIN view_resumo_atuacao_parlamentar vrap on vrc.id_candidato = vrap.id_candidato
    LEFT JOIN view_resumo_processos vrp on vrc.id_candidato = vrp.id_candidato
  ORDER BY
    vrc.id_candidato ASC;

CREATE INDEX id_candidato_index ON hot_resumo_candidato (id_candidato);

DROP VIEW view_resumo_candidaturas;
DROP VIEW view_resumo_atuacao_parlamentar;
DROP VIEW view_resumo_processos;
