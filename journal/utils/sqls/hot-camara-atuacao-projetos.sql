CREATE OR REPLACE VIEW
  view_camara_atuacao_projetos
AS
  SELECT
    deputado.candidato_id as id_candidato,
    camara_proposicao_deputado.deputado_id as id_deputado,
    proposicao.id as id_proposicao,
    proposicao.tipo_sigla as sigla,
    proposicao.numero,
    proposicao.ano,
    proposicao.ementa,
    detalhada.keywords,
    detalhada.url_integra as url
  FROM
    camara_proposicao_deputado
    INNER JOIN deputado on camara_proposicao_deputado.deputado_id = deputado.id
    INNER JOIN hot_dados_candidato on deputado.candidato_id = hot_dados_candidato.id
    LEFT JOIN camara_proposicao proposicao on camara_proposicao_deputado.camara_proposicao_id = proposicao.id
    LEFT JOIN camara_proposicao_detalhada detalhada on proposicao.id = detalhada.id
  WHERE
    proposicao.ano >= 2003 AND
    proposicao.tipo_sigla IN ('PEC', 'PFC', 'PL', 'PLP', 'PDC', 'MPV')
  ORDER BY deputado.candidato_id ASC;

CREATE TABLE hot_camara_atuacao_projetos AS SELECT * FROM view_camara_atuacao_projetos;

CREATE INDEX id_candidato_index ON hot_camara_atuacao_projetos (id_candidato);

DROP VIEW view_camara_atuacao_projetos;
