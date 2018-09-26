CREATE OR REPLACE VIEW
  view_camara_atuacao_numero_proposicoes
AS
  SELECT
    deputado.candidato_id as id_candidato,
    count(camara_proposicao_deputado.id) as numero_proposicoes,
    (
      SELECT
        camara_legislatura.id
      FROM
        camara_legislatura
      WHERE
        proposicao.ano BETWEEN YEAR(camara_legislatura.data_inicio) AND YEAR(camara_legislatura.data_fim)
      ORDER BY
        camara_legislatura.id DESC
      LIMIT 1
    ) as id_legislatura
  FROM
    camara_proposicao_deputado
    INNER JOIN deputado on camara_proposicao_deputado.deputado_id = deputado.id
    INNER JOIN hot_dados_candidato on deputado.candidato_id = hot_dados_candidato.id
    LEFT JOIN camara_proposicao proposicao on camara_proposicao_deputado.camara_proposicao_id = proposicao.id
  WHERE
    proposicao.ano >= 2003
  GROUP BY
    id_candidato,
    id_legislatura;

CREATE TABLE hot_camara_atuacao_numero_proposicoes AS SELECT * FROM view_camara_atuacao_numero_proposicoes;

CREATE INDEX id_candidato_index ON hot_camara_atuacao_numero_proposicoes (id_candidato);

DROP VIEW view_camara_atuacao_numero_proposicoes;
