CREATE OR REPLACE VIEW
  view_dados_candidato
AS
  SELECT
    camara_legislatura_id,
    camara_orgao_id,
    camara_papel_orgao_id,
    cpo.nome,
    o.nome
  FROM
    camara_participacao_orgao
    LEFT JOIN camara_orgao o on camara_participacao_orgao.camara_orgao_id = o.id
    LEFT JOIN camara_papel_orgao cpo on camara_participacao_orgao.camara_papel_orgao_id = cpo.id
  WHERE
    deputado_id = 77701
  GROUP BY
    camara_legislatura_id,
    camara_orgao_id,
    camara_papel_orgao_id
  ORDER BY
    camara_legislatura_id,
    camara_orgao_id,
    camara_papel_orgao_id
