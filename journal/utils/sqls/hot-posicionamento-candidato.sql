CREATE OR REPLACE VIEW
  view_posicionamento_candidato
AS
  SELECT
    tpc.candidato_id as id_candidato,
    tpc.tema_id as id_tema,
    t.titulo,
    t.descricao,

    t.tema_categoria_id as id_categoria,
    tc.titulo as categoria,

    tpc.tema_posicao_id as id_posicao,
    tp.titulo as posicao,
    tpc.url_fonte as fonte_informacao,

    cp.tipo_sigla as sigla_projeto,
    cp.numero as numero_projeto,
    cp.ano as ano_projeto,

    # Referências
    (
      SELECT
        tema_link.id
      FROM
        tema_link
      WHERE
        tema_link.tema_id = t.id
      LIMIT 0, 1
    ) as referencia_1_id,
    (
      SELECT
        tema_link.titulo
      FROM
        tema_link
      WHERE
        tema_link.tema_id = t.id
      LIMIT 0, 1
    ) as referencia_1_titulo,
    (
      SELECT
        tema_link.short_url
      FROM
        tema_link
      WHERE
        tema_link.tema_id = t.id
      LIMIT 0, 1
    ) as referencia_1_url,
    (
      SELECT
        tema_link.id
      FROM
        tema_link
      WHERE
        tema_link.tema_id = t.id
      LIMIT 1, 1
    ) as referencia_2_id,
    (
      SELECT
        tema_link.titulo
      FROM
        tema_link
      WHERE
        tema_link.tema_id = t.id
      LIMIT 1, 1
    ) as referencia_2_titulo,
    (
      SELECT
        tema_link.short_url
      FROM
        tema_link
      WHERE
        tema_link.tema_id = t.id
      LIMIT 1, 1
    ) as referencia_2_url,
    (
      SELECT
        tema_link.id
      FROM
        tema_link
      WHERE
        tema_link.tema_id = t.id
      LIMIT 2, 1
    ) as referencia_3_id,
    (
      SELECT
        tema_link.titulo
      FROM
        tema_link
      WHERE
        tema_link.tema_id = t.id
      LIMIT 2, 1
    ) as referencia_3_titulo,
    (
      SELECT
        tema_link.short_url
      FROM
        tema_link
      WHERE
        tema_link.tema_id = t.id
      LIMIT 2, 1
    ) as referencia_3_url
  FROM
    tema_posicao_candidato tpc
    LEFT JOIN tema t on tpc.tema_id = t.id
    LEFT JOIN tema_posicao tp on tpc.tema_posicao_id = tp.id
    LEFT JOIN tema_categoria tc on t.tema_categoria_id = tc.id
    LEFT JOIN camara_proposicao cp on t.camara_proposicao_id = cp.id
    LEFT JOIN camara_proposicao_detalhada cpd on t.camara_proposicao_id = cpd.id
  ORDER BY
    tpc.candidato_id ASC,
    t.tema_categoria_id ASC,
    t.id;

CREATE TABLE hot_posicionamento_candidato AS SELECT * FROM view_posicionamento_candidato;

CREATE INDEX id_candidato_index ON hot_posicionamento_candidato (id_candidato);

DROP VIEW view_posicionamento_candidato;
