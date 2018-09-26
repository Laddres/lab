CREATE OR REPLACE VIEW
  view_candidaturas_candidato
AS
  SELECT
    DISTINCT candidatura.id,
    candidatura.candidato_id as id_candidato,
    cargo.nome as cargo,
    partido.sigla as partido,
    eleicao.ano as eleicao_ano,
    eleicao.descricao as eleicao_descricao,
    cidade.nome as cidade,
    estado.sigla as estado_sigla,
    estado.nome as estado_nome,
    candidatura.legenda_nome,
    candidatura.legenda_composicao,
    resultado_candidatura.descricao as resultado
  FROM
    candidatura
      INNER JOIN hot_dados_candidato on candidatura.candidato_id = hot_dados_candidato.id
      LEFT JOIN cargo on candidatura.cargo_id = cargo.id
      LEFT JOIN partido on candidatura.partido_id = partido.id
      LEFT JOIN eleicao on candidatura.eleicao_id = eleicao.id
      LEFT JOIN cidade on candidatura.cidade_id = cidade.id
      LEFT JOIN estado on candidatura.estado_id = estado.id
      LEFT JOIN resultado_candidatura on candidatura.resultado_candidatura_id = resultado_candidatura.id
  WHERE
    (candidatura.resultado_candidatura_id != 6 OR candidatura.resultado_candidatura_id IS NULL) AND
    (candidatura.situacao_candidatura_id != 6) AND
    (candidatura.eleicao_id != 202 AND candidatura.eleicao_id != 203)
  ORDER BY
    candidatura.candidato_id ASC,
    eleicao.ano DESC;

CREATE TABLE hot_candidaturas_candidato AS SELECT * FROM view_candidaturas_candidato;

CREATE INDEX id_candidato_index ON hot_candidaturas_candidato (id_candidato);

DROP VIEW view_candidaturas_candidato;
