CREATE OR REPLACE VIEW
  view_dados_candidato
AS
  SELECT
    candidatura.candidato_id as id,
    candidato.nome,
    candidatura.nome_urna,
    candidato.data_nascimento,
    candidato.foto,
    cidade.nome as cidade_natal,
    estado.nome as estado_natal,
    partido.sigla as partido,
    candidatura.numero_candidato as numero,
    candidatura.estado_id as id_estado_candidatura,
    candidatura.cargo_id as id_cargo,
    cargo.nome as cargo,
    grau_instrucao.descricao as grau_instrucao,
    ocupacao.descricao as ocupacao
  FROM
    candidatura
      LEFT JOIN partido on candidatura.partido_id = partido.id
      LEFT JOIN cargo on candidatura.cargo_id = cargo.id
      LEFT JOIN candidato on candidatura.candidato_id = candidato.id
      LEFT JOIN cidade on candidato.cidade_id = cidade.id
      LEFT JOIN estado on cidade.estado_id = estado.id
      LEFT JOIN grau_instrucao on candidato.grau_instrucao_id = grau_instrucao.id
      LEFT JOIN ocupacao on candidato.ocupacao_id = ocupacao.id
  WHERE
    (candidatura.eleicao_id = 202 OR candidatura.eleicao_id = 203) AND
    (
      candidatura.situacao_candidatura_id IS NULL OR
      candidatura.situacao_candidatura_id IN (2, 4, 8, 12, 16, 17, 18, 19)
    );

CREATE TABLE hot_dados_candidato AS SELECT * FROM view_dados_candidato;

CREATE INDEX id_index ON hot_dados_candidato (id);
CREATE INDEX nome_index ON hot_dados_candidato (nome);
CREATE INDEX nome_urna_index ON hot_dados_candidato (nome_urna);

DROP VIEW view_dados_candidato;
