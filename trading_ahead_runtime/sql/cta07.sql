CREATE TABLE workspace_db.CTA7_OperacoesEventos_{{SAFRA}}
WITH (
    format = 'PARQUET',
    external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA7_OperacoesEventos_{{SAFRA}}/',
    partitioned_by = ARRAY['SAFRA']
) AS
SELECT 
    o.codigo_cliente,
    o.cpf_cnpj_cliente,
    o.tipo_operacao,
    o.ativo_negociado,
    o.ativo_negociado_4letras,
    o.preco,
    o.quantidade,
    o.volume_negociado,
    o.data_operacao,
    o.pessoa_fisica,
    o.cnpj_empresa_vinculada,
    o.ticker_empresa_vinculada,
    o.Pessoa_Relacionada,
    o.linha_negocio,
    o.origem_operacao,
    ev.data_evento,
    ev.ativo_atipico,
    '{{SAFRA}}' AS SAFRA
FROM workspace_db.CTA6_Operacoes_{{SAFRA}} o
INNER JOIN workspace_db.CTA3_EventosAtipicos_{{SAFRA}} ev
    ON o.ativo_negociado = ev.ativo_atipico
