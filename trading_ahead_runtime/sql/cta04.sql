
CREATE TABLE workspace_db.CTA4_PrecosEventos_{{SAFRA}}
WITH (
    format = 'PARQUET',
    external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA4_PrecosEventos_{{SAFRA}}/',
    partitioned_by = ARRAY['SAFRA']
) AS

SELECT 
    a.ativo_atipico,
    CAST(a.data_evento AS DATE) AS data_evento,
    CAST(b.data_fechamento AS DATE) AS data_fechamento,
    b.ativo,
    b.preco_fechamento,
    b.valor_fechamento,
    b.ativo_4letras,
    a.safra
FROM workspace_db.CTA3_EventosAtipicos_{{SAFRA}} a
INNER JOIN workspace_db.CTA1_Precos_{{SAFRA}} b
ON a.ativo_atipico = b.ativo
