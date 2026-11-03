CREATE TABLE workspace_db.CTA15_CustodiaAtipicos_{{SAFRA}}
WITH (format = 'PARQUET',
      external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA15_CustodiaAtipicos_{{SAFRA}}/',
      partitioned_by = ARRAY['SAFRA']) AS
SELECT 
  cust.data_referencia,
  cust.codigo_conta,
  cust.ativo,
  -- cust.ativo_4letras,
  cust.quantidade_d0,
  cust.quantidade_d0_shifted,
  -- fr.cnpj_companhia,
  -- fr.nome_empresarial,
  ev.ativo_atipico,
  ev.data_evento,
  '{{SAFRA}}' AS SAFRA
FROM workspace_db.CTA14_Custodia_{{SAFRA}} cust
INNER JOIN workspace_db.CTA3_EventosAtipicos_{{SAFRA}} ev
  ON cust.ativo = ev.ativo_atipico
