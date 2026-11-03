CREATE TABLE workspace_db.CTA18_AlertasTradingAhead_PF_{{SAFRA}}
WITH (format = 'PARQUET',
      external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA18_AlertasTradingAhead_PF_{{SAFRA}}/',
      partitioned_by = ARRAY['SAFRA']) AS
SELECT *
FROM workspace_db.CTA18_CustodiaReversao_{{SAFRA}}
WHERE resultado_final = 'ALERTA'
