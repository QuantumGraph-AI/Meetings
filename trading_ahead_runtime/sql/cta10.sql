CREATE TABLE workspace_db.CTA10_PerfilOperacional_{{SAFRA}}
WITH (format = 'PARQUET',
      external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA10_PerfilOperacional_{{SAFRA}}/',
      partitioned_by = ARRAY['SAFRA']) AS
SELECT 
  codigo_cliente, cpf_cnpj_cliente, ativo_negociado, ativo_negociado_4letras, pessoa_fisica,
  cnpj_empresa_vinculada, ticker_empresa_vinculada, Pessoa_Relacionada, linha_negocio, origem_operacao, data_evento, ativo_atipico,
  -- cnpj_companhia, 
  day_trader,
  soma_volume_compra_historica, soma_volume_venda_historica, 
  media_volume_compra_historica, media_volume_venda_historica,
  desvio_volume_compra, desvio_volume_venda, 
  soma_quantidade_compra_historica, soma_quantidade_venda_historica,
  media_quantidade_compra_historica, media_quantidade_venda_historica, 
  desvio_quantidade_compra, desvio_quantidade_venda,
  -- n_dias_compra_historico, n_dias_venda_historico, 
  soma_volume_compra_15dias, soma_volume_venda_15dias,
  media_volume_compra_15dias, media_volume_venda_15dias, 
  soma_quantidade_compra_15dias, soma_quantidade_venda_15dias,
  media_quantidade_compra_15dias, media_quantidade_venda_15dias, 
  -- n_dias_compra_pre, n_dias_venda_pre,
  volume_compra_D0, volume_venda_D0, quantidade_compra_D0, quantidade_venda_D0, 
  -- n_dias_compra_D0, n_dias_venda_D0,
  -- volume_compra_D1, volume_venda_D1, quantidade_compra_D1, quantidade_venda_D1, 
  -- n_dias_compra_D1, n_dias_venda_D1,
  soma_volume_compra_5diasDepois, soma_volume_venda_5diasDepois, 
  media_volume_compra_5diasDepois, media_volume_venda_5diasDepois,
  soma_quantidade_compra_5diasDepois, soma_quantidade_venda_5diasDepois, 
  media_quantidade_compra_5diasDepois, media_quantidade_venda_5diasDepois,
  
  -- Flags de operação (S/N)
  CASE WHEN soma_quantidade_compra_historica > 0 THEN 'S' ELSE 'N' END AS Op_historica_compra,
  CASE WHEN soma_quantidade_venda_historica > 0 THEN 'S' ELSE 'N' END AS Op_historica_venda,
  CASE WHEN soma_quantidade_compra_historica >= 3 THEN 'S' ELSE 'N' END AS Op_historica_compra_maior3,
  CASE WHEN soma_quantidade_venda_historica >= 3 THEN 'S' ELSE 'N' END AS Op_historica_venda_maior3,
  CASE WHEN soma_quantidade_compra_15dias > 0 THEN 'S' ELSE 'N' END AS Op_pre_compra,
  CASE WHEN soma_quantidade_venda_15dias > 0 THEN 'S' ELSE 'N' END AS Op_pre_venda,
  CASE WHEN quantidade_compra_D0 > 0 THEN 'S' ELSE 'N' END AS Op_D0_compra,
  CASE WHEN quantidade_venda_D0 > 0 THEN 'S' ELSE 'N' END AS Op_D0_venda,
  CASE WHEN soma_quantidade_compra_5diasDepois > 0 THEN 'S' ELSE 'N' END AS Op_pos_compra,
  CASE WHEN soma_quantidade_venda_5diasDepois > 0 THEN 'S' ELSE 'N' END AS Op_pos_venda,
  
  -- Flags "Fugiu" (outlier = média > histórico + 2*desvio)
  CASE WHEN media_volume_compra_15dias > (media_volume_compra_historica + 2*desvio_volume_compra) 
       THEN 'S' ELSE 'N' END AS Fugiu_hist_compra_pre,
  CASE WHEN media_volume_venda_15dias > (media_volume_venda_historica + 2*desvio_volume_venda) 
       THEN 'S' ELSE 'N' END AS Fugiu_hist_venda_pre,
  CASE WHEN media_volume_compra_5diasDepois > (media_volume_compra_historica + 2*desvio_volume_compra) 
       THEN 'S' ELSE 'N' END AS Fugiu_hist_compra_pos,
  CASE WHEN media_volume_venda_5diasDepois > (media_volume_venda_historica + 2*desvio_volume_venda) 
       THEN 'S' ELSE 'N' END AS Fugiu_hist_venda_pos,
  
  '{{SAFRA}}' AS SAFRA
FROM workspace_db.CTA9_VolumesQuantidades_{{SAFRA}}
