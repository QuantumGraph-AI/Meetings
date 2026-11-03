CREATE TABLE workspace_db.CTA19_ProxyReversaoCustodia_{{SAFRA}}
WITH (format = 'PARQUET',
      external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA19_ProxyReversaoCustodia_{{SAFRA}}/',
      partitioned_by = ARRAY['SAFRA']) AS
SELECT
  codigo_cliente,
  cpf_cnpj_cliente,
  ativo_negociado,
  tipo_pessoa,
  cnpj_empresa_vinculada,
  ticker_empresa_vinculada,
  Pessoa_Relacionada,
  linha_negocio,
  origem_operacao,
  data_evento,
  day_trader,
  soma_volume_compra_historica,
  soma_volume_venda_historica,
  media_volume_compra_historica,
  media_volume_venda_historica,
  desvio_volume_compra,
  desvio_volume_venda,
  soma_quantidade_compra_historica,
  soma_quantidade_venda_historica,
  media_quantidade_compra_historica,
  media_quantidade_venda_historica,
  desvio_quantidade_compra,
  desvio_quantidade_venda,
  -- n_dias_compra_historico,
  -- n_dias_venda_historico,
  soma_volume_compra_15dias,
  soma_volume_venda_15dias,
  media_volume_compra_15dias,
  media_volume_venda_15dias,
  soma_quantidade_compra_15dias,
  soma_quantidade_venda_15dias,
  media_quantidade_compra_15dias,
  media_quantidade_venda_15dias,
  -- n_dias_compra_pre,
  -- n_dias_venda_pre,
  volume_compra_D0,
  volume_venda_D0,
  quantidade_compra_D0,
  quantidade_venda_D0,
  -- n_dias_compra_D0,
  -- n_dias_venda_D0,

  soma_volume_compra_5diasDepois,
  soma_volume_venda_5diasDepois,
  media_volume_compra_5diasDepois,
  media_volume_venda_5diasDepois,
  soma_quantidade_compra_5diasDepois,
  soma_quantidade_venda_5diasDepois,
  media_quantidade_compra_5diasDepois,
  media_quantidade_venda_5diasDepois,
  Op_historica_compra,
  Op_historica_venda,
  Op_historica_compra_maior3,
  Op_historica_venda_maior3,
  Op_pre_compra,
  Op_pre_venda,
  Op_D0_compra,
  Op_D0_venda,
  Op_pos_compra,
  Op_pos_venda,
  Fugiu_hist_compra_pre,
  Fugiu_hist_venda_pre,
  Fugiu_hist_compra_pos,
  Fugiu_hist_venda_pos,
  valor_fechamento_d_1,
  preco_fechamento_d_1,
  data_d_1,
  valor_fechamento_d0,
  preco_fechamento_d0,
  data_d0,
  valor_fechamento_d1,
  preco_fechamento_d1,
  data_d1,
  
  -- variacao_preco_d_1_d0,
  tipo_variacao_d_1_d0,
  -- variacao_preco_d0_d1,
  -- tipo_variacao_d0_d1,
  -- tipo_variacao_final,
  preco_medio_pre,
  preco_medio_pos,

  resultado,
  -- Nova coluna: Reversão_PJ
  CASE
    -- Cenário 1: ALTA + repasse_enviado
    -- Calcula percentual que NÃO foi vendido: {1 - [(venda D0 + venda 5 dias) / compra 15 dias]} * 100
    WHEN tipo_variacao_d_1_d0 = 'ALTA' AND origem_operacao = 'repasse_enviado'
      THEN ROUND((1 - (CAST(quantidade_venda_D0 + soma_quantidade_venda_5diasDepois AS DOUBLE) / NULLIF(soma_quantidade_compra_15dias, 0))) * 100, 2)

    -- Cenário 2: QUEDA + repasse_enviado
    -- Calcula percentual que NÃO foi recomprado: {1 - [compra 5 dias / (venda 15 dias + venda D0)]} * 100
    WHEN tipo_variacao_d_1_d0 = 'QUEDA' AND origem_operacao = 'repasse_enviado'
      THEN ROUND((1 - (CAST(soma_quantidade_compra_5diasDepois AS DOUBLE) / NULLIF(soma_quantidade_venda_15dias + quantidade_venda_D0, 0))) * 100, 2)

    -- Demais casos: NULL
    ELSE NULL
  END AS Reversao_PJ,

  -- Nova coluna: resultado_final baseado em Reversao_PJ
  CASE
    -- Se Reversao_PJ for NULL, resultado_final também é NULL
    WHEN (
      CASE
        WHEN tipo_variacao_d_1_d0 = 'ALTA' AND origem_operacao = 'repasse_enviado'
          THEN ROUND((1 - (CAST(quantidade_venda_D0 + soma_quantidade_venda_5diasDepois AS DOUBLE) / NULLIF(soma_quantidade_compra_15dias, 0))) * 100, 2)
        WHEN tipo_variacao_d_1_d0 = 'QUEDA' AND origem_operacao = 'repasse_enviado'
          THEN ROUND((1 - (CAST(soma_quantidade_compra_5diasDepois AS DOUBLE) / NULLIF(soma_quantidade_venda_15dias + quantidade_venda_D0, 0))) * 100, 2)
        ELSE NULL
      END
    ) IS NULL THEN NULL
    -- Se Reversao_PJ > 75, resultado é ALERTA
    WHEN (
      CASE
        WHEN tipo_variacao_d_1_d0 = 'ALTA' AND origem_operacao = 'repasse_enviado'
          THEN ROUND((1 - (CAST(quantidade_venda_D0 + soma_quantidade_venda_5diasDepois AS DOUBLE) / NULLIF(soma_quantidade_compra_15dias, 0))) * 100, 2)
        WHEN tipo_variacao_d_1_d0 = 'QUEDA' AND origem_operacao = 'repasse_enviado'
          THEN ROUND((1 - (CAST(soma_quantidade_compra_5diasDepois AS DOUBLE) / NULLIF(soma_quantidade_venda_15dias + quantidade_venda_D0, 0))) * 100, 2)
        ELSE NULL
      END
    ) > 75 THEN 'ALERTA'
    -- Se Reversao_PJ <= 75, resultado é NÃO ALERTA
    ELSE 'NÃO ALERTA'
  END AS resultado_final,

  SAFRA
FROM workspace_db.CTA13_CalculoResultado_PJ_{{SAFRA}}
