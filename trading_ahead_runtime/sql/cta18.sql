CREATE TABLE workspace_db.CTA18_CustodiaReversao_{{SAFRA}}
WITH (format = 'PARQUET',
      external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA18_CustodiaReversao_{{SAFRA}}/',
      partitioned_by = ARRAY['SAFRA']) AS
WITH
-- Remove casos onde não há operação relevante
custodia_filtrada AS (
  SELECT *
  FROM workspace_db.CTA17_CustodiaOperacoes_{{SAFRA}}
  WHERE NOT (
    -- Condição 1: QUEDA sem venda pré e sem custódia
    (tipo_variacao_d_1_d0 = 'QUEDA'
     AND soma_quantidade_venda_15dias = 0
     AND CAST(custodia_D_1 AS DOUBLE) = 0
     AND CAST(custodia_D0 AS DOUBLE) = 0
     AND custodia_D1 = 0)
    OR
    -- Condição 2: ALTA sem compra pré e custódia não variou
    (tipo_variacao_d_1_d0 = 'ALTA'
     AND soma_quantidade_compra_15dias = 0
     AND CAST(custodia_D_1 AS DOUBLE) = CAST(custodia_D0 AS DOUBLE)
     AND CAST(custodia_D0 AS DOUBLE) = custodia_D1)
  )
),

-- Calcula variações de custódia
custodia_com_variacoes AS (
  SELECT
    *,
    -- Custodia_D(-1)_D0
    CASE
      WHEN custodia_D_1 IS NULL OR custodia_D0 IS NULL THEN NULL
      WHEN CAST(custodia_D_1 AS DOUBLE) > CAST(custodia_D0 AS DOUBLE) THEN 'DIMINUIU'
      WHEN CAST(custodia_D_1 AS DOUBLE) < CAST(custodia_D0 AS DOUBLE) THEN 'AUMENTOU'
      ELSE 'MANTEVE'
    END AS Custodia_D_menos1_D0,

    -- Variacao_custodia_D(-1)_D0
    CASE
      WHEN custodia_D_1 IS NULL OR custodia_D0 IS NULL THEN NULL
      WHEN CAST(custodia_D_1 AS DOUBLE) = 0 THEN
        CASE WHEN CAST(custodia_D0 AS DOUBLE) = 0 THEN '0.00%' ELSE '100.00%' END
      ELSE CAST(ROUND(((CAST(custodia_D0 AS DOUBLE) - CAST(custodia_D_1 AS DOUBLE)) / ABS(CAST(custodia_D_1 AS DOUBLE))) * 100, 2) AS VARCHAR) || '%'
    END AS Variacao_custodia_D_menos1_D0,

    -- Custodia_D0_D1
    CASE
      WHEN custodia_D0 IS NULL OR custodia_D1 IS NULL THEN NULL
      WHEN CAST(custodia_D0 AS DOUBLE) > custodia_D1 THEN 'DIMINUIU'
      WHEN CAST(custodia_D0 AS DOUBLE) < custodia_D1 THEN 'AUMENTOU'
      ELSE 'MANTEVE'
    END AS Custodia_D0_D1,

    -- Variacao_custodia_D0_D1
    CASE
      WHEN custodia_D0 IS NULL OR custodia_D1 IS NULL THEN NULL
      WHEN CAST(custodia_D0 AS DOUBLE) = 0 THEN
        CASE WHEN custodia_D1 = 0 THEN '0.00%' ELSE '100.00%' END
      ELSE CAST(ROUND(((custodia_D1 - CAST(custodia_D0 AS DOUBLE)) / ABS(CAST(custodia_D0 AS DOUBLE))) * 100, 2) AS VARCHAR) || '%'
    END AS Variacao_custodia_D0_D1,

    -- Resultado_custodia
    ((CAST(custodia_D_1 AS DOUBLE) - CAST(custodia_D0 AS DOUBLE)) * preco_fechamento_d0) +
    ((CAST(custodia_D0 AS DOUBLE) - custodia_D1) * preco_fechamento_d1) AS Resultado_custodia

  FROM custodia_filtrada
),
-- ##################################################### A J U S T A D O #####################################################
-- Calcula reversão (retorna como VARCHAR para incluir 'Não consta')
custodia_com_reversao AS (
  SELECT
    *,
    CASE
      -- Para ALTA: (vendas pós) / (custódia D-1 + compras D1)
      WHEN tipo_variacao_d_1_d0 = 'ALTA' THEN
        CASE
          -- Verifica se denominador é NULL ou zero
          WHEN (CAST(custodia_D_1 AS DOUBLE)) IS NULL THEN '0.0'
          WHEN (CAST(custodia_D_1 AS DOUBLE)) = 0 THEN '0.0'
          ELSE CAST(ROUND(((soma_quantidade_venda_5diasDepois + quantidade_venda_D0) /
                (CAST(custodia_D_1 AS DOUBLE))) * 100, 2) AS VARCHAR)
        END

      -- Para QUEDA: vendas pré / (custódia D-15 + compras pré)
      WHEN tipo_variacao_d_1_d0 = 'QUEDA' THEN
        CASE
          -- Primeiro verifica se é 'Não consta'
          WHEN custodia_D_15 = 'Não consta' THEN 'Não consta'
          -- Depois verifica se é NULL
          WHEN custodia_D_15 IS NULL THEN '0.0'
          -- Depois verifica se é zero
          WHEN CAST(custodia_D_15 AS DOUBLE) = 0 THEN '0.0'
          -- Caso contrário, calcula a reversão
          ELSE CAST(ROUND((soma_quantidade_venda_15dias /
                (CAST(custodia_D_15 AS DOUBLE) + soma_quantidade_compra_15dias)) * 100, 2) AS VARCHAR)
        END

      ELSE '0.0'
    END AS reversao
  FROM custodia_com_variacoes
)

-- SELECT final (SAFRA deve ser a última coluna por ser chave de partição)
SELECT DISTINCT
  codigo_conta,
  ativo,
  data_evento,
  data_D_15_custodia,
  custodia_D_15,
  data_D_1_custodia,
  custodia_D_1,
  data_D0_custodia,
  custodia_D0,
  data_D1_custodia,
  custodia_D1,
  codigo_cliente,
  cpf_cnpj_cliente,
  ativo_negociado,
  tipo_pessoa,
  cnpj_empresa_vinculada,
  ticker_empresa_vinculada,
  Pessoa_Relacionada,
  linha_negocio,
  origem_operacao,
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

  Resultado,
  Custodia_D_menos1_D0,
  Variacao_custodia_D_menos1_D0,
  Custodia_D0_D1,
  Variacao_custodia_D0_D1,
  Resultado_custodia,
  COALESCE(reversao, 'Não consta') AS reversao,
  CASE
    WHEN reversao = 'Não consta' THEN 'ALERTA'
    WHEN TRY_CAST(reversao AS DOUBLE) >= 75 THEN 'ALERTA'
    WHEN TRY_CAST(reversao AS DOUBLE) < 75 THEN 'NÃO ALERTA'
    ELSE 'ALERTA'
  END AS resultado_final,
  SAFRA  -- Coluna de partição deve ser a última
FROM custodia_com_reversao
