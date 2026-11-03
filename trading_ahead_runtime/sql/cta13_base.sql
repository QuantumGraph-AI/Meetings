CREATE TABLE workspace_db.CTA13_CalculoResultado_Base_Estudo_{{SAFRA}}
WITH (format = 'PARQUET',
      external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA13_CalculoResultado_Base_Estudo_{{SAFRA}}/',
      partitioned_by = ARRAY['SAFRA']) AS
WITH
-- CTE 1: Carrega base da CTA11
base_cta11 AS (
  SELECT *,
    -- Cria coluna tipo_pessoa baseada em pessoa_fisica
    CASE
      WHEN pessoa_fisica = 'true' THEN 'F'
      ELSE 'J'
    END AS tipo_pessoa
  FROM workspace_db.CTA11_FeaturesFinal_{{SAFRA}}
),

-- CTE 2: Calcula preço médio pré-fato (15 dias antes)
com_preco_medio_pre AS (
  SELECT *,
    CASE
      WHEN tipo_variacao_d_1_d0 = 'ALTA' AND soma_quantidade_compra_15dias > 0
        THEN soma_volume_compra_15dias / soma_quantidade_compra_15dias
      WHEN tipo_variacao_d_1_d0 = 'QUEDA' AND soma_quantidade_venda_15dias > 0
        THEN soma_volume_venda_15dias / soma_quantidade_venda_15dias
      ELSE NULL
    END AS preco_medio_pre
  FROM base_cta11
),

-- CTE 3: Calcula preço médio pós-fato (5 dias depois)
com_preco_medio_pos AS (
  SELECT *,
    CASE
      WHEN tipo_variacao_d_1_d0 = 'ALTA' AND soma_quantidade_venda_5diasDepois > 0
        THEN soma_volume_venda_5diasDepois / soma_quantidade_venda_5diasDepois
      WHEN tipo_variacao_d_1_d0 = 'QUEDA' AND soma_quantidade_compra_5diasDepois > 0
        THEN soma_volume_compra_5diasDepois / soma_quantidade_compra_5diasDepois
      ELSE NULL
    END AS preco_medio_pos
  FROM com_preco_medio_pre
),

-- CTE 4: Calcula o resultado financeiro baseado no tipo de variação
resultado_calculado AS (
  SELECT *,
    -- ========================================
    -- COLUNAS: REVERSÃO PRÉ-EVENTO
    -- ========================================

    -- (1) Coluna reversao_pre
    CASE
      WHEN tipo_variacao_d_1_d0 = 'QUEDA'
        THEN soma_quantidade_venda_15dias - soma_quantidade_compra_15dias
      WHEN tipo_variacao_d_1_d0 = 'ALTA'
        THEN soma_quantidade_compra_15dias - soma_quantidade_venda_15dias
      ELSE NULL
    END AS reversao_pre,

    -- (2) Coluna perc_reversao_pre (% de reversão)
    CASE
      WHEN soma_quantidade_compra_15dias > 0
        THEN ROUND(soma_quantidade_venda_15dias / soma_quantidade_compra_15dias, 4)
      ELSE NULL
    END AS perc_reversao_pre,

    -- (3) Coluna Resultado (resultado baseado na reversão)
    CASE
      -- Cenário 1: QUEDA e sem compra pós-fato (preco_medio_pos é NULL)
      WHEN tipo_variacao_d_1_d0 = 'QUEDA' AND preco_medio_pos IS NULL
        THEN ROUND(
          (soma_volume_venda_15dias - soma_volume_compra_15dias) -
          ((soma_quantidade_venda_15dias - soma_quantidade_compra_15dias) * preco_fechamento_d0),
          2
        )

      -- Cenário 2: QUEDA e com compra pós-fato (preco_medio_pos não é NULL)
      WHEN tipo_variacao_d_1_d0 = 'QUEDA' AND preco_medio_pos IS NOT NULL
        THEN ROUND(
          (preco_medio_pre - preco_medio_pos) *
          LEAST(soma_quantidade_venda_15dias - soma_quantidade_compra_15dias, soma_quantidade_compra_5diasDepois),
          2
        )

      -- Cenário 3: ALTA e vende apenas no pós (não vende em D0)
      WHEN tipo_variacao_d_1_d0 = 'ALTA' AND quantidade_venda_D0 = 0
        THEN ROUND(
          (preco_medio_pos - preco_medio_pre) *
          LEAST(soma_quantidade_compra_15dias - soma_quantidade_venda_15dias, soma_quantidade_venda_5diasDepois),
          2
        )

      -- Cenário 4: ALTA e vende em D0
      WHEN tipo_variacao_d_1_d0 = 'ALTA' AND quantidade_venda_D0 != 0
        THEN ROUND(
          (preco_fechamento_d0 - preco_medio_pre) *
          LEAST(soma_quantidade_compra_15dias - soma_quantidade_venda_15dias,
                quantidade_venda_D0 + soma_quantidade_venda_5diasDepois),
          2
        )

      ELSE NULL
    END AS Resultado,

    -- Para o cenário ALTA com venda em D0, atualiza o preco_medio_pos para preco_fechamento_d0
    CASE
      WHEN tipo_variacao_d_1_d0 = 'ALTA' AND quantidade_venda_D0 != 0
        THEN preco_fechamento_d0
      ELSE preco_medio_pos
    END AS preco_medio_pos_ajustado

  FROM com_preco_medio_pos
)

-- SELECT FINAL: Lista todas as colunas
SELECT
  codigo_cliente,
  cpf_cnpj_cliente,
  ativo_negociado,
  ativo_negociado_4letras,
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
  soma_volume_compra_15dias,
  soma_volume_venda_15dias,
  media_volume_compra_15dias,
  media_volume_venda_15dias,
  soma_quantidade_compra_15dias,
  soma_quantidade_venda_15dias,
  media_quantidade_compra_15dias,
  media_quantidade_venda_15dias,
  volume_compra_D0,
  volume_venda_D0,
  quantidade_compra_D0,
  quantidade_venda_D0,
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
  tipo_variacao_d_1_d0,

  -- Colunas de preço médio calculadas
  preco_medio_pre,
  preco_medio_pos_ajustado AS preco_medio_pos,

  -- Colunas de reversão
  reversao_pre,
  perc_reversao_pre,
  Resultado,

  '{{SAFRA}}' AS SAFRA
FROM resultado_calculado
-- SEM FILTRO: Salva todos os resultados calculados
