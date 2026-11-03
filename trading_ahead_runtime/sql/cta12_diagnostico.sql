CREATE TABLE workspace_db.CTA12_FugiramPerfil_{{SAFRA}}
WITH (format = 'PARQUET',
      external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA12_FugiramPerfil_{{SAFRA}}/',
      partitioned_by = ARRAY['SAFRA']) AS
WITH 
-- Filtra operações fora do perfil (3 condições)
fora_perfil AS (
  SELECT *
  FROM workspace_db.CTA11_FeaturesFinal_{{SAFRA}}
  WHERE 
    -- Condição 1: Alerta em z-score E fugiu do perfil histórico
    ((Fugiu_hist_compra_pre = 'S' OR Fugiu_hist_venda_pre = 'S'))
    OR
    -- Condição 2: Alerta em z-score E NÃO fugiu do perfil mas É pessoa relacionada
    (Fugiu_hist_compra_pre = 'N' AND
     Fugiu_hist_venda_pre = 'N' AND
     Pessoa_Relacionada = 'S')
    OR
    -- Condição 3: NÃO alerta em z-score mas fugiu do perfil E É pessoa relacionada
    ((Fugiu_hist_compra_pre = 'S' OR Fugiu_hist_venda_pre = 'S') AND
      Pessoa_Relacionada = 'S'
    )
),

-- Remove operações não relevantes
operacoes_suspeitas AS (
  SELECT *
  FROM fora_perfil
  WHERE NOT (
    -- Excluir: ALTA com compra pré mas sem venda D0 e pós
    (tipo_variacao_d_1_d0 = 'ALTA' AND soma_quantidade_compra_15dias != 0 AND quantidade_venda_D0 = 0 AND soma_quantidade_venda_5diasDepois = 0)
    OR
    -- Excluir: ALTA sem compra pré mas com venda D0 ou pós
    (tipo_variacao_d_1_d0 = 'ALTA' AND soma_quantidade_compra_15dias = 0 AND (quantidade_venda_D0 != 0 OR soma_quantidade_venda_5diasDepois != 0))
    OR
    -- Excluir: ALTA sem nenhuma operação próxima ao fato
    (tipo_variacao_d_1_d0 = 'ALTA' AND soma_quantidade_compra_15dias = 0 AND quantidade_venda_D0 = 0 AND soma_quantidade_venda_5diasDepois = 0)
    OR
    -- Excluir: QUEDA sem venda pré e sem venda D0
    (tipo_variacao_d_1_d0 = 'QUEDA' AND soma_quantidade_venda_15dias = 0 AND quantidade_venda_D0 = 0)
  )
),

-- Calcula preço médio pré-fato
com_preco_medio_pre AS (
  SELECT *,
    CASE 
      WHEN tipo_variacao_d_1_d0 = 'ALTA' AND soma_quantidade_compra_15dias > 0 
        THEN soma_volume_compra_15dias / soma_quantidade_compra_15dias
      WHEN tipo_variacao_d_1_d0 = 'QUEDA' AND soma_quantidade_venda_15dias > 0 
        THEN soma_volume_venda_15dias / soma_quantidade_venda_15dias
      ELSE NULL
    END AS preco_medio_pre
  FROM operacoes_suspeitas
),

-- Calcula preço médio pós-fato
resultado_final AS (
  SELECT *,
    CASE 
      WHEN tipo_variacao_d_1_d0 = 'ALTA' AND soma_quantidade_venda_5diasDepois > 0 
        THEN soma_volume_venda_5diasDepois / soma_quantidade_venda_5diasDepois
      WHEN tipo_variacao_d_1_d0 = 'QUEDA' AND soma_quantidade_compra_5diasDepois > 0 
        THEN soma_volume_compra_5diasDepois / soma_quantidade_compra_5diasDepois
      ELSE NULL
    END AS preco_medio_pos
  FROM com_preco_medio_pre
)

SELECT 
  codigo_cliente,
  cpf_cnpj_cliente,
  ativo_negociado,
  ativo_negociado_4letras,
  pessoa_fisica,
  cnpj_empresa_vinculada,
  ticker_empresa_vinculada,
  Pessoa_Relacionada,
  linha_negocio,
  origem_operacao,
  data_evento,
  -- ticker_fr,
  --cnpj_companhia,
  --nome_empresarial,
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
  -- desvio_preco_pos,
  -- media_preco_pos,
  -- variacao_preco_d_1_d0,
  tipo_variacao_d_1_d0,
  -- variacao_preco_d0_d1,
  -- tipo_variacao_d0_d1,
  -- tipo_variacao_final,
  preco_medio_pre,
  preco_medio_pos,
  '{{SAFRA}}' AS SAFRA
FROM resultado_final
