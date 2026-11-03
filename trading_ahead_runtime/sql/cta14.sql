CREATE TABLE workspace_db.CTA14_Custodia_{{SAFRA}}
WITH (format = 'PARQUET',
      external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA14_Custodia_{{SAFRA}}/',
      partitioned_by = ARRAY['SAFRA']) AS
WITH
-- Obtém clientes únicos da CTA13 PF
clientes_pf AS (
  SELECT DISTINCT codigo_cliente
  FROM workspace_db.CTA13_CalculoResultado_PF_{{SAFRA}}
),

-- Obtém ativos únicos da CTA13 PF
ativos_pf AS (
  SELECT DISTINCT ativo_negociado
  FROM workspace_db.CTA13_CalculoResultado_PF_{{SAFRA}}
),

-- Calcula período de custódia: dia 20 do mês anterior até dia 4 do mês seguinte
-- Para safra YYYYMM, buscar de (YYYY-MM-1)-20 até (YYYY-MM+1)-04
custodia_bruta AS (
  SELECT DISTINCT
    data_referencia,
    CAST(codigo_conta AS INTEGER) AS codigo_conta,
    ativo,
    SUBSTR(ativo, 1, 4) AS ativo_4letras,
    quantidade_d0
  FROM db_corp_corretora_inoabrokertools_sot_01.tb_ep7_carteiras_equities_sot
  WHERE CAST(data_referencia AS DATE) BETWEEN
    DATE_ADD('month', -1, DATE('{{SAFRA_INICIO}}')) + INTERVAL '19' DAY  -- Dia 20 do mês anterior
    AND DATE_ADD('month', 1, DATE('{{SAFRA_FIM}}')) - INTERVAL '26' DAY  -- Dia 4 do mês seguinte
),

-- Filtra apenas clientes e ativos da PF
custodia_filtrada AS (
  SELECT c.*
  FROM custodia_bruta c
  INNER JOIN clientes_pf cpf ON CAST(c.codigo_conta AS VARCHAR) = cpf.codigo_cliente
  INNER JOIN ativos_pf apf ON c.ativo = apf.ativo_negociado
),

-- Ordena e calcula quantidade_d0_shifted (quantidade 2 dias depois)
custodia_ordenada AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY codigo_conta, ativo, data_referencia ORDER BY quantidade_d0 DESC) AS rn_dedup,
    ROW_NUMBER() OVER (PARTITION BY codigo_conta, ativo ORDER BY data_referencia ASC) AS rn
  FROM custodia_filtrada
),

-- Remove duplicatas mantendo o maior valor de quantidade quando houver duplicatas na mesma data
custodia_dedupada AS (
  SELECT
    data_referencia,
    codigo_conta,
    ativo,
    -- ativo_4letras,
    quantidade_d0
  FROM custodia_ordenada
  WHERE rn_dedup = 1
),

-- Recalcula ROW_NUMBER após deduplicação para ter sequência contínua (sem buracos)
custodia_com_rn_continuo AS (
  SELECT
    data_referencia,
    codigo_conta,
    ativo,
    -- ativo_4letras,
    quantidade_d0,
    ROW_NUMBER() OVER (PARTITION BY codigo_conta, ativo ORDER BY data_referencia ASC) AS rn
  FROM custodia_dedupada
),

custodia_com_shift AS (
  SELECT
    c1.data_referencia,
    c1.codigo_conta,
    c1.ativo,
    -- c1.ativo_4letras,
    c1.quantidade_d0,
    COALESCE(c2.quantidade_d0, 0.0) AS quantidade_d0_shifted
  FROM custodia_com_rn_continuo c1
  LEFT JOIN custodia_com_rn_continuo c2
    ON c1.codigo_conta = c2.codigo_conta
    AND c1.ativo = c2.ativo
    AND c2.rn = c1.rn + 2
)

SELECT
  data_referencia,
  codigo_conta,
  ativo,
  -- ativo_4letras,
  quantidade_d0,
  quantidade_d0_shifted,
  '{{SAFRA}}' AS SAFRA
FROM custodia_com_shift
ORDER BY codigo_conta, ativo, data_referencia
