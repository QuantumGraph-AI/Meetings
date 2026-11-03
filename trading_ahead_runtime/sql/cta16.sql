CREATE TABLE workspace_db.CTA16_CustodiaInfos_{{SAFRA}}
WITH (format = 'PARQUET',
      external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA16_CustodiaInfos_{{SAFRA}}/',
      partitioned_by = ARRAY['SAFRA']) AS
WITH
-- Base ordenada para facilitar as buscas
base_ordenada AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY codigo_conta, ativo ORDER BY data_referencia ASC) AS rn
  FROM workspace_db.CTA15_CustodiaAtipicos_{{SAFRA}}
),

-- Custódia D-15: busca custódia 15 dias antes da data_evento
custodia_D15 AS (
  SELECT
    b.codigo_conta,
    b.ativo,
    b.data_evento,
    COALESCE(
      MAX(CASE WHEN CAST(c.data_referencia AS DATE) = CAST(b.data_evento AS DATE) - INTERVAL '15' DAY
           THEN c.data_referencia END),
      MAX(CASE WHEN CAST(c.data_referencia AS DATE) = CAST(b.data_evento AS DATE) - INTERVAL '14' DAY
           THEN c.data_referencia END),
      MAX(CASE WHEN CAST(c.data_referencia AS DATE) = CAST(b.data_evento AS DATE) - INTERVAL '16' DAY
           THEN c.data_referencia END)
    ) AS data_D_15_custodia,
    COALESCE(
      MAX(CASE WHEN CAST(c.data_referencia AS DATE) = CAST(b.data_evento AS DATE) - INTERVAL '15' DAY
           THEN c.quantidade_d0_shifted END),
      MAX(CASE WHEN CAST(c.data_referencia AS DATE) = CAST(b.data_evento AS DATE) - INTERVAL '14' DAY
           THEN c.quantidade_d0_shifted END),
      MAX(CASE WHEN CAST(c.data_referencia AS DATE) = CAST(b.data_evento AS DATE) - INTERVAL '16' DAY
           THEN c.quantidade_d0_shifted END)
    ) AS custodia_D_15
  FROM (SELECT DISTINCT codigo_conta, ativo, data_evento FROM base_ordenada) b
  LEFT JOIN base_ordenada c
    ON b.codigo_conta = c.codigo_conta
    AND b.ativo = c.ativo
    AND CAST(c.data_referencia AS DATE) BETWEEN
        CAST(b.data_evento AS DATE) - INTERVAL '16' DAY
        AND CAST(b.data_evento AS DATE) - INTERVAL '14' DAY
  GROUP BY b.codigo_conta, b.ativo, b.data_evento
),

-- Custódia D-1: busca a maior data_referencia < data_evento
custodia_D1_ant AS (
  SELECT
    b.codigo_conta,
    b.ativo,
    b.data_evento,
    MAX(c.data_referencia) AS data_D_1_custodia,
    MAX_BY(c.quantidade_d0_shifted, c.data_referencia) AS custodia_D_1
  FROM (SELECT DISTINCT codigo_conta, ativo, data_evento FROM base_ordenada) b
  LEFT JOIN base_ordenada c
    ON b.codigo_conta = c.codigo_conta
    AND b.ativo = c.ativo
    AND CAST(c.data_referencia AS DATE) < CAST(b.data_evento AS DATE)
  GROUP BY b.codigo_conta, b.ativo, b.data_evento
),

-- Custódia D0: busca data_referencia = data_evento, ou a mais próxima anterior
custodia_D0 AS (
  SELECT
    b.codigo_conta,
    b.ativo,
    b.data_evento,
    COALESCE(
      MAX(CASE WHEN CAST(c.data_referencia AS DATE) = CAST(b.data_evento AS DATE)
           THEN c.data_referencia END),
      MAX(CASE WHEN CAST(c.data_referencia AS DATE) < CAST(b.data_evento AS DATE)
           THEN c.data_referencia END)
    ) AS data_D0_custodia,
    COALESCE(
      MAX(CASE WHEN CAST(c.data_referencia AS DATE) = CAST(b.data_evento AS DATE)
           THEN c.quantidade_d0_shifted END),
      MAX_BY(CASE WHEN CAST(c.data_referencia AS DATE) < CAST(b.data_evento AS DATE)
              THEN c.quantidade_d0_shifted END, c.data_referencia)
    ) AS custodia_D0
  FROM (SELECT DISTINCT codigo_conta, ativo, data_evento FROM base_ordenada) b
  LEFT JOIN base_ordenada c
    ON b.codigo_conta = c.codigo_conta
    AND b.ativo = c.ativo
    AND CAST(c.data_referencia AS DATE) <= CAST(b.data_evento AS DATE)
  GROUP BY b.codigo_conta, b.ativo, b.data_evento
),

-- Custódia D+1: busca a menor data_referencia > data_evento
custodia_D1_pos AS (
  SELECT
    b.codigo_conta,
    b.ativo,
    b.data_evento,
    COALESCE(
      MIN(CASE WHEN CAST(c.data_referencia AS DATE) > CAST(b.data_evento AS DATE)
           THEN c.data_referencia END),
      MAX(CASE WHEN CAST(c.data_referencia AS DATE) < CAST(b.data_evento AS DATE)
           THEN c.data_referencia END)
    ) AS data_D1_custodia,
    COALESCE(
      MIN_BY(CASE WHEN CAST(c.data_referencia AS DATE) > CAST(b.data_evento AS DATE)
              THEN c.quantidade_d0_shifted END, c.data_referencia),
      MAX_BY(CASE WHEN CAST(c.data_referencia AS DATE) < CAST(b.data_evento AS DATE)
              THEN c.quantidade_d0_shifted END, c.data_referencia)
    ) AS custodia_D1
  FROM (SELECT DISTINCT codigo_conta, ativo, data_evento FROM base_ordenada) b
  LEFT JOIN base_ordenada c
    ON b.codigo_conta = c.codigo_conta
    AND b.ativo = c.ativo
  GROUP BY b.codigo_conta, b.ativo, b.data_evento
),

-- JOIN de todos os cálculos + filtro por período + limpeza de colunas
resultado_completo AS (
  SELECT
    b.data_referencia,
    b.codigo_conta,
    b.ativo,
    b.ativo_atipico,
    b.data_evento,
    d15.data_D_15_custodia,
    COALESCE(CAST(d15.custodia_D_15 AS VARCHAR), 'Não consta') AS custodia_D_15,
    d1_ant.data_D_1_custodia,
    d1_ant.custodia_D_1,
    d0.data_D0_custodia,
    d0.custodia_D0,
    d1_pos.data_D1_custodia,
    COALESCE(d1_pos.custodia_D1, 0) AS custodia_D1
  FROM base_ordenada b
  LEFT JOIN custodia_D15 d15
    ON b.codigo_conta = d15.codigo_conta
    AND b.ativo = d15.ativo
    AND b.data_evento = d15.data_evento
  LEFT JOIN custodia_D1_ant d1_ant
    ON b.codigo_conta = d1_ant.codigo_conta
    AND b.ativo = d1_ant.ativo
    AND b.data_evento = d1_ant.data_evento
  LEFT JOIN custodia_D0 d0
    ON b.codigo_conta = d0.codigo_conta
    AND b.ativo = d0.ativo
    AND b.data_evento = d0.data_evento
  LEFT JOIN custodia_D1_pos d1_pos
    ON b.codigo_conta = d1_pos.codigo_conta
    AND b.ativo = d1_pos.ativo
    AND b.data_evento = d1_pos.data_evento
  WHERE
    -- Filtra apenas registros dentro do período de interesse (após os cálculos)
    -- Equivale ao filtro Python: df_processado[(df_processado['data_referencia'] >= data_inicio) & (df_processado['data_referencia'] <= data_fim)]
    CAST(b.data_referencia AS DATE) >= CAST(b.data_evento AS DATE) - INTERVAL '20' DAY
    AND CAST(b.data_referencia AS DATE) <= CAST(b.data_evento AS DATE) + INTERVAL '5' DAY
)

-- SELECT DISTINCT final para remover duplicatas
SELECT DISTINCT
  codigo_conta,
  ativo,
  ativo_atipico,
  data_evento,
  data_D_15_custodia,
  custodia_D_15,
  data_D_1_custodia,
  custodia_D_1,
  data_D0_custodia,
  custodia_D0,
  data_D1_custodia,
  custodia_D1,
  '{{SAFRA}}' AS SAFRA
FROM resultado_completo
