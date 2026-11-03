
CREATE TABLE workspace_db.CTA5_EstudoVariacao_{{SAFRA}}
WITH (
    format = 'PARQUET',
    external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA5_EstudoVariacao_{{SAFRA}}/',
    partitioned_by = ARRAY['SAFRA']
) AS
WITH
-- ═══ STEP 2: obter_valor_fechamento_D_1 ═══
step2_d1 AS (
    SELECT
        a.ativo,
        a.data_evento,
        MAX_BY(b.valor_fechamento, b.data_fechamento) AS valor_fechamento_d_1,
        MAX_BY(b.preco_fechamento, b.data_fechamento) AS preco_fechamento_d_1,
        MAX(b.data_fechamento)                        AS data_d_1
    FROM (SELECT DISTINCT ativo, data_evento FROM workspace_db.CTA4_PrecosEventos_{{SAFRA}}) a
    LEFT JOIN (SELECT DISTINCT ativo, data_fechamento, valor_fechamento, preco_fechamento FROM workspace_db.CTA4_PrecosEventos_{{SAFRA}}) b
        ON a.ativo = b.ativo
        AND b.data_fechamento < a.data_evento
    GROUP BY a.ativo, a.data_evento
),
step2 AS (
    SELECT
        a.ativo_atipico,
        CAST(a.data_fechamento AS DATE) AS data_fechamento,
        CAST(a.data_evento AS DATE)    AS data_evento,
        a.ativo,
        CAST(a.valor_fechamento AS DOUBLE) AS valor_fechamento,
        a.preco_fechamento,
        b.valor_fechamento_d_1,
        b.preco_fechamento_d_1,
        b.data_d_1
    FROM workspace_db.CTA4_PrecosEventos_{{SAFRA}} a
    LEFT JOIN step2_d1 b
        ON a.ativo = b.ativo
        AND a.data_evento = b.data_evento
),
-- ═══ STEP 3: obter_valor_fechamento_D0 ═══
step3_d0 AS (
    SELECT DISTINCT
        ativo,
        data_fechamento AS data_d0,
        valor_fechamento AS valor_fechamento_d0,
        preco_fechamento AS preco_fechamento_d0
    FROM step2
    WHERE data_fechamento = data_evento
),
step3 AS (
    SELECT
        a.*,
        COALESCE(b.valor_fechamento_d0, 0) AS valor_fechamento_d0,
        b.preco_fechamento_d0,
        b.data_d0
    FROM step2 a
    LEFT JOIN step3_d0 b
        ON a.ativo = b.ativo
        AND a.data_evento = b.data_d0
),
-- ═══ STEP 4: obter_valor_fechamento_D1 ═══
step4_d1 AS (
    SELECT
        a.ativo,
        a.data_evento,
        MIN_BY(b.valor_fechamento, b.data_fechamento) AS valor_fechamento_d1,
        MIN_BY(b.preco_fechamento, b.data_fechamento) AS preco_fechamento_d1,
        MIN(b.data_fechamento)                        AS data_d1
    FROM (SELECT DISTINCT ativo, data_evento FROM step3) a
    LEFT JOIN (SELECT DISTINCT ativo, data_fechamento, valor_fechamento, preco_fechamento FROM step3) b
        ON a.ativo = b.ativo
        AND b.data_fechamento > a.data_evento
    GROUP BY a.ativo, a.data_evento
),
step4 AS (
    SELECT
        a.*,
        b.valor_fechamento_d1,
        b.preco_fechamento_d1,
        b.data_d1
    FROM step3 a
    LEFT JOIN step4_d1 b
        ON a.ativo = b.ativo
        AND a.data_evento = b.data_evento
),
-- ═══ STEP 7: remoção de colunas + drop_duplicates ═══
step7 AS (
    SELECT DISTINCT
        ativo_atipico,
        data_evento,
        data_fechamento,
        ativo,
        valor_fechamento_d_1,
        preco_fechamento_d_1,
        data_d_1,
        valor_fechamento_d0,
        preco_fechamento_d0,
        data_d0,
        valor_fechamento_d1,
        preco_fechamento_d1,
        data_d1
    FROM step4
),

-- ═══ STEP 9: filtro período + variações e classificações ═══
step9_base AS (
    SELECT *
    FROM step7
    WHERE data_fechamento >= DATE('{{SAFRA_INICIO}}')
      AND data_fechamento <= DATE('{{SAFRA_FIM}}')
),
step9 AS (
    SELECT DISTINCT
        ativo_atipico,
        data_evento,
        ativo,
        valor_fechamento_d_1,
        preco_fechamento_d_1,
        data_d_1,
        valor_fechamento_d0,
        preco_fechamento_d0,
        data_d0,
        valor_fechamento_d1,
        preco_fechamento_d1,
        data_d1,

        -- Classificação D(-1) → D0
        CASE
            WHEN valor_fechamento_d0 IS NULL THEN NULL
            WHEN valor_fechamento_d0 < 0 THEN 'QUEDA'
            WHEN valor_fechamento_d0 > 0 THEN 'ALTA'
            WHEN valor_fechamento_d0 = 0 THEN 'MANTEVE'
        END AS tipo_variacao_d_1_d0

    FROM step9_base
)

SELECT
    ativo_atipico,
    data_evento,
    ativo,
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
    '{{SAFRA}}' AS safra
FROM step9
