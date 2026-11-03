
CREATE TABLE workspace_db.CTA2_VariacaoPreco_{{SAFRA}}
WITH (
    format = 'PARQUET',
    external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA2_VariacaoPreco_{{SAFRA}}/',
    partitioned_by = ARRAY['SAFRA']
) AS
WITH
-- Passo 1: Definir datas importantes
parametros AS (
    SELECT
        DATE('{{SAFRA_INICIO}}') AS data_inicio_safra,
        DATE('{{SAFRA_FIM}}') AS data_fim_safra,
        DATE_ADD('day', -182, DATE('{{SAFRA_INICIO}}')) AS ref_inicio,
        DATE_ADD('day', -1, DATE('{{SAFRA_INICIO}}')) AS ref_fim,
        4 AS k_bollinger  -- Pode parametrizar depois se necessário
),

-- Passo 2: Filtrar apenas dias úteis (seg-sex, sem feriados brasileiros principais)
dados_filtrados AS (
    SELECT
        p.data_fechamento,
        p.ativo,
        p.valor_fechamento,
        p.ativo_4letras
    FROM workspace_db.CTA1_Precos_{{SAFRA}} p
),

-- Passo 3: Calcular estatísticas do período de referência (182 dias antes da safra)
estatisticas_referencia AS (
    SELECT
        d.ativo,
        AVG(d.valor_fechamento) AS media_ref,
        STDDEV_SAMP(d.valor_fechamento) AS desvio_ref,
        COUNT(*) AS n_dias_ref
    FROM dados_filtrados d
    CROSS JOIN parametros par
    WHERE
        DATE_PARSE(d.data_fechamento, '%Y-%m-%d') >= par.ref_inicio
        AND DATE_PARSE(d.data_fechamento, '%Y-%m-%d') <= par.ref_fim
    GROUP BY d.ativo
    HAVING COUNT(*) > 0  -- Só considera ativos com dados no período de referência
),

-- Passo 4: Calcular bandas inicialmente usando SEMPRE a fórmula: média ± (k * desvio)
bandas_calculadas AS (
    SELECT
        d.data_fechamento,
        d.ativo,
        d.valor_fechamento,
        d.ativo_4letras,
        e.media_ref,
        e.desvio_ref,
        par.k_bollinger,
        -- Calcula bandas SEMPRE com desvio padrão
        e.media_ref + (par.k_bollinger * e.desvio_ref) AS banda_sup_calculada,
        e.media_ref - (par.k_bollinger * e.desvio_ref) AS banda_inf_calculada
    FROM dados_filtrados d
    INNER JOIN estatisticas_referencia e ON d.ativo = e.ativo
    CROSS JOIN parametros par
    WHERE
        -- Filtra apenas período da safra
        DATE_PARSE(d.data_fechamento, '%Y-%m-%d') >= par.data_inicio_safra
        AND DATE_PARSE(d.data_fechamento, '%Y-%m-%d') <= par.data_fim_safra
),

-- Passo 5: Aplicar ajuste condicional para FII (finais '11')
-- REGRA: Para FII, substitui AMBAS as bandas APENAS quando:
--   banda_sup < 1.04% E banda_inf > -0.89% (bandas "muito estreitas")
-- Em qualquer outro caso, MANTÉM os valores calculados
dados_com_bandas AS (
    SELECT
        data_fechamento,
        ativo,
        valor_fechamento,
        ativo_4letras,
        media_ref,
        desvio_ref,
        k_bollinger,
        -- Banda superior ajustada
        CASE
            WHEN SUBSTR(ativo, -2) = '11'
                AND banda_sup_calculada < 0.0104
                AND banda_inf_calculada > -0.0089
            THEN 0.0104
            ELSE banda_sup_calculada
        END AS banda_superior,
        -- Banda inferior ajustada
        CASE
            WHEN SUBSTR(ativo, -2) = '11'
                AND banda_sup_calculada < 0.0104
                AND banda_inf_calculada > -0.0089
            THEN -0.0089
            ELSE banda_inf_calculada
        END AS banda_inferior,
        -- Armazena valores originais para log
        banda_sup_calculada,
        banda_inf_calculada
    FROM bandas_calculadas
),

-- Passo 6: Detectar extrapolações usando as bandas ajustadas
dados_com_extrapolacao AS (
    SELECT
        data_fechamento,
        ativo,
        valor_fechamento,
        ativo_4letras,
        media_ref,
        desvio_ref,
        k_bollinger,
        banda_superior,
        banda_inferior,
        -- Detecta extrapolação: valor fora das bandas ajustadas
        CASE
            WHEN valor_fechamento > banda_superior THEN 1
            WHEN valor_fechamento < banda_inferior THEN 1
            ELSE 0
        END AS extrapola
    FROM dados_com_bandas
),

-- Passo 7: Identificar ativos que tiveram pelo menos 1 extrapolação
ativos_com_extrapolacao AS (
    SELECT DISTINCT ativo
    FROM dados_com_extrapolacao
    WHERE extrapola = 1
)

-- Passo 8: Retornar apenas registros de extrapolação de ativos que tiveram pelo menos 1 extrapolação
SELECT
    dce.data_fechamento,
    dce.ativo,
    dce.valor_fechamento,
    dce.ativo_4letras,
    '{{SAFRA}}' AS SAFRA
FROM dados_com_extrapolacao dce
INNER JOIN ativos_com_extrapolacao ace ON dce.ativo = ace.ativo
WHERE dce.extrapola = 1
