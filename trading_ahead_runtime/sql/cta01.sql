
CREATE TABLE workspace_db.CTA1_Precos_{{SAFRA}}
WITH (
    format = 'PARQUET',
    external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA1_Precos_{{SAFRA}}/',
    partitioned_by = ARRAY['SAFRA']
) AS
WITH
-- Passo 1: Extração e deduplicação de preços
dados_ordenados AS (
    SELECT
        DATE_FORMAT(
            DATE_PARSE(
                SUBSTR(dat_base_info, 1, 4) || '-' || SUBSTR(dat_base_info, 5, 2) || '-' || SUBSTR(dat_base_info, 7, 2),
                '%Y-%m-%d'
            ),
            '%Y-%m-%d'
        ) AS data_fechamento,
        nom_ativ_finn AS ativo,
        CAST(vlr_atbt AS DOUBLE) AS preco_fechamento,
        ROW_NUMBER() OVER (
            PARTITION BY
                DATE_FORMAT(
                    DATE_PARSE(
                        SUBSTR(dat_base_info, 1, 4) || '-' || SUBSTR(dat_base_info, 5, 2) || '-' || SUBSTR(dat_base_info, 7, 2),
                        '%Y-%m-%d'
                    ),
                    '%Y-%m-%d'
                ),
                nom_ativ_finn
            ORDER BY
                CAST(COALESCE(NULLIF(timestamp, ''), '0') AS DOUBLE) DESC,
                CAST(COALESCE(NULLIF(versao, ''), '0') AS BIGINT) DESC
        ) AS rn
    FROM
        db_corp_gestaoderiscos_distribuiprecoscorporativos_sor_01.tbim8_acao_btres
    WHERE
        DATE_PARSE(
            SUBSTR(dat_base_info, 1, 4) || '-' || SUBSTR(dat_base_info, 5, 2) || '-' || SUBSTR(dat_base_info, 7, 2),
            '%Y-%m-%d'
        ) BETWEEN DATE_ADD('day', -365, DATE('{{SAFRA_INICIO}}'))
            AND DATE_ADD('day', 7, DATE('{{SAFRA_FIM}}'))
        AND idt_atbt = '11'
        AND nom_font_info = 'B3'
        -- AND nom_ativ_finn = 'BMLT11'
),

-- Passo 2: Remove duplicatas
dados_unicos AS (
    SELECT
        data_fechamento,
        ativo,
        preco_fechamento
    FROM dados_ordenados
    WHERE rn = 1
        AND SUBSTR(ativo, 5) IN ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12')
),

-- Passo 3: Filtrar apenas dias úteis (seg-sex, sem feriados brasileiros principais)
dados_filtrados AS (
    SELECT
        data_fechamento,
        ativo,
        preco_fechamento
    FROM dados_unicos
    WHERE
        -- Remove finais de semana
        DAY_OF_WEEK(DATE_PARSE(data_fechamento, '%Y-%m-%d')) NOT IN (6, 7)  -- 6=sábado, 7=domingo
        -- Remove feriados brasileiros principais
        AND DATE_PARSE(data_fechamento, '%Y-%m-%d') NOT IN (
            DATE('{{SAFRA_ANO}}-01-01'),  -- Ano Novo
            DATE('{{SAFRA_ANO}}-04-18'),  -- Sexta-feira Santa (ajustar conforme ano)
            DATE('{{SAFRA_ANO}}-04-21'),  -- Tiradentes
            DATE('{{SAFRA_ANO}}-05-01'),  -- Dia do Trabalho
            DATE('{{SAFRA_ANO}}-09-07'),  -- Independência
            DATE('{{SAFRA_ANO}}-10-12'),  -- N. Sra. Aparecida
            DATE('{{SAFRA_ANO}}-11-02'),  -- Finados
            DATE('{{SAFRA_ANO}}-11-15'),  -- Proclamação da República
            DATE('{{SAFRA_ANO}}-11-20'),  -- Consciência Negra
            DATE('{{SAFRA_ANO}}-12-25')   -- Natal
        )
),

-- Passo 4: Calcular variação percentual em relação ao dia anterior (por ativo)
dados_com_variacao AS (
    SELECT
        data_fechamento,
        ativo,
        preco_fechamento,
        -- Pega o preço do dia anterior para o mesmo ativo
        LAG(preco_fechamento) OVER (
            PARTITION BY ativo
            ORDER BY DATE_PARSE(data_fechamento, '%Y-%m-%d') ASC
        ) AS preco_anterior,
        SUBSTR(ativo, 1, 4) AS ativo_4letras
    FROM dados_filtrados
)

-- SELECT final com cálculo de RETORNO LOGARÍTMICO
SELECT
    data_fechamento,
    ativo,
    preco_fechamento,
    ativo_4letras,
    -- ALTERAÇÃO: Calcula retorno logarítmico ln(preco_hoje / preco_ontem)
    CASE
        WHEN preco_anterior IS NULL THEN NULL           -- Primeiro registro de cada ativo não tem variação
        WHEN preco_anterior <= 0 THEN NULL              -- Evita ln de zero ou negativo
        WHEN preco_fechamento <= 0 THEN NULL            -- Evita ln de zero ou negativo
        ELSE LN(preco_fechamento / preco_anterior)      -- Retorno logarítmico
    END AS valor_fechamento,
    '{{SAFRA}}' AS SAFRA
FROM dados_com_variacao
