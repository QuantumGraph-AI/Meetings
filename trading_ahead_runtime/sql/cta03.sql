
CREATE TABLE workspace_db.CTA3_EventosAtipicos_{{SAFRA}}
WITH (
    format = 'PARQUET',
    external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA3_EventosAtipicos_{{SAFRA}}/',
    partitioned_by = ARRAY['SAFRA']
) AS
WITH
-- Passo 1: Carregar os Fatos Relevantes do período da safra
fatos_relevantes AS (
    SELECT DISTINCT
        tb_rss.ticker_4letras AS ticker_fr,
        tb_wa5.data_entrega
    FROM
        db_corp_gestaoderiscos_alertastradesurveillance_sor_01.tb_rss_base_ticker_sor tb_rss
    INNER JOIN
        db_corp_modelagem_historicalmarketdata_sor_01.tb_wa5_sor_br_gov_cvm_ipe_cia_aberta tb_wa5
    ON
        LPAD(REPLACE(tb_rss.cnpj_companhia, '.0', ''), 14, '0') =
        LPAD(REPLACE(REPLACE(REPLACE(tb_wa5.cnpj_companhia, '.', ''), '-', ''), '/', ''), 14, '0')
    WHERE
        tb_wa5.data_entrega BETWEEN DATE('{{SAFRA_INICIO}}') AND DATE('{{SAFRA_FIM}}')
        AND tb_wa5.versao = 1
        AND (
            (tb_wa5.categoria = 'Dados Econômico-Financeiros'
                AND tb_wa5.tipo = 'Press-release')
            OR
            (tb_wa5.categoria IN ('Fato Relevante', 'OPA - Edital de Oferta Pública de Ações'))
            OR
            (tb_wa5.categoria = 'Comunicado ao Mercado'
                AND tb_wa5.tipo IN (
                    'Aquisição/Alienação de Participação Acionária (art. 12 da Instr. CVM nº 358)',
                    'Esclaecimentos sobre questionamentos da CVM/B3',
                    'Instalação, mudança ou dissolução do Comitê de Auditoria Estatutário',
                    'Mudança de Auditor (art. 28, Instr. CVM nº308/99)'))
        )
        AND tb_rss.ticker_4letras IS NOT NULL
),

-- Passo 2: Expandir datas dos Fatos Relevantes (data_entrega + 0 e +1 dia)
-- dias_apos_fr = 2, então consideramos data_entrega e data_entrega+1
fatos_relevantes_expandidos AS (
    SELECT
        ticker_fr,
        data_entrega AS data_pos_fr
    FROM fatos_relevantes

    UNION ALL

    SELECT
        ticker_fr,
        DATE_ADD('day', 1, data_entrega) AS data_pos_fr
    FROM fatos_relevantes
),

-- Passo 3: Carregar extrapolações da CTA2
extrapolacoes AS (
    SELECT
        data_fechamento,
        ativo,
        valor_fechamento,
        ativo_4letras,
        SAFRA
    FROM workspace_db.CTA2_VariacaoPreco_{{SAFRA}}
),

-- Passo 4: LEFT JOIN para identificar extrapolações que NÃO coincidem com FR
extrapolacoes_filtradas AS (
    SELECT
        e.data_fechamento,
        e.ativo,
        e.valor_fechamento,
        e.ativo_4letras,
        e.SAFRA,
        fr.ticker_fr,
        fr.data_pos_fr
    FROM extrapolacoes e
    LEFT JOIN fatos_relevantes_expandidos fr
        ON e.ativo_4letras = fr.ticker_fr
        AND DATE_PARSE(e.data_fechamento, '%Y-%m-%d') = fr.data_pos_fr
)

-- Passo 5: Retornar apenas extrapolações SEM justificativa de FR (ticker_fr IS NULL)
SELECT
    data_fechamento AS data_evento,
    ativo AS ativo_atipico,
    -- valor_fechamento,
    -- ativo_4letras,
    SAFRA
FROM extrapolacoes_filtradas
WHERE ticker_fr IS NULL  -- Mantém apenas registros sem match com FR
