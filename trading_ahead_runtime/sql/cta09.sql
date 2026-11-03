CREATE TABLE workspace_db.CTA9_VolumesQuantidades_{{SAFRA}}
WITH (format = 'PARQUET',
      external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA9_VolumesQuantidades_{{SAFRA}}/',
      partitioned_by = ARRAY['SAFRA']) AS
WITH base_raw AS (
    SELECT *,
        CAST(data_operacao AS DATE) AS data_operacao_date,
        CAST(data_evento AS DATE) AS data_evento_date,
        DATE_DIFF('day', CAST(data_operacao AS DATE), CAST(data_evento AS DATE)) AS dias_antes_fato,
        DATE_DIFF('day', CAST(data_evento AS DATE), CAST(data_operacao AS DATE)) AS dias_depois_fato
    FROM workspace_db.CTA8_DayTrader_{{SAFRA}}
),
base AS (
    SELECT DISTINCT * FROM base_raw
),
-- Histórico: 135 a 15 dias antes do evento atípico
hist AS (
    SELECT DISTINCT codigo_cliente, ativo_negociado, data_evento, tipo_operacao, data_operacao, ticker_empresa_vinculada, cnpj_empresa_vinculada, origem_operacao,
        volume_negociado, quantidade
    FROM base
    WHERE dias_antes_fato > 15 AND dias_antes_fato <= 135
),
agg_h AS (
    SELECT codigo_cliente, ativo_negociado, data_evento, ticker_empresa_vinculada, cnpj_empresa_vinculada, origem_operacao,
        SUM(CASE WHEN tipo_operacao='Compra' THEN volume_negociado ELSE 0 END) soma_vc,
        SUM(CASE WHEN tipo_operacao='Venda'  THEN volume_negociado ELSE 0 END) soma_vv,
        SUM(CASE WHEN tipo_operacao='Compra' THEN quantidade       ELSE 0 END) soma_qc,
        SUM(CASE WHEN tipo_operacao='Venda'  THEN quantidade       ELSE 0 END) soma_qv,
        SUM(CASE WHEN tipo_operacao='Compra' THEN volume_negociado*volume_negociado ELSE 0 END) ssq_vc,
        SUM(CASE WHEN tipo_operacao='Venda'  THEN volume_negociado*volume_negociado ELSE 0 END) ssq_vv,
        SUM(CASE WHEN tipo_operacao='Compra' THEN CAST(quantidade AS DECIMAL(38,0))*CAST(quantidade AS DECIMAL(38,0)) ELSE 0 END) ssq_qc,
        SUM(CASE WHEN tipo_operacao='Venda'  THEN CAST(quantidade AS DECIMAL(38,0))*CAST(quantidade AS DECIMAL(38,0)) ELSE 0 END) ssq_qv,
        COUNT(DISTINCT CASE WHEN tipo_operacao='Compra' THEN data_operacao END) n_dias_compra_historico,
        COUNT(DISTINCT CASE WHEN tipo_operacao='Venda'  THEN data_operacao END) n_dias_venda_historico
    FROM hist
    GROUP BY 1,2,3,4,5,6
),
desv_h AS (
    SELECT *,
        ROUND(soma_vc,2) soma_volume_compra_historica,
        ROUND(soma_vv,2) soma_volume_venda_historica,
        ROUND(soma_qc,2) soma_quantidade_compra_historica,
        ROUND(soma_qv,2) soma_quantidade_venda_historica,
        ROUND(soma_vc/120.0,2) media_volume_compra_historica,
        ROUND(soma_vv/120.0,2) media_volume_venda_historica,
        ROUND(soma_qc/120.0,2) media_quantidade_compra_historica,
        ROUND(soma_qv/120.0,2) media_quantidade_venda_historica,
        ROUND(SQRT((ssq_vc - POWER(soma_vc,2)/120.0)/120.0),2) desvio_volume_compra,
        ROUND(SQRT((ssq_vv - POWER(soma_vv,2)/120.0)/120.0),2) desvio_volume_venda,
        ROUND(SQRT((ssq_qc - POWER(soma_qc,2)/120.0)/120.0),2) desvio_quantidade_compra,
        ROUND(SQRT((ssq_qv - POWER(soma_qv,2)/120.0)/120.0),2) desvio_quantidade_venda
    FROM agg_h
),
-- 15 dias antes do evento atípico
d15 AS (
    SELECT DISTINCT codigo_cliente, ativo_negociado, data_evento, tipo_operacao, data_operacao, ticker_empresa_vinculada, cnpj_empresa_vinculada, origem_operacao,
        volume_negociado, quantidade
    FROM base
    WHERE dias_antes_fato > 0 AND dias_antes_fato <= 15
),
agg_15 AS (
    SELECT codigo_cliente, ativo_negociado, data_evento, ticker_empresa_vinculada, cnpj_empresa_vinculada, origem_operacao,
        SUM(CASE WHEN tipo_operacao='Compra' THEN volume_negociado ELSE 0 END) soma_volume_compra_15dias,
        SUM(CASE WHEN tipo_operacao='Venda'  THEN volume_negociado ELSE 0 END) soma_volume_venda_15dias,
        ROUND(SUM(CASE WHEN tipo_operacao='Compra' THEN volume_negociado ELSE 0 END)/15.0,2) media_volume_compra_15dias,
        ROUND(SUM(CASE WHEN tipo_operacao='Venda'  THEN volume_negociado ELSE 0 END)/15.0,2) media_volume_venda_15dias,
        SUM(CASE WHEN tipo_operacao='Compra' THEN quantidade ELSE 0 END) soma_quantidade_compra_15dias,
        SUM(CASE WHEN tipo_operacao='Venda'  THEN quantidade ELSE 0 END) soma_quantidade_venda_15dias,
        ROUND(SUM(CASE WHEN tipo_operacao='Compra' THEN quantidade ELSE 0 END)/15.0,2) media_quantidade_compra_15dias,
        ROUND(SUM(CASE WHEN tipo_operacao='Venda'  THEN quantidade ELSE 0 END)/15.0,2) media_quantidade_venda_15dias,
        COUNT(DISTINCT CASE WHEN tipo_operacao='Compra' THEN data_operacao END) n_dias_compra_pre,
        COUNT(DISTINCT CASE WHEN tipo_operacao='Venda'  THEN data_operacao END) n_dias_venda_pre
    FROM d15
    GROUP BY 1,2,3,4,5,6
),
-- D0: dia do evento atípico
d0 AS (
    SELECT DISTINCT codigo_cliente, ativo_negociado, data_evento, tipo_operacao, data_operacao, ticker_empresa_vinculada, cnpj_empresa_vinculada, origem_operacao,
        volume_negociado, quantidade
    FROM base
    WHERE data_operacao_date = data_evento_date
),
agg_d0 AS (
    SELECT codigo_cliente, ativo_negociado, data_evento, ticker_empresa_vinculada, cnpj_empresa_vinculada, origem_operacao,
        ROUND(SUM(CASE WHEN tipo_operacao='Compra' THEN volume_negociado ELSE 0 END),2) volume_compra_D0,
        ROUND(SUM(CASE WHEN tipo_operacao='Venda'  THEN volume_negociado ELSE 0 END),2) volume_venda_D0,
        ROUND(SUM(CASE WHEN tipo_operacao='Compra' THEN quantidade ELSE 0 END),2) quantidade_compra_D0,
        ROUND(SUM(CASE WHEN tipo_operacao='Venda'  THEN quantidade ELSE 0 END),2) quantidade_venda_D0,
        COUNT(DISTINCT CASE WHEN tipo_operacao='Compra' THEN data_operacao END) n_dias_compra_D0,
        COUNT(DISTINCT CASE WHEN tipo_operacao='Venda'  THEN data_operacao END) n_dias_venda_D0
    FROM d0
    GROUP BY 1,2,3,4,5,6
),
-- 5 dias depois do evento atípico
d5 AS (
    SELECT DISTINCT codigo_cliente, ativo_negociado, data_evento, tipo_operacao, data_operacao, ticker_empresa_vinculada, cnpj_empresa_vinculada, origem_operacao,
        volume_negociado, quantidade
    FROM base
    WHERE dias_depois_fato >= 1 AND dias_depois_fato <= 5
),
agg_d5 AS (
    SELECT codigo_cliente, ativo_negociado, data_evento, ticker_empresa_vinculada, cnpj_empresa_vinculada, origem_operacao,
        SUM(CASE WHEN tipo_operacao='Compra' THEN volume_negociado ELSE 0 END) soma_volume_compra_5diasDepois,
        SUM(CASE WHEN tipo_operacao='Venda'  THEN volume_negociado ELSE 0 END) soma_volume_venda_5diasDepois,
        ROUND(SUM(CASE WHEN tipo_operacao='Compra' THEN volume_negociado ELSE 0 END)/5.0,2) media_volume_compra_5diasDepois,
        ROUND(SUM(CASE WHEN tipo_operacao='Venda'  THEN volume_negociado ELSE 0 END)/5.0,2) media_volume_venda_5diasDepois,
        SUM(CASE WHEN tipo_operacao='Compra' THEN quantidade ELSE 0 END) soma_quantidade_compra_5diasDepois,
        SUM(CASE WHEN tipo_operacao='Venda'  THEN quantidade ELSE 0 END) soma_quantidade_venda_5diasDepois,
        ROUND(SUM(CASE WHEN tipo_operacao='Compra' THEN quantidade ELSE 0 END)/5.0,2) media_quantidade_compra_5diasDepois,
        ROUND(SUM(CASE WHEN tipo_operacao='Venda'  THEN quantidade ELSE 0 END)/5.0,2) media_quantidade_venda_5diasDepois,
        COUNT(DISTINCT CASE WHEN tipo_operacao='Compra' THEN data_operacao END) n_dias_compra_pos,
        COUNT(DISTINCT CASE WHEN tipo_operacao='Venda'  THEN data_operacao END) n_dias_venda_pos
    FROM d5
    GROUP BY 1,2,3,4,5,6
),
-- Base única com informações cadastrais (sem duplicatas por data_operacao/tipo_operacao)
base_unica AS (
    SELECT DISTINCT
        codigo_cliente, cpf_cnpj_cliente, ativo_negociado, ativo_negociado_4letras, pessoa_fisica,
        ticker_empresa_vinculada, cnpj_empresa_vinculada, Pessoa_Relacionada, linha_negocio, origem_operacao, data_evento, ativo_atipico,
        -- cnpj_companhia, 
        day_trader
    FROM base
    WHERE data_operacao_date >= DATE '{{SAFRA_INICIO}}'
      AND data_operacao_date <= DATE '{{SAFRA_FIM}}'
)
-- JOIN de todas agregações com base única
SELECT
    b.codigo_cliente, b.cpf_cnpj_cliente, b.ativo_negociado, b.ativo_negociado_4letras, b.pessoa_fisica,
    b.ticker_empresa_vinculada, b.cnpj_empresa_vinculada, b.Pessoa_Relacionada, b.linha_negocio, b.origem_operacao, b.data_evento, b.ativo_atipico,
    -- b.cnpj_companhia, 
    b.day_trader,
    COALESCE(h.soma_volume_compra_historica,0) soma_volume_compra_historica,
    COALESCE(h.soma_volume_venda_historica,0) soma_volume_venda_historica,
    COALESCE(h.media_volume_compra_historica,0) media_volume_compra_historica,
    COALESCE(h.media_volume_venda_historica,0) media_volume_venda_historica,
    COALESCE(h.desvio_volume_compra,0) desvio_volume_compra,
    COALESCE(h.desvio_volume_venda,0) desvio_volume_venda,
    COALESCE(h.soma_quantidade_compra_historica,0) soma_quantidade_compra_historica,
    COALESCE(h.soma_quantidade_venda_historica,0) soma_quantidade_venda_historica,
    COALESCE(h.media_quantidade_compra_historica,0) media_quantidade_compra_historica,
    COALESCE(h.media_quantidade_venda_historica,0) media_quantidade_venda_historica,
    COALESCE(h.desvio_quantidade_compra,0) desvio_quantidade_compra,
    COALESCE(h.desvio_quantidade_venda,0) desvio_quantidade_venda,
    -- COALESCE(h.n_dias_compra_historico,0) n_dias_compra_historico,
    -- COALESCE(h.n_dias_venda_historico,0) n_dias_venda_historico,
    COALESCE(d15.soma_volume_compra_15dias,0) soma_volume_compra_15dias,
    COALESCE(d15.soma_volume_venda_15dias,0) soma_volume_venda_15dias,
    COALESCE(d15.media_volume_compra_15dias,0) media_volume_compra_15dias,
    COALESCE(d15.media_volume_venda_15dias,0) media_volume_venda_15dias,
    COALESCE(d15.soma_quantidade_compra_15dias,0) soma_quantidade_compra_15dias,
    COALESCE(d15.soma_quantidade_venda_15dias,0) soma_quantidade_venda_15dias,
    COALESCE(d15.media_quantidade_compra_15dias,0) media_quantidade_compra_15dias,
    COALESCE(d15.media_quantidade_venda_15dias,0) media_quantidade_venda_15dias,
    -- COALESCE(d15.n_dias_compra_pre,0) n_dias_compra_pre,
    -- COALESCE(d15.n_dias_venda_pre,0) n_dias_venda_pre,
    COALESCE(d0.volume_compra_D0,0) volume_compra_D0,
    COALESCE(d0.volume_venda_D0,0) volume_venda_D0,
    COALESCE(d0.quantidade_compra_D0,0) quantidade_compra_D0,
    COALESCE(d0.quantidade_venda_D0,0) quantidade_venda_D0,
    -- COALESCE(d0.n_dias_compra_D0,0) n_dias_compra_D0,
    -- COALESCE(d0.n_dias_venda_D0,0) n_dias_venda_D0,
    COALESCE(d5.soma_volume_compra_5diasDepois,0) soma_volume_compra_5diasDepois,
    COALESCE(d5.soma_volume_venda_5diasDepois,0) soma_volume_venda_5diasDepois,
    COALESCE(d5.media_volume_compra_5diasDepois,0) media_volume_compra_5diasDepois,
    COALESCE(d5.media_volume_venda_5diasDepois,0) media_volume_venda_5diasDepois,
    COALESCE(d5.soma_quantidade_compra_5diasDepois,0) soma_quantidade_compra_5diasDepois,
    COALESCE(d5.soma_quantidade_venda_5diasDepois,0) soma_quantidade_venda_5diasDepois,
    COALESCE(d5.media_quantidade_compra_5diasDepois,0) media_quantidade_compra_5diasDepois,
    COALESCE(d5.media_quantidade_venda_5diasDepois,0) media_quantidade_venda_5diasDepois,
    -- COALESCE(d5.n_dias_compra_pos,0) n_dias_compra_pos,
    -- COALESCE(d5.n_dias_venda_pos,0) n_dias_venda_pos,
    '{{SAFRA}}' AS SAFRA
FROM base_unica b
LEFT JOIN desv_h h ON b.codigo_cliente=h.codigo_cliente AND b.ativo_negociado=h.ativo_negociado AND b.data_evento=h.data_evento AND b.ticker_empresa_vinculada=h.ticker_empresa_vinculada AND b.cnpj_empresa_vinculada=h.cnpj_empresa_vinculada AND b.origem_operacao=h.origem_operacao
LEFT JOIN agg_15 d15 ON b.codigo_cliente=d15.codigo_cliente AND b.ativo_negociado=d15.ativo_negociado AND b.data_evento=d15.data_evento AND b.ticker_empresa_vinculada=d15.ticker_empresa_vinculada AND b.cnpj_empresa_vinculada=d15.cnpj_empresa_vinculada AND b.origem_operacao=d15.origem_operacao
LEFT JOIN agg_d0 d0 ON b.codigo_cliente=d0.codigo_cliente AND b.ativo_negociado=d0.ativo_negociado AND b.data_evento=d0.data_evento AND b.ticker_empresa_vinculada=d0.ticker_empresa_vinculada AND b.cnpj_empresa_vinculada=d0.cnpj_empresa_vinculada AND b.origem_operacao=d0.origem_operacao
LEFT JOIN agg_d5 d5 ON b.codigo_cliente=d5.codigo_cliente AND b.ativo_negociado=d5.ativo_negociado AND b.data_evento=d5.data_evento AND b.ticker_empresa_vinculada=d5.ticker_empresa_vinculada AND b.cnpj_empresa_vinculada=d5.cnpj_empresa_vinculada AND b.origem_operacao=d5.origem_operacao
