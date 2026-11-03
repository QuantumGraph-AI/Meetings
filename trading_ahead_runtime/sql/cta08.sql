CREATE TABLE workspace_db.CTA8_DayTrader_{{SAFRA}}
WITH (
    format = 'PARQUET',
    external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA8_DayTrader_{{SAFRA}}/',
    partitioned_by = ARRAY['SAFRA']
) AS
WITH
-- Calcula dias entre operação e fato (CORRIGIDO: CAST de data_evento para DATE)
base_com_dias AS (
    SELECT *,
        DATE_DIFF('day', data_operacao, CAST(data_evento AS DATE)) AS dias_antes_fato
    FROM workspace_db.CTA7_OperacoesEventos_{{SAFRA}}
),

-- Filtra operações nos 15 dias antes do fato
operacoes_15dias AS (
    SELECT *
    FROM base_com_dias
    WHERE dias_antes_fato > 0 AND dias_antes_fato <= 15
),

-- Identifica dias com compra E venda (CORRIGIDO: vírgula após data_evento)
dias_compra_venda AS (
    SELECT
        codigo_cliente,
        data_evento,
        ativo_negociado,
        cnpj_empresa_vinculada,
        ticker_empresa_vinculada,
        data_operacao,
        CASE
            WHEN COUNT(DISTINCT tipo_operacao) = 2 THEN 1
            ELSE 0
        END AS teve_compra_e_venda
    FROM operacoes_15dias
    GROUP BY codigo_cliente, data_evento, ativo_negociado, cnpj_empresa_vinculada, ticker_empresa_vinculada, data_operacao
),

-- Conta quantos dias teve compra e venda
contagem_dias AS (
    SELECT
        codigo_cliente,
        data_evento,
        ativo_negociado,
        cnpj_empresa_vinculada,
        ticker_empresa_vinculada,
        SUM(teve_compra_e_venda) AS dias_compra_venda,
        CASE
            WHEN SUM(teve_compra_e_venda) >= 8 THEN 'S'
            ELSE 'N'
        END AS day_trader
    FROM dias_compra_venda
    GROUP BY codigo_cliente, data_evento, ativo_negociado, cnpj_empresa_vinculada, ticker_empresa_vinculada
)

-- Adiciona flag day_trader à base original
SELECT DISTINCT
    b.codigo_cliente,
    b.cpf_cnpj_cliente,
    b.tipo_operacao,
    b.ativo_negociado,
    b.ativo_negociado_4letras,
    b.preco,
    b.quantidade,
    b.volume_negociado,
    b.data_operacao,
    b.pessoa_fisica,
    b.cnpj_empresa_vinculada,
    b.ticker_empresa_vinculada,
    b.Pessoa_Relacionada,
    b.linha_negocio,
    b.origem_operacao,
    b.data_evento,
    b.ativo_atipico,
    -- b.cnpj_companhia,
    COALESCE(c.day_trader, 'N') AS day_trader,
    b.SAFRA
FROM workspace_db.CTA7_OperacoesEventos_{{SAFRA}} b
LEFT JOIN contagem_dias c
    ON b.codigo_cliente = c.codigo_cliente
    AND b.data_evento = c.data_evento
    AND b.ativo_negociado = c.ativo_negociado
    AND b.ticker_empresa_vinculada = c.ticker_empresa_vinculada
