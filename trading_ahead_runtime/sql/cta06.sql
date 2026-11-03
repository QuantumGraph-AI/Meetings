CREATE TABLE workspace_db.CTA6_Operacoes_{{SAFRA}}
WITH (
    format = 'PARQUET',
    external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA6_Operacoes_{{SAFRA}}/',
    partitioned_by = ARRAY['SAFRA']
) AS
WITH
-- 0) Buscar ativos atípicos da CTA5 (NOVA CTE)
ativos_atipicos AS (
    SELECT DISTINCT ativo_atipico
    FROM workspace_db.CTA5_EstudoVariacao_{{SAFRA}}
    WHERE ativo_atipico IS NOT NULL
),
-- 1) Parâmetros de datas
datas AS (
    SELECT *
    FROM (
        VALUES (
        '{{SAFRA_INICIO}}',
        '{{SAFRA_FIM}}',
        '{{SAFRA}}',
        '{{SAFRA_ANO}}',
        '{{SAFRA_MES}}'
        )
    ) AS d(
        anomesdia_inicio,
        anomesdia_fim,
        anomes,
        ano,
        mes
    )
),
periodo AS (
    SELECT ano,
           mes,
           CAST(anomesdia_inicio AS DATE) - interval '150' day AS inicio_periodo,
           CAST(anomesdia_fim AS DATE) + interval '7' day AS fim_periodo
    FROM datas
),
operacoes_filtradas AS (
    SELECT cliente AS codigo_cliente,
           conta_negociacao,
           conta_master,
           lado AS tipo_operacao,
           ativo AS ativo_negociado,
           -- Remove o 'F' do final do ativo para normalização (ex: PETR3F -> PETR3)
           CASE
               WHEN SUBSTRING(ativo, LENGTH(ativo), 1) = 'F'
               THEN SUBSTRING(ativo, 1, LENGTH(ativo) - 1)
               ELSE ativo
           END AS ativo_normalizado,
           preco,
           quantidade,
           volume_negociado,
           mercado,
           CAST(SUBSTRING(data_pregao, 1, 10) AS DATE) AS data_operacao,
           ano_pregao,
           mes_pregao,
           dia_pregao,
           hora_transacao,
           broker,
           tipo_operacao AS origem_operacao,
           CONCAT(ano_pregao, mes_pregao) AS mes_operacao
    FROM db_corp_corretora_inoabrokertools_sot_01.tb_ep7_alocacoes_sot
    CROSS JOIN periodo
    WHERE CAST(SUBSTRING(data_pregao, 1, 10) AS DATE) >= (SELECT inicio_periodo FROM periodo)
      AND CAST(SUBSTRING(data_pregao, 1, 10) AS DATE) <= (SELECT fim_periodo FROM periodo)
      AND SUBSTRING(ativo, 5) IN ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '1F', '2F', '3F', '4F', '5F', '6F', '7F', '8F', '9F', '10F', '11F', '12F')
      AND mercado = 'XBSP'
      AND tipo_operacao IN ('local', 'repasse_enviado', 'repasse_recebido')
      AND ativo IN (SELECT ativo_atipico FROM ativos_atipicos)
),
clientes_operaram_maio AS (
    SELECT DISTINCT codigo_cliente
    FROM operacoes_filtradas
    WHERE data_operacao >= DATE '{{SAFRA_INICIO}}'
      AND data_operacao <= DATE '{{SAFRA_FIM}}'
),
operacoes_filtradas_maio AS (
    SELECT *
    FROM operacoes_filtradas
    WHERE codigo_cliente IN (SELECT codigo_cliente FROM clientes_operaram_maio)
),
-- Buscar TODOS os complementos (não só dos clientes que operaram) para conseguir fazer o JOIN pelo nome
dfComplementosRawTodos AS (
    SELECT
        CAST(codigo_conta AS VARCHAR) AS codigo_cliente,
        LPAD(cpf_cnpj, 14, '0') AS cpf_cnpj_cliente,
        tipo_cliente,
        tipo_investidor,
        pessoa_fisica,
        ultima_atualizacao AS data_atualizacao
    FROM db_corp_corretora_inoabrokertools_sot_01.tb_ep7_complementos_contas_sot
    WHERE CAST(codigo_conta AS VARCHAR) IN (SELECT DISTINCT codigo_cliente FROM operacoes_filtradas_maio)
      AND data_referencia = (SELECT MAX(data_referencia) FROM db_corp_corretora_inoabrokertools_sot_01.tb_ep7_complementos_contas_sot)
),
-- Mapeamento CPF -> CPF substituído (apenas para PersonForeignInvestor com CPF 990%)
mapa_substituicao_inr AS (
    SELECT DISTINCT
        orig.cpf_cnpj_cliente AS cpf_original,
        COALESCE(norm.cpf_cnpj_cliente, orig.cpf_cnpj_cliente) AS cpf_substituido
    FROM (
        SELECT DISTINCT
            LPAD(cpf_cnpj, 14, '0') AS cpf_cnpj_cliente,
            tipo_cliente,
            apelido
        FROM db_corp_corretora_inoabrokertools_sot_01.tb_ep7_complementos_contas_sot
        WHERE data_referencia = (SELECT MAX(data_referencia) FROM db_corp_corretora_inoabrokertools_sot_01.tb_ep7_complementos_contas_sot)
          AND tipo_cliente = 'PersonForeignInvestor'
          AND LPAD(cpf_cnpj, 14, '0') LIKE '990%'
    ) orig
    LEFT JOIN (
        SELECT DISTINCT
            LPAD(cpf_cnpj, 14, '0') AS cpf_cnpj_cliente,
            apelido
        FROM db_corp_corretora_inoabrokertools_sot_01.tb_ep7_complementos_contas_sot
        WHERE data_referencia = (SELECT MAX(data_referencia) FROM db_corp_corretora_inoabrokertools_sot_01.tb_ep7_complementos_contas_sot)
          AND tipo_cliente = 'Normal'
    ) norm
    ON TRIM(LOWER(orig.apelido)) = TRIM(LOWER(norm.apelido))
),
-- Aplicar substituição nos complementos
dfComplementosRaw AS (
    SELECT
        t.codigo_cliente,
        COALESCE(m.cpf_substituido, t.cpf_cnpj_cliente) AS cpf_cnpj_cliente,
        t.tipo_cliente,
        t.tipo_investidor,
        t.pessoa_fisica,
        t.data_atualizacao
    FROM dfComplementosRawTodos t
    LEFT JOIN mapa_substituicao_inr m
        ON t.cpf_cnpj_cliente = m.cpf_original
),
dfComplementosDistinct AS (
    SELECT DISTINCT codigo_cliente, cpf_cnpj_cliente, tipo_cliente, pessoa_fisica
    FROM dfComplementosRaw
),
dfLiger AS (
    SELECT LPAD(t.cd_cpfcgc, 14, '0') AS cpf_cnpj_cliente,
           t.in_politico_exp AS indicador_exposicao_politica_liger,
           t.in_pess_vinc AS indicador_pessoa_vinculada_liger,
           t.dt_atualiz AS data_atualizacao_liger
    FROM db_corp_corretora_sinacor_sor_01.tb_eg9_tsccliger t
    WHERE ano_carga_ptc = (SELECT MAX(ano_carga_ptc) FROM db_corp_corretora_sinacor_sor_01.tb_eg9_tsccliger)
      AND mes_carga_ptc = (
        SELECT MAX(mes_carga_ptc)
        FROM db_corp_corretora_sinacor_sor_01.tb_eg9_tsccliger
        WHERE ano_carga_ptc = t.ano_carga_ptc
      )
      AND dia_carga_ptc = (
        SELECT MAX(dia_carga_ptc)
        FROM db_corp_corretora_sinacor_sor_01.tb_eg9_tsccliger
        WHERE ano_carga_ptc = t.ano_carga_ptc
          AND mes_carga_ptc = t.mes_carga_ptc
      )
      AND cd_cpfcgc <> ''
),
dfLigerDistinct AS (
    SELECT cpf_cnpj_cliente, indicador_exposicao_politica_liger, indicador_pessoa_vinculada_liger
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY cpf_cnpj_cliente ORDER BY data_atualizacao_liger DESC) AS rank
        FROM dfLiger
    ) t
    WHERE rank = 1
      AND cpf_cnpj_cliente IN (SELECT cpf_cnpj_cliente FROM dfComplementosDistinct)
),
dfComplementosLiger AS (
    SELECT
        c.codigo_cliente,
        LTRIM(REGEXP_REPLACE(c.cpf_cnpj_cliente, '[^0-9]', ''), '0') AS cpf_cnpj_cliente,
        c.tipo_cliente,
        c.pessoa_fisica,
        liger.indicador_exposicao_politica_liger,
        liger.indicador_pessoa_vinculada_liger
    FROM dfComplementosDistinct c
    LEFT JOIN dfLigerDistinct liger
        ON c.cpf_cnpj_cliente = liger.cpf_cnpj_cliente
),
resultado_principal AS (
    SELECT LTRIM(REGEXP_REPLACE(mc.cpf, '[^0-9]', ''), '0') AS doc_clean,
           LTRIM(REGEXP_REPLACE(mc.cnpj_companhia, '[^0-9]', ''), '0') AS cnpj_companhia,
           mc.cargo_ocupado AS cargo_empresa_vinculada
      FROM db_corp_modelagem_historicalmarketdata_sor_01.tb_wa5_sor_br_gov_cvm_fre_cia_aberta_membro_comite mc WHERE mc.cpf IS NOT NULL AND mc.cnpj_companhia IS NOT NULL
    UNION ALL SELECT LTRIM(REGEXP_REPLACE(ad.cpf, '[^0-9]', ''), '0'),
                     LTRIM(REGEXP_REPLACE(ad.cnpj_companhia, '[^0-9]', ''), '0'),
                     ad.cargo_eletivo_ocupado
      FROM db_corp_modelagem_historicalmarketdata_sor_01.tb_wa5_sor_br_gov_cvm_fre_cia_aberta_administrador_membro_conselho_fiscal ad WHERE ad.cpf IS NOT NULL AND ad.cnpj_companhia IS NOT NULL
    UNION ALL SELECT LTRIM(REGEXP_REPLACE(ps.cnpj, '[^0-9]', ''), '0'),
                     LTRIM(REGEXP_REPLACE(ps.cnpj_companhia, '[^0-9]', ''), '0'),
                     ps.razao_social
      FROM db_corp_modelagem_historicalmarketdata_sor_01.tb_wa5_sor_br_gov_cvm_fre_cia_aberta_participacao_sociedade ps WHERE ps.cnpj IS NOT NULL AND ps.cnpj_companhia IS NOT NULL
    UNION ALL SELECT LTRIM(REGEXP_REPLACE(pa.cpf_cnpj_acionista, '[^0-9]', ''), '0'),
                     LTRIM(REGEXP_REPLACE(pa.cnpj_companhia, '[^0-9]', ''), '0'),
                     pa.acionista_relacionado
      FROM db_corp_modelagem_historicalmarketdata_sor_01.tb_wa5_sor_br_gov_cvm_fre_cia_aberta_posicao_acionaria pa WHERE pa.cpf_cnpj_acionista IS NOT NULL AND pa.cnpj_companhia IS NOT NULL
    UNION ALL SELECT LTRIM(REGEXP_REPLACE(rf.cpf_pessoa_relacionada, '[^0-9]', ''), '0'),
                     LTRIM(REGEXP_REPLACE(rf.cnpj_companhia, '[^0-9]', ''), '0'),
                     rf.cargo_pessoa_relacionada
      FROM db_corp_modelagem_historicalmarketdata_sor_01.tb_wa5_sor_br_gov_cvm_fre_cia_aberta_relacao_familiar rf WHERE rf.cpf_pessoa_relacionada IS NOT NULL AND rf.cnpj_companhia IS NOT NULL
    UNION ALL SELECT LTRIM(REGEXP_REPLACE(rs.cpf_administrador, '[^0-9]', ''), '0'),
                     LTRIM(REGEXP_REPLACE(rs.cnpj_companhia, '[^0-9]', ''), '0'),
                     rs.cargo_administrador
      FROM db_corp_modelagem_historicalmarketdata_sor_01.tb_wa5_sor_br_gov_cvm_fre_cia_aberta_relacao_subordinacao rs WHERE rs.cpf_administrador IS NOT NULL AND rs.cnpj_companhia IS NOT NULL
    UNION ALL SELECT LTRIM(REGEXP_REPLACE(ce.cpf_cnpj, '[^0-9]', ''), '0'),
                     LTRIM(REGEXP_REPLACE(ce.cnpj_empresa_empregadora, '[^0-9]', ''), '0'),
                     NULL
      FROM db_corp_corretora_inoabrokertools_sor_01.tb_ep7_re_complementos_investidores_sor ce WHERE ce.cpf_cnpj IS NOT NULL AND ce.cnpj_empresa_empregadora IS NOT NULL
),
vinculos_doc_companhia AS (
    SELECT DISTINCT
        rp.doc_clean,
        rp.cnpj_companhia
    FROM resultado_principal rp
),
base_ticker AS (
    SELECT
        LPAD(REPLACE(REPLACE(REPLACE(cnpj_companhia, '/', ''), '-', ''), '.', ''), 14, '0') AS cnpj_companhia,
        nome_empresarial AS nome_empresa_vinculada,
        ticker_4letras AS ticker_empresa_vinculada,
        CONCAT(CAST(ano_captura AS VARCHAR), LPAD(CAST(mes_captura AS VARCHAR), 2, '0')) as data_referencia
    FROM db_corp_gestaoderiscos_alertastradesurveillance_sor_01.tb_rss_base_ticker_sor
),
base_ticker_distinct AS (
    SELECT t1.*
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY cnpj_companhia ORDER BY data_referencia DESC) AS rn
        FROM base_ticker
    ) t1
    WHERE rn = 1
),
operacoes_com_vinculos AS (
    SELECT o.*,
           COALESCE(v.cnpj_companhia, '0') AS cnpj_empresa_vinculada,
           l.cpf_cnpj_cliente,
           l.pessoa_fisica,
           l.tipo_cliente,
           l.indicador_exposicao_politica_liger,
           l.indicador_pessoa_vinculada_liger
    FROM operacoes_filtradas_maio o
    LEFT JOIN dfComplementosLiger l ON o.codigo_cliente = l.codigo_cliente
    LEFT JOIN vinculos_doc_companhia v
        ON l.cpf_cnpj_cliente = v.doc_clean
),
adm_conselho_fiscal AS (
    SELECT DISTINCT
        LTRIM(REGEXP_REPLACE(ad.cpf, '[^0-9]', ''), '0') AS doc_clean,
        LTRIM(REGEXP_REPLACE(ad.cnpj_companhia, '[^0-9]', ''), '0') AS cnpj_companhia
    FROM db_corp_modelagem_historicalmarketdata_sor_01.tb_wa5_sor_br_gov_cvm_fre_cia_aberta_administrador_membro_conselho_fiscal ad
    WHERE ad.cpf IS NOT NULL AND ad.cnpj_companhia IS NOT NULL
),
membro_comite AS (
    SELECT DISTINCT
        LTRIM(REGEXP_REPLACE(mc.cpf, '[^0-9]', ''), '0') AS doc_clean,
        LTRIM(REGEXP_REPLACE(mc.cnpj_companhia, '[^0-9]', ''), '0') AS cnpj_companhia
    FROM db_corp_modelagem_historicalmarketdata_sor_01.tb_wa5_sor_br_gov_cvm_fre_cia_aberta_membro_comite mc
    WHERE mc.cpf IS NOT NULL AND mc.cnpj_companhia IS NOT NULL
),
participacao_sociedade AS (
    SELECT DISTINCT
        LTRIM(REGEXP_REPLACE(ps.cnpj, '[^0-9]', ''), '0') AS doc_clean,
        LTRIM(REGEXP_REPLACE(ps.cnpj_companhia, '[^0-9]', ''), '0') AS cnpj_companhia
    FROM db_corp_modelagem_historicalmarketdata_sor_01.tb_wa5_sor_br_gov_cvm_fre_cia_aberta_participacao_sociedade ps
    WHERE ps.cnpj IS NOT NULL AND ps.cnpj_companhia IS NOT NULL
),
posicao_acionaria AS (
    SELECT DISTINCT
        LTRIM(REGEXP_REPLACE(pa.cpf_cnpj_acionista, '[^0-9]', ''), '0') AS doc_clean,
        LTRIM(REGEXP_REPLACE(pa.cnpj_companhia, '[^0-9]', ''), '0') AS cnpj_companhia
    FROM db_corp_modelagem_historicalmarketdata_sor_01.tb_wa5_sor_br_gov_cvm_fre_cia_aberta_posicao_acionaria pa
    WHERE pa.cpf_cnpj_acionista IS NOT NULL AND pa.cnpj_companhia IS NOT NULL
),
relacao_familiar AS (
    SELECT DISTINCT
        LTRIM(REGEXP_REPLACE(rf.cpf_pessoa_relacionada, '[^0-9]', ''), '0') AS doc_clean,
        LTRIM(REGEXP_REPLACE(rf.cnpj_companhia, '[^0-9]', ''), '0') AS cnpj_companhia
    FROM db_corp_modelagem_historicalmarketdata_sor_01.tb_wa5_sor_br_gov_cvm_fre_cia_aberta_relacao_familiar rf
    WHERE rf.cpf_pessoa_relacionada IS NOT NULL AND rf.cnpj_companhia IS NOT NULL
),
relacao_subordinacao AS (
    SELECT DISTINCT
        LTRIM(REGEXP_REPLACE(rs.cpf_administrador, '[^0-9]', ''), '0') AS doc_clean,
        LTRIM(REGEXP_REPLACE(rs.cnpj_companhia, '[^0-9]', ''), '0') AS cnpj_companhia
    FROM db_corp_modelagem_historicalmarketdata_sor_01.tb_wa5_sor_br_gov_cvm_fre_cia_aberta_relacao_subordinacao rs
    WHERE rs.cpf_administrador IS NOT NULL AND rs.cnpj_companhia IS NOT NULL
),
relacao_empregador AS (
    SELECT DISTINCT
        LTRIM(REGEXP_REPLACE(ce.cpf_cnpj, '[^0-9]', ''), '0') AS doc_clean,
        LTRIM(REGEXP_REPLACE(ce.cnpj_empresa_empregadora, '[^0-9]', ''), '0') AS cnpj_companhia
    FROM db_corp_corretora_inoabrokertools_sor_01.tb_ep7_re_complementos_investidores_sor ce
    WHERE ce.cpf_cnpj IS NOT NULL AND ce.cnpj_empresa_empregadora IS NOT NULL
),
resultado_com_indicadores AS (
    SELECT
        o.codigo_cliente,
        o.conta_negociacao,
        o.conta_master,
        o.tipo_operacao,
        o.ativo_negociado,
        o.ativo_normalizado,
        SUBSTRING(o.ativo_normalizado, 1, 4) AS ativo_negociado_4letras,
        o.preco,
        o.quantidade,
        o.volume_negociado,
        o.mercado,
        o.data_operacao,
        o.ano_pregao,
        o.mes_pregao,
        o.dia_pregao,
        o.hora_transacao,
        o.broker,
        o.origem_operacao,
        o.mes_operacao,
        o.cpf_cnpj_cliente,
        o.pessoa_fisica,
        o.tipo_cliente,
        o.indicador_exposicao_politica_liger,
        o.indicador_pessoa_vinculada_liger,
        o.cnpj_empresa_vinculada,
        bt.ticker_empresa_vinculada AS ticker_empresa_vinculada_original,
        COALESCE(bt.ticker_empresa_vinculada, 'Não consta') AS ticker_empresa_vinculada,
        CASE
            WHEN o.ativo_negociado = COALESCE(bt.ticker_empresa_vinculada, '')
                 AND EXISTS (
                    SELECT 1
                    FROM adm_conselho_fiscal acf
                    WHERE acf.doc_clean = o.cpf_cnpj_cliente
                      AND acf.cnpj_companhia = o.cnpj_empresa_vinculada
                 )
            THEN TRUE
            ELSE FALSE
        END AS indicador_adm_conselho_fiscal,
        CASE
            WHEN o.ativo_negociado = COALESCE(bt.ticker_empresa_vinculada, '')
                 AND EXISTS (
                    SELECT 1
                    FROM membro_comite mc
                    WHERE mc.doc_clean = o.cpf_cnpj_cliente
                      AND mc.cnpj_companhia = o.cnpj_empresa_vinculada
                 )
            THEN TRUE
            ELSE FALSE
        END AS indicador_membro_comite,
        CASE
            WHEN o.ativo_negociado = COALESCE(bt.ticker_empresa_vinculada, '')
                 AND EXISTS (
                    SELECT 1
                    FROM participacao_sociedade ps
                    WHERE ps.doc_clean = o.cpf_cnpj_cliente
                      AND ps.cnpj_companhia = o.cnpj_empresa_vinculada
                 )
            THEN TRUE
            ELSE FALSE
        END AS indicador_participacao_sociedade,
        CASE
            WHEN o.ativo_negociado = COALESCE(bt.ticker_empresa_vinculada, '')
                 AND EXISTS (
                    SELECT 1
                    FROM posicao_acionaria pa
                    WHERE pa.doc_clean = o.cpf_cnpj_cliente
                      AND pa.cnpj_companhia = o.cnpj_empresa_vinculada
                 )
            THEN TRUE
            ELSE FALSE
        END AS indicador_posicao_acionaria,
        CASE
            WHEN o.ativo_negociado = COALESCE(bt.ticker_empresa_vinculada, '')
                 AND EXISTS (
                    SELECT 1
                    FROM relacao_familiar rf
                    WHERE rf.doc_clean = o.cpf_cnpj_cliente
                      AND rf.cnpj_companhia = o.cnpj_empresa_vinculada
                 )
            THEN TRUE
            ELSE FALSE
        END AS indicador_relacao_familiar,
        CASE
            WHEN o.ativo_negociado = COALESCE(bt.ticker_empresa_vinculada, '')
                 AND EXISTS (
                    SELECT 1
                    FROM relacao_subordinacao rs
                    WHERE rs.doc_clean = o.cpf_cnpj_cliente
                      AND rs.cnpj_companhia = o.cnpj_empresa_vinculada
                 )
            THEN TRUE
            ELSE FALSE
        END AS indicador_relacao_subordinacao,
        CASE
            WHEN o.ativo_negociado = COALESCE(bt.ticker_empresa_vinculada, '')
                 AND EXISTS (
                    SELECT 1
                    FROM relacao_empregador re
                    WHERE re.doc_clean = o.cpf_cnpj_cliente
                      AND re.cnpj_companhia = o.cnpj_empresa_vinculada
                 )
            THEN TRUE
            ELSE FALSE
        END AS indicador_relacao_empregador
    FROM operacoes_com_vinculos o
    LEFT JOIN base_ticker_distinct bt ON LPAD(o.cnpj_empresa_vinculada, 14, '0') = LPAD(bt.cnpj_companhia, 14, '0')
    WHERE o.cpf_cnpj_cliente IS NOT NULL
),
indicadores_por_ticker AS (
    SELECT DISTINCT
        codigo_cliente,
        data_operacao,
        ativo_normalizado,
        ativo_negociado_4letras,
        cpf_cnpj_cliente,
        cnpj_empresa_vinculada,
        ticker_empresa_vinculada,
        CASE
            WHEN ticker_empresa_vinculada_original = ativo_negociado_4letras
                 OR indicador_adm_conselho_fiscal = TRUE
                 OR indicador_membro_comite = TRUE
                 OR indicador_participacao_sociedade = TRUE
                 OR indicador_posicao_acionaria = TRUE
                 OR indicador_relacao_familiar = TRUE
                 OR indicador_relacao_subordinacao = TRUE
                 OR indicador_relacao_empregador = TRUE
            THEN 'S'
            ELSE 'N'
        END AS Pessoa_Relacionada
    FROM resultado_com_indicadores
),
resultado_com_pessoa_relacionada AS (
    SELECT
        ri.codigo_cliente,
        ri.conta_master,
        ri.tipo_operacao,
        ri.origem_operacao,
        ri.ativo_normalizado,
        ri.ativo_negociado_4letras,
        ri.preco,
        ri.quantidade,
        ri.volume_negociado,
        ri.data_operacao,
        ri.broker,
        ri.cpf_cnpj_cliente,
        ri.pessoa_fisica,
        ri.cnpj_empresa_vinculada,
        ind.ticker_empresa_vinculada,
        ind.Pessoa_Relacionada
    FROM resultado_com_indicadores ri
    INNER JOIN indicadores_por_ticker ind
        ON ri.codigo_cliente = ind.codigo_cliente
        AND ri.data_operacao = ind.data_operacao
        AND ri.ativo_normalizado = ind.ativo_normalizado
        AND ri.cpf_cnpj_cliente = ind.cpf_cnpj_cliente
        AND ri.cnpj_empresa_vinculada = ind.cnpj_empresa_vinculada
        AND COALESCE(ri.ticker_empresa_vinculada, 'Não consta') = ind.ticker_empresa_vinculada
),
resultado_agregado AS (
    SELECT
        data_operacao,
        codigo_cliente,
        cpf_cnpj_cliente,
        tipo_operacao,
        origem_operacao,
        ativo_normalizado AS ativo_negociado,
        ativo_negociado_4letras,
        pessoa_fisica,
        broker,
        cnpj_empresa_vinculada,
        ticker_empresa_vinculada,
        Pessoa_Relacionada,
        SUM(preco * volume_negociado) / NULLIF(SUM(volume_negociado), 0) AS preco,
        SUM(quantidade) AS quantidade,
        SUM(volume_negociado) AS volume_negociado,
        MAX(conta_master) AS conta_master
    FROM resultado_com_pessoa_relacionada
    GROUP BY
        data_operacao,
        codigo_cliente,
        cpf_cnpj_cliente,
        tipo_operacao,
        origem_operacao,
        ativo_normalizado,
        ativo_negociado_4letras,
        pessoa_fisica,
        broker,
        cnpj_empresa_vinculada,
        ticker_empresa_vinculada,
        Pessoa_Relacionada
),
complementos_master AS (
    SELECT DISTINCT
        CAST(codigo_conta AS VARCHAR) AS conta_master,
        LPAD(cpf_cnpj, 14, '0') AS cpf_cnpj_master
    FROM db_corp_corretora_inoabrokertools_sot_01.tb_ep7_complementos_contas_sot
    WHERE CAST(codigo_conta AS VARCHAR) IN (SELECT DISTINCT conta_master FROM resultado_agregado WHERE conta_master IS NOT NULL AND conta_master <> '')
      AND data_referencia = (SELECT MAX(data_referencia) FROM db_corp_corretora_inoabrokertools_sot_01.tb_ep7_complementos_contas_sot)
      AND cpf_cnpj IS NOT NULL
      AND cpf_cnpj <> ''
),
resultado_com_linha_negocio AS (
    SELECT
        r.*,
        CASE
            WHEN LTRIM(REGEXP_REPLACE(COALESCE(cm.cpf_cnpj_master, '0'), '[^0-9]', ''), '0') IN ('60701190000104', '17192451000170', '8771962000156', '38658114000104')
            THEN 'Tesouraria'
            WHEN LTRIM(REGEXP_REPLACE(COALESCE(cm.cpf_cnpj_master, '0'), '[^0-9]', ''), '0') IN ('40430971000196')
            THEN 'Asset'
            WHEN LTRIM(REGEXP_REPLACE(COALESCE(cm.cpf_cnpj_master, '0'), '[^0-9]', ''), '0') IN ('61194353000164')
            THEN 'Corretora'
            ELSE 'Não classificado'
        END AS linha_negocio
    FROM resultado_agregado r
    LEFT JOIN complementos_master cm
        ON r.conta_master = cm.conta_master
),
resultado_filtrado AS (
    SELECT
        data_operacao,
        codigo_cliente,
        cpf_cnpj_cliente,
        tipo_operacao,
        ativo_negociado,
        ativo_negociado_4letras,
        pessoa_fisica,
        cnpj_empresa_vinculada,
        ticker_empresa_vinculada,
        broker,
        Pessoa_Relacionada,
        linha_negocio,
        origem_operacao,
        preco,
        quantidade,
        volume_negociado
    FROM resultado_com_linha_negocio
    WHERE (linha_negocio = 'Corretora' AND origem_operacao IN ('local', 'repasse_enviado', 'repasse_recebido'))
       OR (linha_negocio = 'Asset' AND origem_operacao IN ('local', 'repasse_enviado', 'repasse_recebido'))
       OR (linha_negocio = 'Tesouraria' AND origem_operacao IN ('local', 'repasse_enviado', 'repasse_recebido'))
       OR (linha_negocio = 'Não classificado' AND origem_operacao IN ('local', 'repasse_enviado'))
)
SELECT DISTINCT
    *,
    '{{SAFRA}}' AS SAFRA
FROM resultado_filtrado
ORDER BY data_operacao, codigo_cliente, ativo_negociado;
