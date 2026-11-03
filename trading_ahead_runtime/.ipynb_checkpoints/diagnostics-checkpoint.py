"""Consultas observacionais de qualidade; não alteram os filtros do monitoramento."""
from datetime import timedelta
from .policy import month_bounds

def _metric(kind, column, condition):
    return {'kind': kind, 'column': column, 'condition': condition}


def source_probe_specs(pipeline, month, stage):
    """Somente SELECTs agregados; universo e eventuais limites ficam no detalhe.

    As sondagens incluem nulos/erros que o CAST estrito ainda não processou.
    Não são substitutas das consultas oficiais, que seguem literais.
    """
    first, last = month_bounds(month)
    if stage == 'cta01':
        low, high = first - timedelta(days=365), last + timedelta(days=7)
        source = 'db_corp_gestaoderiscos_distribuiprecoscorporativos_sor_01.tbim8_acao_btres'
        relation = (f"{source} WHERE idt_atbt = '11' AND nom_font_info = 'B3' "
                    f"AND CAST(dat_base_info AS VARCHAR) BETWEEN '{low:%Y%m%d}' AND '{high:%Y%m%d}'")
        data_expr = "TRY(DATE_PARSE(SUBSTR(CAST(dat_base_info AS VARCHAR), 1, 8), '%Y%m%d'))"
        metrics = [
            _metric('nulo', 'nom_ativ_finn', 'nom_ativ_finn IS NULL'),
            _metric('tipo_dado', 'dat_base_info', f'dat_base_info IS NOT NULL AND {data_expr} IS NULL'),
            _metric('nulo', 'vlr_atbt', 'vlr_atbt IS NULL'),
            _metric('tipo_dado', 'vlr_atbt', 'vlr_atbt IS NOT NULL AND TRY_CAST(vlr_atbt AS DOUBLE) IS NULL'),
            _metric('nao_finito', 'vlr_atbt', 'TRY_CAST(vlr_atbt AS DOUBLE) IS NOT NULL AND NOT is_finite(TRY_CAST(vlr_atbt AS DOUBLE))'),
            _metric('valor_nao_positivo', 'vlr_atbt', 'TRY_CAST(vlr_atbt AS DOUBLE) <= 0'),
            _metric('tipo_dado', 'timestamp', "TRY_CAST(COALESCE(NULLIF(CAST(timestamp AS VARCHAR), ''), '0') AS DOUBLE) IS NULL"),
            _metric('tipo_dado', 'versao', "TRY_CAST(COALESCE(NULLIF(CAST(versao AS VARCHAR), ''), '0') AS BIGINT) IS NULL"),
        ]
        return {'relation': relation, 'metrics': metrics,
                'entity': 'nom_ativ_finn', 'entity_type': 'ativo',
                'scope': 'B3 fechamento, antes do filtro de ativo; datas inválidas fora do intervalo lexical não são medidas.',
                'duplicate_key': ['dat_base_info', 'nom_ativ_finn'],
                'duplicate_kind': 'versoes_preco',
                'duplicate_note': 'Repetições podem representar versões; a seleção oficial usa timestamp/versão.'}
    if stage == 'cta06':
        low, high = first - timedelta(days=150), last + timedelta(days=7)
        source = 'db_corp_corretora_inoabrokertools_sot_01.tb_ep7_alocacoes_sot'
        event_table = pipeline.table('cta05', month)
        parsed = 'TRY_CAST(SUBSTR(CAST(data_pregao AS VARCHAR), 1, 10) AS DATE)'
        partition = "CONCAT(CAST(ano_pregao AS VARCHAR), LPAD(CAST(mes_pregao AS VARCHAR), 2, '0'))"
        relation = (f"{source} WHERE mercado = 'XBSP' "
                    f"AND ativo IN (SELECT ativo_atipico FROM {event_table}) "
                    f"AND {partition} BETWEEN '{low:%Y%m}' AND '{high:%Y%m}' "
                    f"AND ({parsed} BETWEEN DATE '{low}' AND DATE '{high}' OR {parsed} IS NULL)")
        metrics = [
            _metric('nulo', 'cliente', 'cliente IS NULL'),
            _metric('nulo', 'data_pregao', 'data_pregao IS NULL'),
            _metric('tipo_dado', 'data_pregao', f'data_pregao IS NOT NULL AND {parsed} IS NULL'),
            _metric('dominio', 'lado', "lado IS NULL OR lado NOT IN ('Compra', 'Venda')"),
            _metric('dominio', 'tipo_operacao', "tipo_operacao IS NULL OR tipo_operacao NOT IN ('local', 'repasse_enviado', 'repasse_recebido')"),
        ]
        for column in ('preco', 'quantidade', 'volume_negociado'):
            metrics.extend([
                _metric('nulo', column, column + ' IS NULL'),
                _metric('tipo_dado', column, f'{column} IS NOT NULL AND TRY_CAST({column} AS DOUBLE) IS NULL'),
                _metric('nao_finito', column, f'TRY_CAST({column} AS DOUBLE) IS NOT NULL AND NOT is_finite(TRY_CAST({column} AS DOUBLE))'),
            ])
        return {'relation': relation, 'metrics': metrics,
                'entity': 'cliente', 'entity_type': 'cliente',
                'scope': 'Ativos com evento e mercado XBSP, antes de origem/cadastro/vínculos. Datas inválidas entram nos meses de partição abrangidos; partições ausentes não são medidas.',
                'duplicate_key': ['cliente', 'ativo', 'data_pregao', 'hora_transacao', 'lado',
                                  'quantidade', 'preco', 'volume_negociado', 'tipo_operacao',
                                  'conta_negociacao', 'conta_master', 'broker'],
                'duplicate_kind': 'repeticao_campos_observados',
                'duplicate_note': 'A fonte não fornece aqui uma chave única de transação validada; igualdade dos campos não prova duplicidade indevida.'}
    raise ValueError('Sondagem de fonte não prevista.')


def probe_summary_sql(spec):
    fields = ['COUNT(*) AS total']
    for index, metric in enumerate(spec['metrics']):
        fields.append(f"COUNT_IF({metric['condition']}) AS n_{index}")
    return 'SELECT ' + ', '.join(fields) + ' FROM ' + spec['relation']


def probe_duplicate_sql(spec):
    columns = ', '.join(spec['duplicate_key'])
    return (f"WITH grupos AS (SELECT {columns}, COUNT(*) AS n FROM {spec['relation']} GROUP BY {columns}) "
            "SELECT COALESCE(SUM(n), 0) AS total, COALESCE(SUM(n - 1), 0) AS extras FROM grupos")
