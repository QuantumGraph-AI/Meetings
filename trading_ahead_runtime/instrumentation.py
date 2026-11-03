"""Instrumentação das etapas sem alterar o pipeline aprovado."""
import time
from .pipeline import Pipeline,BASE_STAGES,FINAL_STAGES
from .policy import DEDUP_KEY,dedup_projection_sql,history_months
from .logging import emit,safe_exception
from .diagnostics import source_probe_specs,probe_summary_sql,probe_duplicate_sql

class LoggingPipeline(Pipeline):
    """A medição é Athena; a montagem/decisão operacional é Python, como no Cripto."""
    def __init__(self, config, bundle, runner, logbook):
        super().__init__(config, bundle, runner)
        self.logbook = logbook
        self.completed = set()
        reused = self.validated_history_manifest()
        self.expected = {(month, stage)
                         for month in history_months(config['target_month'], config['history_days']) + [config['target_month']]
                         if month not in reused for stage in BASE_STAGES}
        month = config['target_month']
        self.expected.update((month, stage) for stage in FINAL_STAGES + [
            'calibracao', 'cta13_pf', 'cta13_pj', 'alertas_pf_vinculos',
            'alertas_pf', 'alertas_pj_vinculos', 'alertas_pj'])
        if config['include_cta12_diagnostic']:
            self.expected.add((month, 'cta12_diagnostico'))
        runner.before_success = self.check_expected_stages

    def check_expected_stages(self):
        missing = sorted(self.expected - self.completed)
        self.logbook.summary('cobertura_etapas', affected=len(missing), total=len(self.expected),
                             action='GATE' if missing else 'ok', stage='conclusao', tolerance=0.0)
        if missing:
            self.logbook.detail('etapa_nao_concluida', 'execucao', self.config['run_id'],
                                {'etapas': [month + '_' + stage for month, stage in missing]}, stage='conclusao')
            self.logbook.flush()
            raise RuntimeError('Existem etapas planejadas não concluídas.')

    def rows(self, sql, step, limit=100):
        rows = super().rows(sql, step, limit)
        if step == 'p99_population_stats':
            for row in rows:
                person = 'PF' if row['tipo_pessoa'] == 'F' else 'PJ' if row['tipo_pessoa'] == 'J' else 'DESCONHECIDO'
                total = int(row['total'])
                for check, key in (('nulo_resultado', 'valid_count'), ('nao_finito', 'invalid_count'),
                                   ('resultado_negativo', 'negative_count'), ('resultado_zero', 'zero_count')):
                    number = total - int(row[key]) if key == 'valid_count' else int(row[key])
                    self.logbook.summary(check, column='resultado', affected=number, total=total,
                                         action='GATE' if key == 'invalid_count' and number else 'log',
                                         person=person, stage='calibracao', tolerance=0.0 if key == 'invalid_count' else None)
        elif step in ('verify_pf_published', 'verify_pj_published'):
            for row in rows:
                for key in ('invalid_count', 'duplicate_ids'):
                    n = int(row[key])
                    public_stage = 'alertas_pf' if step == 'verify_pf_published' else 'alertas_pj'
                    total = next((item['row_count'] for item in reversed(self.stage_records)
                                  if item['stage'] == public_stage and item['month'] == self.config['target_month']), 0)
                    self.logbook.summary(key, affected=n, total=total, action='GATE' if n else 'ok',
                                         stage=step, person='PF' if step == 'verify_pf_published' else 'PJ', tolerance=0.0)
        return rows

    def audit_source(self, stage, month):
        spec = source_probe_specs(self, month, stage)
        stem = month + '_' + stage + '_qa'
        data = self.rows(probe_summary_sql(spec), stem + '_summary', 1)[0]
        total = int(data['total'])
        self.logbook.detail('escopo_verificacao', 'fonte', stage,
                            {'escopo': spec['scope'], 'modo': 'OBSERVACIONAL_SEM_FILTRO_ADICIONAL'},
                            stage=stage, month=month)
        self.logbook.summary('base_fonte_vazia', affected=int(total == 0), total=1,
                             action='AVISO' if total == 0 else 'ok', stage=stage, month=month)
        for index, metric in enumerate(spec['metrics']):
            n = int(data[f'n_{index}'])
            self.logbook.summary(metric['kind'], column=metric['column'], affected=n, total=total,
                                 action='log' if n else 'ok', stage=stage, month=month, side='fonte')
            limit = self.logbook.options['max_detalhes_por_verificacao']
            if n and limit:
                query = (f"SELECT DISTINCT CAST({spec['entity']} AS VARCHAR) AS entidade "
                         f"FROM {spec['relation']} AND ({metric['condition']}) "
                         f"ORDER BY entidade LIMIT {limit + 1}")
                entities = self.rows(query, stem + f'_detail_{index}', limit + 1)
                for entity in entities[:limit]:
                    self.logbook.detail(metric['kind'], spec['entity_type'], entity['entidade'] or 'sem_identificador',
                                        {'motivo': metric['kind'], 'coluna': metric['column']},
                                        column=metric['column'], stage=stage, month=month)
                if len(entities) > limit:
                    self.logbook.detail('detalhes_limitados', 'verificacao', metric['column'],
                                        {'limite': limit, 'contagem_agregada_completa': n},
                                        stage=stage, month=month)
        duplicate = self.rows(probe_duplicate_sql(spec), stem + '_repetitions', 1)[0]
        self.logbook.summary(spec['duplicate_kind'], column='+'.join(spec['duplicate_key']),
                             affected=int(duplicate['extras']), total=int(duplicate['total']),
                             action='log' if int(duplicate['extras']) else 'ok', stage=stage, month=month)
        self.logbook.detail(spec['duplicate_kind'], 'fonte', stage,
                            {'interpretacao': spec['duplicate_note']}, stage=stage, month=month)
        self.logbook.flush()

    def audit_financial_fanout(self, month):
        projection = dedup_projection_sql('t')
        fields = ', '.join(DEDUP_KEY)
        sql = (f"WITH base AS (SELECT {projection} FROM {self.table('cta13_base', month)} t), "
               f"grupos AS (SELECT {fields}, COUNT(*) AS n FROM base GROUP BY {fields}) "
               "SELECT COALESCE(SUM(n), 0) AS total, COALESCE(SUM(n - 1), 0) AS extras FROM grupos")
        row = self.rows(sql, month + '_qa_financial_fanout', 1)[0]
        self.logbook.summary('duplicata_chave_contagem', column='chave_17_campos_sem_vinculos',
                             affected=int(row['extras']), total=int(row['total']), action='log',
                             stage='cta13_base', month=month)
        self.logbook.detail('duplicata_chave_contagem', 'etapa', 'cta13_base',
                            {'regra': 'Vínculos preservados no detalhamento; calibração e contagem deduplicam a chave oficial.'},
                            stage='cta13_base', month=month)

    def materialize(self, stage, sql, month=None):
        month = month or self.config['target_month']
        self.logbook.current_stage = month + '_' + stage
        started = time.monotonic()
        record, error = None, None
        try:
            if self.logbook.options['diagnosticos_fontes'] and month == self.config['target_month'] and stage in ('cta01', 'cta06'):
                self.audit_source(stage, month)
            record = super().materialize(stage, sql, month)
            self.completed.add((month, stage))
            self.logbook.summary('volumetria', affected=record['row_count'], total=record['row_count'],
                                 stage=stage, month=month, query_id=record['query_id'])
            if month == self.config['target_month'] and stage == 'cta13_base':
                self.audit_financial_fanout(month)
            return record
        except Exception as caught:
            error = caught
            raise
        finally:
            self.logbook.summary('tempo_etapa', affected=int(error is not None), total=1,
                                 action='GATE' if error else 'ok', stage=stage, month=month,
                                 elapsed=time.monotonic() - started)
            if error is None:
                self.logbook.maybe_flush()

    def base_month(self, month):
        value = super().base_month(month)
        self.logbook.flush()
        return value

    def calibrate(self):
        cuts = super().calibrate()
        for code, values in cuts.items():
            person = 'PF' if code == 'F' else 'PJ'
            self.logbook.summary('calibracao_p99', column='resultado', total=values['sample_size'],
                                 stage='calibracao', person=person)
            self.logbook.detail('calibracao_p99', 'publico', person, values,
                                column='resultado', stage='calibracao', person=person)
        self.logbook.flush()
        return cuts

    def run(self):
        started = time.monotonic()
        try:
            result = super().run()
            emit('status_execucao', run_id=self.config['run_id'], status='SUCESSO',
                 manifesto=self.root + 'SUCCESS.json')
            return result
        except Exception as error:
            self.logbook.failure(error)
            missing = sorted(self.expected - self.completed)
            self.logbook.summary('etapa_nao_concluida', affected=len(missing), total=len(self.expected),
                                 action='GATE', stage='conclusao', tolerance=0.0)
            self.logbook.detail('etapa_nao_concluida', 'execucao', self.config['run_id'],
                                {'etapas': [month + '_' + stage for month, stage in missing]}, stage='conclusao')
            self.logbook.summary('status_execucao', affected=1, total=1, action='GATE', stage='conclusao', tolerance=0.0)
            try:
                self.logbook.flush()
            except Exception as log_error:
                emit('falha_gravacao_logs_na_saida', run_id=self.config['run_id'], **safe_exception(log_error))
            raise
        finally:
            emit('tempo_execucao_total', run_id=self.config['run_id'], segundos=round(time.monotonic() - started, 6))
