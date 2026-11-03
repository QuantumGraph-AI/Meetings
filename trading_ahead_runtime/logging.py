"""Resumo, detalhe, Parquet, erros seguros e tempos das consultas."""
from datetime import datetime,timezone
from pathlib import Path
import importlib.metadata
import json
import platform
import re
import sys
import time
import traceback
import uuid
from .aws_runtime import AthenaRunner,CollisionError,_error_code

OBSERVABILITY_VERSION = 'cripto_logs_adaptados_ta_v1'


COLS_LOG_RESUMO = [
    'data_execucao', 'safra', 'tipo_verificacao', 'coluna',
    'lado_avaliado', 'n_afetados', 'total_avaliado', 'pct',
    'tolerancia_pct', 'acao',
]


COLS_LOG_DETALHE = [
    'data_execucao', 'safra', 'tipo_verificacao', 'coluna',
    'entidade_tipo', 'entidade_id', 'detalhe',
]


DTYPE_LOG_RESUMO = {
    'data_execucao': 'timestamp', 'tipo_verificacao': 'string',
    'coluna': 'string', 'lado_avaliado': 'string', 'n_afetados': 'bigint',
    'total_avaliado': 'bigint', 'pct': 'double',
    'tolerancia_pct': 'double', 'acao': 'string',
}


DTYPE_LOG_DETALHE = {
    'data_execucao': 'timestamp', 'tipo_verificacao': 'string',
    'coluna': 'string', 'entidade_tipo': 'string',
    'entidade_id': 'string', 'detalhe': 'string',
}


EXTRA_LOG_TYPES = {
    'id_execucao': 'string', 'id_tentativa': 'string', 'etapa': 'string',
    'publico': 'string', 'query_id': 'string', 'tempo_total_s': 'double',
    'bytes_lidos': 'bigint',
}


OBSERVABILITY_DEFAULTS = {
    'diagnosticos_fontes': True,
    'max_detalhes_por_verificacao': 100,
    'registrar_requirements': True,
    'max_registros_buffer': 250,
}


class LogPersistenceError(RuntimeError):
    """Os logs obrigatórios não foram confirmados; não publicar SUCCESS."""


def validate_observability(raw):
    if raw is None:
        raw = {}
    if not isinstance(raw, dict) or set(raw) - set(OBSERVABILITY_DEFAULTS):
        raise ValueError('Configuração de observabilidade inválida ou desconhecida.')
    result = dict(OBSERVABILITY_DEFAULTS, **raw)
    for key in ('diagnosticos_fontes', 'registrar_requirements'):
        if type(result[key]) is not bool:
            raise ValueError(key + ' deve ser booleano.')
    for key, low, high in (('max_detalhes_por_verificacao', 0, 1000),
                           ('max_registros_buffer', 10, 10000)):
        if type(result[key]) is not int or not low <= result[key] <= high:
            raise ValueError(key + ' fora do limite permitido.')
    return result


def emit(event, **fields):
    """Somente metadados agregados; dados de cliente não vão ao stdout."""
    print(json.dumps({'timestamp': datetime.now(timezone.utc).isoformat(),
                      'event': event, **fields}, ensure_ascii=False,
                     sort_keys=True, allow_nan=False), flush=True)


def safe_exception(error):
    # Não usar str(error), format_exc() ou locals(): podem conter SQL/CPFs.
    return {
        'error_type': type(error).__name__,
        'frames': [{'arquivo': Path(frame.filename).name, 'linha': frame.lineno,
                    'funcao': frame.name}
                   for frame in traceback.extract_tb(error.__traceback__)[-12:]],
    }


def runtime_inventory():
    packages = {}
    for name in ('awswrangler', 'boto3', 'botocore', 'pandas', 'numpy', 'pyarrow'):
        try:
            packages[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            packages[name] = 'nao_instalado'
    return {'python': platform.python_version(), 'pacotes': packages}


class ParquetLogWriter:
    """Append somente no namespace reservado pelo runner desta execução.

    Como no Cripto, usa Parquet, schema Athena explícito e partição safra.
    Cada tentativa/lote tem nomes próprios. Nenhum overwrite é utilizado.
    """
    def __init__(self, runner):
        self.runner = runner
        self.session = None
        self.checked_tables = set()

    def __call__(self, kind, records, attempt_id, batch_id):
        if not records:
            return None
        import pandas as pd
        import awswrangler as wr
        import boto3
        runner = self.runner
        runner._require_initialized()
        table = runner.table_prefix + 'log_' + kind
        location = runner.run_root + 'logs/parquet/' + kind + '/'
        # Impedir que um nome reservado aponte para os logs de outro projeto.
        if table not in self.checked_tables:
            try:
                existing = runner.glue.get_table(DatabaseName=runner.database, Name=table)['Table']
            except Exception as error:
                if _error_code(error) != 'EntityNotFoundException':
                    raise
            else:
                actual = existing.get('StorageDescriptor', {}).get('Location', '').rstrip('/')
                if actual != location.rstrip('/'):
                    raise CollisionError('Tabela de log existente aponta para outro destino.')
            self.checked_tables.add(table)
        base = DTYPE_LOG_RESUMO if kind == 'resumo' else DTYPE_LOG_DETALHE
        types = dict(base, **EXTRA_LOG_TYPES)
        columns = (COLS_LOG_RESUMO if kind == 'resumo' else COLS_LOG_DETALHE) + list(EXTRA_LOG_TYPES)
        frame = pd.DataFrame(records, columns=columns)
        frame['safra'] = frame['safra'].astype('string')
        for column, datatype in types.items():
            if datatype == 'string':
                frame[column] = frame[column].astype('string')
            elif datatype == 'bigint':
                frame[column] = pd.to_numeric(frame[column], errors='raise').astype('Int64')
            elif datatype == 'double':
                frame[column] = pd.to_numeric(frame[column], errors='raise').astype('Float64')
            else:
                frame[column] = pd.to_datetime(frame[column], utc=True).dt.tz_localize(None)
        if self.session is None:
            self.session = boto3.Session(region_name=runner.config['region'])
        extra = {'ExpectedBucketOwner': runner.owner}
        kms = runner.config.get('kms_key_arn')
        if not kms and runner._encryption and runner._encryption.get('EncryptionOption') == 'SSE_KMS':
            kms = runner._encryption.get('KmsKey')
        if kms:
            extra.update(ServerSideEncryption='aws:kms', SSEKMSKeyId=kms)
        result = wr.s3.to_parquet(
            df=frame, path=location, dataset=True, index=False,
            database=runner.database, table=table,
            partition_cols=['safra'], mode='append', dtype=types,
            compression='snappy', schema_evolution=False,
            filename_prefix=attempt_id + '_' + batch_id + '_',
            boto3_session=self.session, s3_additional_kwargs=extra,
        )
        return {'table': runner.database + '.' + table,
                'location': location, 'rows': len(records), 'paths': result['paths']}


class LogBook:
    """Acumula todos os problemas antes do gate; persiste antes de propagá-lo."""
    def __init__(self, config, options, runner, writer=None, attempt_id=None):
        self.config = config
        self.options = options
        self.runner = runner
        self.writer = writer or ParquetLogWriter(runner)
        self.attempt_id = attempt_id or ('a' + uuid.uuid4().hex)
        if not re.fullmatch(r'a[0-9a-f]{32}', self.attempt_id):
            raise ValueError('Identificador de tentativa inválido.')
        self.resumo, self.detalhe = [], []
        self.sequence = 0
        self.persistence_failed = False
        self.current_stage = 'inicializacao'
        self.query_ids = {}
        self.success_prepared = False

    def common(self, stage=None, month=None, person=None, query_id=None,
               elapsed=None, scanned=None):
        return {'data_execucao': datetime.now(timezone.utc).isoformat(),
                'safra': month or self.config['target_month'],
                'id_execucao': self.config['run_id'], 'id_tentativa': self.attempt_id,
                'etapa': stage or self.current_stage, 'publico': person,
                'query_id': query_id, 'tempo_total_s': elapsed, 'bytes_lidos': scanned}

    def summary(self, check, column='-', affected=0, total=0, action='ok',
                side='ambos', tolerance=None, **context):
        if type(affected) is not int or type(total) is not int or affected < 0 or total < 0:
            raise ValueError('Contagens de log precisam ser inteiros não negativos.')
        row = self.common(**context)
        row.update(tipo_verificacao=check, coluna=column, lado_avaliado=side,
                   n_afetados=affected, total_avaliado=total,
                   pct=round(100 * affected / total, 4) if total else 0.0,
                   tolerancia_pct=tolerance, acao=action)
        self.resumo.append(row)
        return row

    def detail(self, check, entity_type, entity_id, detail, column='-', **context):
        row = self.common(**context)
        row.update(tipo_verificacao=check, coluna=column, entidade_tipo=entity_type,
                   entidade_id=str(entity_id), detalhe=json.dumps(detail, ensure_ascii=False,
                                                                 sort_keys=True, allow_nan=False))
        self.detalhe.append(row)
        return row

    def maybe_flush(self):
        if len(self.resumo) + len(self.detalhe) >= self.options['max_registros_buffer']:
            self.flush()

    def flush(self):
        if not self.resumo and not self.detalhe:
            return
        if self.persistence_failed:
            raise LogPersistenceError('Lote anterior falhou; evidências JSON preservadas quando possível.')
        if not getattr(self.runner, '_initialized', False):
            # Uma colisão/falha de credencial não autoriza gravar no prefixo alheio.
            emit('logs_nao_persistidos', run_id=self.config['run_id'],
                 motivo='namespace_nao_reservado', resumo=len(self.resumo), detalhe=len(self.detalhe))
            return
        batch_id = f'{self.sequence:06d}'
        relative = 'logging/attempts/' + self.attempt_id + '/batches/' + batch_id + '/'
        payload = {'schema_version': 1, 'observability_version': OBSERVABILITY_VERSION,
                   'resumo': self.resumo, 'detalhe': self.detalhe}
        try:
            # Evidência imutável antes do Parquet: mantém diagnóstico se o catálogo falhar.
            self.runner.put_json_once(relative + 'snapshot.json', payload)
            writes = {}
            for kind, records in (('resumo', self.resumo), ('detalhe', self.detalhe)):
                if records:
                    writes[kind] = self.writer(kind, records, self.attempt_id, batch_id)
            self.runner.put_json_once(relative + 'persisted.json', {'status': 'COMPLETE', 'writes': writes})
        except Exception as error:
            self.persistence_failed = True
            emit('log_persistence_failed', run_id=self.config['run_id'], batch_id=batch_id,
                 id_tentativa=self.attempt_id, **safe_exception(error))
            raise LogPersistenceError('Falha de persistência dos logs; execução não pode ser publicada.') from error
        emit('logs_persistidos', run_id=self.config['run_id'], id_tentativa=self.attempt_id,
             batch_id=batch_id, resumo=len(self.resumo), detalhe=len(self.detalhe))
        self.resumo, self.detalhe = [], []
        self.sequence += 1

    def failure(self, error, stage=None):
        context = {'stage': stage or self.current_stage}
        self.summary('erro_processamento', affected=1, total=1, action='GATE',
                     tolerance=0.0, **context)
        self.detail('erro_processamento', 'etapa', context['stage'], safe_exception(error), **context)

    def prepare_success(self):
        if self.success_prepared:
            return
        # OK significa validações concluídas. SUCCESS.json é a confirmação de publicação.
        self.summary('status_execucao', affected=0, total=1, action='OK', stage='conclusao')
        self.detail('status_execucao', 'execucao', self.config['run_id'],
                    {'status': 'VALIDADO_PARA_PUBLICACAO',
                     'confirmacao_publicacao': 'SUCCESS.json',
                     'observability_version': OBSERVABILITY_VERSION}, stage='conclusao')
        self.flush()
        self.success_prepared = True


class LoggingRunner(AthenaRunner):
    """Instrumentação do runner aprovado, sem alterar SQL nem tratamento de falhas."""
    def __init__(self, *args, logbook=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.logbook = logbook
        self.before_success = None

    def _failure(self, step, intent, query_id, query, state, stop=False):
        if self.logbook is not None:
            diagnostic = query.get('Status', {}).get('AthenaError', {})
            self.logbook.detail('erro_consulta', 'consulta', query_id,
                                {'state': state, 'error_category': diagnostic.get('ErrorCategory'),
                                 'error_type_athena': diagnostic.get('ErrorType'),
                                 'retryable': diagnostic.get('Retryable')},
                                stage=step, query_id=query_id)
        return super()._failure(step, intent, query_id, query, state, stop=stop)

    def _log(self, event, **fields):
        super()._log(event, **fields)
        if self.logbook is not None and fields.get('step') and fields.get('query_id'):
            self.logbook.query_ids[fields['step']] = fields['query_id']

    def execute(self, sql, step):
        started = time.monotonic()
        result, error = None, None
        if self.logbook is not None:
            self.logbook.current_stage = step
        try:
            result = super().execute(sql, step)
            # Recibo verificado também autoriza ler o resultado em uma retomada.
            if result.get('state') == 'SUCCEEDED':
                self._known_queries.add(result['query_id'])
            return result
        except Exception as caught:
            error = caught
            raise
        finally:
            elapsed = time.monotonic() - started
            if self.logbook is not None:
                stats = result.get('statistics', {}) if result else {}
                self.logbook.summary(
                    'consulta', column=step, affected=int(error is not None), total=1,
                    action='GATE' if error else 'ok', tolerance=0.0,
                    stage=step, query_id=(result or {}).get('query_id') or self.logbook.query_ids.get(step),
                    elapsed=elapsed, scanned=int(stats.get('DataScannedInBytes', 0)),
                )
            emit('tempo_consulta', run_id=self.run_id, step=step, segundos=round(elapsed, 6),
                 status='FALHA' if error else 'OK')

    def put_json_once(self, relative, data):
        if relative == 'SUCCESS.json':
            if self.logbook is None:
                raise LogPersistenceError('Publicação exige logs conectados ao runner.')
            if self.before_success is not None:
                self.before_success()
            self.logbook.prepare_success()
        return super().put_json_once(relative, data)
