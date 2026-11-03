"""Athena isolado, com recibos imutáveis e nenhuma limpeza destrutiva.

Construir AthenaRunner não cria clientes nem faz chamadas AWS. initialize é o
primeiro ponto de acesso à AWS e precisa ser chamado somente no modo execute.
CTAS não é uma transação: falhas podem deixar arquivos/tabelas, preservados.
"""
from datetime import datetime, timezone
import hashlib
import json
import math
import re
import time
from urllib.parse import urlsplit
import uuid


class RunnerError(RuntimeError):
    """Falha controlada; mensagens não contêm SQL ou dados de clientes."""


class UnsafeSqlError(RunnerError):
    pass


class CollisionError(RunnerError):
    pass


FORBIDDEN = {
    'DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'MERGE', 'UNLOAD', 'MSCK',
    'TRUNCATE', 'GRANT', 'REVOKE', 'CALL', 'EXECUTE', 'PREPARE', 'DEALLOCATE',
    'VACUUM', 'OPTIMIZE', 'REPAIR', 'SET', 'USE', 'RENAME', 'REFRESH',
    'EXTERNAL_FUNCTION', 'LAMBDA',
}
TERMINAL = {'SUCCEEDED', 'FAILED', 'CANCELLED'}


def _digest(value):
    return hashlib.sha256(value).hexdigest()


def _json_bytes(value):
    return (json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'),
                       allow_nan=False) + '\n').encode('utf-8')


def _error_code(error):
    return str(getattr(error, 'response', {}).get('Error', {}).get('Code', type(error).__name__))


def _tokens(sql):
    """Lexer mínimo: comentários/literais não viram comandos ou delimitadores."""
    result, index = [], 0
    while index < len(sql):
        char = sql[index]
        if char.isspace():
            index += 1
        elif sql.startswith('--', index):
            end = sql.find('\n', index)
            index = len(sql) if end == -1 else end + 1
        elif sql.startswith('/*', index):
            depth, index = 1, index + 2
            while index < len(sql) and depth:
                if sql.startswith('/*', index):
                    depth, index = depth + 1, index + 2
                elif sql.startswith('*/', index):
                    depth, index = depth - 1, index + 2
                else:
                    index += 1
            if depth:
                raise UnsafeSqlError('Comentário SQL não encerrado')
        elif char in "'\"`":
            quote, kind, index = char, ('string' if char == "'" else 'identifier'), index + 1
            pieces = []
            while index < len(sql):
                if sql[index] == quote:
                    if index + 1 < len(sql) and sql[index + 1] == quote:
                        pieces.append(quote)
                        index += 2
                    else:
                        index += 1
                        break
                else:
                    pieces.append(sql[index])
                    index += 1
            else:
                raise UnsafeSqlError('Literal SQL não encerrado')
            result.append((kind, ''.join(pieces)))
        elif char.isalpha() or char == '_':
            match = re.match(r'[A-Za-z_][A-Za-z0-9_]*', sql[index:])
            if not match:
                raise UnsafeSqlError('Identificador SQL não suportado')
            result.append(('word', match.group()))
            index += len(match.group())
        else:
            result.append(('symbol', char))
            index += 1
    return result


class AthenaRunner:
    def __init__(self, config, athena=None, s3=None, glue=None, clock=None, sleep=None):
        self.config = dict(config)
        self.athena, self.s3, self.glue = athena, s3, glue
        self.clock = clock or time.monotonic
        self.sleep = sleep or time.sleep
        self.database = self.config['database']
        self.run_id = self.config['run_id']
        if not re.fullmatch(r'[a-z][a-z0-9_]*', self.database):
            raise RunnerError('Database inválido')
        if not re.fullmatch(r'[a-z0-9][a-z0-9_]{0,79}', self.run_id):
            raise RunnerError('run_id inválido')
        self.owner = self.config['expected_bucket_owner']
        if not isinstance(self.owner, str) or not re.fullmatch(r'\d{12}', self.owner):
            raise RunnerError('expected_bucket_owner deve conter 12 dígitos')
        uri = self.config['output_s3_uri'].rstrip('/')
        parts = urlsplit(uri)
        if parts.scheme != 's3' or not parts.netloc or not parts.path.strip('/') or parts.query or parts.fragment:
            raise RunnerError('output_s3_uri precisa conter bucket e prefixo')
        if any(p in ('', '.', '..') for p in parts.path.lstrip('/').split('/')) or '\\' in uri or '%' in uri:
            raise RunnerError('Prefixo S3 inválido')
        self.bucket = parts.netloc
        self.run_root = uri + '/runs/' + self.run_id + '/'
        self.run_key = parts.path.lstrip('/') + '/runs/' + self.run_id + '/'
        self.table_prefix = 'ta_p99_' + self.run_id + '_'
        self._initialized = False
        self._config_hash = None
        self._query_scans = {}
        self._known_queries = set()
        self._encryption = None
        for field in ('query_timeout_seconds', 'poll_seconds', 'max_scan_bytes'):
            if (not isinstance(self.config[field], (int, float)) or isinstance(self.config[field], bool)
                    or not math.isfinite(self.config[field]) or self.config[field] <= 0):
                raise RunnerError('Limites operacionais precisam ser positivos')

    def _ensure_clients(self):
        if self.athena is not None and self.s3 is not None and self.glue is not None:
            return
        import boto3
        from botocore.config import Config
        session = boto3.Session(region_name=self.config['region'])
        sdk = Config(connect_timeout=10, read_timeout=60,
                     retries={'mode': 'standard', 'max_attempts': 3})
        if self.athena is None:
            self.athena = session.client('athena', config=sdk)
        if self.s3 is None:
            self.s3 = session.client('s3', config=sdk)
        if self.glue is None:
            self.glue = session.client('glue', config=sdk)

    def _log(self, event, **fields):
        print(json.dumps({'timestamp': datetime.now(timezone.utc).isoformat(),
                          'event': event, 'run_id': self.run_id, **fields},
                         ensure_ascii=False, sort_keys=True), flush=True)

    def _key(self, relative):
        if not isinstance(relative, str) or not relative or relative.startswith('/'):
            raise RunnerError('Caminho relativo inválido')
        if not re.fullmatch(r'[A-Za-z0-9_./=-]+', relative) or any(p in ('', '.', '..') for p in relative.split('/')):
            raise RunnerError('Caminho fora do namespace permitido')
        return self.run_key + relative

    def _get_bytes(self, relative):
        self._ensure_clients()
        try:
            result = self.s3.get_object(Bucket=self.bucket, Key=self._key(relative), ExpectedBucketOwner=self.owner)
        except Exception as error:
            if _error_code(error) in ('NoSuchKey', '404', 'NotFound'):
                return None
            raise
        body = result['Body']
        try:
            return body.read()
        finally:
            body.close()

    def _put_bytes(self, relative, data, content_type):
        self._ensure_clients()
        params = {'Bucket': self.bucket, 'Key': self._key(relative), 'Body': data,
                  'ContentType': content_type, 'IfNoneMatch': '*', 'ExpectedBucketOwner': self.owner,
                  'Metadata': {'sha256': _digest(data)}}
        kms_key = self.config.get('kms_key_arn')
        if not kms_key and self._encryption and self._encryption.get('EncryptionOption') == 'SSE_KMS':
            kms_key = self._encryption.get('KmsKey')
        if kms_key:
            params.update(ServerSideEncryption='aws:kms', SSEKMSKeyId=kms_key)
        for attempt in range(3):
            try:
                self.s3.put_object(**params)
                return {'uri': self.run_root + relative, 'sha256': _digest(data), 'created': True}
            except Exception as error:
                code = _error_code(error)
                if code in ('PreconditionFailed', '412'):
                    if self._get_bytes(relative) == data:
                        return {'uri': self.run_root + relative, 'sha256': _digest(data), 'created': False}
                    raise CollisionError('Objeto existente tem conteúdo diferente; nada foi sobrescrito') from None
                if code in ('ConditionalRequestConflict', '409') and attempt < 2:
                    self.sleep(0.1 * (attempt + 1))
                    continue
                raise
        raise RunnerError('Falha de escrita condicional')

    def put_json_once(self, relative, data):
        self._require_initialized()
        return self._put_bytes(relative, _json_bytes(data), 'application/json; charset=utf-8')

    def put_text_once(self, relative, text):
        self._require_initialized()
        return self._put_bytes(relative, text.encode('utf-8'), 'text/plain; charset=utf-8')

    def read_json(self, relative):
        data = self._get_bytes(relative)
        return None if data is None else json.loads(data)

    def _list_objects(self, prefix, max_keys=1000):
        continuation = None
        while True:
            params = {'Bucket': self.bucket, 'Prefix': prefix, 'MaxKeys': max_keys,
                      'ExpectedBucketOwner': self.owner}
            if continuation:
                params['ContinuationToken'] = continuation
            page = self.s3.list_objects_v2(**params)
            yield from page.get('Contents', [])
            if not page.get('IsTruncated'):
                return
            continuation = page.get('NextContinuationToken')
            if not continuation:
                raise RunnerError('Paginação S3 incompleta')

    def _assert_empty_prefix(self, prefix):
        if next(self._list_objects(prefix, max_keys=1), None) is not None:
            raise CollisionError('Destino S3 já ocupado; arquivos existentes serão preservados')

    def initialize(self, config_hash):
        if not isinstance(config_hash, str) or not re.fullmatch(r'[0-9a-f]{64}', config_hash):
            raise RunnerError('config_hash precisa ser SHA-256 hexadecimal')
        if self._initialized:
            if self._config_hash != config_hash:
                raise CollisionError('Configuração mudou dentro do mesmo runner')
            return {'run_root': self.run_root, 'config_hash': config_hash, 'resumed': False}
        self._ensure_clients()
        self.s3.head_bucket(Bucket=self.bucket, ExpectedBucketOwner=self.owner)
        group = self.athena.get_work_group(WorkGroup=self.config['workgroup'])['WorkGroup']
        if group.get('State', 'ENABLED') != 'ENABLED':
            raise RunnerError('Workgroup não está habilitado')
        settings = group.get('Configuration', {})
        engine = settings.get('EngineVersion', {}).get('EffectiveEngineVersion')
        if engine != 'Athena engine version 3':
            raise RunnerError('É necessário confirmar Athena engine version 3 no workgroup')
        if settings.get('EnforceWorkGroupConfiguration'):
            raise RunnerError('Workgroup impõe destinos/configuração; namespace exclusivo não é garantido')
        if settings.get('ManagedQueryResultsConfiguration', {}).get('Enabled'):
            raise RunnerError('Resultados gerenciados impedem o destino S3 explícito deste pacote')
        if self.config.get('kms_key_arn'):
            self._encryption = {'EncryptionOption': 'SSE_KMS', 'KmsKey': self.config['kms_key_arn']}
        else:
            self._encryption = settings.get('ResultConfiguration', {}).get('EncryptionConfiguration')
        if self._encryption and self._encryption.get('EncryptionOption') not in ('SSE_S3', 'SSE_KMS'):
            raise RunnerError('Criptografia de resultados não suportada; usar configuração de servidor explícita')
        claim = {'schema_version': 1, 'run_id': self.run_id, 'run_root': self.run_root, 'config_hash': config_hash}
        existing = self.read_json('claim.json')
        resumed = existing is not None
        if resumed:
            if not self.config.get('resume', False):
                raise CollisionError('Execução já existe; retomada explícita é obrigatória')
            if existing != claim:
                raise CollisionError('run_id pertence a outra configuração')
        else:
            self._assert_empty_prefix(self.run_key)
            receipt = self._put_bytes('claim.json', _json_bytes(claim), 'application/json; charset=utf-8')
            if not receipt['created'] and not self.config.get('resume', False):
                raise CollisionError('Outra execução reservou o mesmo run_id')
            resumed = not receipt['created']
        self._config_hash = config_hash
        self._initialized = True
        if resumed:
            for item in self._list_objects(self.run_key + 'receipts/'):
                if item['Key'].endswith(('/success.json', '/failure.json')):
                    receipt = self.read_json(item['Key'][len(self.run_key):])
                    if receipt and receipt.get('query_id'):
                        self._account(receipt['query_id'], receipt.get('statistics', {}))
        self._log('run_initialized', resumed=resumed, engine=engine,
                  cumulative_scanned_bytes=self.cumulative_scanned_bytes)
        return {'run_root': self.run_root, 'config_hash': config_hash, 'resumed': resumed}

    @property
    def cumulative_scanned_bytes(self):
        return sum(self._query_scans.values())

    def _account(self, query_id, statistics):
        scanned = int(statistics.get('DataScannedInBytes', 0))
        if scanned < 0:
            raise RunnerError('Estatística Athena inválida')
        self._query_scans[query_id] = max(scanned, self._query_scans.get(query_id, 0))
        self._known_queries.add(query_id)

    def _require_initialized(self):
        if not self._initialized:
            raise RunnerError('initialize deve ser chamado antes de executar ou gravar')

    def validate_sql(self, sql):
        if not isinstance(sql, str) or not sql.strip() or '{{' in sql or '}}' in sql:
            raise UnsafeSqlError('SQL vazio ou com parâmetros não resolvidos')
        tokens = _tokens(sql)
        semicolons = [i for i, token in enumerate(tokens) if token == ('symbol', ';')]
        if semicolons and semicolons != [len(tokens) - 1]:
            raise UnsafeSqlError('Somente uma instrução SQL é permitida')
        if semicolons:
            tokens.pop()
        words = [value.upper() for kind, value in tokens if kind == 'word']
        if not tokens or tokens[0][0] != 'word' or FORBIDDEN.intersection(words):
            raise UnsafeSqlError('Comando SQL não permitido')
        first = tokens[0][1].upper()
        if first in ('SELECT', 'WITH'):
            if 'CREATE' in words or 'SELECT' not in words:
                raise UnsafeSqlError('Somente SELECT é permitido nesta instrução')
            return {'kind': 'SELECT', 'table': None, 'location': None}
        if first != 'CREATE' or words.count('CREATE') != 1 or len(tokens) < 10:
            raise UnsafeSqlError('Somente SELECT e CTAS exclusivos são permitidos')
        if tokens[1][1].upper() != 'TABLE' or tokens[1][0] != 'word':
            raise UnsafeSqlError('CREATE permitido somente para CTAS')
        if not (tokens[2] == ('word', self.database) and tokens[3] == ('symbol', '.') and tokens[4][0] == 'word'):
            raise UnsafeSqlError('Destino CTAS fora do database autorizado')
        table = tokens[4][1]
        if not table.startswith(self.table_prefix) or not re.fullmatch(r'[a-z0-9_]+', table) or table == self.table_prefix:
            raise UnsafeSqlError('Tabela CTAS fora do namespace exclusivo')
        if tokens[5][1].upper() != 'WITH' or tokens[6] != ('symbol', '('):
            raise UnsafeSqlError('CTAS precisa declarar propriedades e localização')
        depth, closing = 0, None
        for i in range(6, len(tokens)):
            if tokens[i] == ('symbol', '('):
                depth += 1
            elif tokens[i] == ('symbol', ')'):
                depth -= 1
                if depth == 0:
                    closing = i
                    break
        if closing is None or closing + 2 >= len(tokens) or tokens[closing + 1][1].upper() != 'AS' or tokens[closing + 2][1].upper() not in ('SELECT', 'WITH'):
            raise UnsafeSqlError('CREATE não é CTAS válido')
        locations = []
        for i, (kind, value) in enumerate(tokens):
            if kind == 'word' and value.upper() == 'EXTERNAL_LOCATION':
                if i + 2 >= len(tokens) or tokens[i + 1] != ('symbol', '=') or tokens[i + 2][0] != 'string':
                    raise UnsafeSqlError('external_location precisa ser literal')
                if i >= closing:
                    raise UnsafeSqlError('external_location fora das propriedades CTAS')
                locations.append(tokens[i + 2][1])
        if len(locations) != 1 or not locations[0].startswith(self.run_root) or not locations[0].endswith('/'):
            raise UnsafeSqlError('Localização CTAS fora do prefixo exclusivo')
        tail = locations[0][len(self.run_root):-1]
        self._key(tail)
        return {'kind': 'CTAS', 'table': table, 'location': locations[0]}

    def _preflight_target(self, target):
        if target['kind'] != 'CTAS':
            return
        try:
            self.glue.get_table(DatabaseName=self.database, Name=target['table'])
        except Exception as error:
            if _error_code(error) != 'EntityNotFoundException':
                raise
        else:
            raise CollisionError('Tabela já existe; nunca será removida ou substituída')
        self._assert_empty_prefix(urlsplit(target['location']).path.lstrip('/'))

    def _stop(self, query_id):
        try:
            self.athena.stop_query_execution(QueryExecutionId=query_id)
            return True
        except Exception as error:
            self._log('query_stop_failed', query_id=query_id, error_code=_error_code(error))
            return False

    def _failure(self, step, intent, query_id, query, state, stop=False):
        cancelled = self._stop(query_id) if stop else False
        stats = query.get('Statistics', {})
        self._account(query_id, stats)
        receipt = {**intent, 'query_id': query_id, 'state': state,
                   'athena_state': query.get('Status', {}).get('State'),
                   'statistics': stats, 'stop_requested': cancelled}
        self.put_json_once('receipts/' + step + '/failure.json', receipt)
        self._log('query_failed', step=step, query_id=query_id, state=state,
                  data_scanned_bytes=int(stats.get('DataScannedInBytes', 0)),
                  cumulative_scanned_bytes=self.cumulative_scanned_bytes)
        raise RunnerError('Consulta não concluída com sucesso: ' + step + ' (' + state + ')')

    def execute(self, sql, step):
        self._require_initialized()
        if not isinstance(step, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,127}', step):
            raise RunnerError('Identificador de etapa inválido')
        target = self.validate_sql(sql)
        sql_hash = _digest(sql.encode('utf-8'))
        token = _digest(_json_bytes({'config_hash': self._config_hash, 'run_root': self.run_root,
                                    'step': step, 'sql_hash': sql_hash}))
        output = self.run_root + 'athena/' + step + '/'
        intent = {'schema_version': 1, 'step': step, 'config_hash': self._config_hash,
                  'sql_hash': sql_hash, 'client_request_token': token,
                  'table': target['table'], 'table_location': target['location'], 'output_location': output}
        stem = 'receipts/' + step + '/'
        previous_intent = self.read_json(stem + 'intent.json')
        if previous_intent is not None and previous_intent != intent:
            raise CollisionError('Etapa existente contém SQL ou configuração diferente')
        success = self.read_json(stem + 'success.json')
        failure = self.read_json(stem + 'failure.json')
        if failure is not None:
            raise RunnerError('Etapa falhou anteriormente; arquivos órfãos foram preservados. Use outro run_id.')
        if success is not None:
            if previous_intent != intent or any(success.get(k) != v for k, v in intent.items()) or success.get('state') != 'SUCCEEDED':
                raise CollisionError('Recibo de sucesso não corresponde à etapa solicitada')
            self._account(success['query_id'], success['statistics'])
            self._log('query_reused', step=step, query_id=success['query_id'],
                      cumulative_scanned_bytes=self.cumulative_scanned_bytes)
            return {'query_id': success['query_id'], 'statistics': success['statistics'], 'state': 'SUCCEEDED', 'reused': True}
        if self.cumulative_scanned_bytes >= self.config['max_scan_bytes']:
            raise RunnerError('Orçamento cumulativo de bytes lidos já atingido')
        if previous_intent is None:
            self._preflight_target(target)
            self._assert_empty_prefix(urlsplit(output).path.lstrip('/'))
            self.put_text_once('audit/sql/' + step + '.sql', sql)
            self.put_json_once(stem + 'intent.json', intent)
        submission = self.read_json(stem + 'submission.json')
        reused = submission is not None or previous_intent is not None
        if submission is not None:
            if any(submission.get(k) != v for k, v in intent.items()) or not submission.get('query_id'):
                raise CollisionError('Recibo de submissão inválido')
            query_id = submission['query_id']
        else:
            result_configuration = {'OutputLocation': output, 'ExpectedBucketOwner': self.owner}
            if self._encryption:
                result_configuration['EncryptionConfiguration'] = self._encryption
            try:
                response = self.athena.start_query_execution(
                    QueryString=sql, ClientRequestToken=token,
                    QueryExecutionContext={'Database': self.database, 'Catalog': 'AwsDataCatalog'},
                    WorkGroup=self.config['workgroup'], ResultConfiguration=result_configuration,
                    ResultReuseConfiguration={'ResultReuseByAgeConfiguration': {'Enabled': False}})
            except Exception as error:
                # Resultado ambíguo de rede é retomado pelo mesmo ClientRequestToken.
                self.put_json_once(stem + 'submission_errors/' + uuid.uuid4().hex + '.json',
                                   {'step': step, 'error_code': _error_code(error), 'sql_hash': sql_hash})
                self._log('query_submission_error', step=step, error_code=_error_code(error))
                raise RunnerError('Falha de submissão; retomar preservando run_id e configuração') from None
            query_id = response['QueryExecutionId']
            self.put_json_once(stem + 'submission.json', {**intent, 'query_id': query_id})
            self._log('query_submitted', step=step, query_id=query_id)
        self._known_queries.add(query_id)
        started = self.clock()
        last_state = None
        while True:
            query = self.athena.get_query_execution(QueryExecutionId=query_id)['QueryExecution']
            state = query.get('Status', {}).get('State')
            stats = query.get('Statistics', {})
            self._account(query_id, stats)
            actual_output = query.get('ResultConfiguration', {}).get('OutputLocation', '')
            namespace_ok = actual_output.startswith(output)
            namespace_ok &= query.get('WorkGroup', self.config['workgroup']) == self.config['workgroup']
            namespace_ok &= query.get('Query', sql) == sql
            if not namespace_ok:
                self._failure(step, intent, query_id, query, 'NAMESPACE_MISMATCH', stop=state not in TERMINAL)
            if self.cumulative_scanned_bytes > self.config['max_scan_bytes']:
                self._failure(step, intent, query_id, query, 'SCAN_LIMIT_EXCEEDED', stop=state not in TERMINAL)
            if state != last_state:
                self._log('query_state', step=step, query_id=query_id, state=state,
                          data_scanned_bytes=int(stats.get('DataScannedInBytes', 0)))
                last_state = state
            if state == 'SUCCEEDED':
                success = {**intent, 'query_id': query_id, 'state': state, 'statistics': stats}
                self.put_json_once(stem + 'success.json', success)
                self._log('query_succeeded', step=step, query_id=query_id,
                          data_scanned_bytes=int(stats.get('DataScannedInBytes', 0)),
                          engine_execution_ms=int(stats.get('EngineExecutionTimeInMillis', 0)),
                          cumulative_scanned_bytes=self.cumulative_scanned_bytes)
                return {'query_id': query_id, 'statistics': stats, 'state': state, 'reused': reused}
            if state in ('FAILED', 'CANCELLED'):
                self._failure(step, intent, query_id, query, state)
            if state not in ('QUEUED', 'RUNNING'):
                self._failure(step, intent, query_id, query, 'UNKNOWN_STATE', stop=True)
            elapsed = self.clock() - started
            submitted = query.get('Status', {}).get('SubmissionDateTime')
            if isinstance(submitted, datetime):
                if submitted.tzinfo is None:
                    submitted = submitted.replace(tzinfo=timezone.utc)
                elapsed = max(elapsed, (datetime.now(timezone.utc) - submitted).total_seconds())
            if elapsed >= self.config['query_timeout_seconds']:
                self._failure(step, intent, query_id, query, 'TIMEOUT', stop=True)
            self.sleep(min(self.config['poll_seconds'], self.config['query_timeout_seconds'] - elapsed))

    def fetch_rows(self, result, max_rows=100):
        self._require_initialized()
        if type(max_rows) is not int or max_rows < 0:
            raise RunnerError('max_rows deve ser inteiro não negativo')
        if max_rows == 0:
            return []
        query_id = result.get('query_id')
        if result.get('state') != 'SUCCEEDED' or query_id not in self._known_queries:
            raise RunnerError('Resultados só podem ser lidos para consultas concluídas deste runner')
        rows, token, names, seen_tokens = [], None, None, set()
        while len(rows) < max_rows:
            params = {'QueryExecutionId': query_id, 'MaxResults': min(1000, max_rows - len(rows) + (1 if names is None else 0))}
            if token:
                params['NextToken'] = token
            response = self.athena.get_query_results(**params)
            result_set = response.get('ResultSet', {})
            values = result_set.get('Rows', [])
            if names is None:
                names = [column['Name'] for column in result_set.get('ResultSetMetadata', {}).get('ColumnInfo', [])]
                if len(set(names)) != len(names):
                    raise RunnerError('Colunas duplicadas impedem leitura segura como dicionários')
                values = values[1:]  # Athena envia cabeçalho somente na primeira página.
            for row in values:
                data = row.get('Data', [])
                rows.append({name: data[i].get('VarCharValue') if i < len(data) else None for i, name in enumerate(names)})
                if len(rows) == max_rows:
                    break
            token = response.get('NextToken')
            if not token:
                break
            if token in seen_tokens:
                raise RunnerError('Paginação Athena repetiu um token; leitura interrompida')
            seen_tokens.add(token)
        return rows
