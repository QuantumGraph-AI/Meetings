"""Trading Ahead — P99 de 182 dias, por PF/PJ, piso de R$ 100 mil.

Arquivo único para futura implantação no AWS Glue; não precisa do wheel ta_p99.
Inclui SQLs e núcleo aprovado, com logs espelhados em Main-glue-v2.py (Cripto).

Modo padrão: plan (offline). execute precisa ser escolhido explicitamente.
Dependências em execute: boto3, botocore, pandas, pyarrow e awswrangler
compatíveis com a versão de Python do job. O modo plan usa a biblioteca padrão.

Configuração: --config arquivo.json, --config-json '{...}' ou, em execute,
--config-s3 s3://bucket/prefixo/config.json --config-bucket-owner CONTA.
Saídas e tabelas recebem run_id exclusivo; não há DROP/DELETE/overwrite.
Logs de resumo/detalhe: Parquet, partição safra, append no namespace da execução.
SUCESSO só é publicado após os filtros finais, verificações e persistência dos logs.

A entrega deste arquivo não cria/atualiza/aciona job Glue.
"""

# ========================================================================
# NÚCLEO APROVADO — POLICY
# ========================================================================
from datetime import date, timedelta
import calendar
import math
import re

POLICY_VERSION = "p99_piso100k_182d_v2"
APPROVED_HISTORY_DAYS = 182
CONTROL_HISTORY_DAYS = 91
PERCENTILE = 0.99
FLOOR_BRL = 100000.0
DEDUP_KEY = (
    "codigo_cliente", "cpf_cnpj_cliente", "ativo_negociado",
    "ativo_negociado_4letras", "tipo_pessoa", "origem_operacao", "data_evento",
    "soma_quantidade_compra_historica", "soma_quantidade_venda_historica",
    "soma_quantidade_compra_15dias", "soma_quantidade_venda_15dias",
    "quantidade_compra_d0", "quantidade_venda_d0",
    "soma_quantidade_compra_5diasdepois", "soma_quantidade_venda_5diasdepois",
    "resultado", "diferenca_volume",
)


def month_bounds(month):
    if not isinstance(month, str) or not re.fullmatch(r"20\d{2}(0[1-9]|1[0-2])", month):
        raise ValueError("Competência deve estar em AAAAMM, entre 2000 e 2099.")
    year, number = int(month[:4]), int(month[4:])
    return date(year, number, 1), date(year, number, calendar.monthrange(year, number)[1])


def historical_window(month, days):
    if type(days) is not int or days not in (CONTROL_HISTORY_DAYS, APPROVED_HISTORY_DAYS):
        raise ValueError("Histórico deve ser 182 dias; 91 dias é permitido somente para planejamento de controle.")
    start, _ = month_bounds(month)
    return start - timedelta(days=days), start - timedelta(days=1)


def history_months(month, days):
    low, high = historical_window(month, days)
    cursor = low.replace(day=1)
    result = []
    while cursor <= high:
        result.append(cursor.strftime("%Y%m"))
        cursor = date(cursor.year + 1, 1, 1) if cursor.month == 12 else date(cursor.year, cursor.month + 1, 1)
    return result


def quantile_positions(size):
    if type(size) is not int or size < 1:
        raise ValueError("P99 indefinido: amostra histórica vazia.")
    position = (size - 1) * PERCENTILE
    lower = math.floor(position)
    upper = math.ceil(position)
    return lower + 1, upper + 1, position - lower


def interpolate_p99(lower_value, upper_value, size):
    """Mesma interpolação linear e ramo numérico usados por numpy.quantile."""
    _, _, weight = quantile_positions(size)
    low, high = float(lower_value), float(upper_value)
    if not math.isfinite(low) or not math.isfinite(high) or low > high:
        raise ValueError("Valores de interpolação inválidos ou não finitos.")
    delta = high - low
    answer = high - delta * (1.0 - weight) if weight >= 0.5 else low + delta * weight
    if not math.isfinite(answer):
        raise ValueError("Overflow no cálculo do P99.")
    return answer


def linear_p99(values):
    """Referência local; no job, Athena ordena e só entrega as duas posições."""
    ordered = []
    for value in values:
        if value is None:
            continue
        number = float(value)
        if math.isnan(number):
            continue
        if not math.isfinite(number):
            raise ValueError("Histórico contém resultado infinito.")
        ordered.append(number)
    ordered.sort()
    low, high, _ = quantile_positions(len(ordered))
    return interpolate_p99(ordered[low - 1], ordered[high - 1], len(ordered))


def effective_cutoff(p99):
    value = float(p99)
    if not math.isfinite(value):
        raise ValueError("P99 precisa ser definido e finito; não há fallback silencioso.")
    return max(value, FLOOR_BRL)


def qualifies(result, p99, final_classification):
    if final_classification != "ALERTA" or result is None:
        return False
    amount = float(result)
    return math.isfinite(amount) and amount > effective_cutoff(p99)


def dedup_projection_sql(alias="t"):
    """Projeção literal de 17 campos, sem arredondar DOUBLE para centavos."""
    if not re.fullmatch(r"[a-z][a-z0-9_]*", alias):
        raise ValueError("Alias SQL inválido.")
    fields = []
    for name in DEDUP_KEY:
        if name == "ativo_negociado_4letras":
            fields.append(f"SUBSTR({alias}.ativo_negociado, 1, 4) AS {name}")
        elif name == "diferenca_volume":
            fields.append(f"ABS({alias}.soma_volume_venda_15dias - {alias}.soma_volume_compra_15dias) AS {name}")
        else:
            fields.append(f"{alias}.{name}")
    return ",\n    ".join(fields)

# ========================================================================
# NÚCLEO APROVADO — CONFIG
# ========================================================================
from datetime import date, timedelta
from urllib.parse import urlparse
import hashlib
import json
import re
import uuid


DEFAULTS = {
    "history_days": APPROVED_HISTORY_DAYS,
    "region": "sa-east-1", "query_timeout_seconds": 3600,
    "poll_seconds": 3, "max_scan_bytes": 1099511627776,
    "resume": False, "kms_key_arn": None, "history_manifest": None,
    "include_cta12_diagnostic": False,
}
REQUIRED = {"target_month", "database", "workgroup",
            "output_s3_uri", "source_watermark", "expected_bucket_owner"}


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,127}", value):
        raise ValueError("Identificador SQL inválido.")
    return value


def qualified_table(value):
    parts = str(value).split(".")
    if len(parts) != 2:
        raise ValueError("Tabela deve conter database.nome.")
    return ".".join(identifier(p) for p in parts)


def s3_parts(uri):
    value = urlparse(uri)
    if value.scheme != "s3" or not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", value.netloc):
        raise ValueError("Destino S3 inválido.")
    key = value.path.lstrip("/")
    if (not key or value.query or value.fragment or ".." in key.split("/")
            or not re.fullmatch(r"[a-zA-Z0-9_./=-]+", key)):
        raise ValueError("É obrigatório prefixo S3 dedicado e sem caracteres especiais.")
    return value.netloc, key


def validate_config(raw, executing=False, today=None):
    allowed = REQUIRED | set(DEFAULTS) | {"run_id"}
    unknown = set(raw) - allowed
    if unknown:
        raise ValueError("Parâmetros desconhecidos: " + ", ".join(sorted(unknown)))
    missing = REQUIRED - set(raw)
    if missing:
        raise ValueError("Parâmetros obrigatórios ausentes: " + ", ".join(sorted(missing)))
    cfg = dict(DEFAULTS, **raw)
    cfg.setdefault("run_id", "r" + uuid.uuid4().hex[:24])
    if not re.fullmatch(r"[a-z][a-z0-9_]{7,31}", cfg["run_id"]):
        raise ValueError("run_id: 8 a 32 caracteres minúsculos, números e sublinhado.")
    historical_window(cfg["target_month"], cfg["history_days"])
    if executing and cfg["history_days"] != APPROVED_HISTORY_DAYS:
        raise ValueError("Execução autorizada somente com 182 dias (6 meses); 91 dias é apenas controle em --mode plan.")
    identifier(cfg["database"])
    if not re.fullmatch(r"[a-zA-Z0-9._-]{1,128}", cfg["workgroup"]):
        raise ValueError("Workgroup inválido.")
    if not re.fullmatch(r"[a-z]{2}(?:-gov)?-[a-z]+-\d", cfg["region"]):
        raise ValueError("Região inválida.")
    s3_parts(cfg["output_s3_uri"])
    cfg["output_s3_uri"] = cfg["output_s3_uri"].rstrip("/") + "/"
    if "/TRADING_AHEAD/ctas/" in cfg["output_s3_uri"]:
        raise ValueError("Destino original de CTAs não pode ser usado pela nova versão.")
    if not re.fullmatch(r"\d{12}", str(cfg["expected_bucket_owner"])):
        raise ValueError("expected_bucket_owner deve ser uma conta AWS com 12 dígitos.")
    cfg["expected_bucket_owner"] = str(cfg["expected_bucket_owner"])
    for name in ["query_timeout_seconds", "poll_seconds", "max_scan_bytes"]:
        if type(cfg[name]) is not int or cfg[name] <= 0:
            raise ValueError(name + " deve ser inteiro positivo.")
    if cfg["poll_seconds"] > 30:
        raise ValueError("poll_seconds deve ser no máximo 30.")
    for name in ["resume", "include_cta12_diagnostic"]:
        if type(cfg[name]) is not bool:
            raise ValueError(name + " deve ser booleano.")
    watermark = date.fromisoformat(cfg["source_watermark"])
    _, last = month_bounds(cfg["target_month"])
    # CTA1 e CTA6 leem até fim do mês + 7; financeiro usa até D+5.
    required_watermark = last + timedelta(days=7)
    if watermark < required_watermark:
        raise ValueError("Fontes precisam estar completas até " + required_watermark.isoformat() + " (fim do mês + 7 dias, CTA1/CTA6).")
    if executing and watermark > (today or date.today()):
        raise ValueError("Não é possível atestar uma data de fonte futura.")
    if cfg["kms_key_arn"] and not re.fullmatch(r"arn:aws(?:-us-gov|-cn)?:kms:[a-z0-9-]+:\d{12}:key/[a-zA-Z0-9-]+", cfg["kms_key_arn"]):
        raise ValueError("ARN da chave KMS inválido.")
    if cfg["history_manifest"] is not None and not isinstance(cfg["history_manifest"], dict):
        raise ValueError("history_manifest deve ser objeto JSON ou null.")
    if executing and any("substituir" in str(cfg[k]) for k in ("database", "workgroup", "output_s3_uri")):
        raise ValueError("Substitua os parâmetros de ambiente antes de executar.")
    return cfg


def config_fingerprint(config, bundle_hash):
    stable = dict(config)
    stable.pop("resume", None)
    stable["policy_version"] = POLICY_VERSION
    stable["sql_bundle_hash"] = bundle_hash
    return hashlib.sha256(json.dumps(stable, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()

# ========================================================================
# NÚCLEO APROVADO — AWS_RUNTIME
# ========================================================================
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

# ========================================================================
# NÚCLEO APROVADO — PIPELINE
# ========================================================================
from datetime import date, timedelta
from pathlib import Path
import hashlib
import json
import re


BASE_STAGES = [f"cta{i:02d}" for i in range(1, 12)] + ["cta13_base"]
FINAL_STAGES = [f"cta{i:02d}" for i in range(14, 19)] + ["cta18_alertas", "cta19"]
SOURCE_HASH = "85c5124c8db46af4ef8db73ac514b7ecef08f933a4e4f702c81f38960134b8c5"
POPULATION_CONTRACT = "CTA13_BASE_ALL_RESULTS_ORIGINAL_4DP_V1"


def read_bundle(directory):
    root = Path(directory)
    manifest = json.loads((root / "sql/manifesto_sql.json").read_text(encoding="utf-8"))
    if manifest["source_notebook_sha256"] != SOURCE_HASH:
        raise ValueError("Versão de CTA de referência não reconhecida.")
    bundle = {}
    for item in manifest["templates"]:
        if item["stage_id"] in bundle:
            raise ValueError("Etapa duplicada no manifesto SQL: " + item["stage_id"])
        sql = (root / item["file"]).read_text(encoding="utf-8")
        if hashlib.sha256(sql.encode()).hexdigest() != item["sha256"]:
            raise ValueError("SQL alterado sem atualizar o contrato: " + item["stage_id"])
        bundle[item["stage_id"]] = dict(item, sql=sql)
    bundle_fingerprint(bundle)
    return bundle


def bundle_fingerprint(bundle):
    required = set(BASE_STAGES + FINAL_STAGES + ["cta12_diagnostico"])
    if not isinstance(bundle, dict) or set(bundle) != required:
        raise ValueError("Bundle SQL deve conter exatamente as 20 etapas da CTA de referência.")
    for stage, item in bundle.items():
        if not isinstance(item, dict) or item.get("stage_id") != stage:
            raise ValueError("Identificação de etapa inconsistente no bundle SQL: " + stage)
        sql, digest = item.get("sql"), item.get("sha256")
        if (not isinstance(sql, str) or not sql.strip()
                or not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest)
                or hashlib.sha256(sql.encode("utf-8")).hexdigest() != digest):
            raise ValueError("Conteúdo SQL diverge do hash declarado: " + stage)
        if not isinstance(item.get("logical_table"), str) or not item["logical_table"]:
            raise ValueError("Etapa sem nome lógico de tabela: " + stage)
    return hashlib.sha256(json.dumps(
        {key: item["sha256"] for key, item in sorted(bundle.items())},
        sort_keys=True, separators=(",", ":"),
    ).encode()).hexdigest()


class Pipeline:
    def __init__(self, config, bundle, runner=None):
        self.bundle_hash = bundle_fingerprint(bundle)
        self.config = validate_config(config)
        self.bundle = {stage: dict(item) for stage, item in bundle.items()}
        self.runner = runner
        self.fingerprint = config_fingerprint(self.config, self.bundle_hash)
        self.root = self.config["output_s3_uri"].rstrip("/") + "/runs/" + self.config["run_id"] + "/"
        self.stage_records = []
        self.month_records = {}

    def table(self, stage, month=None):
        if not re.fullmatch(r"[a-z][a-z0-9_]*", stage):
            raise ValueError("Etapa inválida.")
        month = month or self.config["target_month"]
        month_bounds(month)
        return self.config["database"] + ".ta_p99_" + self.config["run_id"] + "_" + stage + "_" + month

    def location(self, stage, month=None):
        month = month or self.config["target_month"]
        return self.root + "data/" + month + "/" + stage + "/"

    def render_stage(self, stage, month):
        first, last = month_bounds(month)
        params = {"SAFRA": month, "SAFRA_ANO": month[:4], "SAFRA_MES": month[4:],
                  "SAFRA_INICIO": first.isoformat(), "SAFRA_FIM": last.isoformat()}
        source = self.bundle[stage]["sql"]
        logical = {item["logical_table"]: name for name, item in self.bundle.items()}
        logical.update({"CTA13_CalculoResultado_PF_{{SAFRA}}": "cta13_pf",
                        "CTA13_CalculoResultado_PJ_{{SAFRA}}": "cta13_pj"})
        for old, name in sorted(logical.items(), key=lambda pair: -len(pair[0])):
            source = re.sub(r"workspace_db\." + re.escape(old) + r"(?![a-zA-Z0-9_])",
                            self.table(name, month), source, flags=re.I)
        source, replaced = re.subn(
            r"external_location\s*=\s*'[^']*'",
            "external_location = '" + self.location(stage, month) + "'",
            source, flags=re.I,
        )
        if replaced != 1:
            raise ValueError("Cada template deve conter exatamente um destino CTAS.")
        for key, value in params.items():
            source = re.sub(r"{{\s*" + key + r"\s*}}", value, source)
        if "{{" in source or re.search(r"workspace_db\.CTA", source, flags=re.I):
            raise ValueError("Parâmetro ou tabela original não substituído.")
        if "/TRADING_AHEAD/ctas/" in source:
            raise ValueError("Destino original ainda presente.")
        if not source.lstrip().lower().startswith("create table " + self.table(stage, month)):
            raise ValueError("Destino da consulta fora do namespace da execução.")
        return source

    def ctas(self, stage, select, month=None):
        return ("CREATE TABLE " + self.table(stage, month) + "\n"
                "WITH (format = 'PARQUET', write_compression = 'SNAPPY', "
                "external_location = '" + self.location(stage, month) + "') AS\n" + select)

    def plan(self):
        cfg = self.config
        history = self.validated_history_manifest()
        tasks = []
        for month in history_months(cfg["target_month"], cfg["history_days"]) + [cfg["target_month"]]:
            if month in history:
                tasks.append({"month": month, "action": "reuse_validated_history", "table": history[month]["table"]})
                continue
            for stage in BASE_STAGES:
                tasks.append({"month": month, "stage": stage, "table": self.table(stage, month),
                              "location": self.location(stage, month), "sql": self.render_stage(stage, month)})
        month = cfg["target_month"]
        if cfg["include_cta12_diagnostic"]:
            tasks.append({"month": month, "stage": "cta12_diagnostico", "sql": self.render_stage("cta12_diagnostico", month)})
        tasks.append({"month": month, "stage": "calibracao", "table": self.table("calibracao"),
                      "action": "deduplicar_historico_e_calcular_p99_exato",
                      "depends_on": "bases históricas cta13_base",
                      "sql_generated_at_runtime": "Depende das tabelas históricas reconstruídas ou validadas."})
        for person in ("pf", "pj"):
            tasks.append({"month": month, "stage": "cta13_" + person,
                          "sql": self.candidate_sql(person)})
        for stage in FINAL_STAGES:
            tasks.append({"month": month, "stage": stage, "sql": self.render_stage(stage, month)})
        for person in ("pf", "pj"):
            for suffix in ("_vinculos", ""):
                stage = "alertas_" + person + suffix
                tasks.append({"month": month, "stage": stage, "table": self.table(stage),
                              "location": self.location(stage),
                              "action": "filtro_financeiro_final" if suffix else "deduplicacao_chave_cta",
                              "sql_generated_at_runtime": "Depende do P99 calculado e do esquema original da saída final."})
        low, high = historical_window(month, cfg["history_days"])
        return {"mode": "plan", "aws_calls": 0, "config": cfg, "config_hash": self.fingerprint,
                "policy_version": POLICY_VERSION,
                "execution_window_allowed": cfg["history_days"] == APPROVED_HISTORY_DAYS,
                "history_usage": "approved" if cfg["history_days"] == APPROVED_HISTORY_DAYS else "control_plan_only",
                "source_notebook_sha256": SOURCE_HASH, "sql_bundle_hash": self.bundle_hash,
                "historical_window": [low.isoformat(), high.isoformat()],
                "financial_rule": "resultado_final = ALERTA e resultado > max(P99_por_publico, 100000)",
                "calibration": "DISTINCT chave célula74; ORDER BY resultado; duas posições; interpolação linear Python",
                "publication": "Somente após todas as etapas e verificações; manifesto SUCCESS.json",
                "tasks": tasks}

    def validated_history_manifest(self):
        manifest = self.config.get("history_manifest")
        if manifest is None:
            return {}
        if (manifest.get("status") != "COMPLETE"
                or manifest.get("population_contract") != POPULATION_CONTRACT
                or manifest.get("sql_bundle_hash") != self.bundle_hash
                or manifest.get("source_notebook_sha256") != SOURCE_HASH):
            raise ValueError("Manifesto histórico incompatível com a população original.")
        records = manifest.get("months")
        if not isinstance(records, dict):
            raise ValueError("Manifesto histórico sem competências explícitas.")
        valid = {}
        for month in history_months(self.config["target_month"], self.config["history_days"]):
            if month not in records:
                continue  # Competência faltante será reconstruída, nunca tratada como zero.
            item = records[month]
            table = qualified_table(item["table"])
            previous = re.fullmatch(r"ta_p99_([a-z][a-z0-9_]{7,31})_cta13_base_" + month, table.split(".")[1])
            if (not previous or item.get("stage") != "cta13_base" or item.get("month") != month):
                raise ValueError("Histórico deve vir de uma base desta versão, sem corte financeiro.")
            location = item["location"]
            s3_parts(location)
            if not location.startswith(self.config["output_s3_uri"]):
                raise ValueError("Histórico fora da raiz de resultados autorizada.")
            old_config = dict(self.config, run_id=previous.group(1), database=table.split(".")[0])
            old_pipeline = Pipeline(old_config, self.bundle)
            if location.rstrip("/") != old_pipeline.location("cta13_base", month).rstrip("/"):
                raise ValueError("Localização histórica incompatível com a execução de origem.")
            expected_sql = old_pipeline.render_stage("cta13_base", month)
            if item.get("sql_sha256") != hashlib.sha256(expected_sql.encode()).hexdigest():
                raise ValueError("Histórico não tem proveniência SQL da base sem corte.")
            if not isinstance(item.get("query_id"), str) or not item["query_id"]:
                raise ValueError("Histórico sem identificador da consulta de origem.")
            if type(item["row_count"]) is not int or item["row_count"] < 0:
                raise ValueError("Histórico sem contagem explícita.")
            if date.fromisoformat(item["source_watermark"]) < month_bounds(month)[1] + timedelta(days=7):
                raise ValueError("Histórico ainda não maturado.")
            valid[month] = item
        return valid

    def rows(self, sql, step, limit=100):
        result = self.runner.execute(sql, step)
        return self.runner.fetch_rows(result, max_rows=limit)

    def materialize(self, stage, sql, month=None):
        month = month or self.config["target_month"]
        step = month + "_" + stage
        result = self.runner.execute(sql, step)
        counted = self.rows("SELECT COUNT(*) AS row_count FROM " + self.table(stage, month), step + "_count", 1)
        row_count = int(counted[0]["row_count"])
        record = {"month": month, "stage": stage, "table": self.table(stage, month),
                  "location": self.location(stage, month), "row_count": row_count,
                  "query_id": result["query_id"], "sql_sha256": hashlib.sha256(sql.encode()).hexdigest()}
        self.runner.put_json_once("counts/" + step + ".json", record)
        print(json.dumps(dict(record, event="stage_count", run_id=self.config["run_id"]), sort_keys=True), flush=True)
        self.stage_records.append(record)
        return record

    def base_month(self, month):
        for stage in BASE_STAGES:
            record = self.materialize(stage, self.render_stage(stage, month), month)
        record = dict(record, source_watermark=self.config["source_watermark"])
        self.month_records[month] = record
        return record

    def candidate_sql(self, person):
        if person not in ("pf", "pj"):
            raise ValueError("Público inválido.")
        code = "F" if person == "pf" else "J"
        return self.ctas("cta13_" + person, "SELECT * FROM " + self.table("cta13_base")
                         + " WHERE tipo_pessoa = '" + code + "'")

    def validate_reused_history(self, month, item):
        db, name = item["table"].split(".")
        table = self.runner.glue.get_table(DatabaseName=db, Name=name)["Table"]
        if table["StorageDescriptor"]["Location"].rstrip("/") != item["location"].rstrip("/"):
            raise ValueError("Localização da tabela histórica diverge do manifesto.")
        execution = self.runner.athena.get_query_execution(QueryExecutionId=item["query_id"])["QueryExecution"]
        if (execution["Status"]["State"] != "SUCCEEDED"
                or hashlib.sha256(execution["Query"].encode()).hexdigest() != item["sql_sha256"]):
            raise ValueError("Consulta de origem histórica não comprova a base financeira sem corte.")
        stats = self.rows(
            "SELECT COUNT(*) AS row_count, COUNT_IF(CAST(safra AS VARCHAR) <> '" + month
            + "' OR safra IS NULL) AS wrong_month FROM " + item["table"],
            month + "_history_reuse_count", 1,
        )[0]
        if int(stats["row_count"]) != item["row_count"] or int(stats["wrong_month"]):
            raise ValueError("Conteúdo da tabela histórica diverge do manifesto.")
        self.month_records[month] = item

    def calibration_sql(self):
        low, high = historical_window(self.config["target_month"], self.config["history_days"])
        parts = []
        for month in history_months(self.config["target_month"], self.config["history_days"]):
            source = qualified_table(self.month_records[month]["table"])
            parts.append("SELECT " + dedup_projection_sql("t") + "\nFROM " + source + " t\n"
                         "WHERE CAST(t.data_evento AS DATE) BETWEEN DATE '" + low.isoformat()
                         + "' AND DATE '" + high.isoformat() + "'")
        return self.ctas("calibracao", "SELECT DISTINCT * FROM (\n" + "\nUNION ALL\n".join(parts) + "\n) historico")

    def calibrate(self):
        self.materialize("calibracao", self.calibration_sql())
        table = self.table("calibracao")
        stats = self.rows(
            "SELECT tipo_pessoa, COUNT(*) AS total, COUNT(resultado) AS valid_count, "
            "COUNT_IF(resultado IS NOT NULL AND NOT is_finite(CAST(resultado AS DOUBLE))) AS invalid_count, "
            "COUNT_IF(resultado < 0) AS negative_count, COUNT_IF(resultado = 0) AS zero_count "
            "FROM " + table + " GROUP BY tipo_pessoa", "p99_population_stats", 3,
        )
        counts = {r["tipo_pessoa"]: r for r in stats}
        if set(counts) != {"F", "J"}:
            raise ValueError("Falta amostra histórica de PF/PJ ou existe público desconhecido.")
        predicates = []
        for code in ("F", "J"):
            if int(counts[code]["invalid_count"]):
                raise ValueError("Histórico contém resultado não finito.")
            n = int(counts[code]["valid_count"])
            lo, hi, _ = quantile_positions(n)
            predicates.append(f"(tipo_pessoa = '{code}' AND rank_number IN ({lo}, {hi}))")
        ordered = self.rows(
            "WITH ranked AS (SELECT tipo_pessoa, resultado, ROW_NUMBER() OVER "
            "(PARTITION BY tipo_pessoa ORDER BY resultado) AS rank_number FROM " + table
            + " WHERE resultado IS NOT NULL) SELECT tipo_pessoa, resultado, rank_number FROM ranked WHERE "
            + " OR ".join(predicates), "p99_exact_ranks", 4,
        )
        values = {(r["tipo_pessoa"], int(r["rank_number"])): float(r["resultado"]) for r in ordered}
        cuts = {}
        low, high = historical_window(self.config["target_month"], self.config["history_days"])
        for code in ("F", "J"):
            n = int(counts[code]["valid_count"])
            lo, hi, weight = quantile_positions(n)
            p99 = interpolate_p99(values[(code, lo)], values[(code, hi)], n)
            cuts[code] = {"p99": p99, "cutoff": effective_cutoff(p99), "sample_size": n,
                          "lower_rank": lo, "upper_rank": hi, "weight": weight,
                          "history_start": low.isoformat(), "history_end": high.isoformat(),
                          "history_days": self.config["history_days"], "floor": 100000.0}
            print(json.dumps(dict(cuts[code], event="p99_calibrated", person=code,
                                  run_id=self.config["run_id"]), sort_keys=True), flush=True)
        self.runner.put_json_once("policy/cuts.json", cuts)
        return cuts

    def column_names(self, table):
        db, name = qualified_table(table).split(".")
        metadata = self.runner.glue.get_table(DatabaseName=db, Name=name)["Table"]
        columns = metadata["StorageDescriptor"]["Columns"] + metadata.get("PartitionKeys", [])
        result = [item["Name"].lower() for item in columns]
        if len(result) != len(set(result)) or any(not re.fullmatch(r"[a-z][a-z0-9_]*", c) for c in result):
            raise ValueError("Esquema de saída contém nomes inesperados ou duplicados.")
        return result

    def publish_person(self, person, cut):
        source_stage = "cta18_alertas" if person == "pf" else "cta19"
        source = self.table(source_stage)
        columns = self.column_names(source)
        expressions = ["t.*"]
        if "ativo_negociado_4letras" not in columns:
            expressions.append("SUBSTR(t.ativo_negociado, 1, 4) AS ativo_negociado_4letras")
        if "diferenca_volume" not in columns:
            expressions.append("ABS(t.soma_volume_venda_15dias - t.soma_volume_compra_15dias) AS diferenca_volume")
        # CAST de representação round-trip para DOUBLE: sem arredondar o corte.
        p99 = "CAST('" + repr(cut["p99"]) + "' AS DOUBLE)"
        cutoff = "CAST('" + repr(cut["cutoff"]) + "' AS DOUBLE)"
        label = "P99 3 meses" if self.config["history_days"] == 91 else "P99 6 meses"
        expressions += [p99 + " AS p99_financeiro", cutoff + " AS corte_financeiro_aplicado",
                        str(self.config["history_days"]) + " AS dias_historico_percentil",
                        "'" + POLICY_VERSION + "' AS versao_regra",
                        "'" + self.config["run_id"] + "' AS id_execucao",
                        "'" + label + "' AS cenario_experimento"]
        linked_stage = "alertas_" + person + "_vinculos"
        select = "SELECT " + ",\n".join(expressions) + "\nFROM " + source + " t\nWHERE resultado_final = 'ALERTA' AND resultado > " + cutoff
        linked = self.materialize(linked_stage, self.ctas(linked_stage, select))
        linked_columns = self.column_names(linked["table"])
        if not set(DEDUP_KEY).issubset(linked_columns):
            raise ValueError("Saída não contém a chave completa da CTA.")
        key = ", ".join(DEDUP_KEY)
        ordering = ", ".join("CAST(" + c + " AS VARCHAR)" for c in columns)
        id_values = ", ".join("CAST(" + c + " AS VARCHAR)" for c in DEDUP_KEY)
        # Todos os campos são escalares; desempate estável do representante.
        row_hash = "TO_HEX(SHA256(TO_UTF8(JSON_FORMAT(CAST(ARRAY[" + ordering + "] AS JSON)))))"
        id_hash = "TO_HEX(SHA256(TO_UTF8(JSON_FORMAT(CAST(ARRAY[" + id_values + "] AS JSON)))))"
        final_stage = "alertas_" + person
        unique_sql = ("WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY " + key
                      + " ORDER BY " + row_hash + ") AS ta_row_number FROM " + linked["table"]
                      + ") SELECT " + ", ".join(linked_columns) + ", " + id_hash
                      + " AS id_alerta FROM ranked WHERE ta_row_number = 1")
        final = self.materialize(final_stage, self.ctas(final_stage, unique_sql))
        invalid = self.rows(
            "SELECT COUNT_IF(resultado_final <> 'ALERTA' OR resultado_final IS NULL "
            "OR resultado IS NULL OR NOT is_finite(CAST(resultado AS DOUBLE)) "
            "OR resultado <= corte_financeiro_aplicado OR corte_financeiro_aplicado < 100000) AS invalid_count, "
            "COUNT(*) - COUNT(DISTINCT id_alerta) AS duplicate_ids FROM " + final["table"],
            "verify_" + person + "_published", 1,
        )[0]
        if int(invalid["invalid_count"]) or int(invalid["duplicate_ids"]):
            raise ValueError("Falha na integridade da saída final.")
        return {"alerts": final, "with_links": linked, "cut": cut}

    def run(self):
        # Segunda barreira: chamadas diretas ao Pipeline também não executam 91 dias.
        # A validação ocorre antes de inicializar qualquer cliente/serviço do runner.
        checked = validate_config(self.config, executing=True)
        if config_fingerprint(checked, self.bundle_hash) != self.fingerprint:
            raise ValueError("Configuração alterada após criar o pipeline; crie outra instância antes de executar.")
        if self.runner is None:
            raise ValueError("Execução exige runner explícito; plan não usa AWS.")
        self.runner.initialize(self.fingerprint)
        self.runner.put_json_once("config.json", {k: v for k, v in self.config.items() if k != "resume"})
        reused = self.validated_history_manifest()
        for month in history_months(self.config["target_month"], self.config["history_days"]):
            if month in reused:
                self.validate_reused_history(month, reused[month])
            else:
                self.base_month(month)
        self.base_month(self.config["target_month"])
        if self.config["include_cta12_diagnostic"]:
            self.materialize("cta12_diagnostico", self.render_stage("cta12_diagnostico", self.config["target_month"]))
        cuts = self.calibrate()
        for person in ("pf", "pj"):
            self.materialize("cta13_" + person, self.candidate_sql(person))
        for stage in FINAL_STAGES:
            self.materialize(stage, self.render_stage(stage, self.config["target_month"]))
        outputs = {person.upper(): self.publish_person(person, cuts[code])
                   for person, code in (("pf", "F"), ("pj", "J"))}
        historical = {"status": "COMPLETE", "population_contract": POPULATION_CONTRACT,
                      "source_notebook_sha256": SOURCE_HASH, "sql_bundle_hash": self.bundle_hash,
                      "months": self.month_records}
        self.runner.put_json_once("history_manifest.json", historical)
        success = {"status": "COMPLETE", "policy_version": POLICY_VERSION,
                   "source_notebook_sha256": SOURCE_HASH, "sql_bundle_hash": self.bundle_hash,
                   "config_hash": self.fingerprint, "run_id": self.config["run_id"],
                   "month": self.config["target_month"], "history_days": self.config["history_days"],
                   "source_watermark": self.config["source_watermark"], "outputs": outputs,
                   "history_manifest_uri": self.root + "history_manifest.json",
                   "stage_counts": self.stage_records}
        self.runner.put_json_once("SUCCESS.json", success)
        print(json.dumps({"event": "run_complete", "run_id": self.config["run_id"],
                          "pf_alerts": outputs["PF"]["alerts"]["row_count"],
                          "pj_alerts": outputs["PJ"]["alerts"]["row_count"],
                          "manifest_uri": self.root + "SUCCESS.json"}), flush=True)
        return success

# ========================================================================
# CONSULTAS APROVADAS — conteúdo literal e hashes preservados
# ========================================================================
SQL_BUNDLE = {
    'cta01': {
        'stage_id': 'cta01',
        'logical_table': 'CTA1_Precos_{{SAFRA}}',
        'sha256': '900e07faa63c05e44c77105552b03f9fa9e9ea133eec9fde3bb0f111ec26e52e',
        'sql': r"""
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
""",
    },
    'cta02': {
        'stage_id': 'cta02',
        'logical_table': 'CTA2_VariacaoPreco_{{SAFRA}}',
        'sha256': 'f479f5d598c6e64537dfc50e459f1adc2c311ea453a6aaae02f163fda7d8ff9c',
        'sql': r"""
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
""",
    },
    'cta03': {
        'stage_id': 'cta03',
        'logical_table': 'CTA3_EventosAtipicos_{{SAFRA}}',
        'sha256': '4e082da8e82f6c63d4361a81c8c2b99cd83159050ed6ef59f09b4c31e429910b',
        'sql': r"""
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
""",
    },
    'cta04': {
        'stage_id': 'cta04',
        'logical_table': 'CTA4_PrecosEventos_{{SAFRA}}',
        'sha256': '3ea036e8bd62a1bce130f7caac26fd55f8d5d3175c597ef731b4cfcb2d6c7fb4',
        'sql': r"""
CREATE TABLE workspace_db.CTA4_PrecosEventos_{{SAFRA}}
WITH (
    format = 'PARQUET',
    external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA4_PrecosEventos_{{SAFRA}}/',
    partitioned_by = ARRAY['SAFRA']
) AS

SELECT 
    a.ativo_atipico,
    CAST(a.data_evento AS DATE) AS data_evento,
    CAST(b.data_fechamento AS DATE) AS data_fechamento,
    b.ativo,
    b.preco_fechamento,
    b.valor_fechamento,
    b.ativo_4letras,
    a.safra
FROM workspace_db.CTA3_EventosAtipicos_{{SAFRA}} a
INNER JOIN workspace_db.CTA1_Precos_{{SAFRA}} b
ON a.ativo_atipico = b.ativo
""",
    },
    'cta05': {
        'stage_id': 'cta05',
        'logical_table': 'CTA5_EstudoVariacao_{{SAFRA}}',
        'sha256': '1afca41808ea7eb6992bc8303981249b5a9aa5ab231bcd27a6786482f3ea752c',
        'sql': r"""
CREATE TABLE workspace_db.CTA5_EstudoVariacao_{{SAFRA}}
WITH (
    format = 'PARQUET',
    external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA5_EstudoVariacao_{{SAFRA}}/',
    partitioned_by = ARRAY['SAFRA']
) AS
WITH
-- ═══ STEP 2: obter_valor_fechamento_D_1 ═══
step2_d1 AS (
    SELECT
        a.ativo,
        a.data_evento,
        MAX_BY(b.valor_fechamento, b.data_fechamento) AS valor_fechamento_d_1,
        MAX_BY(b.preco_fechamento, b.data_fechamento) AS preco_fechamento_d_1,
        MAX(b.data_fechamento)                        AS data_d_1
    FROM (SELECT DISTINCT ativo, data_evento FROM workspace_db.CTA4_PrecosEventos_{{SAFRA}}) a
    LEFT JOIN (SELECT DISTINCT ativo, data_fechamento, valor_fechamento, preco_fechamento FROM workspace_db.CTA4_PrecosEventos_{{SAFRA}}) b
        ON a.ativo = b.ativo
        AND b.data_fechamento < a.data_evento
    GROUP BY a.ativo, a.data_evento
),
step2 AS (
    SELECT
        a.ativo_atipico,
        CAST(a.data_fechamento AS DATE) AS data_fechamento,
        CAST(a.data_evento AS DATE)    AS data_evento,
        a.ativo,
        CAST(a.valor_fechamento AS DOUBLE) AS valor_fechamento,
        a.preco_fechamento,
        b.valor_fechamento_d_1,
        b.preco_fechamento_d_1,
        b.data_d_1
    FROM workspace_db.CTA4_PrecosEventos_{{SAFRA}} a
    LEFT JOIN step2_d1 b
        ON a.ativo = b.ativo
        AND a.data_evento = b.data_evento
),
-- ═══ STEP 3: obter_valor_fechamento_D0 ═══
step3_d0 AS (
    SELECT DISTINCT
        ativo,
        data_fechamento AS data_d0,
        valor_fechamento AS valor_fechamento_d0,
        preco_fechamento AS preco_fechamento_d0
    FROM step2
    WHERE data_fechamento = data_evento
),
step3 AS (
    SELECT
        a.*,
        COALESCE(b.valor_fechamento_d0, 0) AS valor_fechamento_d0,
        b.preco_fechamento_d0,
        b.data_d0
    FROM step2 a
    LEFT JOIN step3_d0 b
        ON a.ativo = b.ativo
        AND a.data_evento = b.data_d0
),
-- ═══ STEP 4: obter_valor_fechamento_D1 ═══
step4_d1 AS (
    SELECT
        a.ativo,
        a.data_evento,
        MIN_BY(b.valor_fechamento, b.data_fechamento) AS valor_fechamento_d1,
        MIN_BY(b.preco_fechamento, b.data_fechamento) AS preco_fechamento_d1,
        MIN(b.data_fechamento)                        AS data_d1
    FROM (SELECT DISTINCT ativo, data_evento FROM step3) a
    LEFT JOIN (SELECT DISTINCT ativo, data_fechamento, valor_fechamento, preco_fechamento FROM step3) b
        ON a.ativo = b.ativo
        AND b.data_fechamento > a.data_evento
    GROUP BY a.ativo, a.data_evento
),
step4 AS (
    SELECT
        a.*,
        b.valor_fechamento_d1,
        b.preco_fechamento_d1,
        b.data_d1
    FROM step3 a
    LEFT JOIN step4_d1 b
        ON a.ativo = b.ativo
        AND a.data_evento = b.data_evento
),
-- ═══ STEP 7: remoção de colunas + drop_duplicates ═══
step7 AS (
    SELECT DISTINCT
        ativo_atipico,
        data_evento,
        data_fechamento,
        ativo,
        valor_fechamento_d_1,
        preco_fechamento_d_1,
        data_d_1,
        valor_fechamento_d0,
        preco_fechamento_d0,
        data_d0,
        valor_fechamento_d1,
        preco_fechamento_d1,
        data_d1
    FROM step4
),

-- ═══ STEP 9: filtro período + variações e classificações ═══
step9_base AS (
    SELECT *
    FROM step7
    WHERE data_fechamento >= DATE('{{SAFRA_INICIO}}')
      AND data_fechamento <= DATE('{{SAFRA_FIM}}')
),
step9 AS (
    SELECT DISTINCT
        ativo_atipico,
        data_evento,
        ativo,
        valor_fechamento_d_1,
        preco_fechamento_d_1,
        data_d_1,
        valor_fechamento_d0,
        preco_fechamento_d0,
        data_d0,
        valor_fechamento_d1,
        preco_fechamento_d1,
        data_d1,

        -- Classificação D(-1) → D0
        CASE
            WHEN valor_fechamento_d0 IS NULL THEN NULL
            WHEN valor_fechamento_d0 < 0 THEN 'QUEDA'
            WHEN valor_fechamento_d0 > 0 THEN 'ALTA'
            WHEN valor_fechamento_d0 = 0 THEN 'MANTEVE'
        END AS tipo_variacao_d_1_d0

    FROM step9_base
)

SELECT
    ativo_atipico,
    data_evento,
    ativo,
    valor_fechamento_d_1,
    preco_fechamento_d_1,
    data_d_1,
    valor_fechamento_d0,
    preco_fechamento_d0,
    data_d0,
    valor_fechamento_d1,
    preco_fechamento_d1,
    data_d1,
    tipo_variacao_d_1_d0,
    '{{SAFRA}}' AS safra
FROM step9
""",
    },
    'cta06': {
        'stage_id': 'cta06',
        'logical_table': 'CTA6_Operacoes_{{SAFRA}}',
        'sha256': '81f92e021e6ca537afe807893f5a76d3ff63d3623a71ae09557c8d79bbc3676e',
        'sql': r"""CREATE TABLE workspace_db.CTA6_Operacoes_{{SAFRA}}
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
""",
    },
    'cta07': {
        'stage_id': 'cta07',
        'logical_table': 'CTA7_OperacoesEventos_{{SAFRA}}',
        'sha256': '8e59a8b73e8bff17ecf1bb911c3288bb33a198e473c55579da73b4e5f65858d5',
        'sql': r"""CREATE TABLE workspace_db.CTA7_OperacoesEventos_{{SAFRA}}
WITH (
    format = 'PARQUET',
    external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA7_OperacoesEventos_{{SAFRA}}/',
    partitioned_by = ARRAY['SAFRA']
) AS
SELECT 
    o.codigo_cliente,
    o.cpf_cnpj_cliente,
    o.tipo_operacao,
    o.ativo_negociado,
    o.ativo_negociado_4letras,
    o.preco,
    o.quantidade,
    o.volume_negociado,
    o.data_operacao,
    o.pessoa_fisica,
    o.cnpj_empresa_vinculada,
    o.ticker_empresa_vinculada,
    o.Pessoa_Relacionada,
    o.linha_negocio,
    o.origem_operacao,
    ev.data_evento,
    ev.ativo_atipico,
    '{{SAFRA}}' AS SAFRA
FROM workspace_db.CTA6_Operacoes_{{SAFRA}} o
INNER JOIN workspace_db.CTA3_EventosAtipicos_{{SAFRA}} ev
    ON o.ativo_negociado = ev.ativo_atipico
""",
    },
    'cta08': {
        'stage_id': 'cta08',
        'logical_table': 'CTA8_DayTrader_{{SAFRA}}',
        'sha256': '9d0aabd539cdb2a88cbc05e5f52e857f2dc834aabaaa3dc44f822b64fd1f7ec6',
        'sql': r"""CREATE TABLE workspace_db.CTA8_DayTrader_{{SAFRA}}
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
""",
    },
    'cta09': {
        'stage_id': 'cta09',
        'logical_table': 'CTA9_VolumesQuantidades_{{SAFRA}}',
        'sha256': 'b04d1ab4907cd8e1305cbbf536720b2ad6ee7305b423067f93a88519bcef8a0f',
        'sql': r"""CREATE TABLE workspace_db.CTA9_VolumesQuantidades_{{SAFRA}}
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
""",
    },
    'cta10': {
        'stage_id': 'cta10',
        'logical_table': 'CTA10_PerfilOperacional_{{SAFRA}}',
        'sha256': 'ada456b54299e4493ed832f1cd7c6caa2af07a29b56a8bcb175e7b9371c1e8c2',
        'sql': r"""CREATE TABLE workspace_db.CTA10_PerfilOperacional_{{SAFRA}}
WITH (format = 'PARQUET',
      external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA10_PerfilOperacional_{{SAFRA}}/',
      partitioned_by = ARRAY['SAFRA']) AS
SELECT 
  codigo_cliente, cpf_cnpj_cliente, ativo_negociado, ativo_negociado_4letras, pessoa_fisica,
  cnpj_empresa_vinculada, ticker_empresa_vinculada, Pessoa_Relacionada, linha_negocio, origem_operacao, data_evento, ativo_atipico,
  -- cnpj_companhia, 
  day_trader,
  soma_volume_compra_historica, soma_volume_venda_historica, 
  media_volume_compra_historica, media_volume_venda_historica,
  desvio_volume_compra, desvio_volume_venda, 
  soma_quantidade_compra_historica, soma_quantidade_venda_historica,
  media_quantidade_compra_historica, media_quantidade_venda_historica, 
  desvio_quantidade_compra, desvio_quantidade_venda,
  -- n_dias_compra_historico, n_dias_venda_historico, 
  soma_volume_compra_15dias, soma_volume_venda_15dias,
  media_volume_compra_15dias, media_volume_venda_15dias, 
  soma_quantidade_compra_15dias, soma_quantidade_venda_15dias,
  media_quantidade_compra_15dias, media_quantidade_venda_15dias, 
  -- n_dias_compra_pre, n_dias_venda_pre,
  volume_compra_D0, volume_venda_D0, quantidade_compra_D0, quantidade_venda_D0, 
  -- n_dias_compra_D0, n_dias_venda_D0,
  -- volume_compra_D1, volume_venda_D1, quantidade_compra_D1, quantidade_venda_D1, 
  -- n_dias_compra_D1, n_dias_venda_D1,
  soma_volume_compra_5diasDepois, soma_volume_venda_5diasDepois, 
  media_volume_compra_5diasDepois, media_volume_venda_5diasDepois,
  soma_quantidade_compra_5diasDepois, soma_quantidade_venda_5diasDepois, 
  media_quantidade_compra_5diasDepois, media_quantidade_venda_5diasDepois,
  
  -- Flags de operação (S/N)
  CASE WHEN soma_quantidade_compra_historica > 0 THEN 'S' ELSE 'N' END AS Op_historica_compra,
  CASE WHEN soma_quantidade_venda_historica > 0 THEN 'S' ELSE 'N' END AS Op_historica_venda,
  CASE WHEN soma_quantidade_compra_historica >= 3 THEN 'S' ELSE 'N' END AS Op_historica_compra_maior3,
  CASE WHEN soma_quantidade_venda_historica >= 3 THEN 'S' ELSE 'N' END AS Op_historica_venda_maior3,
  CASE WHEN soma_quantidade_compra_15dias > 0 THEN 'S' ELSE 'N' END AS Op_pre_compra,
  CASE WHEN soma_quantidade_venda_15dias > 0 THEN 'S' ELSE 'N' END AS Op_pre_venda,
  CASE WHEN quantidade_compra_D0 > 0 THEN 'S' ELSE 'N' END AS Op_D0_compra,
  CASE WHEN quantidade_venda_D0 > 0 THEN 'S' ELSE 'N' END AS Op_D0_venda,
  CASE WHEN soma_quantidade_compra_5diasDepois > 0 THEN 'S' ELSE 'N' END AS Op_pos_compra,
  CASE WHEN soma_quantidade_venda_5diasDepois > 0 THEN 'S' ELSE 'N' END AS Op_pos_venda,
  
  -- Flags "Fugiu" (outlier = média > histórico + 2*desvio)
  CASE WHEN media_volume_compra_15dias > (media_volume_compra_historica + 2*desvio_volume_compra) 
       THEN 'S' ELSE 'N' END AS Fugiu_hist_compra_pre,
  CASE WHEN media_volume_venda_15dias > (media_volume_venda_historica + 2*desvio_volume_venda) 
       THEN 'S' ELSE 'N' END AS Fugiu_hist_venda_pre,
  CASE WHEN media_volume_compra_5diasDepois > (media_volume_compra_historica + 2*desvio_volume_compra) 
       THEN 'S' ELSE 'N' END AS Fugiu_hist_compra_pos,
  CASE WHEN media_volume_venda_5diasDepois > (media_volume_venda_historica + 2*desvio_volume_venda) 
       THEN 'S' ELSE 'N' END AS Fugiu_hist_venda_pos,
  
  '{{SAFRA}}' AS SAFRA
FROM workspace_db.CTA9_VolumesQuantidades_{{SAFRA}}
""",
    },
    'cta11': {
        'stage_id': 'cta11',
        'logical_table': 'CTA11_FeaturesFinal_{{SAFRA}}',
        'sha256': 'f49372cf8cad94e265490adbadfc7a1b3c6c1fe4d55040f3611ca3dc813b49af',
        'sql': r"""CREATE TABLE workspace_db.CTA11_FeaturesFinal_{{SAFRA}}
WITH (format = 'PARQUET',
      external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA11_FeaturesFinal_{{SAFRA}}/',
      partitioned_by = ARRAY['SAFRA']) AS
SELECT
  ops.codigo_cliente,
  ops.cpf_cnpj_cliente,
  ops.ativo_negociado,
  -- preco.nome_empresarial,
  ops.ativo_negociado_4letras,
  ops.pessoa_fisica,
  ops.cnpj_empresa_vinculada,
  ops.ticker_empresa_vinculada,
  ops.Pessoa_Relacionada,
  ops.linha_negocio,
  ops.origem_operacao,
  ops.data_evento,
  ops.ativo_atipico,
  -- ops.cnpj_companhia,
  -- ops.nome_empresarial,
  ops.day_trader,
  ops.soma_volume_compra_historica,
  ops.soma_volume_venda_historica,
  ops.media_volume_compra_historica,
  ops.media_volume_venda_historica,
  ops.desvio_volume_compra,
  ops.desvio_volume_venda,
  ops.soma_quantidade_compra_historica,
  ops.soma_quantidade_venda_historica,
  ops.media_quantidade_compra_historica,
  ops.media_quantidade_venda_historica,
  ops.desvio_quantidade_compra,
  ops.desvio_quantidade_venda,
  -- ops.n_dias_compra_historico,
  -- ops.n_dias_venda_historico,
  ops.soma_volume_compra_15dias,
  ops.soma_volume_venda_15dias,
  ops.media_volume_compra_15dias,
  ops.media_volume_venda_15dias,
  ops.soma_quantidade_compra_15dias,
  ops.soma_quantidade_venda_15dias,
  ops.media_quantidade_compra_15dias,
  ops.media_quantidade_venda_15dias,
  -- ops.n_dias_compra_pre,
  -- ops.n_dias_venda_pre,
  ops.volume_compra_D0,
  ops.volume_venda_D0,
  ops.quantidade_compra_D0,
  ops.quantidade_venda_D0,
  -- ops.n_dias_compra_D0,
  -- ops.n_dias_venda_D0,
  ops.soma_volume_compra_5diasDepois,
  ops.soma_volume_venda_5diasDepois,
  ops.media_volume_compra_5diasDepois,
  ops.media_volume_venda_5diasDepois,
  ops.soma_quantidade_compra_5diasDepois,
  ops.soma_quantidade_venda_5diasDepois,
  ops.media_quantidade_compra_5diasDepois,
  ops.media_quantidade_venda_5diasDepois,
  ops.Op_historica_compra,
  ops.Op_historica_venda,
  ops.Op_historica_compra_maior3,
  ops.Op_historica_venda_maior3,
  ops.Op_pre_compra,
  ops.Op_pre_venda,
  ops.Op_D0_compra,
  ops.Op_D0_venda,
  ops.Op_pos_compra,
  ops.Op_pos_venda,
  ops.Fugiu_hist_compra_pre,
  ops.Fugiu_hist_venda_pre,
  ops.Fugiu_hist_compra_pos,
  ops.Fugiu_hist_venda_pos,
  preco.valor_fechamento_d_1,
  preco.preco_fechamento_d_1,
  preco.data_d_1,
  preco.valor_fechamento_d0,
  preco.preco_fechamento_d0,
  preco.data_d0,
  preco.valor_fechamento_d1,
  preco.preco_fechamento_d1,
  preco.data_d1,
  -- preco.variacao_preco_d_1_d0,
  preco.tipo_variacao_d_1_d0,
  -- preco.variacao_preco_d0_d1,
  '{{SAFRA}}' AS SAFRA
FROM workspace_db.CTA10_PerfilOperacional_{{SAFRA}} ops
LEFT JOIN workspace_db.CTA5_EstudoVariacao_{{SAFRA}} preco
  ON ops.ativo_negociado = preco.ativo
  AND ops.ativo_atipico = preco.ativo_atipico
  --AND ops.cnpj_companhia = preco.cnpj_companhia
  --AND ops.nome_empresarial = preco.nome_empresarial
  AND CAST(ops.data_evento AS DATE) = CAST(preco.data_evento AS DATE)

""",
    },
    'cta12_diagnostico': {
        'stage_id': 'cta12_diagnostico',
        'logical_table': 'CTA12_FugiramPerfil_{{SAFRA}}',
        'sha256': '69ce4acc4fcb5f22f77f63b7aa5fd14bda5d1ca7120328f2b1f3250e28df0808',
        'sql': r"""CREATE TABLE workspace_db.CTA12_FugiramPerfil_{{SAFRA}}
WITH (format = 'PARQUET',
      external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA12_FugiramPerfil_{{SAFRA}}/',
      partitioned_by = ARRAY['SAFRA']) AS
WITH 
-- Filtra operações fora do perfil (3 condições)
fora_perfil AS (
  SELECT *
  FROM workspace_db.CTA11_FeaturesFinal_{{SAFRA}}
  WHERE 
    -- Condição 1: Alerta em z-score E fugiu do perfil histórico
    ((Fugiu_hist_compra_pre = 'S' OR Fugiu_hist_venda_pre = 'S'))
    OR
    -- Condição 2: Alerta em z-score E NÃO fugiu do perfil mas É pessoa relacionada
    (Fugiu_hist_compra_pre = 'N' AND
     Fugiu_hist_venda_pre = 'N' AND
     Pessoa_Relacionada = 'S')
    OR
    -- Condição 3: NÃO alerta em z-score mas fugiu do perfil E É pessoa relacionada
    ((Fugiu_hist_compra_pre = 'S' OR Fugiu_hist_venda_pre = 'S') AND
      Pessoa_Relacionada = 'S'
    )
),

-- Remove operações não relevantes
operacoes_suspeitas AS (
  SELECT *
  FROM fora_perfil
  WHERE NOT (
    -- Excluir: ALTA com compra pré mas sem venda D0 e pós
    (tipo_variacao_d_1_d0 = 'ALTA' AND soma_quantidade_compra_15dias != 0 AND quantidade_venda_D0 = 0 AND soma_quantidade_venda_5diasDepois = 0)
    OR
    -- Excluir: ALTA sem compra pré mas com venda D0 ou pós
    (tipo_variacao_d_1_d0 = 'ALTA' AND soma_quantidade_compra_15dias = 0 AND (quantidade_venda_D0 != 0 OR soma_quantidade_venda_5diasDepois != 0))
    OR
    -- Excluir: ALTA sem nenhuma operação próxima ao fato
    (tipo_variacao_d_1_d0 = 'ALTA' AND soma_quantidade_compra_15dias = 0 AND quantidade_venda_D0 = 0 AND soma_quantidade_venda_5diasDepois = 0)
    OR
    -- Excluir: QUEDA sem venda pré e sem venda D0
    (tipo_variacao_d_1_d0 = 'QUEDA' AND soma_quantidade_venda_15dias = 0 AND quantidade_venda_D0 = 0)
  )
),

-- Calcula preço médio pré-fato
com_preco_medio_pre AS (
  SELECT *,
    CASE 
      WHEN tipo_variacao_d_1_d0 = 'ALTA' AND soma_quantidade_compra_15dias > 0 
        THEN soma_volume_compra_15dias / soma_quantidade_compra_15dias
      WHEN tipo_variacao_d_1_d0 = 'QUEDA' AND soma_quantidade_venda_15dias > 0 
        THEN soma_volume_venda_15dias / soma_quantidade_venda_15dias
      ELSE NULL
    END AS preco_medio_pre
  FROM operacoes_suspeitas
),

-- Calcula preço médio pós-fato
resultado_final AS (
  SELECT *,
    CASE 
      WHEN tipo_variacao_d_1_d0 = 'ALTA' AND soma_quantidade_venda_5diasDepois > 0 
        THEN soma_volume_venda_5diasDepois / soma_quantidade_venda_5diasDepois
      WHEN tipo_variacao_d_1_d0 = 'QUEDA' AND soma_quantidade_compra_5diasDepois > 0 
        THEN soma_volume_compra_5diasDepois / soma_quantidade_compra_5diasDepois
      ELSE NULL
    END AS preco_medio_pos
  FROM com_preco_medio_pre
)

SELECT 
  codigo_cliente,
  cpf_cnpj_cliente,
  ativo_negociado,
  ativo_negociado_4letras,
  pessoa_fisica,
  cnpj_empresa_vinculada,
  ticker_empresa_vinculada,
  Pessoa_Relacionada,
  linha_negocio,
  origem_operacao,
  data_evento,
  -- ticker_fr,
  --cnpj_companhia,
  --nome_empresarial,
  day_trader,
  soma_volume_compra_historica,
  soma_volume_venda_historica,
  media_volume_compra_historica,
  media_volume_venda_historica,
  desvio_volume_compra,
  desvio_volume_venda,
  soma_quantidade_compra_historica,
  soma_quantidade_venda_historica,
  media_quantidade_compra_historica,
  media_quantidade_venda_historica,
  desvio_quantidade_compra,
  desvio_quantidade_venda,
  -- n_dias_compra_historico,
  -- n_dias_venda_historico,
  soma_volume_compra_15dias,
  soma_volume_venda_15dias,
  media_volume_compra_15dias,
  media_volume_venda_15dias,
  soma_quantidade_compra_15dias,
  soma_quantidade_venda_15dias,
  media_quantidade_compra_15dias,
  media_quantidade_venda_15dias,
  -- n_dias_compra_pre,
  -- n_dias_venda_pre,
  volume_compra_D0,
  volume_venda_D0,
  quantidade_compra_D0,
  quantidade_venda_D0,
  -- n_dias_compra_D0,
  -- n_dias_venda_D0,
  soma_volume_compra_5diasDepois,
  soma_volume_venda_5diasDepois,
  media_volume_compra_5diasDepois,
  media_volume_venda_5diasDepois,
  soma_quantidade_compra_5diasDepois,
  soma_quantidade_venda_5diasDepois,
  media_quantidade_compra_5diasDepois,
  media_quantidade_venda_5diasDepois,
  Op_historica_compra,
  Op_historica_venda,
  Op_historica_compra_maior3,
  Op_historica_venda_maior3,
  Op_pre_compra,
  Op_pre_venda,
  Op_D0_compra,
  Op_D0_venda,
  Op_pos_compra,
  Op_pos_venda,
  Fugiu_hist_compra_pre,
  Fugiu_hist_venda_pre,
  Fugiu_hist_compra_pos,
  Fugiu_hist_venda_pos,
  valor_fechamento_d_1,
  preco_fechamento_d_1,
  data_d_1,
  valor_fechamento_d0,
  preco_fechamento_d0,
  data_d0,
  valor_fechamento_d1,
  preco_fechamento_d1,
  data_d1,
  -- desvio_preco_pos,
  -- media_preco_pos,
  -- variacao_preco_d_1_d0,
  tipo_variacao_d_1_d0,
  -- variacao_preco_d0_d1,
  -- tipo_variacao_d0_d1,
  -- tipo_variacao_final,
  preco_medio_pre,
  preco_medio_pos,
  '{{SAFRA}}' AS SAFRA
FROM resultado_final
""",
    },
    'cta13_base': {
        'stage_id': 'cta13_base',
        'logical_table': 'CTA13_CalculoResultado_Base_Estudo_{{SAFRA}}',
        'sha256': 'be2e84d3b5626a1776f73af2bd899aa27683afa4a6012d92e72252be8616020d',
        'sql': r"""CREATE TABLE workspace_db.CTA13_CalculoResultado_Base_Estudo_{{SAFRA}}
WITH (format = 'PARQUET',
      external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA13_CalculoResultado_Base_Estudo_{{SAFRA}}/',
      partitioned_by = ARRAY['SAFRA']) AS
WITH
-- CTE 1: Carrega base da CTA11
base_cta11 AS (
  SELECT *,
    -- Cria coluna tipo_pessoa baseada em pessoa_fisica
    CASE
      WHEN pessoa_fisica = 'true' THEN 'F'
      ELSE 'J'
    END AS tipo_pessoa
  FROM workspace_db.CTA11_FeaturesFinal_{{SAFRA}}
),

-- CTE 2: Calcula preço médio pré-fato (15 dias antes)
com_preco_medio_pre AS (
  SELECT *,
    CASE
      WHEN tipo_variacao_d_1_d0 = 'ALTA' AND soma_quantidade_compra_15dias > 0
        THEN soma_volume_compra_15dias / soma_quantidade_compra_15dias
      WHEN tipo_variacao_d_1_d0 = 'QUEDA' AND soma_quantidade_venda_15dias > 0
        THEN soma_volume_venda_15dias / soma_quantidade_venda_15dias
      ELSE NULL
    END AS preco_medio_pre
  FROM base_cta11
),

-- CTE 3: Calcula preço médio pós-fato (5 dias depois)
com_preco_medio_pos AS (
  SELECT *,
    CASE
      WHEN tipo_variacao_d_1_d0 = 'ALTA' AND soma_quantidade_venda_5diasDepois > 0
        THEN soma_volume_venda_5diasDepois / soma_quantidade_venda_5diasDepois
      WHEN tipo_variacao_d_1_d0 = 'QUEDA' AND soma_quantidade_compra_5diasDepois > 0
        THEN soma_volume_compra_5diasDepois / soma_quantidade_compra_5diasDepois
      ELSE NULL
    END AS preco_medio_pos
  FROM com_preco_medio_pre
),

-- CTE 4: Calcula o resultado financeiro baseado no tipo de variação
resultado_calculado AS (
  SELECT *,
    -- ========================================
    -- COLUNAS: REVERSÃO PRÉ-EVENTO
    -- ========================================

    -- (1) Coluna reversao_pre
    CASE
      WHEN tipo_variacao_d_1_d0 = 'QUEDA'
        THEN soma_quantidade_venda_15dias - soma_quantidade_compra_15dias
      WHEN tipo_variacao_d_1_d0 = 'ALTA'
        THEN soma_quantidade_compra_15dias - soma_quantidade_venda_15dias
      ELSE NULL
    END AS reversao_pre,

    -- (2) Coluna perc_reversao_pre (% de reversão)
    CASE
      WHEN soma_quantidade_compra_15dias > 0
        THEN ROUND(soma_quantidade_venda_15dias / soma_quantidade_compra_15dias, 4)
      ELSE NULL
    END AS perc_reversao_pre,

    -- (3) Coluna Resultado (resultado baseado na reversão)
    CASE
      -- Cenário 1: QUEDA e sem compra pós-fato (preco_medio_pos é NULL)
      WHEN tipo_variacao_d_1_d0 = 'QUEDA' AND preco_medio_pos IS NULL
        THEN ROUND(
          (soma_volume_venda_15dias - soma_volume_compra_15dias) -
          ((soma_quantidade_venda_15dias - soma_quantidade_compra_15dias) * preco_fechamento_d0),
          2
        )

      -- Cenário 2: QUEDA e com compra pós-fato (preco_medio_pos não é NULL)
      WHEN tipo_variacao_d_1_d0 = 'QUEDA' AND preco_medio_pos IS NOT NULL
        THEN ROUND(
          (preco_medio_pre - preco_medio_pos) *
          LEAST(soma_quantidade_venda_15dias - soma_quantidade_compra_15dias, soma_quantidade_compra_5diasDepois),
          2
        )

      -- Cenário 3: ALTA e vende apenas no pós (não vende em D0)
      WHEN tipo_variacao_d_1_d0 = 'ALTA' AND quantidade_venda_D0 = 0
        THEN ROUND(
          (preco_medio_pos - preco_medio_pre) *
          LEAST(soma_quantidade_compra_15dias - soma_quantidade_venda_15dias, soma_quantidade_venda_5diasDepois),
          2
        )

      -- Cenário 4: ALTA e vende em D0
      WHEN tipo_variacao_d_1_d0 = 'ALTA' AND quantidade_venda_D0 != 0
        THEN ROUND(
          (preco_fechamento_d0 - preco_medio_pre) *
          LEAST(soma_quantidade_compra_15dias - soma_quantidade_venda_15dias,
                quantidade_venda_D0 + soma_quantidade_venda_5diasDepois),
          2
        )

      ELSE NULL
    END AS Resultado,

    -- Para o cenário ALTA com venda em D0, atualiza o preco_medio_pos para preco_fechamento_d0
    CASE
      WHEN tipo_variacao_d_1_d0 = 'ALTA' AND quantidade_venda_D0 != 0
        THEN preco_fechamento_d0
      ELSE preco_medio_pos
    END AS preco_medio_pos_ajustado

  FROM com_preco_medio_pos
)

-- SELECT FINAL: Lista todas as colunas
SELECT
  codigo_cliente,
  cpf_cnpj_cliente,
  ativo_negociado,
  ativo_negociado_4letras,
  tipo_pessoa,
  cnpj_empresa_vinculada,
  ticker_empresa_vinculada,
  Pessoa_Relacionada,
  linha_negocio,
  origem_operacao,
  data_evento,
  day_trader,
  soma_volume_compra_historica,
  soma_volume_venda_historica,
  media_volume_compra_historica,
  media_volume_venda_historica,
  desvio_volume_compra,
  desvio_volume_venda,
  soma_quantidade_compra_historica,
  soma_quantidade_venda_historica,
  media_quantidade_compra_historica,
  media_quantidade_venda_historica,
  desvio_quantidade_compra,
  desvio_quantidade_venda,
  soma_volume_compra_15dias,
  soma_volume_venda_15dias,
  media_volume_compra_15dias,
  media_volume_venda_15dias,
  soma_quantidade_compra_15dias,
  soma_quantidade_venda_15dias,
  media_quantidade_compra_15dias,
  media_quantidade_venda_15dias,
  volume_compra_D0,
  volume_venda_D0,
  quantidade_compra_D0,
  quantidade_venda_D0,
  soma_volume_compra_5diasDepois,
  soma_volume_venda_5diasDepois,
  media_volume_compra_5diasDepois,
  media_volume_venda_5diasDepois,
  soma_quantidade_compra_5diasDepois,
  soma_quantidade_venda_5diasDepois,
  media_quantidade_compra_5diasDepois,
  media_quantidade_venda_5diasDepois,
  Op_historica_compra,
  Op_historica_venda,
  Op_historica_compra_maior3,
  Op_historica_venda_maior3,
  Op_pre_compra,
  Op_pre_venda,
  Op_D0_compra,
  Op_D0_venda,
  Op_pos_compra,
  Op_pos_venda,
  Fugiu_hist_compra_pre,
  Fugiu_hist_venda_pre,
  Fugiu_hist_compra_pos,
  Fugiu_hist_venda_pos,
  valor_fechamento_d_1,
  preco_fechamento_d_1,
  data_d_1,
  valor_fechamento_d0,
  preco_fechamento_d0,
  data_d0,
  valor_fechamento_d1,
  preco_fechamento_d1,
  data_d1,
  tipo_variacao_d_1_d0,

  -- Colunas de preço médio calculadas
  preco_medio_pre,
  preco_medio_pos_ajustado AS preco_medio_pos,

  -- Colunas de reversão
  reversao_pre,
  perc_reversao_pre,
  Resultado,

  '{{SAFRA}}' AS SAFRA
FROM resultado_calculado
-- SEM FILTRO: Salva todos os resultados calculados
""",
    },
    'cta14': {
        'stage_id': 'cta14',
        'logical_table': 'CTA14_Custodia_{{SAFRA}}',
        'sha256': 'fa3a55ebb07832929c4bf634c27067eddd0020ddf5e90da8f99ca8e8fa47ee19',
        'sql': r"""CREATE TABLE workspace_db.CTA14_Custodia_{{SAFRA}}
WITH (format = 'PARQUET',
      external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA14_Custodia_{{SAFRA}}/',
      partitioned_by = ARRAY['SAFRA']) AS
WITH
-- Obtém clientes únicos da CTA13 PF
clientes_pf AS (
  SELECT DISTINCT codigo_cliente
  FROM workspace_db.CTA13_CalculoResultado_PF_{{SAFRA}}
),

-- Obtém ativos únicos da CTA13 PF
ativos_pf AS (
  SELECT DISTINCT ativo_negociado
  FROM workspace_db.CTA13_CalculoResultado_PF_{{SAFRA}}
),

-- Calcula período de custódia: dia 20 do mês anterior até dia 4 do mês seguinte
-- Para safra YYYYMM, buscar de (YYYY-MM-1)-20 até (YYYY-MM+1)-04
custodia_bruta AS (
  SELECT DISTINCT
    data_referencia,
    CAST(codigo_conta AS INTEGER) AS codigo_conta,
    ativo,
    SUBSTR(ativo, 1, 4) AS ativo_4letras,
    quantidade_d0
  FROM db_corp_corretora_inoabrokertools_sot_01.tb_ep7_carteiras_equities_sot
  WHERE CAST(data_referencia AS DATE) BETWEEN
    DATE_ADD('month', -1, DATE('{{SAFRA_INICIO}}')) + INTERVAL '19' DAY  -- Dia 20 do mês anterior
    AND DATE_ADD('month', 1, DATE('{{SAFRA_FIM}}')) - INTERVAL '26' DAY  -- Dia 4 do mês seguinte
),

-- Filtra apenas clientes e ativos da PF
custodia_filtrada AS (
  SELECT c.*
  FROM custodia_bruta c
  INNER JOIN clientes_pf cpf ON CAST(c.codigo_conta AS VARCHAR) = cpf.codigo_cliente
  INNER JOIN ativos_pf apf ON c.ativo = apf.ativo_negociado
),

-- Ordena e calcula quantidade_d0_shifted (quantidade 2 dias depois)
custodia_ordenada AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY codigo_conta, ativo, data_referencia ORDER BY quantidade_d0 DESC) AS rn_dedup,
    ROW_NUMBER() OVER (PARTITION BY codigo_conta, ativo ORDER BY data_referencia ASC) AS rn
  FROM custodia_filtrada
),

-- Remove duplicatas mantendo o maior valor de quantidade quando houver duplicatas na mesma data
custodia_dedupada AS (
  SELECT
    data_referencia,
    codigo_conta,
    ativo,
    -- ativo_4letras,
    quantidade_d0
  FROM custodia_ordenada
  WHERE rn_dedup = 1
),

-- Recalcula ROW_NUMBER após deduplicação para ter sequência contínua (sem buracos)
custodia_com_rn_continuo AS (
  SELECT
    data_referencia,
    codigo_conta,
    ativo,
    -- ativo_4letras,
    quantidade_d0,
    ROW_NUMBER() OVER (PARTITION BY codigo_conta, ativo ORDER BY data_referencia ASC) AS rn
  FROM custodia_dedupada
),

custodia_com_shift AS (
  SELECT
    c1.data_referencia,
    c1.codigo_conta,
    c1.ativo,
    -- c1.ativo_4letras,
    c1.quantidade_d0,
    COALESCE(c2.quantidade_d0, 0.0) AS quantidade_d0_shifted
  FROM custodia_com_rn_continuo c1
  LEFT JOIN custodia_com_rn_continuo c2
    ON c1.codigo_conta = c2.codigo_conta
    AND c1.ativo = c2.ativo
    AND c2.rn = c1.rn + 2
)

SELECT
  data_referencia,
  codigo_conta,
  ativo,
  -- ativo_4letras,
  quantidade_d0,
  quantidade_d0_shifted,
  '{{SAFRA}}' AS SAFRA
FROM custodia_com_shift
ORDER BY codigo_conta, ativo, data_referencia
""",
    },
    'cta15': {
        'stage_id': 'cta15',
        'logical_table': 'CTA15_CustodiaAtipicos_{{SAFRA}}',
        'sha256': 'e33aff6ff4258812e81fde21117211df080fd6106f988aa45962a421a3693731',
        'sql': r"""CREATE TABLE workspace_db.CTA15_CustodiaAtipicos_{{SAFRA}}
WITH (format = 'PARQUET',
      external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA15_CustodiaAtipicos_{{SAFRA}}/',
      partitioned_by = ARRAY['SAFRA']) AS
SELECT 
  cust.data_referencia,
  cust.codigo_conta,
  cust.ativo,
  -- cust.ativo_4letras,
  cust.quantidade_d0,
  cust.quantidade_d0_shifted,
  -- fr.cnpj_companhia,
  -- fr.nome_empresarial,
  ev.ativo_atipico,
  ev.data_evento,
  '{{SAFRA}}' AS SAFRA
FROM workspace_db.CTA14_Custodia_{{SAFRA}} cust
INNER JOIN workspace_db.CTA3_EventosAtipicos_{{SAFRA}} ev
  ON cust.ativo = ev.ativo_atipico
""",
    },
    'cta16': {
        'stage_id': 'cta16',
        'logical_table': 'CTA16_CustodiaInfos_{{SAFRA}}',
        'sha256': '9818f4e5b06fab9916ee206152cf10dec6976d18c7d191a96c4968a3af3bd49e',
        'sql': r"""CREATE TABLE workspace_db.CTA16_CustodiaInfos_{{SAFRA}}
WITH (format = 'PARQUET',
      external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA16_CustodiaInfos_{{SAFRA}}/',
      partitioned_by = ARRAY['SAFRA']) AS
WITH
-- Base ordenada para facilitar as buscas
base_ordenada AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY codigo_conta, ativo ORDER BY data_referencia ASC) AS rn
  FROM workspace_db.CTA15_CustodiaAtipicos_{{SAFRA}}
),

-- Custódia D-15: busca custódia 15 dias antes da data_evento
custodia_D15 AS (
  SELECT
    b.codigo_conta,
    b.ativo,
    b.data_evento,
    COALESCE(
      MAX(CASE WHEN CAST(c.data_referencia AS DATE) = CAST(b.data_evento AS DATE) - INTERVAL '15' DAY
           THEN c.data_referencia END),
      MAX(CASE WHEN CAST(c.data_referencia AS DATE) = CAST(b.data_evento AS DATE) - INTERVAL '14' DAY
           THEN c.data_referencia END),
      MAX(CASE WHEN CAST(c.data_referencia AS DATE) = CAST(b.data_evento AS DATE) - INTERVAL '16' DAY
           THEN c.data_referencia END)
    ) AS data_D_15_custodia,
    COALESCE(
      MAX(CASE WHEN CAST(c.data_referencia AS DATE) = CAST(b.data_evento AS DATE) - INTERVAL '15' DAY
           THEN c.quantidade_d0_shifted END),
      MAX(CASE WHEN CAST(c.data_referencia AS DATE) = CAST(b.data_evento AS DATE) - INTERVAL '14' DAY
           THEN c.quantidade_d0_shifted END),
      MAX(CASE WHEN CAST(c.data_referencia AS DATE) = CAST(b.data_evento AS DATE) - INTERVAL '16' DAY
           THEN c.quantidade_d0_shifted END)
    ) AS custodia_D_15
  FROM (SELECT DISTINCT codigo_conta, ativo, data_evento FROM base_ordenada) b
  LEFT JOIN base_ordenada c
    ON b.codigo_conta = c.codigo_conta
    AND b.ativo = c.ativo
    AND CAST(c.data_referencia AS DATE) BETWEEN
        CAST(b.data_evento AS DATE) - INTERVAL '16' DAY
        AND CAST(b.data_evento AS DATE) - INTERVAL '14' DAY
  GROUP BY b.codigo_conta, b.ativo, b.data_evento
),

-- Custódia D-1: busca a maior data_referencia < data_evento
custodia_D1_ant AS (
  SELECT
    b.codigo_conta,
    b.ativo,
    b.data_evento,
    MAX(c.data_referencia) AS data_D_1_custodia,
    MAX_BY(c.quantidade_d0_shifted, c.data_referencia) AS custodia_D_1
  FROM (SELECT DISTINCT codigo_conta, ativo, data_evento FROM base_ordenada) b
  LEFT JOIN base_ordenada c
    ON b.codigo_conta = c.codigo_conta
    AND b.ativo = c.ativo
    AND CAST(c.data_referencia AS DATE) < CAST(b.data_evento AS DATE)
  GROUP BY b.codigo_conta, b.ativo, b.data_evento
),

-- Custódia D0: busca data_referencia = data_evento, ou a mais próxima anterior
custodia_D0 AS (
  SELECT
    b.codigo_conta,
    b.ativo,
    b.data_evento,
    COALESCE(
      MAX(CASE WHEN CAST(c.data_referencia AS DATE) = CAST(b.data_evento AS DATE)
           THEN c.data_referencia END),
      MAX(CASE WHEN CAST(c.data_referencia AS DATE) < CAST(b.data_evento AS DATE)
           THEN c.data_referencia END)
    ) AS data_D0_custodia,
    COALESCE(
      MAX(CASE WHEN CAST(c.data_referencia AS DATE) = CAST(b.data_evento AS DATE)
           THEN c.quantidade_d0_shifted END),
      MAX_BY(CASE WHEN CAST(c.data_referencia AS DATE) < CAST(b.data_evento AS DATE)
              THEN c.quantidade_d0_shifted END, c.data_referencia)
    ) AS custodia_D0
  FROM (SELECT DISTINCT codigo_conta, ativo, data_evento FROM base_ordenada) b
  LEFT JOIN base_ordenada c
    ON b.codigo_conta = c.codigo_conta
    AND b.ativo = c.ativo
    AND CAST(c.data_referencia AS DATE) <= CAST(b.data_evento AS DATE)
  GROUP BY b.codigo_conta, b.ativo, b.data_evento
),

-- Custódia D+1: busca a menor data_referencia > data_evento
custodia_D1_pos AS (
  SELECT
    b.codigo_conta,
    b.ativo,
    b.data_evento,
    COALESCE(
      MIN(CASE WHEN CAST(c.data_referencia AS DATE) > CAST(b.data_evento AS DATE)
           THEN c.data_referencia END),
      MAX(CASE WHEN CAST(c.data_referencia AS DATE) < CAST(b.data_evento AS DATE)
           THEN c.data_referencia END)
    ) AS data_D1_custodia,
    COALESCE(
      MIN_BY(CASE WHEN CAST(c.data_referencia AS DATE) > CAST(b.data_evento AS DATE)
              THEN c.quantidade_d0_shifted END, c.data_referencia),
      MAX_BY(CASE WHEN CAST(c.data_referencia AS DATE) < CAST(b.data_evento AS DATE)
              THEN c.quantidade_d0_shifted END, c.data_referencia)
    ) AS custodia_D1
  FROM (SELECT DISTINCT codigo_conta, ativo, data_evento FROM base_ordenada) b
  LEFT JOIN base_ordenada c
    ON b.codigo_conta = c.codigo_conta
    AND b.ativo = c.ativo
  GROUP BY b.codigo_conta, b.ativo, b.data_evento
),

-- JOIN de todos os cálculos + filtro por período + limpeza de colunas
resultado_completo AS (
  SELECT
    b.data_referencia,
    b.codigo_conta,
    b.ativo,
    b.ativo_atipico,
    b.data_evento,
    d15.data_D_15_custodia,
    COALESCE(CAST(d15.custodia_D_15 AS VARCHAR), 'Não consta') AS custodia_D_15,
    d1_ant.data_D_1_custodia,
    d1_ant.custodia_D_1,
    d0.data_D0_custodia,
    d0.custodia_D0,
    d1_pos.data_D1_custodia,
    COALESCE(d1_pos.custodia_D1, 0) AS custodia_D1
  FROM base_ordenada b
  LEFT JOIN custodia_D15 d15
    ON b.codigo_conta = d15.codigo_conta
    AND b.ativo = d15.ativo
    AND b.data_evento = d15.data_evento
  LEFT JOIN custodia_D1_ant d1_ant
    ON b.codigo_conta = d1_ant.codigo_conta
    AND b.ativo = d1_ant.ativo
    AND b.data_evento = d1_ant.data_evento
  LEFT JOIN custodia_D0 d0
    ON b.codigo_conta = d0.codigo_conta
    AND b.ativo = d0.ativo
    AND b.data_evento = d0.data_evento
  LEFT JOIN custodia_D1_pos d1_pos
    ON b.codigo_conta = d1_pos.codigo_conta
    AND b.ativo = d1_pos.ativo
    AND b.data_evento = d1_pos.data_evento
  WHERE
    -- Filtra apenas registros dentro do período de interesse (após os cálculos)
    -- Equivale ao filtro Python: df_processado[(df_processado['data_referencia'] >= data_inicio) & (df_processado['data_referencia'] <= data_fim)]
    CAST(b.data_referencia AS DATE) >= CAST(b.data_evento AS DATE) - INTERVAL '20' DAY
    AND CAST(b.data_referencia AS DATE) <= CAST(b.data_evento AS DATE) + INTERVAL '5' DAY
)

-- SELECT DISTINCT final para remover duplicatas
SELECT DISTINCT
  codigo_conta,
  ativo,
  ativo_atipico,
  data_evento,
  data_D_15_custodia,
  custodia_D_15,
  data_D_1_custodia,
  custodia_D_1,
  data_D0_custodia,
  custodia_D0,
  data_D1_custodia,
  custodia_D1,
  '{{SAFRA}}' AS SAFRA
FROM resultado_completo
""",
    },
    'cta17': {
        'stage_id': 'cta17',
        'logical_table': 'CTA17_CustodiaOperacoes_{{SAFRA}}',
        'sha256': '8459948a75fd9e5be3e5cb6751ee83685f96a1c5bb21aaa520bf9cc029fbd9ef',
        'sql': r"""CREATE TABLE workspace_db.CTA17_CustodiaOperacoes_{{SAFRA}}
WITH (format = 'PARQUET',
      external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA17_CustodiaOperacoes_{{SAFRA}}/',
      partitioned_by = ARRAY['SAFRA']) AS
SELECT
  -- Colunas da custódia (CTA16)
  cust.codigo_conta,
  cust.ativo,
  cust.ativo_atipico,
  cust.data_evento,
  COALESCE(CAST(cust.data_D_15_custodia AS DATE), CAST(cust.data_evento AS DATE) - INTERVAL '15' DAY) as data_D_15_custodia,
  cust.custodia_D_15,
  cust.data_D_1_custodia,
  cust.custodia_D_1,
  cust.data_D0_custodia,
  cust.custodia_D0,
  cust.data_D1_custodia,
  cust.custodia_D1,

  -- Colunas das operações PF (CTA14)
  ops.codigo_cliente,
  ops.cpf_cnpj_cliente,
  ops.ativo_negociado,
  ops.tipo_pessoa,
  ops.cnpj_empresa_vinculada,
  ops.ticker_empresa_vinculada,
  ops.Pessoa_Relacionada,
  ops.linha_negocio,
  ops.origem_operacao,
  ops.day_trader,
  ops.soma_volume_compra_historica,
  ops.soma_volume_venda_historica,
  ops.media_volume_compra_historica,
  ops.media_volume_venda_historica,
  ops.desvio_volume_compra,
  ops.desvio_volume_venda,
  ops.soma_quantidade_compra_historica,
  ops.soma_quantidade_venda_historica,
  ops.media_quantidade_compra_historica,
  ops.media_quantidade_venda_historica,
  ops.desvio_quantidade_compra,
  ops.desvio_quantidade_venda,
  -- ops.n_dias_compra_historico,
  -- ops.n_dias_venda_historico,
  ops.soma_volume_compra_15dias,
  ops.soma_volume_venda_15dias,
  ops.media_volume_compra_15dias,
  ops.media_volume_venda_15dias,
  ops.soma_quantidade_compra_15dias,
  ops.soma_quantidade_venda_15dias,
  ops.media_quantidade_compra_15dias,
  ops.media_quantidade_venda_15dias,
  -- ops.n_dias_compra_pre,
  -- ops.n_dias_venda_pre,
  ops.volume_compra_D0,
  ops.volume_venda_D0,
  ops.quantidade_compra_D0,
  ops.quantidade_venda_D0,
  -- ops.n_dias_compra_D0,
  -- ops.n_dias_venda_D0,
  ops.soma_volume_compra_5diasDepois,
  ops.soma_volume_venda_5diasDepois,
  ops.media_volume_compra_5diasDepois,
  ops.media_volume_venda_5diasDepois,
  ops.soma_quantidade_compra_5diasDepois,
  ops.soma_quantidade_venda_5diasDepois,
  ops.media_quantidade_compra_5diasDepois,
  ops.media_quantidade_venda_5diasDepois,
  ops.Op_historica_compra,
  ops.Op_historica_venda,
  ops.Op_historica_compra_maior3,
  ops.Op_historica_venda_maior3,
  ops.Op_pre_compra,
  ops.Op_pre_venda,
  ops.Op_D0_compra,
  ops.Op_D0_venda,
  ops.Op_pos_compra,
  ops.Op_pos_venda,
  ops.Fugiu_hist_compra_pre,
  ops.Fugiu_hist_venda_pre,
  ops.Fugiu_hist_compra_pos,
  ops.Fugiu_hist_venda_pos,
  ops.valor_fechamento_d_1,
  ops.preco_fechamento_d_1,
  ops.data_d_1,
  ops.valor_fechamento_d0,
  ops.preco_fechamento_d0,
  ops.data_d0,
  ops.valor_fechamento_d1,
  ops.preco_fechamento_d1,
  ops.data_d1,

  ops.tipo_variacao_d_1_d0,
  -- ops.variacao_preco_d_1_d0,
  -- ops.tipo_variacao_d0_d1,
  -- ops.variacao_preco_d0_d1,
  -- ops.tipo_variacao_final,
  ops.preco_medio_pre,
  ops.preco_medio_pos,
  ops.resultado,
  '{{SAFRA}}' AS SAFRA
FROM workspace_db.CTA16_CustodiaInfos_{{SAFRA}} cust
INNER JOIN workspace_db.CTA13_CalculoResultado_PF_{{SAFRA}} ops
  ON CAST(cust.data_evento AS DATE) = CAST(ops.data_evento AS DATE)
  AND CAST(cust.codigo_conta AS VARCHAR) = ops.codigo_cliente
  AND cust.ativo = ops.ativo_negociado
""",
    },
    'cta18': {
        'stage_id': 'cta18',
        'logical_table': 'CTA18_CustodiaReversao_{{SAFRA}}',
        'sha256': '876ad1ea8abf4eab3e22b30f8b9169f8719fb85bb5aa4431da4c77c6fe9d04fb',
        'sql': r"""CREATE TABLE workspace_db.CTA18_CustodiaReversao_{{SAFRA}}
WITH (format = 'PARQUET',
      external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA18_CustodiaReversao_{{SAFRA}}/',
      partitioned_by = ARRAY['SAFRA']) AS
WITH
-- Remove casos onde não há operação relevante
custodia_filtrada AS (
  SELECT *
  FROM workspace_db.CTA17_CustodiaOperacoes_{{SAFRA}}
  WHERE NOT (
    -- Condição 1: QUEDA sem venda pré e sem custódia
    (tipo_variacao_d_1_d0 = 'QUEDA'
     AND soma_quantidade_venda_15dias = 0
     AND CAST(custodia_D_1 AS DOUBLE) = 0
     AND CAST(custodia_D0 AS DOUBLE) = 0
     AND custodia_D1 = 0)
    OR
    -- Condição 2: ALTA sem compra pré e custódia não variou
    (tipo_variacao_d_1_d0 = 'ALTA'
     AND soma_quantidade_compra_15dias = 0
     AND CAST(custodia_D_1 AS DOUBLE) = CAST(custodia_D0 AS DOUBLE)
     AND CAST(custodia_D0 AS DOUBLE) = custodia_D1)
  )
),

-- Calcula variações de custódia
custodia_com_variacoes AS (
  SELECT
    *,
    -- Custodia_D(-1)_D0
    CASE
      WHEN custodia_D_1 IS NULL OR custodia_D0 IS NULL THEN NULL
      WHEN CAST(custodia_D_1 AS DOUBLE) > CAST(custodia_D0 AS DOUBLE) THEN 'DIMINUIU'
      WHEN CAST(custodia_D_1 AS DOUBLE) < CAST(custodia_D0 AS DOUBLE) THEN 'AUMENTOU'
      ELSE 'MANTEVE'
    END AS Custodia_D_menos1_D0,

    -- Variacao_custodia_D(-1)_D0
    CASE
      WHEN custodia_D_1 IS NULL OR custodia_D0 IS NULL THEN NULL
      WHEN CAST(custodia_D_1 AS DOUBLE) = 0 THEN
        CASE WHEN CAST(custodia_D0 AS DOUBLE) = 0 THEN '0.00%' ELSE '100.00%' END
      ELSE CAST(ROUND(((CAST(custodia_D0 AS DOUBLE) - CAST(custodia_D_1 AS DOUBLE)) / ABS(CAST(custodia_D_1 AS DOUBLE))) * 100, 2) AS VARCHAR) || '%'
    END AS Variacao_custodia_D_menos1_D0,

    -- Custodia_D0_D1
    CASE
      WHEN custodia_D0 IS NULL OR custodia_D1 IS NULL THEN NULL
      WHEN CAST(custodia_D0 AS DOUBLE) > custodia_D1 THEN 'DIMINUIU'
      WHEN CAST(custodia_D0 AS DOUBLE) < custodia_D1 THEN 'AUMENTOU'
      ELSE 'MANTEVE'
    END AS Custodia_D0_D1,

    -- Variacao_custodia_D0_D1
    CASE
      WHEN custodia_D0 IS NULL OR custodia_D1 IS NULL THEN NULL
      WHEN CAST(custodia_D0 AS DOUBLE) = 0 THEN
        CASE WHEN custodia_D1 = 0 THEN '0.00%' ELSE '100.00%' END
      ELSE CAST(ROUND(((custodia_D1 - CAST(custodia_D0 AS DOUBLE)) / ABS(CAST(custodia_D0 AS DOUBLE))) * 100, 2) AS VARCHAR) || '%'
    END AS Variacao_custodia_D0_D1,

    -- Resultado_custodia
    ((CAST(custodia_D_1 AS DOUBLE) - CAST(custodia_D0 AS DOUBLE)) * preco_fechamento_d0) +
    ((CAST(custodia_D0 AS DOUBLE) - custodia_D1) * preco_fechamento_d1) AS Resultado_custodia

  FROM custodia_filtrada
),
-- ##################################################### A J U S T A D O #####################################################
-- Calcula reversão (retorna como VARCHAR para incluir 'Não consta')
custodia_com_reversao AS (
  SELECT
    *,
    CASE
      -- Para ALTA: (vendas pós) / (custódia D-1 + compras D1)
      WHEN tipo_variacao_d_1_d0 = 'ALTA' THEN
        CASE
          -- Verifica se denominador é NULL ou zero
          WHEN (CAST(custodia_D_1 AS DOUBLE)) IS NULL THEN '0.0'
          WHEN (CAST(custodia_D_1 AS DOUBLE)) = 0 THEN '0.0'
          ELSE CAST(ROUND(((soma_quantidade_venda_5diasDepois + quantidade_venda_D0) /
                (CAST(custodia_D_1 AS DOUBLE))) * 100, 2) AS VARCHAR)
        END

      -- Para QUEDA: vendas pré / (custódia D-15 + compras pré)
      WHEN tipo_variacao_d_1_d0 = 'QUEDA' THEN
        CASE
          -- Primeiro verifica se é 'Não consta'
          WHEN custodia_D_15 = 'Não consta' THEN 'Não consta'
          -- Depois verifica se é NULL
          WHEN custodia_D_15 IS NULL THEN '0.0'
          -- Depois verifica se é zero
          WHEN CAST(custodia_D_15 AS DOUBLE) = 0 THEN '0.0'
          -- Caso contrário, calcula a reversão
          ELSE CAST(ROUND((soma_quantidade_venda_15dias /
                (CAST(custodia_D_15 AS DOUBLE) + soma_quantidade_compra_15dias)) * 100, 2) AS VARCHAR)
        END

      ELSE '0.0'
    END AS reversao
  FROM custodia_com_variacoes
)

-- SELECT final (SAFRA deve ser a última coluna por ser chave de partição)
SELECT DISTINCT
  codigo_conta,
  ativo,
  data_evento,
  data_D_15_custodia,
  custodia_D_15,
  data_D_1_custodia,
  custodia_D_1,
  data_D0_custodia,
  custodia_D0,
  data_D1_custodia,
  custodia_D1,
  codigo_cliente,
  cpf_cnpj_cliente,
  ativo_negociado,
  tipo_pessoa,
  cnpj_empresa_vinculada,
  ticker_empresa_vinculada,
  Pessoa_Relacionada,
  linha_negocio,
  origem_operacao,
  day_trader,
  soma_volume_compra_historica,
  soma_volume_venda_historica,
  media_volume_compra_historica,
  media_volume_venda_historica,
  desvio_volume_compra,
  desvio_volume_venda,
  soma_quantidade_compra_historica,
  soma_quantidade_venda_historica,
  media_quantidade_compra_historica,
  media_quantidade_venda_historica,
  desvio_quantidade_compra,
  desvio_quantidade_venda,
  -- n_dias_compra_historico,
  -- n_dias_venda_historico,
  soma_volume_compra_15dias,
  soma_volume_venda_15dias,
  media_volume_compra_15dias,
  media_volume_venda_15dias,
  soma_quantidade_compra_15dias,
  soma_quantidade_venda_15dias,
  media_quantidade_compra_15dias,
  media_quantidade_venda_15dias,
  -- n_dias_compra_pre,
  -- n_dias_venda_pre,
  volume_compra_D0,
  volume_venda_D0,
  quantidade_compra_D0,
  quantidade_venda_D0,
  -- n_dias_compra_D0,
  -- n_dias_venda_D0,

  soma_volume_compra_5diasDepois,
  soma_volume_venda_5diasDepois,
  media_volume_compra_5diasDepois,
  media_volume_venda_5diasDepois,
  soma_quantidade_compra_5diasDepois,
  soma_quantidade_venda_5diasDepois,
  media_quantidade_compra_5diasDepois,
  media_quantidade_venda_5diasDepois,
  Op_historica_compra,
  Op_historica_venda,
  Op_historica_compra_maior3,
  Op_historica_venda_maior3,
  Op_pre_compra,
  Op_pre_venda,
  Op_D0_compra,
  Op_D0_venda,
  Op_pos_compra,
  Op_pos_venda,
  Fugiu_hist_compra_pre,
  Fugiu_hist_venda_pre,
  Fugiu_hist_compra_pos,
  Fugiu_hist_venda_pos,
  valor_fechamento_d_1,
  preco_fechamento_d_1,
  data_d_1,
  valor_fechamento_d0,
  preco_fechamento_d0,
  data_d0,
  valor_fechamento_d1,
  preco_fechamento_d1,
  data_d1,

  -- variacao_preco_d_1_d0,
  tipo_variacao_d_1_d0,
  -- variacao_preco_d0_d1,
  -- tipo_variacao_d0_d1,
  -- tipo_variacao_final,
  preco_medio_pre,
  preco_medio_pos,

  Resultado,
  Custodia_D_menos1_D0,
  Variacao_custodia_D_menos1_D0,
  Custodia_D0_D1,
  Variacao_custodia_D0_D1,
  Resultado_custodia,
  COALESCE(reversao, 'Não consta') AS reversao,
  CASE
    WHEN reversao = 'Não consta' THEN 'ALERTA'
    WHEN TRY_CAST(reversao AS DOUBLE) >= 75 THEN 'ALERTA'
    WHEN TRY_CAST(reversao AS DOUBLE) < 75 THEN 'NÃO ALERTA'
    ELSE 'ALERTA'
  END AS resultado_final,
  SAFRA  -- Coluna de partição deve ser a última
FROM custodia_com_reversao
""",
    },
    'cta18_alertas': {
        'stage_id': 'cta18_alertas',
        'logical_table': 'CTA18_AlertasTradingAhead_PF_{{SAFRA}}',
        'sha256': '8adad4e3947e9db34862094eef3210076442afa6f1cb20788db32e8cbf7d874b',
        'sql': r"""CREATE TABLE workspace_db.CTA18_AlertasTradingAhead_PF_{{SAFRA}}
WITH (format = 'PARQUET',
      external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA18_AlertasTradingAhead_PF_{{SAFRA}}/',
      partitioned_by = ARRAY['SAFRA']) AS
SELECT *
FROM workspace_db.CTA18_CustodiaReversao_{{SAFRA}}
WHERE resultado_final = 'ALERTA'
""",
    },
    'cta19': {
        'stage_id': 'cta19',
        'logical_table': 'CTA19_ProxyReversaoCustodia_{{SAFRA}}',
        'sha256': '7f5336686f9ef7391762e11603e579c89cea6dcf724fb977eb5e7db6c7006bb0',
        'sql': r"""CREATE TABLE workspace_db.CTA19_ProxyReversaoCustodia_{{SAFRA}}
WITH (format = 'PARQUET',
      external_location = 's3://itau-self-wkp-sa-east-1-886436964833/TRADING_AHEAD/ctas/CTA19_ProxyReversaoCustodia_{{SAFRA}}/',
      partitioned_by = ARRAY['SAFRA']) AS
SELECT
  codigo_cliente,
  cpf_cnpj_cliente,
  ativo_negociado,
  tipo_pessoa,
  cnpj_empresa_vinculada,
  ticker_empresa_vinculada,
  Pessoa_Relacionada,
  linha_negocio,
  origem_operacao,
  data_evento,
  day_trader,
  soma_volume_compra_historica,
  soma_volume_venda_historica,
  media_volume_compra_historica,
  media_volume_venda_historica,
  desvio_volume_compra,
  desvio_volume_venda,
  soma_quantidade_compra_historica,
  soma_quantidade_venda_historica,
  media_quantidade_compra_historica,
  media_quantidade_venda_historica,
  desvio_quantidade_compra,
  desvio_quantidade_venda,
  -- n_dias_compra_historico,
  -- n_dias_venda_historico,
  soma_volume_compra_15dias,
  soma_volume_venda_15dias,
  media_volume_compra_15dias,
  media_volume_venda_15dias,
  soma_quantidade_compra_15dias,
  soma_quantidade_venda_15dias,
  media_quantidade_compra_15dias,
  media_quantidade_venda_15dias,
  -- n_dias_compra_pre,
  -- n_dias_venda_pre,
  volume_compra_D0,
  volume_venda_D0,
  quantidade_compra_D0,
  quantidade_venda_D0,
  -- n_dias_compra_D0,
  -- n_dias_venda_D0,

  soma_volume_compra_5diasDepois,
  soma_volume_venda_5diasDepois,
  media_volume_compra_5diasDepois,
  media_volume_venda_5diasDepois,
  soma_quantidade_compra_5diasDepois,
  soma_quantidade_venda_5diasDepois,
  media_quantidade_compra_5diasDepois,
  media_quantidade_venda_5diasDepois,
  Op_historica_compra,
  Op_historica_venda,
  Op_historica_compra_maior3,
  Op_historica_venda_maior3,
  Op_pre_compra,
  Op_pre_venda,
  Op_D0_compra,
  Op_D0_venda,
  Op_pos_compra,
  Op_pos_venda,
  Fugiu_hist_compra_pre,
  Fugiu_hist_venda_pre,
  Fugiu_hist_compra_pos,
  Fugiu_hist_venda_pos,
  valor_fechamento_d_1,
  preco_fechamento_d_1,
  data_d_1,
  valor_fechamento_d0,
  preco_fechamento_d0,
  data_d0,
  valor_fechamento_d1,
  preco_fechamento_d1,
  data_d1,
  
  -- variacao_preco_d_1_d0,
  tipo_variacao_d_1_d0,
  -- variacao_preco_d0_d1,
  -- tipo_variacao_d0_d1,
  -- tipo_variacao_final,
  preco_medio_pre,
  preco_medio_pos,

  resultado,
  -- Nova coluna: Reversão_PJ
  CASE
    -- Cenário 1: ALTA + repasse_enviado
    -- Calcula percentual que NÃO foi vendido: {1 - [(venda D0 + venda 5 dias) / compra 15 dias]} * 100
    WHEN tipo_variacao_d_1_d0 = 'ALTA' AND origem_operacao = 'repasse_enviado'
      THEN ROUND((1 - (CAST(quantidade_venda_D0 + soma_quantidade_venda_5diasDepois AS DOUBLE) / NULLIF(soma_quantidade_compra_15dias, 0))) * 100, 2)

    -- Cenário 2: QUEDA + repasse_enviado
    -- Calcula percentual que NÃO foi recomprado: {1 - [compra 5 dias / (venda 15 dias + venda D0)]} * 100
    WHEN tipo_variacao_d_1_d0 = 'QUEDA' AND origem_operacao = 'repasse_enviado'
      THEN ROUND((1 - (CAST(soma_quantidade_compra_5diasDepois AS DOUBLE) / NULLIF(soma_quantidade_venda_15dias + quantidade_venda_D0, 0))) * 100, 2)

    -- Demais casos: NULL
    ELSE NULL
  END AS Reversao_PJ,

  -- Nova coluna: resultado_final baseado em Reversao_PJ
  CASE
    -- Se Reversao_PJ for NULL, resultado_final também é NULL
    WHEN (
      CASE
        WHEN tipo_variacao_d_1_d0 = 'ALTA' AND origem_operacao = 'repasse_enviado'
          THEN ROUND((1 - (CAST(quantidade_venda_D0 + soma_quantidade_venda_5diasDepois AS DOUBLE) / NULLIF(soma_quantidade_compra_15dias, 0))) * 100, 2)
        WHEN tipo_variacao_d_1_d0 = 'QUEDA' AND origem_operacao = 'repasse_enviado'
          THEN ROUND((1 - (CAST(soma_quantidade_compra_5diasDepois AS DOUBLE) / NULLIF(soma_quantidade_venda_15dias + quantidade_venda_D0, 0))) * 100, 2)
        ELSE NULL
      END
    ) IS NULL THEN NULL
    -- Se Reversao_PJ > 75, resultado é ALERTA
    WHEN (
      CASE
        WHEN tipo_variacao_d_1_d0 = 'ALTA' AND origem_operacao = 'repasse_enviado'
          THEN ROUND((1 - (CAST(quantidade_venda_D0 + soma_quantidade_venda_5diasDepois AS DOUBLE) / NULLIF(soma_quantidade_compra_15dias, 0))) * 100, 2)
        WHEN tipo_variacao_d_1_d0 = 'QUEDA' AND origem_operacao = 'repasse_enviado'
          THEN ROUND((1 - (CAST(soma_quantidade_compra_5diasDepois AS DOUBLE) / NULLIF(soma_quantidade_venda_15dias + quantidade_venda_D0, 0))) * 100, 2)
        ELSE NULL
      END
    ) > 75 THEN 'ALERTA'
    -- Se Reversao_PJ <= 75, resultado é NÃO ALERTA
    ELSE 'NÃO ALERTA'
  END AS resultado_final,

  SAFRA
FROM workspace_db.CTA13_CalculoResultado_PJ_{{SAFRA}}
""",
    },
}

SOURCE_FILE_HASHES = {'policy': 'ed584b56416780551ea3527d41629dfa9982d1a0787ff1ece0c3faae494aa733', 'config': '474537b2c9ba5fc63159c1fc55106e861eab009cb1d9829596ef73765bff90ec', 'aws_runtime': '4be7b88e654cdaad58a4f37ef1e2328f0adaa017666c050450df027b546a548e', 'pipeline': '5fe0d65bda0b13cff4af68a940a024a67fdf9c1b4afa35c04f35c0bf8446dd61'}
# =========================================================
# OBSERVABILIDADE — padrão de Produção/Main-glue-v2.py (Cripto)
# =========================================================
# As colunas originais de resumo/detalhe são preservadas. Campos adicionais
# permitem correlacionar execução, tentativa, etapa, público e consulta.
# Diagnósticos de nulos/tipos/domínio/repetições são observacionais: não
# acrescentam limpeza, filtro de clientes ou tolerância de negócio ao modelo.
import argparse
import importlib.metadata
import platform
import sys
import traceback

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


def production_parser():
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    choice = parser.add_mutually_exclusive_group(required=True)
    choice.add_argument('--config', help='Configuração JSON local.')
    choice.add_argument('--config-json', help='Configuração JSON como argumento do Glue.')
    choice.add_argument('--config-s3', help='Configuração no S3, somente em execute.')
    parser.add_argument('--config-bucket-owner', help='Conta dona do bucket da configuração S3.')
    parser.add_argument('--mode', choices=('plan', 'execute'), default='plan')
    parser.add_argument('--output-dir', help='Novo diretório para plano local.')
    parser.add_argument('--target-month')
    parser.add_argument('--run-id')
    parser.add_argument('--resume', action='store_true')
    for key in ('JOB_NAME', 'JOB_ID', 'JOB_RUN_ID', 'SECURITY_CONFIGURATION',
                'TempDir', 'RedshiftTempDir', 'library-set', 'extra-py-files',
                'additional-python-modules', 'job-bookmark-option', 'glue-version',
                'job-language', 'enable-glue-datacatalog', 'enable-metrics',
                'enable-continuous-cloudwatch-log', 'enable-observability-metrics',
                'enable-auto-scaling'):
        parser.add_argument('--' + key, dest='glue_' + key.replace('-', '_'), nargs='?', const='true')
    return parser


def load_production_config(args):
    if args.config_s3:
        if args.mode != 'execute':
            raise ValueError('Planejamento offline requer --config ou --config-json, sem acesso ao S3.')
        if not args.config_bucket_owner or not re.fullmatch(r'\d{12}', args.config_bucket_owner):
            raise ValueError('--config-bucket-owner precisa conter 12 dígitos.')
        bucket, key = s3_parts(args.config_s3)
        import boto3
        from botocore.config import Config
        client = boto3.client('s3', config=Config(connect_timeout=10, read_timeout=60,
                                               retries={'mode': 'standard', 'max_attempts': 3}))
        response = client.get_object(Bucket=bucket, Key=key, ExpectedBucketOwner=args.config_bucket_owner)
        try:
            raw = json.load(response['Body'])
        finally:
            response['Body'].close()
    elif args.config:
        raw = json.loads(Path(args.config).read_text(encoding='utf-8'))
    else:
        raw = json.loads(args.config_json)
    if not isinstance(raw, dict):
        raise ValueError('A configuração precisa ser um objeto JSON.')
    raw = dict(raw)
    options = validate_observability(raw.pop('observabilidade', None))
    for key in ('target_month', 'run_id'):
        if getattr(args, key) is not None:
            raw[key] = getattr(args, key)
    if args.resume:
        raw['resume'] = True
    config = validate_config(raw, executing=args.mode == 'execute')
    if config['history_days'] != APPROVED_HISTORY_DAYS:
        raise ValueError('Este arquivo de produção admite somente P99 de 182 dias; três meses foi controle do estudo.')
    return config, options


def main(argv=None):
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(line_buffering=True)
    args = production_parser().parse_args(argv)
    config, options = load_production_config(args)
    bundle_fingerprint(SQL_BUNDLE)
    if args.mode == 'plan':
        pipeline = Pipeline(config, SQL_BUNDLE)
        plan = pipeline.plan()
        plan['observabilidade'] = {
            'version': OBSERVABILITY_VERSION, 'opcoes': options,
            'log_resumo': config['database'] + '.ta_p99_' + config['run_id'] + '_log_resumo',
            'log_detalhe': config['database'] + '.ta_p99_' + config['run_id'] + '_log_detalhe',
            'path': pipeline.root + 'logs/', 'partition_cols': ['safra'], 'mode': 'append',
            'schema_resumo': dict(DTYPE_LOG_RESUMO, **EXTRA_LOG_TYPES),
            'schema_detalhe': dict(DTYPE_LOG_DETALHE, **EXTRA_LOG_TYPES),
            'sem_novos_filtros_de_negocio': True,
        }
        plan['diagnosticos_fonte'] = [
            {'stage': stage, 'sql_resumo': probe_summary_sql(source_probe_specs(pipeline, config['target_month'], stage)),
             'sql_repeticoes': probe_duplicate_sql(source_probe_specs(pipeline, config['target_month'], stage))}
            for stage in ('cta01', 'cta06')
        ] if options['diagnosticos_fontes'] else []
        folder = Path(args.output_dir or ('plano_' + config['run_id']))
        folder.mkdir(parents=True, exist_ok=False)
        (folder / 'plano.json').write_text(json.dumps(plan, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
        sql_dir = folder / 'sql'; sql_dir.mkdir()
        for index, task in enumerate(plan['tasks'], 1):
            if 'sql' in task:
                (sql_dir / f"{index:03d}_{task['month']}_{task['stage']}.sql").write_text(task['sql'], encoding='utf-8')
        emit('plan_created', aws_calls=0, run_id=config['run_id'], path=str(folder.resolve()),
             history_days=182, financial_rule=plan['financial_rule'])
        return 0
    runner = LoggingRunner(config)
    logbook = LogBook(config, options, runner)
    runner.logbook = logbook
    logbook.summary('inicio_execucao', total=1, stage='inicializacao')
    logbook.detail('inicio_execucao', 'execucao', config['run_id'],
                   {'regra': POLICY_VERSION, 'observabilidade': OBSERVABILITY_VERSION,
                    'glue_job_name': args.glue_JOB_NAME, 'glue_job_run_id': args.glue_JOB_RUN_ID,
                    'opcoes_logs': options}, stage='inicializacao')
    if options['registrar_requirements']:
        inventory = runtime_inventory()
        print('=== REQUIREMENTS BEGIN ===', flush=True)
        print(json.dumps(inventory, ensure_ascii=False, sort_keys=True), flush=True)
        print('=== REQUIREMENTS END ===', flush=True)
        logbook.detail('ambiente_execucao', 'runtime', 'python', inventory, stage='inicializacao')
    pipeline = LoggingPipeline(config, SQL_BUNDLE, runner, logbook)
    pipeline.run()
    return 0


def entrypoint(argv=None):
    try:
        return main(argv)
    except Exception as error:
        emit('job_failed', **safe_exception(error))
        return 1


if __name__ == '__main__':
    raise SystemExit(entrypoint())
