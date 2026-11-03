"""Janela aprovada de 182 dias; leitura e validação não acessam a AWS."""
from datetime import date, timedelta
from urllib.parse import urlparse
import hashlib
import json
import re
import uuid

from .policy import APPROVED_HISTORY_DAYS, POLICY_VERSION, historical_window, month_bounds

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
