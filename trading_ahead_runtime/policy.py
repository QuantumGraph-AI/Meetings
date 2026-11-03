"""Contrato matemático independente de AWS; não muda os filtros da CTA."""
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
