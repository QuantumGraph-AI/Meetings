"""Trading Ahead — entrada do job AWS Glue.

Regra: P99 de 182 dias, separado por PF/PJ, com piso de R$ 100 mil.
Modo padrão: plan. Executar requer --mode execute explicitamente.

Local: manter trading_ahead_runtime ao lado deste arquivo.
Glue: informar TradingAhead_Runtime.zip em --extra-py-files.
Configuração: --config, --config-json ou --config-s3 (execute).
Dependências: boto3, botocore, pandas, pyarrow e awswrangler.

As regras, consultas e técnicas de logs estão nos módulos do runtime.
"""
from trading_ahead_runtime.job import entrypoint


if __name__ == '__main__':
    raise SystemExit(entrypoint())
