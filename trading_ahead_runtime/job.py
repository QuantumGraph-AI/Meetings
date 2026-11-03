"""Entrada local/Glue: P99 de 182 dias por PF/PJ, piso de R$ 100 mil."""
from pathlib import Path
import argparse
import json
import re
import sys
from .config import s3_parts,validate_config
from .policy import APPROVED_HISTORY_DAYS,POLICY_VERSION
from .pipeline import Pipeline,bundle_fingerprint
from .bundle import load_sql_bundle
from .logging import (OBSERVABILITY_VERSION,DTYPE_LOG_RESUMO,DTYPE_LOG_DETALHE,
                      EXTRA_LOG_TYPES,validate_observability,emit,safe_exception,
                      runtime_inventory,LoggingRunner,LogBook)
from .instrumentation import LoggingPipeline
from .diagnostics import source_probe_specs,probe_summary_sql,probe_duplicate_sql

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
    bundle = load_sql_bundle()
    bundle_fingerprint(bundle)
    if args.mode == 'plan':
        pipeline = Pipeline(config, bundle)
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
    pipeline = LoggingPipeline(config, bundle, runner, logbook)
    pipeline.run()
    return 0


def entrypoint(argv=None):
    try:
        return main(argv)
    except Exception as error:
        emit('job_failed', **safe_exception(error))
        return 1
