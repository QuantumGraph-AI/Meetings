"""Lê e verifica os SQLs, tanto da pasta local como do pacote ZIP do Glue."""
from importlib import resources
from pathlib import PurePosixPath
import hashlib
import json
from .pipeline import SOURCE_HASH,bundle_fingerprint


def load_sql_bundle():
    root=resources.files(__package__)
    manifest=json.loads(root.joinpath('sql').joinpath('manifesto_sql.json').read_text(encoding='utf-8'))
    if manifest['source_notebook_sha256']!=SOURCE_HASH:
        raise ValueError('Versão da referência SQL não reconhecida.')
    bundle={}
    for item in manifest['templates']:
        relative=PurePosixPath(item['file'])
        if relative.is_absolute() or len(relative.parts)!=2 or relative.parts[0]!='sql' or '..' in relative.parts:
            raise ValueError('Caminho SQL fora do pacote.')
        if item['stage_id'] in bundle:
            raise ValueError('Etapa SQL duplicada no manifesto.')
        sql=root.joinpath(relative.parts[0]).joinpath(relative.parts[1]).read_text(encoding='utf-8')
        if hashlib.sha256(sql.encode('utf-8')).hexdigest()!=item['sha256']:
            raise ValueError('SQL diverge do conteúdo aprovado: '+item['stage_id'])
        bundle[item['stage_id']]=dict(item,sql=sql)
    bundle_fingerprint(bundle)
    return bundle
