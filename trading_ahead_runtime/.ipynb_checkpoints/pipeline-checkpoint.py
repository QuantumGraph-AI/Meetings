"""SQL Athena preservado e trilha financeira nova, com publicação ao concluir."""
from datetime import date, timedelta
from pathlib import Path
import hashlib
import json
import re

from .config import config_fingerprint, qualified_table, s3_parts, validate_config
from .policy import (APPROVED_HISTORY_DAYS, DEDUP_KEY, POLICY_VERSION, dedup_projection_sql,
                     effective_cutoff, historical_window, history_months,
                     interpolate_p99, month_bounds, quantile_positions)

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
