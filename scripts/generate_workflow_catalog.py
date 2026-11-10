#!/usr/bin/env python3
"""Freeze typed, topology-distinct *derived* QA/SQL DAG templates.

No model, retriever, database, benchmark label, or credential is used here.
Structural generation is separate from task-quality/performance evaluation.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import random
import shutil
import statistics
import sys
import tempfile

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from scripts.workflow_catalog_runtime import compile_template
from scripts.workflow_catalog_topology import graph_metrics, graph_signature, is_isomorphic


VERSION = "geoleap-typed-dag-v1"
SEED = 20261111
QUOTAS = (("small", 3, 10, 70), ("medium", 11, 19, 20),
          ("large", 20, 29, 7), ("extra_large", 30, 50, 3))
LIMITS = {"max_nodes": 50, "max_antichain_width": 8, "max_parent_count": 4,
          "max_retrievals_per_path": 4, "max_sql_repairs_per_path": 2,
          "max_same_operator_per_path": 2, "max_branch_fanout": 4}
SOURCES = {
    "basic_multihop": {"title": "DSPy BasicMultiHop / SimplifiedBaleen",
        "paper": "https://arxiv.org/html/2310.03714#S7",
        "code": "https://github.com/stanfordnlp/dspy/blob/e08bf0e4635764839800bd9bde17f472128433dc/tests/examples/test_baleen.py",
        "use": "Query generation, retrieval, answer stages; generated branches are our extensions."},
    "din_sql": {"title": "DIN-SQL",
        "paper": "https://arxiv.org/abs/2304.11015",
        "code": "https://github.com/MohammadrezaPourreza/Few-shot-NL2SQL-with-prompting/blob/79842fdfcba7598c1a69873348649d6b0dbbb551/DIN-SQL_BIRD.py",
        "use": "Schema linking, classification/decomposition, generation, self-correction stages; execution-guided repair is our extension."},
}


def contract(inputs, output, kind, description, required, arity=(1, 1)):
    return {"input_types": list(inputs), "min_inputs": arity[0], "max_inputs": arity[1],
            "output_type": output, "kind": kind, "description": description,
            "required_data": required, "side_effects": "none; resource access must be read-only",
            "contract_version": VERSION}


P, E, A = "qa.query_plan", "qa.evidence", "qa.answer"
C, S, X, O = "sql.context", "sql.candidate", "sql.checked", "sql.answer"
PLAN = {"queries": "array", "documents": "array"}
EVIDENCE = {"documents": "array"}
ANSWER = {"answer": "string", "documents": "array"}
CONTEXT = {"schema_context": "object"}
CANDIDATE = {"sql": "string", "schema_context": "object"}
CHECKED = dict(CANDIDATE, execution="object")
SQLANSWER = {"sql": "string"}
OPERATORS = {
    "qa.query_plan": contract([], P, "LLM", "Plan an initial evidence query from the current question; initialize carried documents to an empty list.", PLAN, (0, 0)),
    "qa.rewrite_query": contract([P], P, "LLM", "Rewrite a query for a declared complementary evidence intent; preserve carried documents.", PLAN),
    "qa.retrieve": contract([P], E, "Tool", "Retrieve real passages using the declared strategy; merge carried documents with new passages and retain provenance.", EVIDENCE),
    "qa.expand_query": contract([E], P, "LLM", "Generate the next-hop query using current question and intermediate evidence; carry all input evidence documents forward.", PLAN),
    "qa.rerank": contract([E], E, "Tool", "Rank passages for the declared evidence intent.", EVIDENCE),
    "qa.extract_facts": contract([E], E, "LLM", "Extract supported facts while retaining source passage IDs.", EVIDENCE),
    "qa.summarize": contract([E], E, "LLM", "Compress evidence with source provenance.", EVIDENCE),
    "qa.verify_evidence": contract([E], E, "LLM", "Check whether evidence supports the current question's constraints.", EVIDENCE),
    "qa.merge_evidence": contract([E], E, "Compute", "Union and deduplicate all parent evidence by source ID.", EVIDENCE, (2, 4)),
    "qa.answer": contract([E], A, "LLM", "Answer the current question using all parent evidence artifacts.", ANSWER, (1, 4)),
    "qa.select_answer": contract([A], A, "LLM", "Compare all candidate answers and their retained evidence.", ANSWER, (2, 4)),
    "sql.schema_linking": contract([], C, "LLM", "Link the current question to a fixed database schema and evidence.", CONTEXT, (0, 0)),
    "sql.classify": contract([C], C, "LLM", "Classify query complexity and annotate generation context.", CONTEXT),
    "sql.decompose": contract([C], C, "LLM", "Decompose the question into relational requirements.", CONTEXT),
    "sql.prune_schema": contract([C], C, "LLM", "Annotate selected schema elements for a relational concern; retain immutable base schema and join keys rather than destructively removing them.", CONTEXT),
    "sql.join_analysis": contract([C], C, "LLM", "Resolve join paths, keys and cardinality constraints.", CONTEXT),
    "sql.retrieve_values": contract([C], C, "Tool", "Read database values for grounding without modifying the database.", CONTEXT),
    "sql.merge_context": contract([C], C, "Compute", "Merge all parent schema annotations, preserving disagreements.", CONTEXT, (2, 4)),
    "sql.generate": contract([C], S, "LLM", "Generate one candidate SQL using every parent context.", CANDIDATE, (1, 4)),
    "sql.execute": contract([S], X, "Tool", "Execute candidate SQL read-only with timeout; return execution diagnostics, never gold comparisons.", CHECKED),
    "sql.repair": contract([X], S, "LLM", "Revise SQL from current question, schema and observed execution diagnostics.", CANDIDATE),
    "sql.select_candidate": contract([S, X], S, "LLM", "Compare candidate SQL and available diagnostics without reference SQL.", CANDIDATE, (2, 4)),
    "sql.self_correct": contract([S, X], O, "LLM", "Review and correct the selected SQL against the current task and schema.", SQLANSWER),
    "sql.select_and_correct": contract([S, X], O, "LLM", "Compare all candidates, then review the selected SQL; fused terminal selection.", SQLANSWER, (2, 4)),
}

# These are semantic branch roles, not random prompt/seed labels. Bindings must
# implement them. Changing roles never makes an isomorphic DAG count as new.
ROLES = {
    "qa.retrieve": ["entity_anchor", "relation_bridge", "constraint_detail", "independent_verification"],
    "qa.rewrite_query": ["entity_disambiguation", "relation_expansion", "temporal_constraint", "alternative_surface_form"],
    "qa.expand_query": ["bridge_entity", "supporting_relation", "constraint_resolution", "cross_check"],
    "qa.rerank": ["entity_relevance", "relation_relevance", "constraint_relevance", "source_diversity"],
    "qa.extract_facts": ["entities", "relations", "dates_and_numbers", "question_constraints"],
    "qa.summarize": ["entity_profile", "relation_summary", "constraint_summary", "source_comparison"],
    "qa.verify_evidence": ["entity_identity", "relation_support", "constraint_support", "contradiction_check"],
    "qa.answer": ["direct_evidence", "decomposed_reasoning", "constraint_first", "cross_checked_reasoning"],
    "sql.classify": ["join_complexity", "aggregation_complexity", "nesting_complexity", "filter_complexity"],
    "sql.decompose": ["projection", "filter_and_join", "aggregation", "ordering_and_nesting"],
    "sql.prune_schema": ["projection_columns", "filter_columns", "join_keys", "aggregation_columns"],
    "sql.join_analysis": ["foreign_key_paths", "key_cardinality", "bridge_tables", "alternative_join_paths"],
    "sql.retrieve_values": ["entity_values", "categorical_values", "date_ranges", "numeric_ranges"],
    "sql.generate": ["direct", "decomposition_guided", "join_first", "aggregation_first"],
    "sql.repair": ["syntax_and_names", "join_semantics", "aggregation_semantics", "constraint_semantics"],
}


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n", encoding="utf-8")


class Builder:
    def __init__(self, scenario):
        self.scenario = scenario
        self.nodes = {}
        self.counter = 0
        self.blocks = []
        self.history = []
        if scenario == "qa":
            root = self.add("qa.query_plan", [])
            body = self.add("qa.retrieve", [root])
            self.add("qa.answer", [body], terminal=True)
        else:
            root = self.add("sql.schema_linking", [])
            body = self.add("sql.generate", [root])
            self.add("sql.self_correct", [body], terminal=True)
        self.history.append({"rule": "seed", "variant": "single_hop_rag" if scenario == "qa" else "reduced_din_sql"})

    def add(self, operator, parents, *, terminal=False, parameters=None):
        if parameters is None and operator in ROLES:
            occupied = {n["parameters"].get("role") for n in self.nodes.values()
                        if n["operator"] == operator and set(n["dependencies"]) == set(parents)}
            roles = [role for role in ROLES[operator] if role not in occupied]
            if not roles:
                raise ValueError("No unused semantic sibling role is available")
            parameters = {"role": roles[0]}
        self.counter += 1
        key = "n%03d" % self.counter
        c = OPERATORS[operator]
        self.nodes[key] = {"id": key, "operator": operator, "dependencies": list(parents),
            "output_type": c["output_type"], "parameters": parameters or {"role": "default"},
            "kind": c["kind"], "terminal": terminal, "contract_version": VERSION}
        return key

    def edges(self):
        return [(p, n["id"]) for n in self.nodes.values() for p in n["dependencies"]]

    def children(self, key):
        return [n["id"] for n in self.nodes.values() if key in n["dependencies"]]

    def replace_in_blocks(self, old, new):
        for block in self.blocks:
            if old in block:
                block.remove(old)
                block.update(new)

    def insert(self, parent, child, operators):
        last = parent
        added = []
        for op in operators:
            last = self.add(op, [last])
            added.append(last)
        deps = self.nodes[child]["dependencies"]
        deps[deps.index(parent)] = last
        for block in self.blocks:
            if parent in block and child in block:
                block.update(added)
        self.blocks.append(set(added))
        self.history.append({"rule": "bounded_next_hop" if operators == ["qa.expand_query", "qa.retrieve"]
                             else "bounded_execute_repair" if operators == ["sql.execute", "sql.repair"]
                             else "typed_stage_insertion", "operators": operators,
                             "replaced_edge": [parent, child], "new_nodes": added})

    def fork(self, key, k, fused):
        old = self.nodes[key]
        children = self.children(key)
        branches = []
        available = self.fork_roles(key)
        if len(available) < k:
            raise ValueError("Fork would duplicate a sibling strategy")
        for role in available[:k]:
            branches.append(self.add(old["operator"], old["dependencies"],
                parameters=dict(old["parameters"], role=role)))
        if fused:
            child = self.nodes[children[0]]
            deps = child["dependencies"]
            position = deps.index(key)
            deps[position:position + 1] = branches
            if child["operator"] == "sql.self_correct":
                child["operator"] = "sql.select_and_correct"
            replacement = branches
        else:
            output = old["output_type"]
            merge = {E: "qa.merge_evidence", A: "qa.select_answer", C: "sql.merge_context", S: "sql.select_candidate"}[output]
            joined = self.add(merge, branches, terminal=old["terminal"])
            for c in children:
                deps = self.nodes[c]["dependencies"]
                deps[deps.index(key)] = joined
            replacement = branches + [joined]
        self.replace_in_blocks(key, replacement)
        self.blocks.append(set(replacement))
        del self.nodes[key]
        self.history.append({"rule": "strategy_fork_fused_join" if fused else "strategy_fork_explicit_join",
                             "operator": old["operator"], "replaced_node": key,
                             "branch_roles": [self.nodes[b]["parameters"]["role"] for b in branches],
                             "new_nodes": replacement})

    def fork_roles(self, key):
        node = self.nodes[key]
        occupied = {n["parameters"].get("role") for other, n in self.nodes.items()
                    if other != key and n["operator"] == node["operator"]
                    and set(n["dependencies"]) == set(node["dependencies"])}
        return [role for role in ROLES[node["operator"]] if role not in occupied]

    def actions(self, remaining):
        choices = []
        # An insertion splits a real artifact dependency, never adds a
        # transitive edge or an unused stage.
        for parent, child in self.edges():
            output = self.nodes[parent]["output_type"]
            unary = {P: ["qa.rewrite_query"], E: ["qa.rerank", "qa.extract_facts", "qa.summarize", "qa.verify_evidence"],
                     C: ["sql.classify", "sql.decompose", "sql.prune_schema", "sql.join_analysis", "sql.retrieve_values"],
                     S: ["sql.execute"] if X in OPERATORS[self.nodes[child]["operator"]]["input_types"] else [],
                     X: ["sql.repair"] if S in OPERATORS[self.nodes[child]["operator"]]["input_types"] else []}.get(output, [])
            for op in unary:
                if self.nodes[parent]["operator"] == op or self.nodes[child]["operator"] == op:
                    continue
                choices.append((1, "insert", parent, child, [op]))
            if remaining >= 2 and output == E:
                choices.append((2, "insert", parent, child, ["qa.expand_query", "qa.retrieve"]))
            if remaining >= 2 and output == S and S in OPERATORS[self.nodes[child]["operator"]]["input_types"]:
                choices.append((2, "insert", parent, child, ["sql.execute", "sql.repair"]))
        for key, node in self.nodes.items():
            if not node["dependencies"] or len(node["dependencies"]) != 1 or node["operator"] not in ROLES:
                continue
            children = self.children(key)
            for k in range(2, 5):
                if k > len(self.fork_roles(key)):
                    continue
                # QA final answer may fan out into genuinely distinct answer
                # strategies and end in an explicit answer selection stage.
                if node["output_type"] in (E, A, C, S) and k <= remaining:
                    choices.append((k, "fork", key, k, False))
                if node["terminal"] or len(children) != 1 or k - 1 > remaining:
                    continue
                child = self.nodes[children[0]]
                op = "sql.select_and_correct" if child["operator"] == "sql.self_correct" else child["operator"]
                if len(child["dependencies"]) - 1 + k <= OPERATORS[op]["max_inputs"]:
                    choices.append((k - 1, "fork", key, k, True))
        return choices

    def valid(self):
        metrics = graph_metrics(self.nodes, self.edges())
        if (metrics["maximum_antichain_width"] > LIMITS["max_antichain_width"]
                or metrics["max_out_degree"] > LIMITS["max_branch_fanout"]):
            return False
        sibling_keys = [(tuple(sorted(n["dependencies"])), n["operator"], canonical(n["parameters"]))
                        for n in self.nodes.values()]
        if len(sibling_keys) != len(set(sibling_keys)):
            return False
        counts = {key: {} for key in self.nodes}
        for key in metrics["topological_order"]:
            node = self.nodes[key]
            c = OPERATORS[node["operator"]]
            if not c["min_inputs"] <= len(node["dependencies"]) <= c["max_inputs"]:
                return False
            if any(self.nodes[p]["output_type"] not in c["input_types"] for p in node["dependencies"]):
                return False
            for parent in node["dependencies"]:
                for op, count in counts[parent].items():
                    counts[key][op] = max(counts[key].get(op, 0), count)
            op = node["operator"]
            counts[key][op] = counts[key].get(op, 0) + 1
            for operation, count in counts[key].items():
                bound = LIMITS["max_same_operator_per_path"]
                if operation == "qa.retrieve":
                    bound = LIMITS["max_retrievals_per_path"]
                elif operation in ("qa.expand_query",):
                    bound = LIMITS["max_retrievals_per_path"] - 1
                elif operation == "sql.execute":
                    bound = LIMITS["max_sql_repairs_per_path"] + 1
                elif operation == "sql.repair":
                    bound = LIMITS["max_sql_repairs_per_path"]
                if count > bound:
                    return False
        return True

    def finish(self, template_id, seed, candidate_number, bucket):
        metrics = graph_metrics(self.nodes, self.edges())
        # Canonical in-file numbering follows dependency order. It is not
        # used as a graph-isomorphism certificate.
        rename = {old: "n%03d" % (i + 1) for i, old in enumerate(metrics["topological_order"])}
        ordered = []
        for old in metrics["topological_order"]:
            node = deepcopy(self.nodes[old])
            node["id"] = rename[old]
            node["dependencies"] = sorted(rename[p] for p in node["dependencies"])
            node["output_field"] = node["id"] + "__payload"
            ordered.append(node)
        nodes = [n["id"] for n in ordered]
        edges = [(p, n["id"]) for n in ordered for p in n["dependencies"]]
        metrics = graph_metrics(nodes, edges)
        metrics["operator_kind_counts"] = dict(sorted(Counter(n["kind"] for n in ordered).items()))
        metrics["operator_counts"] = dict(sorted(Counter(n["operator"] for n in ordered).items()))
        metrics["branched"] = metrics["maximum_antichain_width"] > 1
        used = {n["operator"] for n in ordered}
        template = {"template_id": template_id, "scenario": self.scenario, "version": VERSION,
            "nodes": ordered, "edges": [list(e) for e in edges],
            "operator_contracts": {op: OPERATORS[op] for op in sorted(used)},
            "cache_regions": [], "metrics": metrics,
            "generation": {"seed": seed, "scenario_rng_seed": seed + (0 if self.scenario == "qa" else 1000003),
                "candidate_number": candidate_number, "size_stratum": bucket,
                "node_budget": len(nodes), "rules": self.history, "history_node_ids": "construction-time IDs; final IDs are renumbered",
                "limits": LIMITS, "topology_bucket_key": graph_signature(nodes, edges),
                "deduplication": "Exact directed isomorphism ignoring IDs, operator names and parameters; signature is only a bucket key."},
            "provenance": {"category": "constrained_generated_derivative",
                "base_method": "basic_multihop" if self.scenario == "qa" else "din_sql",
                "stage_aligned_reference": any(h["rule"] == "stage_aligned_reference" for h in self.history),
                "original_method_reproduction": False, "benchmark_tasks_used_during_generation": False,
                "note": "Shorter variants omit stages; fork/join and bounded refinements extend the cited stage organization."},
            "execution_contract": {"initial_fields": {"question": "string", "resource_version": "string", "model_version": "string"},
                "guards": ["resource_version", "model_version"], "reference_answers_available_to_service_nodes": False,
                "real_operator_bindings": "explicit injection required; not provided by this generator",
                "resource_resolution": "Bindings resolve immutable corpus/schema/database snapshots from resource_version.",
                "scheduler": "current GeoLeapRuntime is serial", "profile_cost_and_latency": "unspecified; compiler defaults zero, never measured costs"},
            "validation": {"structure_validated": True, "fixture_execution": "pending_independent_validation",
                "real_service_execution": "not_run", "task_quality_evaluation": "not_run", "performance_evaluation": "not_run"}}
        # Let the actual core, rather than a separate approximation of its
        # SESE/field rules, decide which candidate cache regions are legal.
        workflow, _ = compile_template(template, fixture=True)
        candidates = {frozenset([n["id"]]) for n in ordered if not n["terminal"]}
        for block in self.blocks:
            candidates.add(frozenset(rename[key] for key in block))
        by_id = {n["id"]: n for n in ordered}
        children = {key: [t for s, t in edges if s == key] for key in nodes}
        for start in nodes:
            path = [start]
            while len(path) < 8 and len(children[path[-1]]) == 1:
                following = children[path[-1]][0]
                if len(by_id[following]["dependencies"]) != 1 or by_id[following]["terminal"]:
                    break
                path.append(following)
                candidates.add(frozenset(path))
        discarded = 0
        for members in sorted(candidates, key=lambda group: (len(group), sorted(group))):
            if any(by_id[key]["terminal"] for key in members):
                continue
            ordered_members = [key for key in nodes if key in members]
            try:
                workflow.subgraph("candidate", ordered_members, ("resource_version", "model_version"))
            except ValueError:
                discarded += 1
                continue
            template["cache_regions"].append({"name": "region_%03d" % (len(template["cache_regions"]) + 1), "nodes": ordered_members})
        template["metrics"]["cache_region_count"] = len(template["cache_regions"])
        template["metrics"]["rejected_cache_region_candidates"] = discarded
        compile_template(template, fixture=True)
        return template


def build_candidate(scenario, target, rng):
    result = Builder(scenario)
    while len(result.nodes) < target:
        choices = result.actions(target - len(result.nodes))
        rng.shuffle(choices)
        # Randomize among valid grammar rewrites, not among node labels.
        accepted = False
        for _, kind, *args in choices[:150]:
            trial = deepcopy(result)
            try:
                if kind == "insert":
                    trial.insert(*args)
                else:
                    trial.fork(*args)
            except ValueError:
                continue
            if trial.valid():
                result, accepted = trial, True
                break
        if not accepted:
            return None
    return result


def scenario_catalog(scenario, seed):
    rng = random.Random(seed + (0 if scenario == "qa" else 1000003))
    seen = defaultdict(list)
    accepted = []
    stats = Counter()
    # Small sizes have few possible nonisomorphic graphs. This schedule
    # intentionally allocates most small-graph diversity to 8--10 nodes.
    small = [3] + [4] * 2 + [5] * 4 + [6] * 7 + [7] * 10 + [8] * 14 + [9] * 16 + [10] * 16
    for bucket, lower, upper, count in QUOTAS:
        for local_index in range(count):
            desired = small[local_index] if bucket == "small" else (50 if bucket == "extra_large" and local_index == 2 else rng.randint(lower, upper))
            for attempt in range(6000):
                # If a small target exhausts useful topologies, move within
                # the frozen stratum; this rule never consults task outcomes.
                target = desired if attempt < 350 else rng.randint(max(lower, 7), upper)
                stats["candidates_attempted"] += 1
                anchor = bucket == "small" and attempt == 0 and (
                    (scenario == "qa" and local_index == 3) or (scenario == "sql" and local_index == 1))
                if anchor:
                    built = Builder(scenario)
                    if scenario == "qa":
                        built.insert("n002", "n003", ["qa.expand_query", "qa.retrieve"])
                    else:
                        built.insert("n001", "n002", ["sql.classify"])
                        built.nodes["n004"]["parameters"] = {"role": "full_query_classification"}
                    built.history.append({"rule": "stage_aligned_reference",
                        "note": "Reference stage topology only; not original prompts or method reproduction."})
                else:
                    built = build_candidate(scenario, target, rng)
                if built is None:
                    stats["grammar_dead_ends"] += 1
                    continue
                nodes, edges = list(built.nodes), built.edges()
                signature = graph_signature(nodes, edges)
                if any(is_isomorphic(nodes, edges, old_nodes, old_edges) for old_nodes, old_edges in seen[signature]):
                    stats["isomorphic_duplicates_rejected"] += 1
                    continue
                template = built.finish("%s_%03d" % (scenario, len(accepted) + 1), seed,
                                        stats["candidates_attempted"], bucket)
                seen[signature].append((nodes, edges))
                accepted.append(template)
                break
            else:
                raise RuntimeError(f"Cannot fill {scenario}/{bucket}; no partial catalog will be installed")
    stats["templates_accepted"] = len(accepted)
    return accepted, dict(stats)


def describe(templates):
    counts = [len(t["nodes"]) for t in templates]
    widths = [t["metrics"]["maximum_antichain_width"] for t in templates]
    return {"template_count": len(templates), "exact_nonisomorphic_topologies": len(templates),
        "nodes": {"min": min(counts), "median": statistics.median(counts), "mean": statistics.mean(counts), "max": max(counts),
                  "histogram": dict(sorted(Counter(counts).items()))},
        "size_strata": dict(Counter(t["generation"]["size_stratum"] for t in templates)),
        "branched_template_count": sum(t["metrics"]["branched"] for t in templates),
        "maximum_antichain_width": {"min": min(widths), "max": max(widths)},
        "total_declared_nodes": sum(counts), "cache_region_count": sum(len(t["cache_regions"]) for t in templates)}


def documentation(summary):
    qa, sql = summary["scenarios"]["qa"], summary["scenarios"]["sql"]
    paragraph = (
        "我们为多跳检索增强问答与文本到SQL两个场景分别构建100个派生工作流模板。"
        "生成器以BasicMultiHop和DIN-SQL的阶段组织为基础，采用带输入输出类型约束的图重写语法，"
        "组合阶段插入、互补策略分叉与汇合，以及预先限定轮数的追加检索或执行诊断修订。"
        "每个场景包含70个3–10节点、20个11–19节点、7个20–29节点和3个30–50节点模板，"
        "以小图为主，并覆盖少量较大的组合流程。生成后检查无环性、单一入口与终点、算子接口和全节点终点可达性，"
        "并通过忽略节点名称、算子标签及参数的精确有向图同构检查去重；仅更换提示词或节点名称不计为新模板。"
        "所有模板在任务执行前固定，分支与修订轮数不随运行反馈增加。该集合用于受控的结构多样性评估，"
        "属于对公开方法的组合扩展，而非200个原生公开工作流；其真实任务质量和执行性能另行评估。"
    )
    methods = "# 可用于论文的方法描述\n\n" + paragraph + "\n\n"
    methods += (f"固定种子：{summary['seed']}；QA实际节点范围{qa['nodes']['min']}–{qa['nodes']['max']}，中位数{qa['nodes']['median']}；"
                f"SQL实际范围{sql['nodes']['min']}–{sql['nodes']['max']}，中位数{sql['nodes']['median']}。\n\n"
                "节点口径为模型、检索、只读数据库及必要本地处理阶段，不计外部输入、输出占位符或独立质量评价。"
                "最大反链宽度仅反映图结构；当前GeoLeap执行器串行调度，不能将它解释为已测并行加速。\n\n"
                "最小QA模板为单轮“检索词生成→检索→回答”，不是原版两轮BasicMultiHop；最小SQL模板省略分类阶段，"
                "不是完整DIN-SQL。执行诊断修订、多策略候选及分叉汇合均为本集合的扩展。"
                "几十节点的长图主要用于结构与规模压力测试，不代表数据集天然提供的执行程序，也未证明比短流程质量更高。"
                "模板数量不是独立任务样本数，也不意味着跨模板缓存自动兼容。\n")
    readme = """# GeoLeap constrained workflow template catalog v1

This is a reproducible **generated derivative workload**, not 200 published
methods, benchmark traces, or evidence of benchmark quality. There are exactly
100 QA and 100 SQL templates, with topology uniqueness checked **within each
scenario**. Across scenarios, identical bare topology is allowed.

## Generation

Typed graph rewrites expand a minimal functional pipeline using real stage
insertions, complementary strategy branches with consumed joins, and bounded
next-hop/execute-repair expansions. No empty nodes, input/output sentinels or
transitive redundant edges are inserted to inflate size or diversity. Branch
roles are declared in node parameters and must be honored by real bindings.
Budgets per scenario: 70 × 3–10, 20 × 11–19, 7 × 20–29, 3 × 30–50 nodes.
The seed, rewrite history, operator contracts, bounds, code and artifact checksums
are frozen in JSON. No benchmark labels or task outcomes guide selection.

`graph_signature` is only an invariant bucket key. Exact directed graph
isomorphism resolves every potential collision, ignoring names, operator
labels and parameters. Templates within a scenario therefore cannot differ
only in prompts, strategies, IDs or random seeds.

## Files and reproduction

- `qa/` and `sql/`: standalone DAG declarations with ports, parameters,
  provenance, topology metrics and core-validated cache regions.
- `manifest.json`: frozen generation settings and SHA-256 of all 200 files.
- `operators.json`: stage contracts and method references.
- `summary.json`: actual size/width counts and generation rejection counts.
- `METHODS.zh.md`: paper-ready description and scope.
- `EXAMPLES.md`: small reference and representative fork/join diagrams.
- `validation.json`: independent structural/core fixture checks, created by
  the validation command below; reports are generated locally and are not
  included in the published catalog.

From the repository root:

```sh
python3 scripts/generate_workflow_catalog.py --output /tmp/geoleap-catalog-reproduction
python3 checks/check_workflow_catalog.py --catalog /tmp/geoleap-catalog-reproduction --output /tmp/geoleap-catalog-reproduction/validation.json
```

Generation refuses to overwrite an existing folder. The files have no clock
timestamps, so the same seed and code reproduce byte-identical declarations.

## Execution boundary

The graph compiler is `scripts/workflow_catalog_runtime.py`. In real mode,
`compile_template(template, bindings=...)` requires an explicit callable for
**every used operator** and fails before execution if any is absent. A callable
receives `(node_declaration, declared_input_values)` and returns
`{"artifact_type": node["output_type"], "data": {...}}`, satisfying
`required_data`. Bindings must implement the declared branch role, consume all
parents, preserve necessary context/provenance, use the **current** question,
resolve versioned resources, and enforce read-only tools and finite JSON.
Question, resource version and model version are current initial inputs to
every node; parent payload fields have one writer each. Reference SQL, gold
answers and benchmark quality checks stay outside the service DAG.

QA query-plan artifacts carry `documents` as well as `queries`: the initial
list is empty, next-hop expansion preserves earlier evidence, and retrieval
merges new passages with the carried set. Answer artifacts retain supporting
documents for candidate selection. SQL `schema_context` must retain the
immutable base schema, keys and annotations; pruning records a selected view
rather than deleting schema information needed by other branches. Required
field types are checked; faithful evidence/schema preservation must also be
tested when real bindings are attached.

`fixture_execute(template)` explicitly uses type-only synthetic lineage
artifacts, core `GeoLeapRuntime`, no-cache mode, no feedback and no record
collection. It verifies every node executes once and every branch reaches the
terminal payload. It performs **zero model, retrieval or database calls** and
does not generate answers or SQL. Fixture success is neither task quality nor
real service end-to-end validation. Real benchmark execution, quality,
latency, cost and concurrency measurements remain **not_run**.

The current runtime is serial even for fork/join DAGs. Zero latency/cost
defaults in the compiler mean *unprofiled*, not free real services. Actual
experiments must inject measured profiles/accounting before comparing plans.
Cache identity remains scoped to template ID and version; this catalog does
not enable cross-template cache equivalence. Resource/model versions are
critical guards. Valid single-entry/single-exit regions are checked by the
actual core; a whole fork with multiple boundary entries is not automatically
cacheable.

## Provenance and scope

QA stage organization is inspired by [DSPy §7 BasicMultiHop](https://arxiv.org/html/2310.03714#S7)
and SQL by [DIN-SQL](https://arxiv.org/abs/2304.11015), with pinned author
code references in `operators.json`. These sources do not supply this catalog.
The 3-node QA case uses one retrieval hop; the 3-node SQL case omits
classification. Fork/join, candidate selection and execution-guided repairs
are our declared adaptations. HotpotQA/BIRD task attachment and resource
snapshots are a separate future experimental step; no benchmark records are
bundled or silently substituted here.

`qa_004` preserves the two-hop BasicMultiHop **stage topology** and `sql_002`
the four-stage DIN-SQL topology as reference anchors within the 100. They do
not reproduce the authors' original prompts, retrievers or optimized programs.
The few long DAGs are structure/scale stress templates; there is no evidence
yet that extra analysis branches improve answer or SQL quality. Total retrieval
stages across branches differ from sequential hop depth (bounded at four).

The published catalog contains generated DAG declarations and generation
metadata. Benchmark records, service execution logs, credentials and local
validation reports are excluded.
"""
    return readme, methods


def example_diagrams(templates):
    text = ["# Representative generated DAGs", "",
            "Actual declarations from this catalog; diagrams indicate data dependencies, not concurrent execution.", ""]
    for scenario, selected in templates.items():
        # Reference anchor, a branching small DAG, and one larger example.
        anchor = next(t for t in selected if t["provenance"]["stage_aligned_reference"])
        fork = next(t for t in selected if t["metrics"]["branched"] and len(t["nodes"]) >= 8)
        larger = next(t for t in selected if t["generation"]["size_stratum"] == "large")
        for template in (anchor, fork, larger):
            m = template["metrics"]
            text.extend([f"## {template['template_id']}: {m['node_count']} nodes / {m['edge_count']} edges", "", "```mermaid", "flowchart TD"])
            for n in template["nodes"]:
                text.append(f'  {n["id"]}["{n["id"]}: {n["operator"]}<br/>{n["parameters"].get("role", "")}"]')
            for s, t in template["edges"]:
                text.append(f"  {s} --> {t}")
            text.extend(["```", ""])
    return "\n".join(text)


def generate(output, seed):
    if output.exists():
        raise FileExistsError(f"Refusing to overwrite existing output: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".workflow-catalog-", dir=output.parent))
    try:
        entries, summaries, all_templates = [], {}, {}
        for scenario in ("qa", "sql"):
            templates, stats = scenario_catalog(scenario, seed)
            all_templates[scenario] = templates
            summaries[scenario] = dict(describe(templates), generation_statistics=stats)
            (staging / scenario).mkdir()
            for template in templates:
                relative = f"{scenario}/{template['template_id']}.json"
                path = staging / relative
                write_json(path, template)
                entries.append({"path": relative, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                                "template_id": template["template_id"], "scenario": scenario,
                                "node_count": len(template["nodes"])})
            print(f"{scenario}: {len(templates)} templates; nodes {summaries[scenario]['nodes']}; stats {stats}", flush=True)
        summary = {"version": VERSION, "seed": seed, "scenarios": summaries,
                   "real_service_execution": "not_run", "quality_evaluation": "not_run", "performance_evaluation": "not_run"}
        code_files = ("scripts/generate_workflow_catalog.py", "scripts/workflow_catalog_runtime.py", "scripts/workflow_catalog_topology.py",
                      "checks/check_workflow_catalog.py", "checks/check_workflow_catalog_topology.py")
        code_hashes = {path: hashlib.sha256((REPO_ROOT / path).read_bytes()).hexdigest() for path in code_files}
        manifest = {"version": VERSION, "seed": seed, "scenario_count": 2, "template_count": 200,
            "code_sha256": code_hashes,
            "size_quotas_per_scenario": [{"stratum": k, "min": lo, "max": hi, "count": n} for k, lo, hi, n in QUOTAS],
            "limits": LIMITS, "generation_selection": "Seeded grammar sampling, typed validity and exact topology uniqueness only; no outcome-based selection.",
            "small_size_schedule": {"3": 1, "4": 2, "5": 4, "6": 7, "7": 10, "8": 14, "9": 16, "10": 16},
            "fallback_rule": "After 350 unsuccessful attempts for a requested size, sample uniformly within the frozen stratum (small: 7–10); fail after 6000 attempts.",
            "templates": entries, "sources": SOURCES,
            "validation_command": "python3 checks/check_workflow_catalog.py --catalog datasets/workflow_catalog_v1 --output datasets/workflow_catalog_v1/validation.json"}
        write_json(staging / "manifest.json", manifest)
        write_json(staging / "summary.json", summary)
        write_json(staging / "operators.json", {"version": VERSION, "sources": SOURCES, "operators": OPERATORS,
            "branch_roles": ROLES, "limits": LIMITS})
        readme, methods = documentation(summary)
        (staging / "README.md").write_text(readme, encoding="utf-8")
        (staging / "METHODS.zh.md").write_text(methods, encoding="utf-8")
        (staging / "EXAMPLES.md").write_text(example_diagrams(all_templates), encoding="utf-8")
        staging.rename(output)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("datasets/workflow_catalog_v1"))
    parser.add_argument("--seed", type=int, default=SEED)
    args = parser.parse_args()
    generate(args.output, args.seed)


if __name__ == "__main__":
    main()
