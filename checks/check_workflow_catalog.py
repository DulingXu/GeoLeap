"""Validate the generated DAG catalog and run explicit, zero-API fixtures.

This checks artifact integrity, topology uniqueness, typed declarations and
execution through the real GeoLeap core.  Fixture execution is a dataflow
check; it is not a benchmark-quality or service-performance measurement.

Usage: python checks/check_workflow_catalog.py [catalog_dir] [--output PATH]
       python checks/check_workflow_catalog.py --catalog DIR [--output PATH]
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path, PurePosixPath
import sys
import time
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.workflow_catalog_topology import (  # noqa: E402
    graph_metrics, graph_signature, is_isomorphic,
)
from scripts.workflow_catalog_runtime import (  # noqa: E402
    MissingOperatorBindings, compile_template, fixture_execute,
)


SCENARIOS = ("qa", "sql")
EXPECTED_BANDS = {"3-10": 70, "11-19": 20, "20-29": 7, "30-50": 3}
NODE_FIELDS = {
    "id", "operator", "dependencies", "output_type", "parameters",
    "kind", "terminal", "contract_version",
}
CODE_FILES = (
    "scripts/generate_workflow_catalog.py",
    "scripts/workflow_catalog_runtime.py",
    "scripts/workflow_catalog_topology.py",
    "checks/check_workflow_catalog.py",
    "checks/check_workflow_catalog_topology.py",
)


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def _band(count: int) -> str:
    for lower, upper in ((3, 10), (11, 19), (20, 29), (30, 50)):
        if lower <= count <= upper:
            return f"{lower}-{upper}"
    raise ValueError(f"Node count {count} is outside 3-50")


def _graph(template: dict[str, Any]) -> tuple[list[str], list[tuple[str, str]]]:
    nodes = template.get("nodes")
    _require(isinstance(nodes, list) and bool(nodes), "nodes must be a nonempty list")
    ids, edges = [], []
    for node in nodes:
        _require(isinstance(node, dict), "Each node must be an object")
        missing = NODE_FIELDS - node.keys()
        _require(not missing, f"Node fields missing: {sorted(missing)}")
        node_id = node["id"]
        _require(isinstance(node_id, str) and bool(node_id), "Node IDs must be nonempty strings")
        parents = node["dependencies"]
        _require(isinstance(parents, list) and all(isinstance(p, str) for p in parents),
                 f"{node_id}: dependencies must be a list of IDs")
        ids.append(node_id)
        edges.extend((parent, node_id) for parent in parents)
    return ids, edges


def _reachable(start: str, children: dict[str, list[str]],
               *, skip: tuple[str, str] | None = None) -> set[str]:
    seen, pending = {start}, [start]
    while pending:
        current = pending.pop()
        for child in children[current]:
            if (current, child) != skip and child not in seen:
                seen.add(child)
                pending.append(child)
    return seen


def _check_declarations(template: dict[str, Any], metrics: dict[str, Any],
                        ids: list[str], edges: list[tuple[str, str]]) -> dict[str, int]:
    """Check semantic ports separately from the compiler's own validation."""
    _require(metrics["root_count"] == metrics["sink_count"] == 1,
             "Exactly one root and one sink are required")
    _require(3 <= len(ids) <= 50, "Each DAG must contain 3-50 actual operators")
    _require(metrics["maximum_antichain_width"] <= 8, "Maximum antichain width exceeds 8")
    _require(metrics["max_out_degree"] <= 4, "Branch fanout exceeds 4")
    declared_edges = template.get("edges")
    _require(isinstance(declared_edges, list) and all(
        isinstance(edge, list) and len(edge) == 2 and all(isinstance(n, str) for n in edge)
        for edge in declared_edges), "edges must be a list of [source, target] ID pairs")
    _require(Counter(map(tuple, declared_edges)) == Counter(edges),
             "edges differs from the data dependencies declared on nodes")
    children: dict[str, list[str]] = {key: [] for key in ids}
    reverse: dict[str, list[str]] = {key: [] for key in ids}
    for source, target in edges:
        children[source].append(target)
        reverse[target].append(source)
    _require(_reachable(metrics["roots"][0], children) == set(ids),
             "Every operator must be reachable from the root")
    _require(_reachable(metrics["sinks"][0], reverse) == set(ids),
             "Every operator must contribute a path to the final output")
    for source, target in edges:
        _require(target not in _reachable(source, children, skip=(source, target)),
                 f"Transitive redundant edge: {source} -> {target}")

    contracts = template.get("operator_contracts")
    _require(isinstance(contracts, dict), "operator_contracts must be an object")
    by_id = {node["id"]: node for node in template["nodes"]}
    terminal_ids = []
    semantic_siblings = {}
    for node in template["nodes"]:
        key, operator = node["id"], node["operator"]
        _require(isinstance(operator, str) and operator in contracts,
                 f"{key}: missing operator contract")
        contract = contracts[operator]
        _require(isinstance(contract, dict), f"{key}: contract must be an object")
        _require(isinstance(node["parameters"], dict), f"{key}: parameters must be an object")
        sibling_key = (
            operator, tuple(sorted(node["dependencies"])),
            json.dumps(node["parameters"], sort_keys=True, ensure_ascii=False,
                       separators=(",", ":"), allow_nan=False),
        )
        _require(sibling_key not in semantic_siblings,
                 f"Semantically duplicate siblings: {semantic_siblings.get(sibling_key)} and {key} "
                 "have identical operators, parent sets and parameters")
        semantic_siblings[sibling_key] = key
        _require(isinstance(node["contract_version"], str) and bool(node["contract_version"]),
                 f"{key}: contract_version must be a nonempty string")
        _require(type(node["terminal"]) is bool, f"{key}: terminal must be boolean")
        if node["terminal"]:
            terminal_ids.append(key)
        _require(node["kind"] in ("LLM", "Tool", "Compute")
                 and node["kind"] == contract.get("kind"), f"{key}: invalid operator kind")
        _require(isinstance(node["output_type"], str) and bool(node["output_type"])
                 and node["output_type"] == contract.get("output_type"),
                 f"{key}: output artifact type differs from its contract")
        lower, upper = contract.get("min_inputs"), contract.get("max_inputs")
        _require(type(lower) is int and type(upper) is int
                 and 0 <= lower <= len(node["dependencies"]) <= upper,
                 f"{key}: parent arity violates its contract")
        accepted = contract.get("input_types")
        _require(isinstance(accepted, list) and all(isinstance(t, str) for t in accepted),
                 f"{key}: input_types must be a list of artifact types")
        for parent in node["dependencies"]:
            _require(by_id[parent]["output_type"] in accepted,
                     f"{key}: does not accept {parent}'s artifact type")
    _require(terminal_ids == metrics["sinks"], "Only the unique sink may be terminal")
    _require(isinstance(template.get("cache_regions"), list), "cache_regions must be a list")
    declared = template.get("metrics")
    _require(isinstance(declared, dict), "metrics must be an object")
    for key, value in metrics.items():
        _require(key in declared and declared[key] == value,
                 f"metrics.{key} does not match the recomputed graph")
    business_metrics = {
        "operator_kind_counts": dict(Counter(node["kind"] for node in template["nodes"])),
        "operator_counts": dict(Counter(node["operator"] for node in template["nodes"])),
        "cache_region_count": len(template["cache_regions"]),
        "branched": metrics["maximum_antichain_width"] > 1,
    }
    for key, value in business_metrics.items():
        if key in declared:
            _require(declared[key] == value, f"metrics.{key} does not match the declaration")
    maxima = {}
    for operator, bound in (("qa.retrieve", 4), ("sql.repair", 2)):
        counts = {}
        for node_id in metrics["topological_order"]:
            node = by_id[node_id]
            counts[node_id] = max((counts[p] for p in node["dependencies"]), default=0)
            counts[node_id] += node["operator"] == operator
        maxima[operator] = max(counts.values(), default=0)
        _require(maxima[operator] <= bound,
                 f"A root-to-sink path has {maxima[operator]} occurrences of {operator}; maximum is {bound}")
    return maxima


def _duplicate_of(ids: list[str], edges: list[tuple[str, str]],
                  buckets: dict[str, list[tuple[str, list[str], list[tuple[str, str]]]]]) -> str | None:
    for template_id, other_ids, other_edges in buckets.get(graph_signature(ids, edges), []):
        if is_isomorphic(ids, edges, other_ids, other_edges):
            return template_id
    return None


def _negative_checks(template: dict[str, Any]) -> list[dict[str, Any]]:
    """Exercise rejection paths using a genuine, otherwise-valid declaration."""
    ids, edges = _graph(template)
    results = []

    def check(name, callback):
        try:
            detail = callback()
            results.append({"name": name, "passed": True, "detail": detail})
        except Exception as error:
            results.append({"name": name, "passed": False,
                            "error": f"{type(error).__name__}: {error}"})

    def renamed_duplicate():
        renamed = {key: f"renamed_{number}" for number, key in enumerate(reversed(ids))}
        renamed_ids = [renamed[key] for key in reversed(ids)]
        renamed_edges = [(renamed[a], renamed[b]) for a, b in reversed(edges)]
        buckets = {graph_signature(ids, edges): [(template["template_id"], ids, edges)]}
        _require(_duplicate_of(renamed_ids, renamed_edges, buckets) == template["template_id"],
                 "Renamed topology was incorrectly accepted as new")
        return "Node IDs and declaration order do not create a new topology"

    def cycle_rejected():
        metrics = graph_metrics(ids, edges)
        try:
            graph_metrics(ids, edges + [(metrics["sinks"][0], metrics["roots"][0])])
        except ValueError as error:
            _require("cycle" in str(error).lower(), "Cyclic graph failed for an unrelated reason")
            return str(error)
        raise AssertionError("Graph validator accepted a directed cycle")

    def wrong_parent_type_rejected():
        broken = deepcopy(template)
        parent_id = edges[0][0]
        parent = next(node for node in broken["nodes"] if node["id"] == parent_id)
        # Keep the changed producer internally valid so rejection must occur at
        # its consumer port rather than at a node/contract spelling mismatch.
        old_operator = parent["operator"]
        new_operator = old_operator + ".negative_wrong_parent_type"
        contract = deepcopy(broken["operator_contracts"][old_operator])
        invalid_type = "negative_test_incompatible_parent_artifact"
        contract["output_type"] = invalid_type
        broken["operator_contracts"][new_operator] = contract
        parent["operator"] = new_operator
        parent["output_type"] = invalid_type
        try:
            compile_template(broken, fixture=True)
        except ValueError as error:
            _require("parent artifact type" in str(error).lower(),
                     f"Bad parent type rejected for an unrelated reason: {error}")
            return str(error)
        raise AssertionError("Compiler accepted an incompatible parent artifact type")

    def missing_real_bindings_rejected():
        try:
            compile_template(template, bindings={})
        except MissingOperatorBindings as error:
            return str(error)
        raise AssertionError("Real mode compiled without bindings, possibly falling back to fixtures")

    check("renamed_isomorphic_graph_is_duplicate", renamed_duplicate)
    check("directed_cycle_is_rejected", cycle_rejected)
    check("incompatible_parent_output_type_is_rejected", wrong_parent_type_rejected)
    check("real_mode_without_bindings_is_rejected", missing_real_bindings_rejected)
    return results


def _code_integrity(manifest: dict[str, Any]) -> dict[str, Any]:
    hashes = manifest.get("code_sha256")
    result: dict[str, Any] = {"passed": False, "files": [], "errors": []}
    if not isinstance(hashes, dict):
        result["errors"].append("manifest code_sha256 must map repository-relative source paths to SHA-256")
        return result
    if set(hashes) != set(CODE_FILES):
        result["errors"].append("code_sha256 must freeze exactly the generator, runtime, topology, catalog validator and topology regression sources")
    for relative in CODE_FILES:
        item: dict[str, Any] = {"path": relative, "declared_sha256": hashes.get(relative), "passed": False}
        result["files"].append(item)
        try:
            item["actual_sha256"] = hashlib.sha256((REPO_ROOT / relative).read_bytes()).hexdigest()
            _require(item["declared_sha256"] == item["actual_sha256"], "Source SHA-256 mismatch")
            item["passed"] = True
        except Exception as error:
            item["error"] = f"{type(error).__name__}: {error}"
            result["errors"].append(f"{relative}: {item['error']}")
    result["passed"] = not result["errors"]
    return result


def validate_catalog(directory: Path) -> dict[str, Any]:
    """Return a serializable report; malformed entries do not hide later errors."""
    started = time.perf_counter()
    errors: list[str] = []
    report: dict[str, Any] = {
        "schema_version": "workflow-catalog-validation-v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "Structural integrity, typed contracts and synthetic core dataflow only",
        "real_model_calls": 0, "real_service_execution": "not_run",
        "quality_evaluation": "not_run", "performance_evaluation": "not_run",
        "parallel_execution": False,
        "expected_per_scenario": 100, "expected_node_bands_per_scenario": EXPECTED_BANDS,
        "errors": errors, "templates": [], "negative_tests": [], "scenarios": {},
    }
    manifest_path = directory / "manifest.json"
    manifest: dict[str, Any] = {}
    try:
        raw_manifest = manifest_path.read_bytes()
        report["manifest_sha256"] = hashlib.sha256(raw_manifest).hexdigest()
        manifest = json.loads(raw_manifest)
        _require(isinstance(manifest, dict) and isinstance(manifest.get("templates"), list),
                 "manifest.json must contain a templates list")
        entries = manifest["templates"]
    except Exception as error:
        errors.append(f"manifest.json: {type(error).__name__}: {error}")
        manifest = {}
        entries = []
    report["code_integrity"] = _code_integrity(manifest)
    errors.extend(report["code_integrity"]["errors"])

    listed_paths, seen_ids = set(), set()
    entry_counts: Counter = Counter()
    bands: dict[str, Counter] = {scenario: Counter() for scenario in SCENARIOS}
    buckets: dict[str, dict] = {scenario: defaultdict(list) for scenario in SCENARIOS}
    fixture_counts: Counter = Counter()
    executed_nodes: Counter = Counter()
    valid_templates = []
    for index, entry in enumerate(entries):
        item: dict[str, Any] = {"manifest_index": index, "passed": False}
        report["templates"].append(item)
        try:
            _require(isinstance(entry, dict), "Manifest template entry must be an object")
            for key in ("path", "sha256", "scenario", "template_id"):
                _require(isinstance(entry.get(key), str) and bool(entry[key]),
                         f"Manifest entry requires nonempty {key}")
            item.update({key: entry[key] for key in ("path", "scenario", "template_id")})
            scenario, template_id, relative = entry["scenario"], entry["template_id"], entry["path"]
            _require(scenario in SCENARIOS, f"Unknown scenario: {scenario}")
            entry_counts[scenario] += 1
            path = PurePosixPath(relative)
            _require(not path.is_absolute() and ".." not in path.parts
                     and str(path) == relative and "\\" not in relative,
                     "Manifest paths must be normalized, relative POSIX paths")
            _require(path.suffix == ".json" and path.parts[0] == scenario,
                     "Each template must be a JSON file under its scenario directory")
            _require(relative not in listed_paths, f"Duplicate manifest path: {relative}")
            _require(template_id not in seen_ids, f"Duplicate template_id: {template_id}")
            listed_paths.add(relative)
            seen_ids.add(template_id)
            template_path = directory / relative
            _require(template_path.resolve().is_relative_to(directory.resolve()),
                     "Template path resolves outside the catalog directory")
            raw = template_path.read_bytes()
            _require(hashlib.sha256(raw).hexdigest() == entry["sha256"], "File SHA-256 mismatch")
            template = json.loads(raw)
            _require(isinstance(template, dict), "Template JSON must be an object")
            _require(template.get("scenario") == scenario and template.get("template_id") == template_id,
                     "Manifest identity differs from template identity")
            ids, edges = _graph(template)
            metrics = graph_metrics(ids, edges)
            bands[scenario][_band(len(ids))] += 1
            path_maxima = _check_declarations(template, metrics, ids, edges)
            duplicate = _duplicate_of(ids, edges, buckets[scenario])
            _require(duplicate is None, f"Topology is isomorphic to {duplicate}")
            signature = graph_signature(ids, edges)
            buckets[scenario][signature].append((template_id, ids, edges))
            # Compiling validates every declared SESE cache region through the
            # actual core before the complete, uncached execution below.
            workflow, regions = compile_template(template, fixture=True)
            _require(len(workflow.nodes) == len(ids), "Compiler lost or added operators")
            _require(len(regions) == len(template["cache_regions"]), "Compiler lost a cache region")
            fixture = fixture_execute(template)
            _require(fixture.get("passed") is True and fixture.get("fixture_only") is True
                     and fixture.get("runtime_compiled") is True, "Fixture did not report a valid core execution")
            _require(fixture.get("real_model_calls") == 0 and fixture.get("quality_evaluated") is False,
                     "Fixture execution must make no model calls or quality claims")
            for key in ("node_count", "executed_node_count", "terminal_visited_node_count"):
                _require(fixture.get(key) == len(ids), f"Fixture {key} differs from the complete DAG")
            fixture_counts[scenario] += 1
            executed_nodes[scenario] += len(ids)
            valid_templates.append(template)
            item.update({"passed": True, "node_band": _band(len(ids)),
                         "graph_metrics": metrics, "topology_bucket_signature": signature,
                         "operator_path_maxima": path_maxima,
                         "fixture": fixture})
        except Exception as error:
            item["error"] = f"{type(error).__name__}: {error}"
            errors.append(f"entry[{index}] {item.get('path', '')}: {item['error']}")

    actual_paths = set()
    for scenario in SCENARIOS:
        actual_paths.update(path.relative_to(directory).as_posix()
                            for path in (directory / scenario).rglob("*.json"))
    # Also catch misplaced template JSON files; root metadata such as summary,
    # operators and this report are allowed and are not workflow declarations.
    for path in directory.rglob("*.json"):
        relative = path.relative_to(directory).as_posix()
        if relative in actual_paths or relative == "manifest.json":
            continue
        try:
            value = json.loads(path.read_bytes())
        except (OSError, ValueError):
            continue
        if isinstance(value, dict) and {"nodes", "scenario", "template_id"} <= value.keys():
            actual_paths.add(relative)
    missing, unlisted = sorted(listed_paths - actual_paths), sorted(actual_paths - listed_paths)
    report["manifest_closure"] = {"passed": not missing and not unlisted,
                                  "listed_count": len(listed_paths), "actual_template_count": len(actual_paths),
                                  "missing_files": missing, "unlisted_files": unlisted}
    if missing or unlisted:
        errors.append(f"Manifest is not closed: missing={missing}, unlisted={unlisted}")
    for scenario in SCENARIOS:
        actual_bands = {key: bands[scenario][key] for key in EXPECTED_BANDS}
        report["scenarios"][scenario] = {
            "manifest_count": entry_counts[scenario], "node_bands": actual_bands,
            "fixture_passed_count": fixture_counts[scenario],
            "fixture_executed_node_count": executed_nodes[scenario],
            "unique_valid_topology_count": sum(len(items) for items in buckets[scenario].values()),
        }
        if entry_counts[scenario] != 100:
            errors.append(f"{scenario}: expected 100 entries, found {entry_counts[scenario]}")
        if actual_bands != EXPECTED_BANDS:
            errors.append(f"{scenario}: node bands {actual_bands} differ from {EXPECTED_BANDS}")
        if fixture_counts[scenario] != 100:
            errors.append(f"{scenario}: only {fixture_counts[scenario]}/100 core fixtures passed")
    if valid_templates:
        report["negative_tests"] = _negative_checks(valid_templates[0])
        for result in report["negative_tests"]:
            if not result["passed"]:
                errors.append(f"Negative test {result['name']}: {result['error']}")
    else:
        errors.append("No valid template is available for negative tests")
    report["template_count"] = len(entries)
    report["fixture_passed_count"] = sum(fixture_counts.values())
    report["fixture_executed_node_count"] = sum(executed_nodes.values())
    report["validation_elapsed_seconds"] = round(time.perf_counter() - started, 6)
    report["passed"] = not errors
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("catalog_dir", type=Path, nargs="?", help="Catalog directory")
    parser.add_argument("--catalog", type=Path, help="Alias for the positional catalog directory")
    parser.add_argument("--output", type=Path, help="Report path (default: CATALOG/validation.json)")
    args = parser.parse_args(argv)
    if args.catalog is not None and args.catalog_dir is not None:
        if args.catalog.resolve() != args.catalog_dir.resolve():
            parser.error("--catalog and the positional catalog directory disagree")
    directory = args.catalog or args.catalog_dir or REPO_ROOT / "datasets" / "workflow_catalog_v1"
    output = args.output or directory / "validation.json"
    report = validate_catalog(directory)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    status = "PASS" if report["passed"] else "FAIL"
    print(f"{status}: {report['fixture_passed_count']}/{report['template_count']} templates passed core fixtures; "
          f"{report['fixture_executed_node_count']} nodes executed; 0 model calls")
    print(f"Validation report: {output}")
    for error in report["errors"]:
        print(f"  {error}", file=sys.stderr)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
