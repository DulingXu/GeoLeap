"""Compile catalog DAGs into GeoLeap, with an explicitly synthetic dataflow check.

``fixture_execute`` checks declarations and state propagation, never benchmark
quality, model responses, retrieval, SQL execution, or parallel speedup. Real
execution requires one caller-provided binding per operator; there is no
implicit mock, network client, credential loading, or fallback execution path.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Mapping
import hashlib
import json
from pathlib import Path
import sys
from typing import Any


_SRC = str(Path(__file__).resolve().parents[1] / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from workflow.planner import PlannerConfig  # noqa: E402
from workflow.runtime import GeoLeapRuntime  # noqa: E402
from workflow.spec import OperatorSpec, WorkflowSpec, canonical_json  # noqa: E402


GUARD_FIELDS = ("resource_version", "model_version")
INITIAL_FIELDS = ("question",) + GUARD_FIELDS
_TAG_ALIASES = {
    "str": "string", "int": "integer", "float": "number",
    "bool": "boolean", "dict": "object", "list": "array",
}
_TAGS = {"string", "integer", "number", "boolean", "object", "array", "null", "json"}
Binding = Callable[[Mapping[str, Any], Mapping[str, Any]], Mapping[str, Any]]


class MissingOperatorBindings(ValueError):
    """A real workflow cannot be compiled without all required operators."""


class OperatorContractError(ValueError):
    """A template or binding violates a declared artifact contract."""


def _json_copy(value: Any) -> Any:
    return json.loads(canonical_json(value))


def _tag_matches(value: Any, tag: str) -> bool:
    tag = _TAG_ALIASES.get(tag, tag)
    if tag not in _TAGS:
        raise OperatorContractError(f"Unsupported required_data type: {tag!r}")
    return {
        "string": isinstance(value, str),
        "integer": type(value) is int,
        "number": type(value) in (int, float),
        "boolean": type(value) is bool,
        "object": isinstance(value, dict),
        "array": isinstance(value, list),
        "null": value is None,
        "json": True,
    }[tag]


def _validate_template(template: Mapping[str, Any]) -> tuple[list[dict], dict[str, dict]]:
    if not isinstance(template, Mapping):
        raise OperatorContractError("Template must be a JSON object")
    canonical_json(template)
    for field in ("template_id", "version"):
        if not isinstance(template.get(field), str) or not template[field]:
            raise OperatorContractError(f"Template requires nonempty {field}")
    if template.get("scenario") not in ("qa", "sql"):
        raise OperatorContractError("Template scenario must be qa or sql")
    nodes = template.get("nodes")
    contracts = template.get("operator_contracts")
    if not isinstance(nodes, list) or not nodes or not isinstance(contracts, Mapping):
        raise OperatorContractError("Template requires nodes and operator_contracts")
    nodes, contracts = _json_copy(nodes), _json_copy(contracts)
    node_ids = [node.get("id") for node in nodes if isinstance(node, dict)]
    if (len(node_ids) != len(nodes) or any(not isinstance(i, str) or not i for i in node_ids)
            or len(set(node_ids)) != len(node_ids)):
        raise OperatorContractError("Nodes require nonempty, unique string ids")
    by_id = {node["id"]: node for node in nodes}
    for node in nodes:
        key = node["id"]
        op = node.get("operator")
        if not isinstance(op, str) or op not in contracts:
            raise OperatorContractError(f"{key}: operator contract is missing")
        contract = contracts[op]
        parents = node.get("dependencies")
        if (not isinstance(parents, list) or any(not isinstance(p, str) for p in parents)
                or len(parents) != len(set(parents)) or any(p not in by_id for p in parents)):
            raise OperatorContractError(f"{key}: invalid or unknown dependencies")
        if not isinstance(contract, dict):
            raise OperatorContractError(f"{key}: contract must be an object")
        if node.get("kind") not in ("LLM", "Tool", "Compute") or node["kind"] != contract.get("kind"):
            raise OperatorContractError(f"{key}: kind does not match operator contract")
        if not isinstance(node.get("output_type"), str) or node["output_type"] != contract.get("output_type"):
            raise OperatorContractError(f"{key}: output_type does not match operator contract")
        if not isinstance(node.get("parameters"), dict):
            raise OperatorContractError(f"{key}: parameters must be an object")
        if not isinstance(node.get("contract_version"), str) or not node["contract_version"]:
            raise OperatorContractError(f"{key}: contract_version is required")
        if type(node.get("terminal")) is not bool:
            raise OperatorContractError(f"{key}: terminal must be boolean")
        lower, upper = contract.get("min_inputs"), contract.get("max_inputs")
        if (type(lower) is not int or type(upper) is not int
                or not 0 <= lower <= len(parents) <= upper):
            raise OperatorContractError(f"{key}: parent count violates operator contract")
        allowed = contract.get("input_types")
        if not isinstance(allowed, list) or any(not isinstance(t, str) for t in allowed):
            raise OperatorContractError(f"{key}: input_types must be a list of artifact types")
        if any(by_id[parent].get("output_type") not in allowed for parent in parents):
            raise OperatorContractError(f"{key}: parent artifact type is not accepted")
        required = contract.get("required_data", {})
        if not isinstance(required, dict) or any(
            not isinstance(tag, str) or _TAG_ALIASES.get(tag, tag) not in _TAGS
            for tag in required.values()
        ):
            raise OperatorContractError(f"{key}: unsupported required_data schema")
    sinks = set(by_id) - {parent for node in nodes for parent in node["dependencies"]}
    terminals = {node["id"] for node in nodes if node["terminal"]}
    if len(sinks) != 1 or terminals != sinks:
        raise OperatorContractError("Template requires exactly one terminal, equal to its unique sink")
    return nodes, contracts


def _validate_payload(payload: Any, node: Mapping[str, Any], contract: Mapping[str, Any]) -> dict:
    key = node["id"]
    if not isinstance(payload, Mapping):
        raise OperatorContractError(f"{key}: binding must return an artifact object")
    payload = _json_copy(payload)
    if payload.get("fixture") is not None and payload["fixture"] is not False:
        raise OperatorContractError(f"{key}: fixture artifacts are forbidden in real mode")
    if payload.get("artifact_type") != node["output_type"]:
        raise OperatorContractError(f"{key}: binding returned the wrong artifact_type")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise OperatorContractError(f"{key}: real artifacts require a data object")
    for field, tag in contract.get("required_data", {}).items():
        if field not in data or not _tag_matches(data[field], tag):
            raise OperatorContractError(f"{key}: data.{field} must match {tag}")
    return payload


def _compile_template(
    template: Mapping[str, Any], bindings: Mapping[str, Binding] | None, *,
    fixture: bool, execution_counts: Counter | None = None,
):
    nodes, contracts = _validate_template(template)
    if type(fixture) is not bool:
        raise TypeError("fixture must be boolean")
    if fixture:
        if bindings:
            raise ValueError("Do not supply real operator bindings in fixture mode")
        selected_bindings: Mapping[str, Binding] = {}
    else:
        if bindings is not None and not isinstance(bindings, Mapping):
            raise TypeError("bindings must map operator names to callables")
        selected_bindings = dict(bindings or {})
        missing = sorted({node["operator"] for node in nodes
                          if not callable(selected_bindings.get(node["operator"]))})
        if missing:
            raise MissingOperatorBindings(
                "Real execution requires explicit callable bindings for: " + ", ".join(missing)
                + ". No fixture fallback is available."
            )
    operators = []
    for node in nodes:
        contract = contracts[node["operator"]]
        parents = tuple(node["dependencies"])
        payload_key = node["id"] + "__payload"
        reads = INITIAL_FIELDS + tuple(parent + "__payload" for parent in parents)

        def execute(values, *, _node=node, _contract=contract, _key=payload_key):
            if execution_counts is not None:
                execution_counts[_node["id"]] += 1
            if fixture:
                visited = set()
                for parent in _node["dependencies"]:
                    payload = values[parent + "__payload"]
                    if payload.get("fixture") is not True:
                        raise OperatorContractError("Fixture execution received a real artifact")
                    visited.update(payload["visited_nodes"])
                visited.add(_node["id"])
                payload = {"artifact_type": _node["output_type"], "fixture": True,
                           "visited_nodes": sorted(visited)}
            else:
                # Detach arguments so a binding cannot mutate declarations or
                # earlier state; each call only receives its declared inputs.
                payload = selected_bindings[_node["operator"]](_json_copy(_node), _json_copy(values))
                payload = _validate_payload(payload, _node, _contract)
            return {_key: payload}

        semantic_hash = hashlib.sha256(canonical_json({
            "operator": node["operator"],
            "contract_version": node["contract_version"],
            "parameters": node["parameters"],
            # Fixture artifacts must never share a cache identity with real
            # operator results, even if a caller enables caching manually.
            "execution_mode": "fixture" if fixture else "real",
        }).encode("utf-8")).hexdigest()
        semantic_version = node["contract_version"] + ":" + semantic_hash
        operators.append(OperatorSpec(
            node_id=node["id"], function=execute, dependencies=parents,
            read_fields=reads, write_fields=(payload_key,), semantic_version=semantic_version,
            kind=node["kind"], cacheable=not node["terminal"],
            stable=node["kind"] == "Compute", effect_free=True,
            latency_ms=0.0, cost_usd=0.0,
            input_schema=tuple((field, "string" if field in INITIAL_FIELDS else "object")
                               for field in reads),
            output_schema=((payload_key, "object"),),
        ))
    workflow = WorkflowSpec(template["template_id"], operators, version=template["version"])
    regions = template.get("cache_regions", [])
    if not isinstance(regions, list):
        raise OperatorContractError("cache_regions must be a list")
    subgraphs = []
    names, identities = set(), set()
    for region in regions:
        if not isinstance(region, Mapping) or not isinstance(region.get("name"), str) or not region["name"]:
            raise OperatorContractError("Each cache region requires a name")
        if region["name"] in names:
            raise OperatorContractError("Cache region names must be unique")
        selected = region.get("nodes")
        if (not isinstance(selected, list) or any(not isinstance(n, str) for n in selected)
                or len(selected) != len(set(selected))):
            raise OperatorContractError(f"{region['name']}: invalid cache region nodes")
        # Core checks connectivity, entry/exit, effect safety and membership.
        first = workflow.subgraph(region["name"], selected, GUARD_FIELDS)
        soft = tuple(field for field in first.input_fields if field not in GUARD_FIELDS)
        spec = workflow.subgraph(region["name"], selected, GUARD_FIELDS, similarity_fields=soft)
        if spec.identity in identities:
            raise OperatorContractError("Cache regions must refer to distinct subgraphs")
        subgraphs.append(spec)
        names.add(region["name"])
        identities.add(spec.identity)
    return workflow, tuple(subgraphs)


def compile_template(
    template: Mapping[str, Any], bindings: Mapping[str, Binding] | None = None, *, fixture: bool = False,
):
    """Return ``(WorkflowSpec, subgraphs)``; real mode needs all bindings.

    A binding receives ``(node_declaration, declared_input_values)`` and returns
    ``{"artifact_type": ..., "data": {...}}``. Binding functions own resource
    resolution, real services, side-effect isolation and measured accounting.
    Callers must update model_version/resource_version, contract_version and
    parameters whenever the corresponding binding semantics change; Python
    callable objects are not an immutable implementation fingerprint.
    The zero profile values here are unspecified, not measured service costs.
    """
    return _compile_template(template, bindings, fixture=fixture)


def fixture_execute(template: Mapping[str, Any]) -> dict[str, Any]:
    """Run every node once through the core using type-only fixture artifacts."""
    counts: Counter = Counter()
    workflow, subgraphs = _compile_template(template, None, fixture=True, execution_counts=counts)
    runtime = GeoLeapRuntime(workflow, subgraphs, planner_config=PlannerConfig(mode="no_cache"))
    result = runtime.run(
        {"question": "Fixture dataflow validation only",
         "resource_version": "fixture-resource-v1", "model_version": "fixture-model-v1"},
        request_id="fixture:" + template["template_id"],
        use_cache=False, collect_records=False, collect_feedback=False,
    )
    expected = set(workflow.nodes)
    if set(counts) != expected or any(counts[key] != 1 for key in expected):
        raise AssertionError("Every declared node must actually execute exactly once")
    if set(result.executed_node_ids) != expected or len(result.executed_node_ids) != len(expected):
        raise AssertionError("Runtime execution trace differs from declared nodes")
    if result.reused_subgraphs or result.new_record_ids:
        raise AssertionError("Fixture validation must not restore or collect cache records")
    for node in template["nodes"]:
        payload = result.state.get(node["id"] + "__payload")
        if not isinstance(payload, dict) or payload.get("artifact_type") != node["output_type"]:
            raise AssertionError("An operator output field is missing or has the wrong type")
    terminal = next(node for node in template["nodes"] if node["terminal"])
    final_payload = result.state[terminal["id"] + "__payload"]
    if set(final_payload["visited_nodes"]) != expected:
        raise AssertionError("Terminal artifact does not depend on the complete DAG")
    return {
        "template_id": template["template_id"], "scenario": template["scenario"],
        "passed": True, "fixture_only": True, "runtime_compiled": True,
        "node_count": len(expected),
        "edge_count": sum(len(node["dependencies"]) for node in template["nodes"]),
        "executed_node_count": len(result.executed_node_ids),
        "terminal_visited_node_count": len(final_payload["visited_nodes"]),
        "cache_region_count": len(subgraphs), "real_model_calls": 0,
        "quality_evaluated": False, "parallel_execution": False,
        "real_service_execution": "not_run", "performance_evaluation": "not_run",
    }
