"""Declared, versioned workflows for the incremental B/C/D execution path.

The first executor uses one worker and JSON state with one writer per field.
Operators receive only their declared inputs and return a declared output patch.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable, Dict, Mapping, Sequence, Tuple


def canonical_json(value: Any) -> str:
    def validate(item):
        if item is None or isinstance(item, (str, bool, int)):
            return
        if isinstance(item, float) and math.isfinite(item):
            return
        if isinstance(item, (list, tuple)):
            for child in item:
                validate(child)
            return
        if isinstance(item, Mapping) and all(isinstance(key, str) for key in item):
            for child in item.values():
                validate(child)
            return
        raise ValueError("Workflow state must contain finite JSON values and string keys")

    validate(value)
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def encode_input(value: Any, dimensions: int = 128) -> Tuple[float, ...]:
    """Stable local character encoder; no service, API key, or Python hash seed."""
    text = " " + canonical_json(value).lower() + " "
    vector = [0.0] * dimensions
    for index in range(max(1, len(text) - 2)):
        gram = text[index:index + 3].encode("utf-8")
        bucket = int.from_bytes(hashlib.sha256(gram).digest()[:4], "big") % dimensions
        vector[bucket] += 1
    norm = math.sqrt(sum(item * item for item in vector)) or 1
    return tuple(item / norm for item in vector)


def cosine(left: Sequence[float], right: Sequence[float]) -> float:
    if len(left) != len(right):
        return 0.0
    return max(-1.0, min(1.0, sum(a * b for a, b in zip(left, right))))


_SCHEMA_TYPES = {"json", "string", "integer", "number", "boolean", "object", "array", "null"}


def _matches(value: Any, tag: str) -> bool:
    return {
        "json": True,
        "string": isinstance(value, str),
        "integer": type(value) is int,
        "number": type(value) in (int, float),
        "boolean": type(value) is bool,
        "object": isinstance(value, dict),
        "array": isinstance(value, list),
        "null": value is None,
    }.get(tag, False)


def accepts_schema(values: Mapping[str, Any], schema: Tuple[Tuple[str, str], ...]) -> bool:
    try:
        canonical_json(values)
        return set(values) == {key for key, _ in schema} and all(
            _matches(values[key], tag) for key, tag in schema
        )
    except (ValueError, TypeError, KeyError):
        return False


@dataclass(frozen=True)
class OperatorSpec:
    node_id: str
    function: Callable[[Mapping[str, Any]], Mapping[str, Any]]
    dependencies: Tuple[str, ...] = ()
    read_fields: Tuple[str, ...] = ()
    write_fields: Tuple[str, ...] = ()
    semantic_version: str = "1"
    kind: str = "Compute"
    cacheable: bool = False
    stable: bool = False
    effect_free: bool = False
    latency_ms: float = 1.0
    cost_usd: float = 0.0
    input_schema: Tuple[Tuple[str, str], ...] = ()
    output_schema: Tuple[Tuple[str, str], ...] = ()

    def __post_init__(self):
        for field_name in ("dependencies", "read_fields", "write_fields"):
            values = tuple(getattr(self, field_name))
            if len(values) != len(set(values)):
                raise ValueError(f"Duplicate {field_name} in {self.node_id}")
            object.__setattr__(self, field_name, values)
        if not self.node_id or not callable(self.function):
            raise ValueError("Operator needs a stable ID and callable")
        for value in (self.latency_ms, self.cost_usd):
            if not math.isfinite(value) or value < 0:
                raise ValueError("Operator cost and latency must be finite and nonnegative")
        for schema_name, fields in (("input_schema", self.read_fields), ("output_schema", self.write_fields)):
            schema = tuple(tuple(pair) for pair in getattr(self, schema_name))
            schema = schema or tuple((key, "json") for key in fields)
            if len(schema) != len(fields) or {key for key, _ in schema} != set(fields):
                raise ValueError(f"{schema_name} must describe every declared field exactly once")
            if any(tag not in _SCHEMA_TYPES for _, tag in schema):
                raise ValueError("Unsupported JSON schema type")
            object.__setattr__(self, schema_name, schema)


@dataclass(frozen=True)
class SubgraphSpec:
    name: str
    identity: str
    node_ids: Tuple[str, ...]
    entry_node_id: str
    exit_node_id: str
    input_fields: Tuple[str, ...]
    output_fields: Tuple[str, ...]
    critical_fields: Tuple[str, ...]
    input_schema: Tuple[Tuple[str, str], ...]
    output_schema: Tuple[Tuple[str, str], ...]
    exact_safe: bool

    def project(self, state: Mapping[str, Any]) -> Dict[str, Any]:
        return json.loads(canonical_json({key: state[key] for key in self.input_fields}))

    def accepts_input(self, values: Mapping[str, Any]) -> bool:
        return accepts_schema(values, self.input_schema)

    def accepts_output(self, values: Mapping[str, Any]) -> bool:
        return accepts_schema(values, self.output_schema)


class WorkflowSpec:
    def __init__(self, workflow_id: str, operators: Sequence[OperatorSpec], version: str = "1"):
        self.workflow_id = workflow_id
        self.version = version
        self.operators = tuple(operators)
        self.nodes = {operator.node_id: operator for operator in self.operators}
        if not self.nodes or len(self.nodes) != len(self.operators):
            raise ValueError("Workflow must have nonempty, unique operator IDs")
        self.children = {key: [] for key in self.nodes}
        self.writers = {}
        for operator in self.operators:
            for parent in operator.dependencies:
                if parent not in self.nodes:
                    raise ValueError(f"Unknown dependency: {parent}")
                self.children[parent].append(operator.node_id)
            for key in operator.write_fields:
                if key in self.writers:
                    raise ValueError(f"First version requires one writer per field: {key}")
                self.writers[key] = operator.node_id
        remaining = {key: len(op.dependencies) for key, op in self.nodes.items()}
        ready = deque(key for key in self.nodes if remaining[key] == 0)
        order = []
        while ready:
            key = ready.popleft()
            order.append(key)
            for child in self.children[key]:
                remaining[child] -= 1
                if remaining[child] == 0:
                    ready.append(child)
        if len(order) != len(self.nodes):
            raise ValueError("Declared workflow must be acyclic")
        self.topological_order = tuple(order)
        for operator in self.operators:
            for key in operator.read_fields:
                writer = self.writers.get(key)
                if writer and writer not in operator.dependencies:
                    raise ValueError(f"Missing direct data dependency for {operator.node_id}.{key}")
                if writer:
                    source_schema = dict(self.nodes[writer].output_schema)[key]
                    target_schema = dict(operator.input_schema)[key]
                    if source_schema != target_schema and "json" not in (source_schema, target_schema):
                        raise ValueError(f"Incompatible field schema: {key}")

    def ready(self, completed: set) -> Tuple[str, ...]:
        return tuple(key for key in self.topological_order if key not in completed and
                     set(self.nodes[key].dependencies).issubset(completed))

    def subgraph(self, name: str, node_ids: Sequence[str], critical_fields: Sequence[str] = ()) -> SubgraphSpec:
        selected = set(node_ids)
        if not selected or not selected.issubset(self.nodes):
            raise ValueError("Subgraph nodes must belong to this workflow")
        # Virtual Start/End connect all workflow roots/sinks, but never enter candidates.
        entries = {key for key in selected if not self.nodes[key].dependencies or
                   any(parent not in selected for parent in self.nodes[key].dependencies)}
        exits = {key for key in selected if not self.children[key] or
                 any(child not in selected for child in self.children[key])}
        if len(entries) != 1 or len(exits) != 1:
            raise ValueError("Candidate must have one entry node and one exit node")
        entry, exit_node = next(iter(entries)), next(iter(exits))
        visited, pending = set(), [entry]
        while pending:
            key = pending.pop()
            if key in visited:
                continue
            visited.add(key)
            pending.extend((set(self.nodes[key].dependencies) | set(self.children[key])) & selected - visited)
        if visited != selected:
            raise ValueError("Candidate must be weakly connected")
        ordered = tuple(key for key in self.topological_order if key in selected)
        if any(not self.nodes[key].cacheable or not self.nodes[key].effect_free for key in ordered):
            raise ValueError("Candidate crosses a noncacheable or effectful operation")
        inputs = {}
        for key in ordered:
            for field, tag in self.nodes[key].input_schema:
                if self.writers.get(field) not in selected:
                    if field in inputs and inputs[field] != tag:
                        raise ValueError("Conflicting boundary input schemas")
                    inputs[field] = tag
        outputs = tuple(self.nodes[exit_node].output_schema)
        critical = tuple(sorted(set(critical_fields)))
        if not set(critical).issubset(inputs):
            raise ValueError("Critical fields must be declared boundary inputs")
        description = {
            "workflow": workflow_id_identity(self.workflow_id, self.version),
            "operators": [
                {"id": key, "semantics": self.nodes[key].semantic_version,
                 "kind": self.nodes[key].kind,
                 "parents": sorted(set(self.nodes[key].dependencies) & selected),
                 "inputs": self.nodes[key].input_schema, "outputs": self.nodes[key].output_schema,
                 "cacheable": self.nodes[key].cacheable, "stable": self.nodes[key].stable,
                 "effect_free": self.nodes[key].effect_free}
                for key in ordered
            ],
            "entry": entry, "exit": exit_node, "critical": critical,
        }
        identity = hashlib.sha256(canonical_json(description).encode("utf-8")).hexdigest()
        return SubgraphSpec(name, identity, ordered, entry, exit_node, tuple(sorted(inputs)),
                            tuple(key for key, _ in outputs), critical, tuple(sorted(inputs.items())), outputs,
                            all(self.nodes[key].stable for key in ordered))


def workflow_id_identity(workflow_id: str, version: str) -> str:
    return canonical_json([workflow_id, version])
