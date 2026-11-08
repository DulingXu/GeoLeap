"""Controlled sequential D2 streams; configuration export never runs requests.

The runner observes demand only after a request has completed. Economic profile
updates happen at an explicit request boundary and never change cache semantics.
Measured time is wall time; default monetary values are operator profiles, not
provider receipts. No counterfactual API calls are made to invent net savings.
"""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass
from enum import Enum
import json
import math
from pathlib import Path
import time
import uuid
from typing import Any, Callable, Mapping, Optional, Sequence, TYPE_CHECKING

from .planner import PlanningMode
from .residency import ResidencyMode
from .spec import canonical_json

if TYPE_CHECKING:
    from .runtime import GeoLeapRuntime, RuntimeResult


class ChangeStream(str, Enum):
    DEMAND_SHIFT = "demand_shift"
    COST_SHIFT = "cost_shift"

    @classmethod
    def parse(cls, value):
        if isinstance(value, cls):
            return value
        try:
            return cls(value.strip().lower().replace("-", "_"))
        except (AttributeError, TypeError, ValueError):
            raise ValueError("D2 stream must be demand_shift or cost_shift") from None


@dataclass(frozen=True)
class D2Config:
    stream: ChangeStream = ChangeStream.DEMAND_SHIFT
    variant: ResidencyMode = ResidencyMode.FULL
    window_size: int = 16
    change_at: int = 33
    maintenance_every: int = 8

    def __post_init__(self):
        object.__setattr__(self, "stream", ChangeStream.parse(self.stream))
        object.__setattr__(self, "variant", ResidencyMode.parse(self.variant))
        for name in ("window_size", "maintenance_every"):
            if type(getattr(self, name)) is not int or getattr(self, name) < 1:
                raise ValueError(f"{name} must be a positive integer")
        if type(self.change_at) is not int or self.change_at < 2:
            raise ValueError("change_at must be at least 2, with a nonempty pre-change phase")

    def to_dict(self) -> dict:
        return {
            "stream": self.stream.value, "variant": self.variant.value,
            "window_size": self.window_size, "change_at": self.change_at,
            "maintenance_every": self.maintenance_every,
            "effective": {
                "change_boundary": "before the 1-based change_at request",
                "demand_information": "completed requests only; runtime rolling window",
                "fixed_controls": "B/C parameters, workflow semantics and epoch",
                "cost_shift_distribution": "identical empirical context+input distribution before/after",
                "economic_updates": "latency_ms/cost_usd profiles only; no simulated service sleep",
                "memory_accounting": "managed serialized resident bytes, not process RSS",
                "default_cost_source": "operator_profile_not_provider_receipt",
                "net_savings": "unmeasured without paired baseline and transition/audit ledger",
                "quality": "unmeasured unless an explicit quality_fn is supplied",
            },
        }


@dataclass
class D2Result:
    config: dict
    request_rows: list
    window_rows: list
    change_events: list
    run_id: str = ""

    def to_dict(self) -> dict:
        return {"run_id": self.run_id, "config": self.config, "request_rows": self.request_rows,
                "window_rows": self.window_rows, "change_events": self.change_events,
                "execution": "sequential_runtime_requests", "runtime_workers": 1}

    def write_output(self, output_dir: Path) -> None:
        destination = Path(output_dir)
        destination.mkdir(parents=True, exist_ok=True)
        for name, payload in (("change_response.json", self.to_dict()),
                              ("request_rows.json", self.request_rows),
                              ("window_rows.json", self.window_rows),
                              ("change_events.json", self.change_events)):
            (destination / name).write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


class D2Runner:
    """Drive one preconstructed D1 runtime under one controlled change stream.

    ``candidate_hook(index, request, result, runtime)`` may submit candidates
    from completed execution or an explicitly historical catalogue. It runs
    after that request, never during pre-validation of future requests.
    ``cost_fn(result)`` supplies billed serving cost, excluding extra audit,
    transition and maintenance fees. It can be used alone to measure serving.
    Net savings additionally require ``baseline_cost_fn(index, request, result)``
    and ``overhead_cost_fn(index, request, result)`` reading already collected
    paired ledgers. The latter explicitly supplies all three extra fee fields,
    including zero values. Baseline measurement calls are not production audit
    fees and must not be charged a second time as overhead.
    """

    def __init__(self, runtime: "GeoLeapRuntime", config: Optional[D2Config] = None, *,
                 quality_fn: Optional[Callable[[Mapping[str, Any], "RuntimeResult"], float]] = None,
                 cost_fn: Optional[Callable[["RuntimeResult"], float]] = None,
                 cost_source: Optional[str] = None,
                 baseline_cost_fn: Optional[Callable[[int, Mapping[str, Any], "RuntimeResult"], float]] = None,
                 overhead_cost_fn: Optional[Callable[[int, Mapping[str, Any], "RuntimeResult"], Mapping[str, float]]] = None,
                 candidate_hook: Optional[Callable] = None):
        self.runtime = runtime
        self.config = config or D2Config()
        if runtime.config.fixed_cache:
            raise ValueError("D2 requires a mutable D cache; C1 fixed-cache runtimes are unsuitable")
        if runtime.planner.config.mode == PlanningMode.NO_CACHE:
            raise ValueError("D2 residency changes require a caching execution policy")
        if ResidencyMode.parse(runtime.manager.config.policy) != self.config.variant:
            raise ValueError("D2 variant must match the runtime's D1 residency policy")
        if cost_fn is not None and (not isinstance(cost_source, str) or not cost_source.strip()
                                    or cost_source.strip().lower() in
                                    {"operator_profile_not_provider_receipt", "operator_profile", "profile"}):
            raise ValueError("An external cost_fn requires an explicit receipt-ledger cost_source, not a profile label")
        if ((baseline_cost_fn is not None or overhead_cost_fn is not None)
                and not all(callback is not None for callback in (cost_fn, baseline_cost_fn, overhead_cost_fn))):
            raise ValueError("Net savings require cost_fn, baseline_cost_fn and overhead_cost_fn together")
        self.quality_fn, self.cost_fn = quality_fn, cost_fn
        self.baseline_cost_fn, self.overhead_cost_fn = baseline_cost_fn, overhead_cost_fn
        self.net_ledger_enabled = baseline_cost_fn is not None
        self.cost_source = cost_source if cost_fn is not None else "operator_profile_not_provider_receipt"
        self.candidate_hook = candidate_hook

    @staticmethod
    def _amount(value, label: str) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{label} must be a finite nonnegative ledger amount")
        amount = float(value)
        if not math.isfinite(amount) or amount < 0:
            raise ValueError(f"{label} must be a finite nonnegative ledger amount")
        return amount

    def _cost_ledger(self, index, request, result) -> dict:
        values = {"billed_serving_cost_usd": None, "paired_baseline_cost_usd": None,
                  "audit_cost_usd": None, "transition_cost_usd": None, "maintenance_cost_usd": None,
                  "net_cost_usd": None, "net_cost_savings_usd": None}
        if self.cost_fn is None:
            return values
        values["billed_serving_cost_usd"] = self._amount(self.cost_fn(result), "serving cost")
        if not self.net_ledger_enabled:
            return values
        values["paired_baseline_cost_usd"] = self._amount(
            self.baseline_cost_fn(index, request, result), "paired baseline cost")
        overhead = self.overhead_cost_fn(index, request, result)
        required = {"audit_cost_usd", "transition_cost_usd", "maintenance_cost_usd"}
        if not isinstance(overhead, Mapping) or set(overhead) != required:
            raise ValueError("overhead_cost_fn must explicitly provide only audit_cost_usd, transition_cost_usd and maintenance_cost_usd")
        for field in required:
            values[field] = self._amount(overhead[field], field)
        try:
            production = math.fsum(values[field] for field in required) + values["billed_serving_cost_usd"]
        except OverflowError:
            raise ValueError("Total production cost must remain finite") from None
        if not math.isfinite(production):
            raise ValueError("Total production cost must remain finite")
        values["net_cost_usd"] = production
        values["net_cost_savings_usd"] = values["paired_baseline_cost_usd"] - production
        return values

    @staticmethod
    def _service_signature(runtime) -> tuple:
        return tuple((node.node_id, node.latency_ms, node.cost_usd) for node in runtime.workflow.operators)

    @staticmethod
    def _controls(runtime) -> tuple:
        return (runtime.workflow.workflow_id, runtime.workflow.version,
                tuple((node.node_id, id(node.function), node.semantic_version, node.kind,
                       node.dependencies, node.read_fields, node.write_fields,
                       node.input_schema, node.output_schema, node.cacheable, node.stable, node.effect_free)
                      for node in runtime.workflow.operators),
                tuple(runtime.workflow.subgraph(spec.name, spec.node_ids, spec.critical_fields).identity
                      for spec in runtime.subgraphs), runtime.config,
                runtime.matcher.config, runtime.matcher.sim_threshold, runtime.matcher.allow_exact,
                runtime.planner.config, runtime.manager.config)

    @staticmethod
    def _records(runtime) -> dict:
        return {(record.record_id, record.revision): record.byte_size
                for record in runtime.store.snapshot(include_audit=True).records}

    def _validate(self, requests: Sequence[Mapping], service_updates):
        rows = []
        for request in requests:
            if not isinstance(request, Mapping) or set(request) - {"initial_state", "demand_context"}:
                raise ValueError("Each D2 request needs initial_state and demand_context only")
            state, context = request.get("initial_state"), request.get("demand_context")
            if not isinstance(state, Mapping) or not isinstance(context, str) or not context:
                raise ValueError("Each D2 request needs a JSON object and a nonempty demand_context")
            if context not in self.runtime.demand_contexts:
                raise ValueError("Each D2 demand_context must have an installed historical forecast profile")
            rows.append({"initial_state": json.loads(canonical_json(state)), "demand_context": context})
        if len(rows) < self.config.change_at:
            raise ValueError("change_at must leave a nonempty post-change phase")
        if self.config.stream == ChangeStream.DEMAND_SHIFT:
            if service_updates:
                raise ValueError("demand_shift must keep service costs and latency profiles fixed")
            boundary = self.config.change_at - 1
            first = Counter(row["demand_context"] for row in rows[:boundary])
            second = Counter(row["demand_context"] for row in rows[boundary:])
            if all(first[key] * (len(rows) - boundary) == second[key] * boundary
                   for key in first.keys() | second.keys()):
                raise ValueError("demand_shift requires different observed context proportions across phases")
        else:
            if not isinstance(service_updates, Mapping) or not service_updates:
                raise ValueError("cost_shift requires a nonempty service_updates mapping")
            for node_id, fields in service_updates.items():
                if node_id not in self.runtime.workflow.nodes or not isinstance(fields, Mapping) or not fields:
                    raise ValueError("Service updates need known node IDs and nonempty economic fields")
                if set(fields) - {"latency_ms", "cost_usd"}:
                    raise ValueError("cost_shift changes economic profiles only, never semantic configuration")
                if any(type(value) not in (int, float) or not math.isfinite(value) or value < 0
                       for value in fields.values()):
                    raise ValueError("Economic profiles must be finite and nonnegative")
            if not any(getattr(self.runtime.workflow.nodes[node_id], field) != value
                       for node_id, fields in service_updates.items() for field, value in fields.items()):
                raise ValueError("cost_shift requires an actual change to an economic profile")
            boundary = self.config.change_at - 1
            before, after = rows[:boundary], rows[boundary:]
            first = Counter(canonical_json(row) for row in before)
            second = Counter(canonical_json(row) for row in after)
            if any(first[key] * len(after) != second[key] * len(before) for key in first.keys() | second.keys()):
                raise ValueError("cost_shift must preserve the empirical context+input distribution")
        return rows

    def run(self, requests: Sequence[Mapping[str, Any]], *,
            service_updates: Optional[Mapping[str, Mapping[str, float]]] = None) -> D2Result:
        rows = self._validate(requests, service_updates)
        run_id = uuid.uuid4().hex
        controls = self._controls(self.runtime)
        expected_service = self._service_signature(self.runtime)
        expected_revision = self.runtime.cost_profile_revision
        original_profile = self.runtime.planner.config
        self.runtime.manager.reset_maintenance_stats()
        request_rows, change_events = [], []
        total_maintenance_cpu = total_maintenance_wall = 0.0
        cumulative_net_savings = 0.0
        peak_bytes = self.runtime.store.byte_size
        for index, request in enumerate(rows, 1):
            if self._controls(self.runtime) != controls:
                raise RuntimeError("D2 B/C parameters, semantics or epoch changed during the stream")
            if (self._service_signature(self.runtime) != expected_service
                    or self.runtime.cost_profile_revision != expected_revision):
                raise RuntimeError("Economic profile changed outside the configured change boundary")
            if index == self.config.change_at:
                event = {"run_id": run_id, "request_index": index, "applies": "before_request", "stream": self.config.stream.value,
                         "profile_revision_before": expected_revision}
                if self.config.stream == ChangeStream.COST_SHIFT:
                    expected_revision = self.runtime.update_service_profiles(service_updates)
                    expected_service = self._service_signature(self.runtime)
                    event["updates"] = json.loads(canonical_json(service_updates))
                    event["profile_scope"] = "economic_prediction_and_profile_accounting; not a simulated WAN delay"
                event["profile_revision_after"] = expected_revision
                change_events.append(event)
            before_records = self._records(self.runtime)
            before_stats = self.runtime.manager.maintenance_stats()
            request_id = f"d2-{self.config.variant.value}-{self.config.stream.value}-{run_id}-{index}"
            result = self.runtime.run(request["initial_state"], request_id=request_id,
                                      demand_context=request["demand_context"])
            candidate_cpu = candidate_wall = 0.0
            if self.candidate_hook is not None:
                wall, cpu = time.perf_counter(), time.thread_time()
                self.candidate_hook(index, request, result, self.runtime)
                candidate_cpu = (time.thread_time() - cpu) * 1000
                candidate_wall = (time.perf_counter() - wall) * 1000
            maintenance_cpu = maintenance_wall = 0.0
            maintenance_reasons = []
            if index % self.config.maintenance_every == 0 or index == len(rows):
                wall, cpu = time.perf_counter(), time.thread_time()
                maintenance = self.runtime.maintenance_tick()
                maintenance_cpu = (time.thread_time() - cpu) * 1000
                maintenance_wall = (time.perf_counter() - wall) * 1000
                maintenance_reasons = [item.reason for item in maintenance]
            if self._controls(self.runtime) != controls:
                raise RuntimeError("D2 B/C parameters, semantics or epoch changed during execution")
            if (self._service_signature(self.runtime) != expected_service
                    or self.runtime.cost_profile_revision != expected_revision):
                raise RuntimeError("Economic profile changed outside the configured change boundary")
            after_records = self._records(self.runtime)
            stats = self.runtime.manager.maintenance_stats()
            peak_bytes = max(peak_bytes, self.runtime.store.byte_size, stats["peak_managed_bytes"])
            total_maintenance_cpu += maintenance_cpu
            total_maintenance_wall += maintenance_wall
            ledger = self._cost_ledger(index, request, result)
            billed_cost = ledger["billed_serving_cost_usd"]
            if self.net_ledger_enabled:
                cumulative_net_savings += ledger["net_cost_savings_usd"]
                if not math.isfinite(cumulative_net_savings):
                    raise ValueError("Cumulative net savings must remain finite")
            quality = None if self.quality_fn is None else float(self.quality_fn(request, result))
            if quality is not None and (not math.isfinite(quality) or not 0 <= quality <= 1):
                raise ValueError("quality_fn must return a finite score in [0, 1]")
            if (self._controls(self.runtime) != controls
                    or self._service_signature(self.runtime) != expected_service
                    or self.runtime.cost_profile_revision != expected_revision):
                raise RuntimeError("D2 ledger/quality callbacks must not change the execution environment")
            cost = result.cost_usd if billed_cost is None else billed_cost
            objective = (original_profile.alpha * cost / original_profile.cost_reference
                         + original_profile.beta * result.elapsed_ms / original_profile.latency_reference_ms)
            inserted, evicted = after_records.keys() - before_records.keys(), before_records.keys() - after_records.keys()
            request_rows.append({
                "run_id": run_id, "request_id": result.request_id,
                "request_index": index, "phase": "before" if index < self.config.change_at else "after",
                "demand_context": request["demand_context"], "profile_revision": expected_revision,
                "request_elapsed_ms": result.elapsed_ms, "planning_elapsed_ms": result.planning_elapsed_ms,
                "profile_cost_usd": result.cost_usd, "cost_usd": cost, "cost_source": self.cost_source,
                "actual_objective_J": objective,
                "objective_cost_source": self.cost_source,
                "quality_score": quality, "quality_status": "unmeasured" if quality is None else "measured",
                "executed_node_ids": list(result.executed_node_ids), "reused_subgraphs": list(result.reused_subgraphs),
                "admitted_count": stats["admitted_count"] - before_stats["admitted_count"],
                "evicted_count": stats["evicted_count"] - before_stats["evicted_count"],
                "admitted_bytes": stats["admitted_bytes"] - before_stats["admitted_bytes"],
                "evicted_bytes": stats["evicted_bytes"] - before_stats["evicted_bytes"],
                "resident_ids_added": [key[0] for key in sorted(inserted)],
                "resident_ids_removed": [key[0] for key in sorted(evicted)],
                "managed_bytes": self.runtime.store.byte_size, "peak_managed_bytes": peak_bytes,
                "maintenance_cpu_ms": maintenance_cpu, "maintenance_wall_ms": maintenance_wall,
                "candidate_submission_cpu_ms": candidate_cpu, "candidate_submission_wall_ms": candidate_wall,
                "maintenance_reasons": maintenance_reasons,
                "observed_demand": self.runtime.residency_demand_snapshot(),
                **ledger,
                "cumulative_net_cost_savings_usd": cumulative_net_savings if self.net_ledger_enabled else None,
                "net_savings_status": "measured_from_paired_ledger" if self.net_ledger_enabled else
                    "unmeasured: paired baseline and transition/audit/maintenance ledger required",
            })
        windows = []
        for start in range(0, len(request_rows), self.config.window_size):
            window = request_rows[start:start + self.config.window_size]
            qualities = [row["quality_score"] for row in window if row["quality_score"] is not None]
            windows.append({
                "run_id": run_id,
                "window_index": len(windows) + 1, "first_request": window[0]["request_index"],
                "last_request": window[-1]["request_index"], "request_count": len(window),
                "contains_change": window[0]["request_index"] <= self.config.change_at <= window[-1]["request_index"],
                "mean_request_elapsed_ms": math.fsum(row["request_elapsed_ms"] for row in window) / len(window),
                "planning_elapsed_ms": math.fsum(row["planning_elapsed_ms"] for row in window),
                "cost_usd": math.fsum(row["cost_usd"] for row in window), "cost_source": self.cost_source,
                "mean_actual_objective_J": math.fsum(row["actual_objective_J"] for row in window) / len(window),
                "quality_mean": None if not qualities else math.fsum(qualities) / len(qualities),
                "quality_measured_requests": len(qualities),
                "admitted_count": sum(row["admitted_count"] for row in window),
                "evicted_count": sum(row["evicted_count"] for row in window),
                "admitted_bytes": sum(row["admitted_bytes"] for row in window),
                "evicted_bytes": sum(row["evicted_bytes"] for row in window),
                "peak_managed_bytes": max(row["peak_managed_bytes"] for row in window),
                "maintenance_cpu_ms": math.fsum(row["maintenance_cpu_ms"] for row in window),
                "maintenance_wall_ms": math.fsum(row["maintenance_wall_ms"] for row in window),
                "candidate_submission_cpu_ms": math.fsum(row["candidate_submission_cpu_ms"] for row in window),
                "candidate_submission_wall_ms": math.fsum(row["candidate_submission_wall_ms"] for row in window),
                **{field: math.fsum(row[field] for row in window) if self.net_ledger_enabled else None
                   for field in ("paired_baseline_cost_usd", "audit_cost_usd", "transition_cost_usd",
                                 "maintenance_cost_usd", "net_cost_usd", "net_cost_savings_usd")},
                "billed_serving_cost_usd": math.fsum(row["billed_serving_cost_usd"] for row in window)
                    if self.cost_fn is not None else None,
                "cumulative_net_cost_savings_usd": window[-1]["cumulative_net_cost_savings_usd"],
                "net_savings_status": window[-1]["net_savings_status"],
            })
        payload = self.config.to_dict()
        payload["run_id"] = run_id
        payload["measurement"] = {
            "cost_source": self.cost_source, "alpha": original_profile.alpha, "beta": original_profile.beta,
            "cost_reference": original_profile.cost_reference,
            "latency_reference_ms": original_profile.latency_reference_ms,
            "maintenance_cpu_ms": total_maintenance_cpu, "maintenance_wall_ms": total_maintenance_wall,
            "peak_managed_bytes": peak_bytes, "net_savings_measured": self.net_ledger_enabled,
            "objective_scope": "measured serving request time and declared serving cost; maintenance separately reported",
            "net_ledger_scope": {
                "serving": "cost_fn; excludes extra audit, transition and maintenance fees",
                "paired_baseline": "baseline_cost_fn; same request without reuse, read from existing ledger",
                "production_overhead": "overhead_cost_fn; explicitly supplies audit/transition/maintenance fees, including zeros",
                "baseline_measurement": "offline measurement scope; never charged again as production audit overhead",
                "net_cost": "serving + audit + transition + maintenance",
                "net_savings": "paired baseline - net_cost; cumulative savings may cross zero repeatedly",
                "callback_execution": "read existing ledgers only; runner never executes a paired baseline or extra API calls",
            },
        }
        return D2Result(payload, request_rows, windows, change_events, run_id)


def add_d2_arguments(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    group = parser.add_argument_group("D2 change-response switches")
    group.add_argument("--d2-stream", type=ChangeStream.parse, choices=tuple(ChangeStream),
                       default=ChangeStream.DEMAND_SHIFT, metavar="{demand_shift,cost_shift}")
    group.add_argument("--d2-variant", type=ResidencyMode.parse, choices=tuple(ResidencyMode),
                       default=ResidencyMode.FULL, metavar="{lru,legacy_density,additive_value,full}")
    group.add_argument("--d2-window-size", type=int, default=16)
    group.add_argument("--d2-change-at", type=int, default=33)
    group.add_argument("--d2-maintenance-every", type=int, default=8)
    return parser


def d2_config_from_args(args: argparse.Namespace) -> D2Config:
    return D2Config(args.d2_stream, args.d2_variant, args.d2_window_size,
                    args.d2_change_at, args.d2_maintenance_every)


def main() -> int:
    parser = add_d2_arguments(argparse.ArgumentParser(description="查看或导出 D2 开关；不运行请求或实验"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        config = d2_config_from_args(args)
    except ValueError as error:
        parser.error(str(error))
    payload = json.dumps(config.to_dict(), ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload, encoding="utf-8")
    print(payload, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
