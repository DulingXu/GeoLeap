"""C2 search budgets and an equal-information small-DAG reference.

The CLI exports configuration only. Comparison functions inspect declared cost
profiles and historical forecasts; they never execute operators or observe
future outputs. Runtime measurements still include actual planning overhead.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, replace
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence, TYPE_CHECKING

from .c1 import C1Config
from .planner import ForecastTable, LookaheadPlanner, PlannerConfig, PlanningMode, SearchStrategy
from .records import CacheSnapshot
from .spec import canonical_json

if TYPE_CHECKING:
    from .reliability import EligibleRecord
    from .runtime import GeoLeapRuntime, RuntimeConfig, RuntimeResult
    from .spec import WorkflowSpec, SubgraphSpec


@dataclass(frozen=True)
class C2Config:
    variant: SearchStrategy = SearchStrategy.BOUNDED
    depth: int = 3
    beam_width: int = 8
    max_workers: int = 1
    exhaustive_max_nodes: int = 10
    exhaustive_max_plans: int = 100000

    def __post_init__(self):
        object.__setattr__(self, "variant", SearchStrategy.parse(self.variant))
        self.planner_config()

    def planner_config(self, base: Optional[PlannerConfig] = None) -> PlannerConfig:
        return replace(
            base or PlannerConfig(), mode=PlanningMode.FULL, search_strategy=self.variant,
            depth=self.depth, beam_width=self.beam_width, max_workers=self.max_workers,
            exhaustive_max_nodes=self.exhaustive_max_nodes,
            exhaustive_max_plans=self.exhaustive_max_plans,
        )

    def make_planner(self, forecasts: Optional[ForecastTable] = None,
                     base: Optional[PlannerConfig] = None) -> LookaheadPlanner:
        return LookaheadPlanner(self.planner_config(base), forecasts)

    def make_runtime(self, workflow: "WorkflowSpec", subgraphs: Sequence["SubgraphSpec"], *,
                     forecasts: Optional[ForecastTable] = None,
                     planner_config: Optional[PlannerConfig] = None,
                     config: Optional["RuntimeConfig"] = None, **kwargs) -> "GeoLeapRuntime":
        return C1Config(variant="full", depth=self.depth, beam_width=self.beam_width,
                        max_workers=self.max_workers).make_runtime(
            workflow, subgraphs, forecasts=forecasts,
            planner_config=self.planner_config(planner_config), config=config, **kwargs,
        )

    def to_dict(self, base: Optional[PlannerConfig] = None) -> dict:
        config = self.planner_config(base)
        return {
            "variant": self.variant.value, "depth": self.depth, "beam_width": self.beam_width,
            "max_workers": self.max_workers,
            "exhaustive_max_nodes": self.exhaustive_max_nodes,
            "exhaustive_max_plans": self.exhaustive_max_plans,
            "effective": {
                "planning_mode": "full", "fixed_cache": True,
                "depth_and_beam_limits_active": self.variant == SearchStrategy.BOUNDED,
                "reference_scope": "complete feasible action plans under the same forecast/scenario model",
                "future_outputs_available": False, "scenario_limit": config.scenario_limit,
                "on_exhaustive_limit": "raise ExhaustiveLimitExceeded; no partial reference",
                "runtime_support": "serial" if self.max_workers == 1 else "planner_diagnostic_only",
                "planning_time_measurement": "perf_counter; distinct from modeled planning_ms",
            },
        }


def _decision_summary(decision) -> dict:
    action = decision.action
    return {
        "first_action": None if action is None else action.kind,
        "first_node_id": None if action is None else action.node_id,
        "first_record_id": None if action is None or action.candidate is None else action.candidate.record.record_id,
        "predicted_objective_J": decision.objective,
        "predicted_latency_ms": decision.estimated_latency_ms,
        "predicted_profile_cost_usd": decision.estimated_cost_usd,
        "planning_elapsed_ms": decision.planning_elapsed_ms,
        "search_statistics": decision.trace.get("search_statistics", {}),
        "trace": dict(decision.trace),
    }


def compare_search(
    workflow: "WorkflowSpec", subgraphs: Sequence["SubgraphSpec"], completed: set,
    state: Mapping[str, Any], snapshot: CacheSnapshot, eligible: Sequence["EligibleRecord"], epoch: str, *,
    bounded: Optional[C2Config] = None, forecasts: Optional[ForecastTable] = None,
    planner_config: Optional[PlannerConfig] = None,
) -> dict:
    """Compare one bounded budget with a complete same-information reference.

    The gap concerns the predicted objective, not realized task quality or
    actual request speed. Exceeding an exhaustive guard propagates an exception
    so an unfinished search cannot silently become the reference.
    """
    bounded = bounded or C2Config()
    if bounded.variant != SearchStrategy.BOUNDED:
        raise ValueError("bounded configuration must use the bounded variant")
    reference = replace(bounded, variant=SearchStrategy.EXHAUSTIVE)
    frozen = (forecasts or ForecastTable()).snapshot()
    subgraphs, eligible, completed = tuple(subgraphs), tuple(eligible), set(completed)
    material = {
        "workflow_id": workflow.workflow_id, "workflow_version": workflow.version,
        "completed": sorted(completed), "state": dict(state), "epoch": epoch,
        "snapshot_version": snapshot.version,
        "records": [hashlib.sha256(record.serialized()).hexdigest() for record in snapshot.records],
        "audit_ids": sorted(snapshot.audit_ids),
        "eligible_records": [(item.spec.identity, item.record.record_id, item.record.revision)
                             for item in eligible],
        "forecasts": [json.loads(frozen.export_record(record.record_id)) for record in snapshot.records],
    }
    common = bounded.planner_config(planner_config)
    material["common_objective_and_scenarios"] = {
        name: getattr(common, name) for name in (
            "alpha", "beta", "cost_reference", "latency_reference_ms", "lookup_ms",
            "planning_ms", "validation_ms", "restore_ms", "scenario_limit", "max_workers",
        )
    }
    information_fingerprint = hashlib.sha256(canonical_json(material).encode()).hexdigest()
    bounded_decision = bounded.make_planner(frozen, planner_config).choose(
        workflow, subgraphs, completed, state, snapshot, eligible, epoch,
    )
    reference_decision = reference.make_planner(frozen, planner_config).choose(
        workflow, subgraphs, completed, state, snapshot, eligible, epoch,
    )
    gap = bounded_decision.objective - reference_decision.objective
    denominator = abs(reference_decision.objective)
    relative = gap / denominator if denominator > 0 else (0.0 if gap == 0 else None)
    return {
        "information_fingerprint": information_fingerprint,
        "common_objective_and_scenarios": material["common_objective_and_scenarios"],
        "bounded_config": bounded.to_dict(planner_config),
        "reference_config": reference.to_dict(planner_config),
        "reference_scope": "same-information predicted objective over complete feasible action plans",
        "future_outputs_used": False,
        "scenario_protocol": "shared exact-enumeration threshold and deterministic record-key samples",
        "objective_gap": gap, "relative_gap": relative,
        "relative_gap_denominator": "abs(reference predicted J)",
        "relative_gap_defined": relative is not None,
        "bounded": _decision_summary(bounded_decision),
        "exhaustive": _decision_summary(reference_decision),
    }


def request_metrics(result: "RuntimeResult", config: Optional[PlannerConfig] = None) -> dict:
    """Actual request time with explicitly profiled, rather than billed, cost."""
    config = config or PlannerConfig()
    objective = (config.alpha * result.cost_usd / config.cost_reference
                 + config.beta * result.elapsed_ms / config.latency_reference_ms)
    if not math.isfinite(objective):
        raise ValueError("request objective must be finite")
    return {
        "request_elapsed_ms": result.elapsed_ms, "planning_elapsed_ms": result.planning_elapsed_ms,
        "profile_cost_usd": result.cost_usd, "actual_time_profile_cost_J": objective,
        "cost_source": "operator_profile_not_provider_receipt",
        "executed_node_count": len(result.executed_node_ids), "reuse_count": len(result.reused_subgraphs),
    }


def add_c2_arguments(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    group = parser.add_argument_group("C2 search budget switches")
    group.add_argument("--c2-variant", type=SearchStrategy.parse, choices=tuple(SearchStrategy),
                       default=SearchStrategy.BOUNDED, metavar="{bounded,exhaustive}")
    group.add_argument("--c2-depth", type=int, default=3)
    group.add_argument("--c2-beam-width", type=int, default=8)
    group.add_argument("--c2-max-workers", type=int, default=1)
    group.add_argument("--c2-exhaustive-max-nodes", type=int, default=10)
    group.add_argument("--c2-exhaustive-max-plans", type=int, default=100000)
    return parser


def c2_config_from_args(args: argparse.Namespace) -> C2Config:
    return C2Config(args.c2_variant, args.c2_depth, args.c2_beam_width, args.c2_max_workers,
                    args.c2_exhaustive_max_nodes, args.c2_exhaustive_max_plans)


def main() -> int:
    parser = add_c2_arguments(argparse.ArgumentParser(description="查看或导出 C2 开关；不运行效果实验"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        config = c2_config_from_args(args)
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
