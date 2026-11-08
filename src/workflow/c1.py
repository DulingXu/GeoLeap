"""C1 NoCache/Greedy/Full switches; no workload experiments run here."""

from __future__ import annotations

import argparse
from dataclasses import dataclass, replace
import json
from pathlib import Path
from typing import Optional, Sequence, TYPE_CHECKING

from .planner import ForecastTable, LookaheadPlanner, PlannerConfig, PlanningMode

if TYPE_CHECKING:
    from .runtime import GeoLeapRuntime, RuntimeConfig
    from .spec import WorkflowSpec, SubgraphSpec


@dataclass(frozen=True)
class C1Config:
    variant: PlanningMode = PlanningMode.FULL
    depth: int = 3
    beam_width: int = 8
    max_workers: int = 1

    def __post_init__(self):
        object.__setattr__(self, "variant", PlanningMode.parse(self.variant))
        self.planner_config()

    def planner_config(self, base: Optional[PlannerConfig] = None) -> PlannerConfig:
        return replace(base or PlannerConfig(), mode=self.variant, depth=self.depth,
                       beam_width=self.beam_width, max_workers=self.max_workers)

    def make_planner(self, forecasts: Optional[ForecastTable] = None,
                     base: Optional[PlannerConfig] = None) -> LookaheadPlanner:
        return LookaheadPlanner(self.planner_config(base), forecasts)

    def runtime_config(self, base: Optional["RuntimeConfig"] = None) -> "RuntimeConfig":
        from .runtime import RuntimeConfig
        return replace(base or RuntimeConfig(), fixed_cache=True, feedback_on_normal_execution=False)

    def make_runtime(self, workflow: "WorkflowSpec", subgraphs: Sequence["SubgraphSpec"], *,
                     forecasts: Optional[ForecastTable] = None, planner_config: Optional[PlannerConfig] = None,
                     config: Optional["RuntimeConfig"] = None, **kwargs) -> "GeoLeapRuntime":
        from .runtime import GeoLeapRuntime
        if self.max_workers != 1:
            raise ValueError("The serving runtime is serial; use make_planner() for parallel cost-model diagnostics")
        runtime = GeoLeapRuntime(workflow, subgraphs, config=self.runtime_config(config),
                                planner_config=self.planner_config(planner_config), **kwargs)
        if forecasts is not None:
            runtime.forecasts = forecasts.snapshot()
            runtime.planner = LookaheadPlanner(runtime.planner.config, runtime.forecasts)
        return runtime

    def to_dict(self) -> dict:
        config = self.planner_config()
        cache_enabled = self.variant != PlanningMode.NO_CACHE
        selection = ("normal_execution" if not cache_enabled else
                     "current_immediate_gain" if self.variant == PlanningMode.GREEDY else "bounded_lookahead")
        return {
            "variant": self.variant.value, "depth": self.depth, "beam_width": self.beam_width,
            "max_workers": self.max_workers,
            "effective": {
                "selection": selection,
                "cache_enabled": cache_enabled,
                "planning_enabled": cache_enabled,
                "ranking_time": "scenario_makespan" if cache_enabled else "not_used",
                "forecast_authorizes_substitution": False,
                "fixed_cache": True, "feedback_updates": False, "residency_updates": False,
                "runtime_support": "serial" if self.max_workers == 1 else "planner_diagnostic_only",
                "scenario_limit": config.scenario_limit,
                "planning_time_measurement": "perf_counter; separate from modeled planning_ms" if cache_enabled
                else "disabled; runtime planning_elapsed_ms is 0",
                "static_baseline_estimation": not cache_enabled,
            },
        }


def add_c1_arguments(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    group = parser.add_argument_group("C1 planning switches")
    group.add_argument("--c1-variant", type=PlanningMode.parse, choices=tuple(PlanningMode),
                       default=PlanningMode.FULL, metavar="{no_cache,greedy,full}",
                       help="no_cache: direct execution; greedy: immediate gain; full: lookahead")
    group.add_argument("--c1-depth", type=int, default=3)
    group.add_argument("--c1-beam-width", type=int, default=8)
    group.add_argument("--c1-max-workers", type=int, default=1)
    return parser


def c1_config_from_args(args: argparse.Namespace) -> C1Config:
    return C1Config(args.c1_variant, args.c1_depth, args.c1_beam_width, args.c1_max_workers)


def main() -> int:
    parser = add_c1_arguments(argparse.ArgumentParser(description="查看或导出 C1 开关；不运行请求或效果实验"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        config = c1_config_from_args(args)
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
