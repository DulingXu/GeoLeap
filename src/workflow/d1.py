"""D1 residency presets and configuration export; no workloads run here."""

from __future__ import annotations

import argparse
from dataclasses import dataclass, replace
import json
import math
from pathlib import Path
from typing import Optional

from .planner import ForecastTable, LookaheadPlanner, PlannerConfig
from .residency import OpportunityCacheManager, ResidencyConfig, ResidencyMode


@dataclass(frozen=True)
class D1Config:
    variant: ResidencyMode = ResidencyMode.FULL
    capacity_mib: float = 4.0

    def __post_init__(self):
        object.__setattr__(self, "variant", ResidencyMode.parse(self.variant))
        if isinstance(self.capacity_mib, bool) or not math.isfinite(self.capacity_mib) or self.capacity_mib <= 0:
            raise ValueError("capacity_mib must be finite and positive")
        self.residency_config()

    def residency_config(self, base: Optional[ResidencyConfig] = None,
                         planner_config: Optional[PlannerConfig] = None) -> ResidencyConfig:
        changes = {"max_bytes": int(self.capacity_mib * 1024 * 1024), "policy": self.variant}
        if planner_config is not None:
            changes.update(legacy_alpha=planner_config.alpha, legacy_beta=planner_config.beta,
                           legacy_cost_reference=planner_config.cost_reference,
                           legacy_latency_reference_ms=planner_config.latency_reference_ms)
        return replace(base or ResidencyConfig(), **changes)

    def make_manager(self, store, value_fn, *, record_benefit_fn=None,
                     base: Optional[ResidencyConfig] = None,
                     planner_config: Optional[PlannerConfig] = None) -> OpportunityCacheManager:
        return OpportunityCacheManager(store, self.residency_config(base, planner_config), value_fn,
                                       record_benefit_fn=record_benefit_fn)

    def make_runtime(self, workflow, subgraphs, *, config=None, planner_config=None,
                     residency_config=None, forecasts: Optional[ForecastTable] = None, **kwargs):
        from .runtime import GeoLeapRuntime, RuntimeConfig
        policy = planner_config or PlannerConfig()
        runtime = GeoLeapRuntime(
            workflow, subgraphs,
            config=replace(config or RuntimeConfig(), fixed_cache=False,
                           feedback_on_normal_execution=False),
            planner_config=policy,
            residency_config=self.residency_config(residency_config, policy), **kwargs,
        )
        if forecasts is not None:
            runtime.forecasts = forecasts.snapshot()
            runtime.planner = LookaheadPlanner(policy, runtime.forecasts)
        return runtime

    def to_dict(self) -> dict:
        config = self.residency_config()
        return {
            "variant": self.variant.value, "capacity_mib": self.capacity_mib,
            "max_bytes": config.max_bytes,
            "effective": {
                "b_config_preserved": True, "c_config_preserved": True,
                "feedback_on_normal_execution": False, "fixed_cache": False,
                "value_model": {
                    ResidencyMode.LRU: "successful_reuse_recency",
                    ResidencyMode.LEGACY_DENSITY: "legacy_decayed_reuse_times_independent_benefit_per_byte",
                    ResidencyMode.ADDITIVE_VALUE: "sum_of_singleton_gains_above_empty_baseline",
                    ResidencyMode.FULL: "whole_set_utility_with_marginal_loss_eviction",
                }[self.variant],
                "positive_transition_gain_required": self.variant in
                    (ResidencyMode.FULL, ResidencyMode.ADDITIVE_VALUE),
                "memory_accounting": "managed serialized payload plus metadata reserve; not RSS",
                "lifecycle": "explicit maintenance; common audit reserve, pins and version checks",
                "legacy_source": "workflow.residency.OpportunityCacheManager.legacy_density",
            },
        }


def add_d1_arguments(parser):
    group = parser.add_argument_group("D1 residency switches")
    group.add_argument("--d1-variant", type=ResidencyMode.parse, choices=tuple(ResidencyMode),
                       default=ResidencyMode.FULL,
                       metavar="{lru,legacy_density,additive_value,full}")
    group.add_argument("--d1-capacity-mib", type=float, default=4.0)
    return parser


def d1_config_from_args(args):
    return D1Config(args.d1_variant, args.d1_capacity_mib)


def main():
    parser = add_d1_arguments(argparse.ArgumentParser(description="查看或导出 D1 驻留开关；不运行实验"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        config = d1_config_from_args(args)
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
