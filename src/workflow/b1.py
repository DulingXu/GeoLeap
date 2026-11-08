"""B1 switch management only: configure screening, without running experiments.

All variants share a record pool, similarity threshold, guards, and calibration
statistics. The preset disables exact bypasses and screening-score budgets.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, replace
import json
import math
from pathlib import Path
from typing import Optional, Sequence, TYPE_CHECKING

from .reliability import Matcher, ReliabilityConfig, ReliabilityTable, ScreeningMode

if TYPE_CHECKING:
    from .runtime import GeoLeapRuntime, RuntimeConfig
    from .spec import SubgraphSpec, WorkflowSpec


@dataclass(frozen=True)
class B1Config:
    variant: ScreeningMode = ScreeningMode.FULL
    min_groups: int = 3
    max_score: float = 0.2
    similarity_threshold: float = 0.8

    def __post_init__(self) -> None:
        object.__setattr__(self, "variant", ScreeningMode.parse(self.variant))
        if not math.isfinite(self.similarity_threshold) or not -1 <= self.similarity_threshold <= 1:
            raise ValueError("similarity_threshold must be finite and lie in [-1, 1]")
        self.reliability_config()  # Validate thresholds before constructing any runtime.

    def reliability_config(self, base: Optional[ReliabilityConfig] = None) -> ReliabilityConfig:
        return replace(base or ReliabilityConfig(), min_groups=self.min_groups, max_score=self.max_score,
                       screening_mode=self.variant, rank_by_score=False)

    def runtime_config(self, base: Optional["RuntimeConfig"] = None) -> "RuntimeConfig":
        from .runtime import RuntimeConfig
        return replace(base or RuntimeConfig(), allow_exact=False, screening_budget=None,
                       similarity_threshold=self.similarity_threshold)

    def make_matcher(self, table: ReliabilityTable) -> Matcher:
        """Different B1 rules can read the very same immutable record/history view."""
        return Matcher(table, self.reliability_config(table.config), self.similarity_threshold, allow_exact=False)

    def make_runtime(
        self, workflow: "WorkflowSpec", subgraphs: Sequence["SubgraphSpec"], *,
        config: Optional["RuntimeConfig"] = None,
        reliability_config: Optional[ReliabilityConfig] = None,
        **kwargs,
    ) -> "GeoLeapRuntime":
        """Apply both configs together so lookup/commit cannot silently use Full."""
        from .runtime import GeoLeapRuntime
        return GeoLeapRuntime(workflow, subgraphs, config=self.runtime_config(config),
                             reliability_config=self.reliability_config(reliability_config), **kwargs)

    def to_dict(self) -> dict:
        config = self.reliability_config()
        return {
            "variant": self.variant.value,
            "min_groups": self.min_groups,
            "max_score": self.max_score,
            "similarity_threshold": self.similarity_threshold,
            "effective": {
                "uses_history": self.variant != ScreeningMode.SIMILARITY,
                "min_groups": config.effective_min_groups,
                "score_rule": config.score_rule,
                "max_score": None if self.variant == ScreeningMode.SIMILARITY else self.max_score,
                "allow_exact": False,
                "screening_budget": None,
                "retrieval_order": "similarity_desc_then_record_id_revision",
            },
        }


def add_b1_arguments(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """Reusable argument group for a future B1 experiment driver."""
    group = parser.add_argument_group("B1 screening switches")
    group.add_argument("--b1-variant", type=ScreeningMode.parse, choices=tuple(ScreeningMode),
                       default=ScreeningMode.FULL, help="similarity / no_margin / min1 / full")
    group.add_argument("--b1-min-groups", type=int, default=3, help="shared support threshold; Min1 uses 1")
    group.add_argument("--b1-max-score", type=float, default=0.2)
    group.add_argument("--b1-sim-threshold", type=float, default=0.8)
    return parser


def b1_config_from_args(args: argparse.Namespace) -> B1Config:
    return B1Config(args.b1_variant, args.b1_min_groups, args.b1_max_score, args.b1_sim_threshold)


def main() -> int:
    parser = add_b1_arguments(argparse.ArgumentParser(description="查看或保存 B1 筛选开关；不执行工作流或实验"))
    parser.add_argument("--output", type=Path, help="可选：保存开关配置 JSON")
    args = parser.parse_args()
    try:
        config = b1_config_from_args(args)
    except ValueError as error:
        parser.error(str(error))
    payload = json.dumps(config.to_dict(), ensure_ascii=False, indent=2) + "\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload, encoding="utf-8")
    print(payload, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
