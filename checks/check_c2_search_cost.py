"""Local C2 checks: bounded budgets, complete references and real timers."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from fixtures import six_node_workflow
from reporting import CheckReport, run_report
from workflow.c1 import C1Config
from workflow.c2 import C2Config, add_c2_arguments, c2_config_from_args, compare_search, request_metrics
from workflow.planner import ExhaustiveLimitExceeded, ForecastTable, PlannerConfig, PlanningMode, SearchStrategy
from workflow.records import CacheSnapshot, CachedRecord, RecordStore
from workflow.reliability import Matcher, ReliabilityTable
from workflow.runtime import RuntimeConfig
from workflow.spec import OperatorSpec, WorkflowSpec


EPOCH = "c2-search-cost-v1"


def checks(report: CheckReport) -> None:
    workflow, specs, initial, counters = six_node_workflow()
    sa, sb, sc = specs
    records = tuple(CachedRecord.create(spec, inputs, outputs, EPOCH, record_id=spec.name,
                                        metadata_capacity=0)
                    for spec, inputs, outputs in (
                        (sa, {"v0": 0}, {"v4": 4}),
                        (sb, {"v0": 0}, {"v2": 2}),
                        (sc, {"v2": 2}, {"v6": 6}),
                    ))
    store = RecordStore()
    assert store.publish(records, frozenset(), store.version, 4 * 1024 * 1024)
    snapshot = store.snapshot()
    matcher = Matcher(ReliabilityTable())
    eligible = matcher.eligible(sa, initial, snapshot, EPOCH) + matcher.eligible(sb, initial, snapshot, EPOCH)
    forecasts = ForecastTable()
    forecasts.observe(sc.identity, records[2].record_id, records[2].revision, EPOCH, True)
    comparisons = []
    for depth, width in ((1, 1), (1, 8), (2, 1), (2, 8), (3, 8), (6, 64)):
        comparison = compare_search(
            workflow, specs, set(), initial, snapshot, eligible, EPOCH,
            bounded=C2Config(depth=depth, beam_width=width), forecasts=forecasts,
        )
        comparisons.append(comparison)
        reference = comparison["exhaustive"]
        stats = reference["search_statistics"]
        report.check(f"depth_{depth}_width_{width}_has_complete_same_information_reference",
                     stats["exhaustive_complete"] and stats["pruned_plans"] == 0
                     and stats["complete_plans"] == 5 and stats["expanded_plans"] == 16
                     and reference["first_record_id"] == "Sb"
                     and math.isclose(reference["predicted_objective_J"], 0.004, abs_tol=1e-12)
                     and comparison["objective_gap"] >= -1e-12,
                     {"objective_gap": comparison["objective_gap"], "reference_statistics": stats})
        report.check(f"depth_{depth}_width_{width}_records_actual_planning_time_and_search_counts",
                     all(math.isfinite(comparison[variant]["planning_elapsed_ms"])
                         and comparison[variant]["planning_elapsed_ms"] >= 0.0
                         and comparison[variant]["search_statistics"]["expanded_plans"] > 0
                         and comparison[variant]["search_statistics"]["scored_plans"] > 0
                         for variant in ("bounded", "exhaustive")))
    by_budget = {(item["bounded_config"]["depth"], item["bounded_config"]["beam_width"]): item
                 for item in comparisons}
    report.check("small_depth_and_narrow_beam_can_lose_the_complementary_combination",
                 by_budget[(1, 8)]["objective_gap"] > 0
                 and by_budget[(2, 1)]["objective_gap"] > 0
                 and math.isclose(by_budget[(2, 8)]["objective_gap"], 0.0, abs_tol=1e-12)
                 and by_budget[(2, 1)]["bounded"]["search_statistics"]["pruned_plans"] > 0)
    report.check("comparison_freezes_one_information_view_and_executes_no_operator",
                 len({item["information_fingerprint"] for item in comparisons}) == 1
                 and sum(counters.values()) == 0 and store.snapshot() == snapshot)

    # The same record-key samples are used by both search strategies. A narrow
    # scenario limit exercises deterministic sampling rather than enumeration.
    uncertain = ForecastTable()
    for passed in (True, False):
        uncertain.observe(sc.identity, records[2].record_id, records[2].revision, EPOCH, passed)
    sampled = compare_search(
        workflow, (sb, sc), set(), initial, snapshot,
        tuple(item for item in eligible if item.spec.identity == sb.identity), EPOCH,
        bounded=C2Config(depth=6, beam_width=64), forecasts=uncertain,
        planner_config=PlannerConfig(scenario_limit=1),
    )
    report.check("bounded_and_exhaustive_share_scenario_samples_for_the_same_plan",
                 sampled["bounded"]["trace"]["scenario_method"] == "bounded_sample"
                 and sampled["bounded"]["trace"]["steps"] == sampled["exhaustive"]["trace"]["steps"]
                 and sampled["bounded"]["trace"]["scenarios"] == sampled["exhaustive"]["trace"]["scenarios"]
                 and math.isclose(sampled["objective_gap"], 0.0, abs_tol=1e-12))
    no_current_descriptors = C2Config(variant="exhaustive").make_planner(forecasts).choose(
        workflow, specs, {"n1", "n2"}, {"v2": 2}, snapshot, (), EPOCH,
    )
    report.check("exhaustive_forecasts_never_authorize_a_live_substitution",
                 no_current_descriptors.action.kind == "execute"
                 and no_current_descriptors.action.node_id == "n3"
                 and sum(counters.values()) == 0)
    future = [step for step in by_budget[(2, 8)]["exhaustive"]["trace"]["steps"]
              if step.get("record_id") == "Sc"]
    report.check("reference_uses_future_entry_only_as_a_forecast",
                 len(future) == 1 and future[0]["checked"] is False and "v2" not in initial)

    for label, options in (("nodes", {"exhaustive_max_nodes": 5}),
                           ("plans", {"exhaustive_max_plans": 2})):
        rejected = False
        try:
            C2Config(variant="exhaustive", **options).make_planner(forecasts).choose(
                workflow, specs, set(), initial, snapshot, eligible, EPOCH,
            )
        except ExhaustiveLimitExceeded as error:
            rejected = not error.statistics.get("exhaustive_complete", False)
        report.check(f"exhaustive_{label}_guard_rejects_instead_of_returning_partial_reference", rejected)
    report.check("c1_modes_and_default_bounded_search_remain_unchanged",
                 tuple(mode.value for mode in PlanningMode) == ("no_cache", "greedy", "full")
                 and C1Config().planner_config().search_strategy == SearchStrategy.BOUNDED)
    parser = add_c2_arguments(argparse.ArgumentParser())
    parsed = c2_config_from_args(parser.parse_args([
        "--c2-variant", "exhaustive", "--c2-depth", "2", "--c2-beam-width", "4",
        "--c2-exhaustive-max-nodes", "6", "--c2-exhaustive-max-plans", "1000",
    ]))
    report.check("c2_cli_exports_explicit_search_budget_and_reference_guards",
                 parsed.variant == SearchStrategy.EXHAUSTIVE and parsed.depth == 2
                 and parsed.beam_width == 4 and parsed.exhaustive_max_nodes == 6
                 and parsed.exhaustive_max_plans == 1000
                 and parsed.to_dict()["effective"]["future_outputs_available"] is False)

    # A zero reference objective has a defined zero gap without division by zero.
    zero = WorkflowSpec("c2_zero_objective", (OperatorSpec(
        "zero", lambda inputs: {"answer": 1}, write_fields=("answer",),
        latency_ms=0, cost_usd=0,
    ),))
    zero_comparison = compare_search(
        zero, (), set(), {}, CacheSnapshot(0), (), EPOCH,
        planner_config=PlannerConfig(lookup_ms=0, planning_ms=0, validation_ms=0, restore_ms=0),
    )
    report.check("zero_reference_objective_does_not_divide_by_zero",
                 zero_comparison["objective_gap"] == 0.0
                 and zero_comparison["relative_gap"] == 0.0
                 and zero_comparison["relative_gap_defined"])
    zero_reference_gap = compare_search(
        workflow, specs, set(), initial, snapshot, eligible, EPOCH,
        bounded=C2Config(depth=1), forecasts=forecasts,
        planner_config=PlannerConfig(lookup_ms=0, planning_ms=0, validation_ms=0, restore_ms=0),
    )
    report.check("positive_gap_at_zero_reference_reports_undefined_relative_gap",
                 zero_reference_gap["exhaustive"]["predicted_objective_J"] == 0.0
                 and zero_reference_gap["objective_gap"] > 0.0
                 and zero_reference_gap["relative_gap"] is None
                 and not zero_reference_gap["relative_gap_defined"])

    runtime_rows = []
    for config in (C2Config(depth=1, beam_width=1), C2Config(variant="exhaustive")):
        for key in counters:
            counters[key] = 0
        runtime = config.make_runtime(workflow, specs, forecasts=forecasts, store=store,
                                      config=RuntimeConfig(epoch=EPOCH))
        result = runtime.run(initial, request_id="c2-local-" + config.variant.value)
        metrics = request_metrics(result, config.planner_config())
        metrics.update(variant=config.variant.value, final_output=result.state["v6"],
                       executed_nodes=list(result.executed_node_ids),
                       reused_subgraphs=list(result.reused_subgraphs))
        runtime_rows.append(metrics)
        plans = [item for item in result.trace if item.get("event") == "plan"]
        report.check(f"runtime_{config.variant.value}_measures_total_request_and_planning_time",
                     result.state["v6"] == 6 and result.state["untouched"] == "keep"
                     and result.elapsed_ms >= result.planning_elapsed_ms >= 0.0
                     and math.isclose(result.planning_elapsed_ms,
                                      math.fsum(item["planning_elapsed_ms"] for item in plans), abs_tol=1e-12)
                     and all(item["details"]["search_strategy"] == config.variant.value for item in plans)
                     and metrics["cost_source"] == "operator_profile_not_provider_receipt"
                     and store.snapshot() == snapshot and runtime.manager.queued_count == 0,
                     metrics)
    report.check("runtime_bounded_and_exhaustive_execute_their_distinct_local_paths",
                 runtime_rows[0]["executed_nodes"] == ["n5", "n6"]
                 and runtime_rows[1]["executed_nodes"] == []
                 and runtime_rows[1]["reused_subgraphs"] == ["Sb", "Sc"])

    report.output_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "check_type": "local_functional_smoke", "experiment_run": False, "network_used": False,
        "reference_scope": "same-information predicted action-plan objective; no future output oracle",
        "cost_source": "operator_profile_not_provider_receipt",
        "budget_comparisons": [{
            "depth": item["bounded_config"]["depth"], "beam_width": item["bounded_config"]["beam_width"],
            "objective_gap": item["objective_gap"], "relative_gap": item["relative_gap"],
            "bounded": {key: value for key, value in item["bounded"].items() if key != "trace"},
            "exhaustive": {key: value for key, value in item["exhaustive"].items() if key != "trace"},
        } for item in comparisons],
        "runtime_measurements": runtime_rows,
    }
    (report.output_dir / "search_cost_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8",
    )
    report.check("c2_search_cost_summary_saved", (report.output_dir / "search_cost_summary.json").is_file())


if __name__ == "__main__":
    raise SystemExit(run_report("c2_search_cost", "C2：搜索预算与同信息完整搜索参照", checks))
