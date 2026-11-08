"""Local C1 policy checks with fixed records, forecasts and matching rules.

The serial runtime checks execute only deterministic local callables. The
parallel case is a planner/scheduler diagnostic, not an online parallel run.
"""

from __future__ import annotations

from dataclasses import replace
import json
import math
from pathlib import Path
import sys
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from reporting import CheckReport, run_report
from fixtures import six_node_workflow
from workflow.c1 import C1Config
from workflow.planner import ForecastTable, PlannerConfig
from workflow.records import CacheSnapshot, CachedRecord, RecordStore
from workflow.reliability import Matcher, ReliabilityTable
from workflow.runtime import GeoLeapRuntime, RuntimeConfig
from workflow.scheduling import ScheduleTask, schedule_tasks
from workflow.spec import OperatorSpec, WorkflowSpec


EPOCH = "c1-planning-switch-check-v1"
MODES = ("no_cache", "greedy", "full")
REUSE_MODES = ("greedy", "full")


def chain_records(specs):
    sa, sb, sc = specs
    return tuple(CachedRecord.create(spec, inputs, outputs, EPOCH, record_id=spec.name,
                                     metadata_capacity=0)
                 for spec, inputs, outputs in (
                     (sa, {"v0": 0}, {"v4": 4}),
                     (sb, {"v0": 0}, {"v2": 2}),
                     (sc, {"v2": 2}, {"v6": 6}),
                 ))


def serving_store(records):
    store = RecordStore()
    if not store.publish(tuple(records), frozenset(), store.version, 4 * 1024 * 1024):
        raise RuntimeError("Could not seed the common serving record pool")
    return store


def selected(decision):
    if decision.action is None:
        return None
    candidate = decision.action.candidate
    return candidate.spec.name if candidate is not None else decision.action.node_id


def decision_row(scenario, mode, decision, snapshot, **details):
    return {
        "scenario": scenario, "mode": mode,
        "snapshot_version": snapshot.version,
        "record_ids": [record.record_id for record in snapshot.records],
        "selected": selected(decision),
        "action_kind": decision.action.kind if decision.action is not None else None,
        "estimated_latency_ms": decision.estimated_latency_ms,
        "estimated_cost_usd": decision.estimated_cost_usd,
        "objective": decision.objective,
        "planning_elapsed_ms": decision.planning_elapsed_ms,
        "plan": dict(decision.trace), **details,
    }


def parallel_diagnostic():
    """A=50 ms; B=entry -> four parallel 20 ms tasks -> exit, then join."""
    calls = []

    def operation(node_id, output, value):
        def execute(inputs):
            calls.append(node_id)
            return {output: value}
        return execute

    common = {"cacheable": True, "stable": True, "effect_free": True, "cost_usd": 0.0}
    operators = [
        OperatorSpec("a", operation("a", "a_out", 1), read_fields=("task",), write_fields=("a_out",),
                     latency_ms=50, **common),
        OperatorSpec("b_entry", operation("b_entry", "b_seed", 1), read_fields=("task",),
                     write_fields=("b_seed",), latency_ms=0, **common),
    ]
    for index in range(1, 5):
        operators.append(OperatorSpec(
            f"b{index}", operation(f"b{index}", f"b{index}_out", index),
            dependencies=("b_entry",), read_fields=("b_seed",), write_fields=(f"b{index}_out",),
            latency_ms=20, **common,
        ))
    operators.extend((
        OperatorSpec("b_exit", operation("b_exit", "b_out", 10),
                     dependencies=tuple(f"b{index}" for index in range(1, 5)),
                     read_fields=tuple(f"b{index}_out" for index in range(1, 5)),
                     write_fields=("b_out",), latency_ms=0, **common),
        OperatorSpec("join", operation("join", "answer", 11), dependencies=("a", "b_exit"),
                     read_fields=("a_out", "b_out"), write_fields=("answer",), latency_ms=0, **common),
    ))
    workflow = WorkflowSpec("c1_parallel_planner_diagnostic", operators)
    a = workflow.subgraph("A_critical_50ms", ("a",))
    b = workflow.subgraph("B_parallel_80ms_work", ("b_entry", "b1", "b2", "b3", "b4", "b_exit"))
    records = (
        CachedRecord.create(a, {"task": 1}, {"a_out": 1}, EPOCH, record_id="parallel-A", metadata_capacity=0),
        CachedRecord.create(b, {"task": 1}, {"b_out": 10}, EPOCH, record_id="parallel-B", metadata_capacity=0),
    )
    return workflow, (a, b), records, {"task": 1}, calls


def checks(report: CheckReport) -> None:
    workflow, specs, initial, counters = six_node_workflow()
    sa, sb, sc = specs
    records = chain_records(specs)
    store = serving_store(records)
    snapshot = store.snapshot()
    matcher = Matcher(ReliabilityTable())
    eligible = matcher.eligible(sa, initial, snapshot, EPOCH) + matcher.eligible(sb, initial, snapshot, EPOCH)
    forecasts = ForecastTable()
    forecasts.observe(sc.identity, records[2].record_id, records[2].revision, EPOCH, True)
    decisions, runtime_rows = [], []
    for path, constructor in (("c1_config", C1Config), ("planner_config", PlannerConfig)):
        rejected = False
        try:
            constructor(**{"variant" if path == "c1_config" else "mode": "additive_time"})
        except ValueError:
            rejected = True
        report.check(f"{path}_rejects_removed_additive_time_mode", rejected)

    baseline_configs = [C1Config(variant=mode).planner_config() for mode in MODES]
    common_fields = ("alpha", "beta", "cost_reference", "latency_reference_ms", "lookup_ms",
                     "planning_ms", "validation_ms", "restore_ms", "max_workers")
    report.check("all_modes_share_objective_weights_scales_overheads_and_workers",
                 all(all(getattr(config, key) == getattr(baseline_configs[0], key) for key in common_fields)
                     for config in baseline_configs),
                 {key: getattr(baseline_configs[0], key) for key in common_fields})

    for mode in MODES:
        for key in counters:
            counters[key] = 0
        config = C1Config(variant=mode, depth=3, beam_width=8)
        planner = config.make_planner(forecasts)
        choice = planner.choose(workflow, specs, set(), initial, snapshot, eligible, EPOCH)
        row = decision_row("overlapping_six_node_chain", mode, choice, snapshot)
        decisions.append(row)
        expected = {"no_cache": "n1", "greedy": "Sa", "full": "Sb"}[mode]
        report.check(f"{mode}_chooses_expected_first_action_under_overlap",
                     choice.action is not None
                     and choice.action.kind == ("execute" if mode == "no_cache" else "reuse")
                     and selected(choice) == expected,
                     row)
        report.check(f"{mode}_reports_measured_nonnegative_planning_time",
                     math.isfinite(choice.planning_elapsed_ms) and choice.planning_elapsed_ms >= 0.0)
        if mode == "full":
            future = [step for step in choice.trace["steps"] if step.get("record_id") == "Sc"]
            report.check(f"{mode}_uses_sc_only_as_an_unknown_input_forecast",
                         len(future) == 1 and future[0]["checked"] is False
                         and "v2" not in initial and sum(counters.values()) == 0,
                         {"future_steps": future, "call_count": sum(counters.values())})

        for key in counters:
            counters[key] = 0
        runtime = config.make_runtime(workflow, specs, forecasts=forecasts, store=store,
                                      config=RuntimeConfig(epoch=EPOCH))
        run = runtime.run(initial, request_id=f"c1-chain-{mode}")
        expected_reuse = {"no_cache": (), "greedy": ("Sa",), "full": ("Sb", "Sc")}[mode]
        expected_calls = {"no_cache": workflow.topological_order,
                          "greedy": ("n5", "n6"), "full": ()}[mode]
        plan_events = [event for event in run.trace if event.get("event") == "plan"]
        measured_sum = math.fsum(event["planning_elapsed_ms"] for event in plan_events)
        runtime_row = {
            "scenario": "serial_runtime_overlap", "mode": mode,
            "reused_subgraphs": list(run.reused_subgraphs), "executed_nodes": list(run.executed_node_ids),
            "call_counts": dict(counters), "final_output": run.state["v6"],
            "executed_node_count": len(run.executed_node_ids), "reuse_count": len(run.reused_subgraphs),
            "profile_cost_usd": run.cost_usd,
            "cost_source": "operator_profile_not_provider_receipt",
            "planning_elapsed_ms": run.planning_elapsed_ms,
            "sum_plan_events_ms": measured_sum, "request_elapsed_ms": run.elapsed_ms,
            "execution_config": [event for event in run.trace if event.get("event") == "execution_config"],
            "plan_events": plan_events,
        }
        runtime_rows.append(runtime_row)
        report.check(f"runtime_{mode}_really_executes_the_selected_compatible_combination",
                     run.reused_subgraphs == expected_reuse and run.executed_node_ids == expected_calls
                     and run.state["v6"] == 6 and run.state["untouched"] == "keep"
                     and sum(counters.values()) == len(expected_calls)
                     and math.isclose(run.cost_usd, sum(workflow.nodes[node].cost_usd
                                                       for node in expected_calls), abs_tol=1e-12), runtime_row)
        report.check(f"runtime_{mode}_accumulates_actual_planning_time",
                     math.isfinite(run.planning_elapsed_ms) and run.planning_elapsed_ms >= 0.0
                     and math.isclose(run.planning_elapsed_ms, measured_sum, rel_tol=1e-9, abs_tol=1e-9)
                     and run.elapsed_ms >= run.planning_elapsed_ms
                     and all(event["planning_mode"] == mode
                             and math.isfinite(event["planner_reported_elapsed_ms"])
                             and 0.0 <= event["planner_reported_elapsed_ms"] <= event["planning_elapsed_ms"]
                             for event in plan_events))
        execution_config = runtime_row["execution_config"]
        report.check(f"runtime_{mode}_reports_effective_execution_switches",
                     len(execution_config) == 1
                     and execution_config[0]["planning_mode"] == mode
                     and execution_config[0]["cache_enabled"] is (mode != "no_cache")
                     and execution_config[0]["planning_enabled"] is (mode != "no_cache")
                     and (mode != "no_cache" or (not plan_events and run.planning_elapsed_ms == 0.0)),
                     execution_config)
        report.check(f"runtime_{mode}_keeps_cache_and_feedback_fixed",
                     store.snapshot() == snapshot and runtime.manager.queued_count == 0
                     and not run.new_record_ids
                     and all(not event.get("event") == "output_feedback" for event in run.trace))

    # The same input and resident set must not confer a future hit if no
    # historical forecast exists. A forecast at an actually reached entry also
    # cannot replace B's live descriptor.
    for mode in REUSE_MODES:
        planner = C1Config(variant=mode).make_planner(ForecastTable())
        without_forecast = planner.choose(workflow, specs, set(), initial, snapshot, eligible, EPOCH)
        row = decision_row("unknown_future_probability_zero", mode, without_forecast, snapshot)
        decisions.append(row)
        report.check(f"{mode}_does_not_assume_unobserved_future_reuse",
                     selected(without_forecast) == "Sa", row)
        at_sc = C1Config(variant=mode).make_planner(forecasts).choose(
            workflow, specs, {"n1", "n2"}, {"v2": 2}, snapshot, (), EPOCH)
        report.check(f"{mode}_cannot_authorize_reuse_from_a_forecast_alone",
                     at_sc.action.kind == "execute" and at_sc.action.node_id == "n3",
                     decision_row("future_recheck_without_live_descriptor", mode, at_sc, snapshot))

    # Without the competing Sa, both policies choose the same available
    # non-overlapping opportunity. This is a control case, not a speedup claim.
    nonconflicting = (sb, sc)
    nonconflicting_eligible = tuple(item for item in eligible if item.spec.identity == sb.identity)
    for mode in REUSE_MODES:
        choice = C1Config(variant=mode).make_planner(forecasts).choose(
            workflow, nonconflicting, set(), initial, snapshot, nonconflicting_eligible, EPOCH)
        row = decision_row("nonconflicting_candidates", mode, choice, snapshot)
        decisions.append(row)
        report.check(f"{mode}_agrees_on_the_nonconflicting_first_action", selected(choice) == "Sb", row)

    # A populated, usable cache must not add lookup/planning/evidence work to
    # the baseline. Sentinels prove the bypass rather than infer it from hits.
    baseline_store = serving_store(records)
    baseline_runtime = C1Config(variant="no_cache").make_runtime(
        workflow, specs, forecasts=forecasts, store=baseline_store, config=RuntimeConfig(epoch=EPOCH))
    baseline_snapshot = baseline_store.snapshot(include_audit=True)
    baseline_evidence = tuple(baseline_runtime.reliability.export_record(record.record_id) for record in records)
    baseline_forecasts = tuple(baseline_runtime.forecasts.export_record(record.record_id) for record in records)
    for key in counters:
        counters[key] = 0

    def forbidden_cache_path(*args, **kwargs):
        raise AssertionError("no_cache must bypass cache, matching and planning")

    with patch.object(baseline_store, "snapshot", side_effect=forbidden_cache_path), \
            patch.object(baseline_store, "acquire_pin", side_effect=forbidden_cache_path), \
            patch.object(baseline_runtime.matcher, "eligible", side_effect=forbidden_cache_path), \
            patch.object(baseline_runtime.planner, "choose", side_effect=forbidden_cache_path), \
            patch.object(baseline_runtime, "_check_fixed_planning_state", side_effect=forbidden_cache_path):
        baseline_first = baseline_runtime.run(initial, request_id="c1-no-cache-sentinels", use_cache=True)
        baseline_second = baseline_runtime.run({"v0": 10, "untouched": "keep"},
                                                request_id="c1-no-cache-second-input", use_cache=True)
    baseline_runs = (baseline_first, baseline_second)
    report.check("no_cache_bypasses_cache_matching_planning_even_when_call_requests_cache",
                 all(run.executed_node_ids == workflow.topological_order and not run.reused_subgraphs
                     and not run.new_record_ids and run.planning_elapsed_ms == 0.0
                     and math.isclose(run.cost_usd, 0.06, abs_tol=1e-12)
                     and not run.reference_inputs and not run.reference_outputs
                     and not any(event.get("event") in ("plan", "reuse", "output_feedback") for event in run.trace)
                     for run in baseline_runs)
                 and baseline_first.state["v6"] == 6 and baseline_second.state["v6"] == 16
                 and all(count == 2 for count in counters.values()),
                 {"final_outputs": [run.state["v6"] for run in baseline_runs], "call_counts": dict(counters)})
    report.check("no_cache_keeps_resident_cache_evidence_and_forecasts_untouched",
                 baseline_store.snapshot(include_audit=True) == baseline_snapshot
                 and baseline_evidence == tuple(baseline_runtime.reliability.export_record(record.record_id)
                                                for record in records)
                 and baseline_forecasts == tuple(baseline_runtime.forecasts.export_record(record.record_id)
                                                 for record in records)
                 and baseline_runtime.manager.queued_count == 0)

    diagnostic_baseline = C1Config(variant="no_cache").make_planner(
        forecasts, base=PlannerConfig(lookup_ms=99.0, planning_ms=99.0, validation_ms=99.0, restore_ms=99.0))
    resident_estimate = diagnostic_baseline.estimate(workflow, specs, snapshot, EPOCH)
    empty_estimate = diagnostic_baseline.estimate(workflow, specs, CacheSnapshot(snapshot.version), EPOCH)
    remaining = diagnostic_baseline.choose(workflow, specs, {"n1", "n2"}, {"v2": 2}, snapshot, eligible, EPOCH)
    report.check("no_cache_static_estimates_ignore_resident_cache_and_cache_overheads",
                 math.isclose(resident_estimate.latency_ms, 60.0, abs_tol=1e-9)
                 and math.isclose(resident_estimate.cost_usd, 0.06, abs_tol=1e-12)
                 and math.isclose(empty_estimate.objective, resident_estimate.objective, abs_tol=1e-12)
                 and remaining.action.kind == "execute" and remaining.action.node_id == "n3"
                 and math.isclose(remaining.estimated_latency_ms, 40.0, abs_tol=1e-9)
                 and math.isclose(remaining.estimated_cost_usd, 0.04, abs_tol=1e-12),
                 {"full_workflow_ms": resident_estimate.latency_ms,
                  "remaining_workflow_ms": remaining.estimated_latency_ms,
                  "scope": "static_profile_diagnostic_not_runtime_planning"})

    # Verify the actual commit boundary with an optimistic but wrong Sc profile.
    changed, (changed_sa, changed_sb, changed_sc), state, changed_counters = six_node_workflow()
    def multiply(inputs):
        changed_counters["n2"] += 1
        return {"v2": inputs["v1"] * 20}
    changed = WorkflowSpec("c1_live_recheck_chain", tuple(
        replace(operator, function=multiply, semantic_version="multiply-20")
        if operator.node_id == "n2" else operator for operator in changed.operators))
    changed_specs = (changed.subgraph("Sa", changed_sa.node_ids),
                     changed.subgraph("Sb", changed_sb.node_ids),
                     changed.subgraph("Sc", changed_sc.node_ids, critical_fields=("v2",)))
    changed_records = tuple(CachedRecord.create(spec, inputs, outputs, EPOCH, record_id=spec.name,
                                               metadata_capacity=0)
                            for spec, inputs, outputs in (
                                (changed_specs[0], {"v0": 0}, {"v4": 22}),
                                (changed_specs[1], {"v0": 0}, {"v2": 20}),
                                (changed_specs[2], {"v2": 0}, {"v6": 4}),
                            ))
    wrong_forecast = ForecastTable()
    wrong_forecast.observe(changed_specs[2].identity, "Sc", 1, EPOCH, True)
    changed_runtime = C1Config(variant="full").make_runtime(
        changed, changed_specs, forecasts=wrong_forecast, store=serving_store(changed_records),
        config=RuntimeConfig(epoch=EPOCH))
    rechecked = changed_runtime.run(state, request_id="c1-wrong-future-forecast")
    report.check("full_rechecks_actual_future_input_and_falls_back_after_wrong_forecast",
                 rechecked.reused_subgraphs == ("Sb",)
                 and rechecked.executed_node_ids == ("n3", "n4", "n5", "n6")
                 and rechecked.state["v6"] == 24,
                 {"reused_subgraphs": list(rechecked.reused_subgraphs),
                  "executed_nodes": list(rechecked.executed_node_ids), "actual_v2": rechecked.state["v2"],
                  "final_output": rechecked.state["v6"]})

    # One-action, time-only scheduling diagnostic. B contains more cumulative
    # work, while A occupies the critical branch. A four-worker DAG model must
    # distinguish them from a node-time sum. No callable is run here.
    dag, dag_specs, dag_records, dag_state, diagnostic_calls = parallel_diagnostic()
    dag_snapshot = CacheSnapshot(7, dag_records)
    dag_eligible = tuple(item for spec in dag_specs for item in matcher.eligible(spec, dag_state, dag_snapshot, EPOCH))
    time_only = PlannerConfig(alpha=0.0, beta=1.0, lookup_ms=0.0, planning_ms=0.0,
                              validation_ms=0.0, restore_ms=0.0)
    config = C1Config(variant="full", depth=1, beam_width=8, max_workers=4)
    parallel_choice = config.make_planner(ForecastTable(), base=time_only).choose(
        dag, dag_specs, set(), dag_state, dag_snapshot, dag_eligible, EPOCH)
    decisions.append(decision_row("parallel_planner_diagnostic_only", "full", parallel_choice, dag_snapshot,
                                  online_parallel_runtime=False))
    report.check("full_selects_the_critical_parallel_opportunity",
                 selected(parallel_choice) == "A_critical_50ms"
                 and math.isclose(parallel_choice.estimated_latency_ms, 20.0, abs_tol=1e-9)
                 and not diagnostic_calls,
                 {"selected": selected(parallel_choice),
                  "estimated_latency_ms": parallel_choice.estimated_latency_ms,
                  "trace": dict(parallel_choice.trace)})

    uncertain = ForecastTable()
    for spec, record in zip(dag_specs, dag_records):
        uncertain.observe(spec.identity, record.record_id, record.revision, EPOCH, True)
        uncertain.observe(spec.identity, record.record_id, record.revision, EPOCH, False)
    scenario_estimate = C1Config(variant="full", depth=2, beam_width=8, max_workers=4).make_planner(
        uncertain, base=time_only).estimate(dag, dag_specs, dag_snapshot, EPOCH)
    # A is 50 or 0 ms; B's four parallel tasks jointly are 20 or 0 ms.
    # Four equally weighted scenarios have makespans 50, 50, 20 and 0.
    # Scheduling the mean durations instead would yield 25 ms, not 30 ms.
    average_schedule = schedule_tasks(tuple(
        ScheduleTask(operator.node_id, operator.dependencies, operator.latency_ms * 0.5, index)
        for index, operator in enumerate(dag.operators)
    ), max_workers=4)
    scenarios = scenario_estimate.trace["scenarios"]
    mean_of_schedules = math.fsum(item["weight"] * item["makespan_ms"] for item in scenarios)
    report.check("joint_scenarios_average_complete_makespans_instead_of_mean_node_times",
                 scenario_estimate.trace["scenario_count"] == 4
                 and math.isclose(scenario_estimate.latency_ms, 30.0, abs_tol=1e-9)
                 and math.isclose(mean_of_schedules, scenario_estimate.latency_ms, abs_tol=1e-9)
                 and math.isclose(average_schedule.makespan_ms, 25.0, abs_tol=1e-9)
                 and not math.isclose(scenario_estimate.latency_ms, average_schedule.makespan_ms)
                 and not diagnostic_calls,
                 {"scenarios": scenarios, "mean_of_scenario_makespans_ms": mean_of_schedules,
                  "makespan_of_mean_node_durations_ms": average_schedule.makespan_ms,
                  "scope": "pure_planner_and_scheduler_diagnostic"})
    decisions.append({
        "scenario": "joint_hit_miss_scenarios", "mode": "full",
        "snapshot_version": dag_snapshot.version,
        "record_ids": [record.record_id for record in dag_snapshot.records],
        "mean_of_scenario_makespans_ms": mean_of_schedules,
        "makespan_of_mean_node_durations_ms": average_schedule.makespan_ms,
        "plan": dict(scenario_estimate.trace), "online_parallel_runtime": False,
    })

    mutated_store = serving_store(records)
    frozen_runtime = C1Config().make_runtime(workflow, specs, forecasts=forecasts,
                                            store=mutated_store, config=RuntimeConfig(epoch=EPOCH))
    frozen_runtime.run(initial, request_id="c1-before-cache-mutation")
    view = mutated_store.snapshot(include_audit=True)
    assert mutated_store.publish(view.records[1:], frozenset(), view.version, 4 * 1024 * 1024)
    rejected_change = False
    before_mutation_check = dict(counters)
    try:
        frozen_runtime.run(initial, request_id="c1-after-cache-mutation")
    except RuntimeError:
        rejected_change = True
    report.check("fixed_cache_runtime_rejects_mutated_resident_view_before_execution",
                 rejected_change and counters == before_mutation_check
                 and frozen_runtime.manager.queued_count == 0)

    fixed_records = tuple(CachedRecord.create(spec, record.inputs, record.outputs, EPOCH,
                                              record_id=record.record_id)
                          for spec, record in zip(specs, records))
    for field in ("forecasts", "matching_evidence"):
        frozen_runtime = C1Config().make_runtime(workflow, specs, forecasts=forecasts,
                                                store=serving_store(fixed_records),
                                                config=RuntimeConfig(epoch=EPOCH))
        frozen_runtime.run(initial, request_id=f"c1-before-{field}-mutation")
        if field == "forecasts":
            frozen_runtime.forecasts.observe(sc.identity, fixed_records[2].record_id, 1, EPOCH, False)
        else:
            assert frozen_runtime.reliability.observe(sa, fixed_records[0], EPOCH, 1.0,
                                                      "changed-after-first-run", 0.0)
            frozen_runtime.reliability.refresh()
        before_mutation_check = dict(counters)
        rejected_change = False
        try:
            frozen_runtime.run(initial, request_id=f"c1-after-{field}-mutation")
        except RuntimeError:
            rejected_change = True
        report.check(f"fixed_state_runtime_rejects_mutated_{field}",
                     rejected_change and counters == before_mutation_check)

    for path in ("c1_preset", "runtime_direct"):
        rejected = False
        try:
            if path == "c1_preset":
                C1Config(max_workers=4).make_runtime(dag, dag_specs)
            else:
                GeoLeapRuntime(dag, dag_specs, planner_config=C1Config(max_workers=4).planner_config())
        except ValueError:
            rejected = True
        report.check(f"{path}_explicitly_rejects_unsupported_online_parallel_execution", rejected)

    report.output_dir.mkdir(parents=True, exist_ok=True)
    output = {
        "check_type": "local_functional_smoke", "experiment_run": False, "network_used": False,
        "variants": list(MODES), "cost_source": "operator_profile_not_provider_receipt",
        "online_runtime_workers": 1, "parallel_case_scope": "planner_and_scheduler_diagnostic_only",
        "decisions": decisions, "runtime_decisions": runtime_rows,
    }
    destination = report.output_dir / "mode_decisions.json"
    destination.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    summary_destination = report.output_dir / "three_mode_summary.json"
    summary_destination.write_text(json.dumps({
        "check_type": "local_functional_smoke", "experiment_run": False, "network_used": False,
        "cost_source": "operator_profile_not_provider_receipt", "runtime_workers": 1,
        "variants": [{key: row[key] for key in (
            "mode", "final_output", "executed_nodes", "executed_node_count", "reused_subgraphs",
            "reuse_count", "request_elapsed_ms", "planning_elapsed_ms", "profile_cost_usd",
        )} for row in runtime_rows],
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    report.check("c1_mode_decisions_are_saved", destination.is_file(),
                 {"path": str(destination.resolve()), "planner_decisions": len(decisions),
                  "runtime_decisions": len(runtime_rows)})
    report.check("c1_three_mode_summary_is_saved", summary_destination.is_file(),
                 {"path": str(summary_destination.resolve()), "variants": list(MODES)})


if __name__ == "__main__":
    raise SystemExit(run_report("c1_planning_switches", "C1：规划策略开关与计时接口检查", checks))
