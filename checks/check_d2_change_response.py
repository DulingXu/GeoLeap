"""Local D2 checks: observed demand, economic-only changes and window output."""

from __future__ import annotations

from dataclasses import replace
import json
import math
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from reporting import CheckReport, run_report
from workflow.c1 import C1Config
from workflow.d1 import D1Config
from workflow.d2 import D2Config, D2Runner
from workflow.planner import ForecastTable
from workflow.records import CachedRecord, CacheSnapshot, RecordStore
from workflow.residency import ResidencyConfig
from workflow.runtime import RuntimeConfig
from workflow.spec import OperatorSpec, WorkflowSpec


EPOCH = "d2-local-v1"


def seed_historical_support(runtime, spec, record):
    for group in range(3):
        runtime.reliability.observe(spec, record, EPOCH, 1.0,
                                    f"historical-calibration-{record.record_id}-{group}", 0.0)
    runtime.reliability.refresh()


def fixture(preload=True, variant="lru"):
    calls = []

    def execute(inputs):
        calls.append(inputs["x"])
        return {"answer": inputs["x"] + 1}

    workflow = WorkflowSpec("d2_local_workflow", (
        OperatorSpec("service", execute, read_fields=("x",), write_fields=("answer",),
                     cacheable=True, stable=True, effect_free=True, latency_ms=10.0, cost_usd=0.01,
                     input_schema=(("x", "integer"),), output_schema=(("answer", "integer"),)),
    ))
    spec = workflow.subgraph("cached_service", ("service",))
    catalogue = {
        context: replace(CachedRecord.create(spec, {"x": x}, {"answer": x + 1}, EPOCH,
                                            record_id=f"historical-{context}", metadata_capacity=4096), created_at=1000.0)
        for context, x in (("A", 1), ("B", 2))
    }
    budget = max(record.byte_size for record in catalogue.values())
    store = RecordStore()
    if preload:
        assert store.publish((catalogue["A"],), frozenset(), store.version, budget)
    runtime = D1Config(variant=variant, capacity_mib=budget / 1024 / 1024).make_runtime(
        workflow, (spec,), store=store,
        config=RuntimeConfig(epoch=EPOCH, request_window=2),
        residency_config=ResidencyConfig(audit_fraction=0.0),
    )
    profiles = {context: ForecastTable() for context in catalogue}
    for context, table in profiles.items():
        for label, record in catalogue.items():
            table.observe(spec.identity, record.record_id, record.revision, EPOCH, context == label)
    runtime.set_demand_profiles(profiles)
    if variant == "full":
        for record in catalogue.values():
            seed_historical_support(runtime, spec, record)
    return runtime, spec, catalogue, calls, budget


def request(context):
    return {"initial_state": {"x": 1 if context == "A" else 2}, "demand_context": context}


def checks(report: CheckReport):
    runtime, spec, catalogue, calls, budget = fixture()
    controls_before = (runtime.matcher.config, runtime.planner.config, runtime.config.epoch)
    submissions = []

    def submit_observed_candidate(index, item, result, active):
        # This catalogue is explicitly historical. Submission is delayed until
        # the actual request supplies its live input and normal reference exit.
        if not result.executed_node_ids:
            return
        candidate = catalogue[item["demand_context"]]
        assert result.reference_inputs[spec.identity] == candidate.inputs
        assert result.reference_outputs[spec.identity] == candidate.outputs
        assert active.matcher.eligible(spec, item["initial_state"], CacheSnapshot(0, (candidate,)), EPOCH)
        assert active.manager.enqueue(candidate, audit_only=False)
        submissions.append(index)

    demand = D2Runner(runtime, D2Config(stream="demand_shift", variant="lru", window_size=2,
                                       change_at=3, maintenance_every=1),
                      quality_fn=lambda item, run: float(run.state["answer"] == item["initial_state"]["x"] + 1),
                      candidate_hook=submit_observed_candidate).run([request(context) for context in "AABB"])
    report.check("demand_shift_executes_real_requests_and_adapts_from_observed_candidate",
                 calls == [2] and submissions == [3]
                 and [bool(row["reused_subgraphs"]) for row in demand.request_rows] == [True, True, False, True]
                 and runtime.store.get(catalogue["A"].record_id) is None
                 and runtime.store.get(catalogue["B"].record_id) is not None,
                 {"service_inputs": calls, "candidate_submission_requests": submissions,
                  "resident": [record.record_id for record in runtime.store.snapshot().records]})
    report.check("demand_estimation_uses_only_completed_rolling_window_requests",
                 demand.request_rows[0]["observed_demand"]["context_counts"] == {"A": 1}
                 and demand.request_rows[1]["observed_demand"]["context_counts"] == {"A": 2}
                 and demand.request_rows[2]["observed_demand"]["context_counts"] == {"A": 1, "B": 1}
                 and demand.request_rows[3]["observed_demand"]["context_counts"] == {"B": 2},
                 [row["observed_demand"] for row in demand.request_rows])
    report.check("demand_shift_keeps_service_economics_and_B_C_parameters_fixed",
                 runtime.cost_profile_revision == 0
                 and controls_before == (runtime.matcher.config, runtime.planner.config, runtime.config.epoch)
                 and all(row["profile_revision"] == 0 for row in demand.request_rows))
    report.check("windows_record_real_turnover_CPU_managed_bytes_and_measured_quality",
                 len(demand.window_rows) == 2
                 and sum(row["admitted_count"] for row in demand.request_rows) == 1
                 and sum(row["evicted_count"] for row in demand.request_rows) == 1
                 and sum(row["admitted_bytes"] for row in demand.request_rows) == catalogue["B"].byte_size
                 and sum(row["evicted_bytes"] for row in demand.request_rows) == catalogue["A"].byte_size
                 and all(row["quality_mean"] == 1.0 and row["maintenance_cpu_ms"] >= 0
                         and row["peak_managed_bytes"] <= budget for row in demand.window_rows),
                 demand.window_rows)

    full_runtime, full_spec, full_catalogue, full_calls, _ = fixture(variant="full")
    full_submissions = []

    def submit_full_historical_candidate(index, item, result, active):
        if not result.executed_node_ids:
            return
        candidate = full_catalogue[item["demand_context"]]
        assert result.reference_inputs[full_spec.identity] == candidate.inputs
        assert result.reference_outputs[full_spec.identity] == candidate.outputs
        # Reinstall fixed past calibration when the external historical
        # catalogue becomes an observed admission candidate. No new/future
        # output label is fabricated from this request or later requests.
        seed_historical_support(active, full_spec, candidate)
        assert active.matcher.eligible(full_spec, item["initial_state"], CacheSnapshot(0, (candidate,)), EPOCH)
        assert active.manager.enqueue(candidate, audit_only=False)
        full_submissions.append(index)

    full_demand = D2Runner(
        full_runtime, D2Config(variant="full", window_size=2, change_at=3, maintenance_every=1),
        candidate_hook=submit_full_historical_candidate,
    ).run([request(context) for context in "AABBB"])
    report.check("Full_reweights_set_utility_from_observed_demand_and_reuses_new_hot_record",
                 full_submissions == [3, 4] and full_calls == [2, 2]
                 and [bool(row["reused_subgraphs"]) for row in full_demand.request_rows] == [True, True, False, False, True]
                 and full_demand.request_rows[2]["admitted_count"] == 0
                 and full_demand.request_rows[3]["admitted_count"] == 1
                 and full_demand.request_rows[3]["evicted_count"] == 1
                 and full_runtime.store.get(full_catalogue["B"].record_id) is not None,
                 {"service_inputs": full_calls, "candidate_submission_requests": full_submissions,
                  "maintenance_reasons": [row["maintenance_reasons"] for row in full_demand.request_rows],
                  "observed_counts": [row["observed_demand"]["context_counts"] for row in full_demand.request_rows]})

    cost_runtime, cost_spec, cost_catalogue, cost_calls, _ = fixture(preload=False)
    cost = D2Runner(cost_runtime, D2Config(stream="cost_shift", variant="lru", window_size=2,
                                          change_at=3, maintenance_every=1)).run(
        [request(context) for context in "ABAB"],
        service_updates={"service": {"cost_usd": 0.2, "latency_ms": 50.0}})
    report.check("cost_shift_applies_economic_profiles_before_exact_1_based_boundary",
                 cost_calls == [1, 2, 1, 2]
                 and [row["profile_revision"] for row in cost.request_rows] == [0, 0, 1, 1]
                 and all(math.isclose(row["profile_cost_usd"], expected, abs_tol=1e-12)
                         for row, expected in zip(cost.request_rows, (0.01, 0.01, 0.2, 0.2)))
                 and cost.change_events[0]["request_index"] == 3
                 and cost.change_events[0]["applies"] == "before_request",
                 cost.change_events)
    report.check("unmeasured_quality_and_net_savings_are_not_fabricated",
                 all(row["quality_status"] == "unmeasured" and row["quality_score"] is None
                     and row["net_cost_savings_usd"] is None
                     and row["cost_source"] == "operator_profile_not_provider_receipt"
                     for row in cost.request_rows)
                 and all(row["quality_mean"] is None for row in cost.window_rows))
    profile = cost_runtime.planner.config
    report.check("actual_J_uses_fixed_weights_actual_request_time_and_declared_cost_source",
                 all(math.isclose(row["actual_objective_J"],
                                  profile.alpha * row["cost_usd"] / profile.cost_reference
                                  + profile.beta * row["request_elapsed_ms"] / profile.latency_reference_ms,
                                  abs_tol=1e-12) for row in cost.request_rows))

    isolated, isolated_spec, isolated_catalogue, _, _ = fixture()
    record = isolated_catalogue["A"]
    isolated.reliability.observe(isolated_spec, record, EPOCH, 0.9, "historical-comparison", 0.0)
    isolated.reliability.refresh()
    evidence = isolated.reliability.export_record(record.record_id)
    predictions = isolated._demand_profiles["A"].export_record(record.record_id)
    snapshot = isolated.store.snapshot(include_audit=True)
    epoch = isolated.config.epoch
    isolated.update_service_profiles({"service": {"cost_usd": 0.4}})
    report.check("economic_updates_preserve_record_identity_epoch_B_evidence_and_forecasts",
                 isolated.config.epoch == epoch and isolated.subgraphs[0] == isolated_spec
                 and isolated.store.snapshot(include_audit=True) == snapshot
                 and isolated.reliability.export_record(record.record_id) == evidence
                 and isolated._demand_profiles["A"].export_record(record.record_id) == predictions)

    for label, config, inputs, updates in (
        ("demand_and_cost_changes_cannot_be_mixed", D2Config(variant="lru", change_at=3),
         [request(context) for context in "AABB"], {"service": {"cost_usd": 0.2}}),
        ("cost_stream_rejects_changed_request_distribution", D2Config(stream="cost_shift", variant="lru", change_at=3),
         [request(context) for context in "AABB"], {"service": {"cost_usd": 0.2}}),
        ("cost_stream_rejects_semantic_configuration_changes", D2Config(stream="cost_shift", variant="lru", change_at=3),
         [request(context) for context in "ABAB"], {"service": {"semantic_version": "new"}}),
        ("cost_stream_rejects_noop_economic_update", D2Config(stream="cost_shift", variant="lru", change_at=3),
         [request(context) for context in "ABAB"], {"service": {"cost_usd": 0.01}}),
        ("unknown_context_is_rejected_before_any_request", D2Config(variant="lru", change_at=3),
         [request("A"), request("A"), {"initial_state": {"x": 2}, "demand_context": "unknown"}], None),
    ):
        rejected = False
        fresh, _, _, fresh_calls, _ = fixture(preload=False)
        try:
            D2Runner(fresh, config).run(inputs, service_updates=updates)
        except ValueError:
            rejected = True
        report.check(label, rejected and not fresh_calls)

    fixed = C1Config().make_runtime(isolated.workflow, isolated.subgraphs)
    rejected = False
    try:
        D2Runner(fixed)
    except ValueError:
        rejected = True
    report.check("D2_rejects_C1_fixed_cache_runtime", rejected)

    ledger_runtime, _, _, ledger_calls, _ = fixture(preload=False)
    overhead_rows = (
        {"audit_cost_usd": 2.0, "transition_cost_usd": 0.0, "maintenance_cost_usd": 0.0},
        {"audit_cost_usd": 0.0, "transition_cost_usd": 0.0, "maintenance_cost_usd": 0.0},
        {"audit_cost_usd": 0.0, "transition_cost_usd": 0.0, "maintenance_cost_usd": 0.0},
        {"audit_cost_usd": 0.0, "transition_cost_usd": 1.5, "maintenance_cost_usd": 2.5},
    )
    paired_reads = []
    paired_request_ids = []

    def baseline_ledger(index, item, run):
        paired_reads.append(index)
        paired_request_ids.append(run.request_id)
        return 2.0

    ledger = D2Runner(
        ledger_runtime, D2Config(variant="lru", window_size=2, change_at=3, maintenance_every=1),
        cost_fn=lambda run: 1.0, cost_source="local_fake_receipt_ledger",
        baseline_cost_fn=baseline_ledger,
        overhead_cost_fn=lambda index, item, run: overhead_rows[index - 1],
    ).run([request(context) for context in "AABB"])
    report.check("paired_net_ledger_crosses_zero_and_can_become_negative_again",
                 [row["net_cost_savings_usd"] for row in ledger.request_rows] == [-1.0, 1.0, 1.0, -3.0]
                 and [row["cumulative_net_cost_savings_usd"] for row in ledger.request_rows] == [-1.0, 0.0, 1.0, -2.0]
                 and ledger.config["measurement"]["net_savings_measured"] is True,
                 {"net": [row["net_cost_savings_usd"] for row in ledger.request_rows],
                  "cumulative": [row["cumulative_net_cost_savings_usd"] for row in ledger.request_rows]})
    report.check("paired_ledger_accounts_all_production_fees_without_executing_a_baseline",
                 ledger_calls == [1, 1, 2, 2] and paired_reads == [1, 2, 3, 4]
                 and [row["net_cost_usd"] for row in ledger.request_rows] == [3.0, 1.0, 1.0, 5.0]
                 and ledger.request_rows[-1]["transition_cost_usd"] == 1.5
                 and ledger.request_rows[-1]["maintenance_cost_usd"] == 2.5
                 and all(row["billed_serving_cost_usd"] == 1.0 and row["paired_baseline_cost_usd"] == 2.0
                         and row["cost_source"] == "local_fake_receipt_ledger" for row in ledger.request_rows))
    report.check("windows_sum_ledger_components_and_keep_global_cumulative_savings",
                 [row["paired_baseline_cost_usd"] for row in ledger.window_rows] == [4.0, 4.0]
                 and [row["billed_serving_cost_usd"] for row in ledger.window_rows] == [2.0, 2.0]
                 and [row["audit_cost_usd"] for row in ledger.window_rows] == [2.0, 0.0]
                 and [row["net_cost_usd"] for row in ledger.window_rows] == [4.0, 6.0]
                 and [row["net_cost_savings_usd"] for row in ledger.window_rows] == [0.0, -2.0]
                 and [row["cumulative_net_cost_savings_usd"] for row in ledger.window_rows] == [0.0, -2.0])

    serving_runtime, _, _, _, _ = fixture(preload=False)
    serving_only = D2Runner(
        serving_runtime, D2Config(variant="lru", window_size=2, change_at=3),
        cost_fn=lambda run: 0.5, cost_source="local_fake_serving_receipts",
    ).run([request(context) for context in "AABB"])
    report.check("serving_cost_only_API_remains_valid_and_does_not_invent_net_savings",
                 all(row["billed_serving_cost_usd"] == 0.5 and row["net_cost_savings_usd"] is None
                     and row["net_cost_usd"] is None and row["paired_baseline_cost_usd"] is None
                     for row in serving_only.request_rows))
    report.check("ledger_callbacks_and_outputs_share_unique_run_and_request_ids",
                 ledger.run_id and serving_only.run_id != ledger.run_id
                 and paired_request_ids == [row["request_id"] for row in ledger.request_rows]
                 and len(set(paired_request_ids)) == len(ledger.request_rows)
                 and not set(paired_request_ids) & {row["request_id"] for row in serving_only.request_rows}
                 and all(row["run_id"] == ledger.run_id and ledger.run_id in row["request_id"]
                         for row in ledger.request_rows)
                 and ledger.to_dict()["run_id"] == ledger.run_id)

    partial_cases = (
        {"baseline_cost_fn": baseline_ledger},
        {"overhead_cost_fn": lambda index, item, run: overhead_rows[0]},
        {"baseline_cost_fn": baseline_ledger, "overhead_cost_fn": lambda index, item, run: overhead_rows[0]},
        {"cost_fn": lambda run: 1.0, "cost_source": "local_fake_receipts", "baseline_cost_fn": baseline_ledger},
        {"cost_fn": lambda run: 1.0, "cost_source": "local_fake_receipts",
         "overhead_cost_fn": lambda index, item, run: overhead_rows[0]},
    )
    partial_rejected = []
    for kwargs in partial_cases:
        fresh, _, _, fresh_calls, _ = fixture(preload=False)
        try:
            D2Runner(fresh, D2Config(variant="lru", change_at=3), **kwargs)
        except ValueError:
            partial_rejected.append(not fresh_calls)
    report.check("partial_baseline_or_overhead_ledger_configuration_is_rejected",
                 len(partial_rejected) == len(partial_cases) and all(partial_rejected))

    invalid_cases = (
        (float("nan"), 2.0, overhead_rows[0]),
        (1.0, -1.0, overhead_rows[0]),
        (1.0, 2.0, {"audit_cost_usd": 0.0, "transition_cost_usd": 0.0}),
        (1.0, 2.0, {**overhead_rows[0], "baseline_measurement_cost_usd": 2.0}),
        (1.0, 2.0, {**overhead_rows[0], "transition_cost_usd": float("inf")}),
        (1.0, 2.0, {**overhead_rows[0], "audit_cost_usd": True}),
    )
    invalid_rejected = []
    for serving_amount, baseline_amount, extra in invalid_cases:
        fresh, _, _, _, _ = fixture(preload=False)
        try:
            D2Runner(fresh, D2Config(variant="lru", change_at=3),
                     cost_fn=lambda run, value=serving_amount: value, cost_source="local_fake_receipts",
                     baseline_cost_fn=lambda index, item, run, value=baseline_amount: value,
                     overhead_cost_fn=lambda index, item, run, value=extra: value).run(
                         [request(context) for context in "AABB"])
        except ValueError:
            invalid_rejected.append(True)
    report.check("net_ledger_rejects_nonfinite_negative_missing_unknown_and_boolean_amounts",
                 len(invalid_rejected) == len(invalid_cases))

    for name, result in (("demand_shift", demand), ("full_demand_shift", full_demand),
                         ("cost_shift", cost), ("paired_ledger", ledger)):
        destination = report.output_dir / name
        result.write_output(destination)
        report.check(f"{name}_request_window_and_change_outputs_are_saved",
                     all((destination / filename).is_file() for filename in (
                         "change_response.json", "request_rows.json", "window_rows.json", "change_events.json")))
    (report.output_dir / "scope.json").write_text(json.dumps({
        "check_type": "local_functional_smoke", "experiment_run": False, "network_used": False,
        "service_operations": "deterministic local callable", "memory": "managed resident serialization, not RSS",
        "cost_source": "operator_profile_not_provider_receipt", "real_provider_cost_measured": False,
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(run_report("d2_change_response", "D2：变化流开关、执行与窗口计量", checks))
