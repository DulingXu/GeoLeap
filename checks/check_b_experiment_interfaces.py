"""Check B experiment snapshot/capture interfaces without external services."""

from __future__ import annotations

from dataclasses import replace
import json
import math
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from reporting import CheckReport, run_report
from workflow.planner import PlannerConfig
from workflow.records import CachedRecord, RecordStore
from workflow.reliability import Matcher, ReliabilityConfig, ReliabilityTable, ScreeningMode
from workflow.residency import ResidencyConfig
from workflow.runtime import GeoLeapRuntime, RuntimeConfig
from workflow.spec import OperatorSpec, WorkflowSpec, canonical_json


EPOCH = "b-experiment-interface-v1"


def fixture():
    calls = {"read": 0, "after": 0}

    def read(inputs):
        calls["read"] += 1
        return {"parsed": {"n": inputs["payload"]["n"], "items": [1, 2]}}

    def after(inputs):
        calls["after"] += 1
        return {"answer": inputs["parsed"]["n"] + 1}

    workflow = WorkflowSpec("b_experiment_interface", (
        OperatorSpec("read", read, read_fields=("payload",), write_fields=("parsed",),
                     input_schema=(("payload", "object"),), output_schema=(("parsed", "object"),),
                     cacheable=True, effect_free=True, stable=False, latency_ms=40, cost_usd=0.01),
        OperatorSpec("after", after, dependencies=("read",), read_fields=("parsed",),
                     write_fields=("answer",), input_schema=(("parsed", "object"),),
                     output_schema=(("answer", "integer"),), latency_ms=10, cost_usd=0.01),
    ))
    spec = workflow.subgraph("read_prefix", ("read",))
    inputs = {"payload": {"n": 4, "query": "historical"}}
    record = CachedRecord.create(spec, inputs, {"parsed": {"n": 4, "items": [1, 2]}},
                                 EPOCH, record_id="historical-record")
    return workflow, spec, record, calls


def store_for(record):
    store = RecordStore()
    assert store.publish((record,), frozenset(), store.version, 1024 * 1024)
    return store


def checks(report: CheckReport):
    workflow, spec, record, calls = fixture()
    now = [1000.0]
    base_config = ReliabilityConfig(min_groups=3, max_score=0.5, similarity_bins=(),
                                    bootstrap_samples=32, max_age_seconds=10.0, rank_by_score=False)
    base = ReliabilityTable(base_config, clock=lambda: now[0])
    for index, disagreement in enumerate((0.0, 0.25, 0.0)):
        assert base.observe(spec, record, EPOCH, 0.9, f"calibration-{index}", disagreement)
    base.refresh()
    original = base.export_record(record.record_id)
    original_evidence = base.lookup(spec, record, EPOCH, 0.9)
    rows = []

    for mode in ScreeningMode:
        config = replace(base_config, screening_mode=mode)
        cloned = base.clone(config=config)
        report.check(f"{mode.value}_clone_preserves_original_statistics_and_age",
                     cloned.config == config and cloned.export_record(record.record_id) == original
                     and cloned.lookup(spec, record, EPOCH, 0.9) == original_evidence
                     and cloned._lock is not base._lock)
        captured = []

        def capture(hit):
            captured.append(json.loads(canonical_json(hit)))
            # Observers receive a detached value, never the active state/record.
            hit["live_inputs"]["payload"]["n"] = 999
            hit["cached_outputs"]["parsed"]["items"].append(999)

        before_calls = dict(calls)
        runtime = GeoLeapRuntime(
            workflow, (spec,), reliability_table=cloned, reliability_config=config,
            config=RuntimeConfig(epoch=EPOCH, fixed_cache=True, allow_exact=False,
                                 similarity_threshold=0.0, screening_budget=None),
            planner_config=PlannerConfig(mode="full"), store=store_for(record), hit_observer=capture,
        )
        run = runtime.run({"payload": {"n": 4, "query": "held-out input"}}, request_id=f"frozen-{mode.value}")
        hit = run.served_hits[0]
        report.check(f"{mode.value}_frozen_runtime_captures_only_real_commit_and_executes_downstream",
                     len(run.served_hits) == len(captured) == 1 and run.reused_subgraphs == (spec.name,)
                     and run.executed_node_ids == ("after",) and run.state["answer"] == 5
                     and calls["read"] == before_calls["read"] and calls["after"] == before_calls["after"] + 1
                     and hit["request_id"] == run.request_id and hit["hit_index"] == 1
                     and hit["hit_id"] == f"{run.request_id}:hit:1"
                     and hit["subgraph_id"] == spec.identity and hit["subgraph_name"] == spec.name
                     and hit["record_id"] == record.record_id and hit["revision"] == record.revision
                     and hit["epoch"] == EPOCH and hit["exact"] is False and hit["input_equal"] is False)
        if mode == ScreeningMode.SIMILARITY:
            correct_score = (hit["score"] is None and hit["groups"] is None
                             and hit["mean"] is None and hit["margin"] is None)
        else:
            expected = original_evidence.mean if mode == ScreeningMode.NO_MARGIN else original_evidence.score
            correct_score = (hit["groups"] == 3 and math.isclose(hit["score"], expected)
                             and hit["mean"] == original_evidence.mean and hit["margin"] == original_evidence.margin)
        report.check(f"{mode.value}_capture_retains_actual_score_and_unfabricated_support", correct_score, hit)
        report.check(f"{mode.value}_observer_does_not_change_state_record_or_frozen_evidence",
                     run.state["parsed"] == {"n": 4, "items": [1, 2]}
                     and record.outputs == {"parsed": {"n": 4, "items": [1, 2]}}
                     and cloned.export_record(record.record_id) == original
                     and base.export_record(record.record_id) == original)
        run.state["parsed"]["items"].append(3)
        report.check(f"{mode.value}_captured_entry_and_exit_are_deep_copies",
                     hit["live_inputs"]["payload"]["n"] == 4
                     and hit["cached_outputs"]["parsed"]["items"] == [1, 2]
                     and captured[0] == hit)
        rows.append(hit)

    independent = base.snapshot(config=replace(base_config, screening_mode="min1"))
    assert independent.observe(spec, record, EPOCH, 0.9, "new-clone-only-label", 0.0)
    independent.refresh()
    report.check("subsequent_clone_feedback_does_not_pollute_shared_calibration_snapshot",
                 independent.lookup(spec, record, EPOCH, 0.9).groups == 4
                 and base.lookup(spec, record, EPOCH, 0.9).groups == 3
                 and base.export_record(record.record_id) == original)
    rejected = []
    for label, config in (
        ("bins", replace(base_config, similarity_bins=(0.8,))),
        ("bootstrap", replace(base_config, bootstrap_samples=64)),
        ("delta", replace(base_config, delta=0.1)),
        ("history_capacity", replace(base_config, max_groups=64)),
        ("ttl", replace(base_config, max_age_seconds=20.0)),
    ):
        try:
            base.clone(config=config)
        except ValueError:
            rejected.append(label)
    report.check("clone_rejects_changes_that_would_reinterpret_existing_statistics", len(rejected) == 5, rejected)

    mismatch_rejected = []
    for config in (replace(base_config, similarity_bins=(0.8,)), replace(base_config, screening_mode="min1")):
        try:
            GeoLeapRuntime(workflow, (spec,), reliability_table=base, reliability_config=config)
        except ValueError:
            mismatch_rejected.append(True)
    report.check("runtime_rejects_inconsistent_injected_table_and_qualification_configuration",
                 len(mismatch_rejected) == 2)

    now[0] = 1011.0
    report.check("cloning_preserves_expiry_instead_of_resetting_evidence_age",
                 base.lookup(spec, record, EPOCH, 0.9) is None
                 and independent.lookup(spec, record, EPOCH, 0.9) is None
                 and base.export_record(record.record_id) == original)

    missing = ReliabilityTable(replace(base_config, screening_mode="min1"))
    descriptor = Matcher(missing, sim_threshold=0.0, allow_exact=False).eligible(
        spec, {"payload": {"n": 4, "query": "zero observations"}}, store_for(record).snapshot(), EPOCH)
    report.check("Min1_still_rejects_records_with_zero_observations", not descriptor)
    normal = GeoLeapRuntime(workflow, (spec,), config=RuntimeConfig(epoch=EPOCH)).run(
        {"payload": {"n": 4, "query": "normal execution"}}, use_cache=False, collect_records=False,
        collect_feedback=False)
    report.check("normal_no_reuse_execution_has_no_served_hit_capture",
                 normal.served_hits == [] and normal.state["answer"] == 5)

    failing_captured = []

    def fail_after(inputs):
        raise ValueError("intentional local downstream failure")

    failing_workflow = WorkflowSpec(workflow.workflow_id, tuple(
        replace(operator, function=fail_after) if operator.node_id == "after" else operator
        for operator in workflow.operators))
    failing_runtime = GeoLeapRuntime(
        failing_workflow, (spec,), config=RuntimeConfig(epoch=EPOCH, fixed_cache=True, allow_exact=False,
                                                       similarity_threshold=0.0),
        reliability_table=base.clone(config=replace(base_config, screening_mode="similarity")),
        store=store_for(record), hit_observer=failing_captured.append)
    failed = False
    try:
        failing_runtime.run({"payload": {"n": 4, "query": "downstream failure"}}, request_id="failed-request")
    except ValueError:
        failed = True
    report.check("hit_observer_retains_actual_commit_even_when_downstream_execution_fails",
                 failed and len(failing_captured) == 1
                 and failing_captured[0]["request_id"] == "failed-request")

    # The current request serves before its reference information is revealed.
    # Only a later request can use the newly refreshed qualification.
    cold_config = ReliabilityConfig(min_groups=1, max_score=0.1, similarity_bins=(), bootstrap_samples=16)
    cold = GeoLeapRuntime(
        workflow, (spec,), config=RuntimeConfig(epoch=EPOCH, allow_exact=False, similarity_threshold=0.0,
                                               feedback_on_normal_execution=False, audit_top_k=3),
        reliability_table=ReliabilityTable(cold_config),
        residency_config=ResidencyConfig(policy="lru", max_bytes=1024 * 1024,
                                         audit_fraction=1.0, audit_per_identity=3),
    )
    first_before = cold.store.snapshot(include_audit=True)
    first = cold.run({"payload": {"n": 4, "query": "first"}}, request_id="cold-1", collect_feedback=False)
    cold.observe_reference_result(first, first_before)
    cold.maintenance_tick()
    before = cold.store.snapshot(include_audit=True)
    prior_record = before.records[0]
    second = cold.run({"payload": {"n": 4, "query": "second"}}, request_id="cold-2", collect_feedback=False)
    evidence_before_reveal = cold.reliability.lookup(spec, prior_record, EPOCH, 0.5)
    cold.observe_reference_result(second, before)
    learned = cold.reliability.lookup(spec, prior_record, EPOCH, 0.5)
    cold.maintenance_tick()
    third = cold.run({"payload": {"n": 4, "query": "third"}}, request_id="cold-3",
                     collect_records=False, collect_feedback=False)
    report.check("cold_start_post_service_labels_cannot_authorize_the_current_request",
                 not first.served_hits and not second.served_hits and evidence_before_reveal is None
                 and learned is not None and learned.groups == 1)
    report.check("post_service_learning_is_available_to_the_next_request_and_real_downstream",
                 len(third.served_hits) == 1 and third.served_hits[0]["input_equal"] is False
                 and third.state["answer"] == 5 and third.executed_node_ids == ("after",))

    report.output_dir.mkdir(parents=True, exist_ok=True)
    (report.output_dir / "served_hit_capture.json").write_text(json.dumps({
        "check_type": "local_functional_smoke", "network_used": False,
        "frozen_hits": rows, "cold_start_served_hits": third.served_hits,
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(run_report("b_experiment_interfaces", "B：独立历史快照、实际命中捕获与后置学习", checks))
