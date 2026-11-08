"""Local B2 checks for natural feedback, audit provenance and bin status.

Only deterministic in-process callables are used. No dataset or API is run.
"""

from __future__ import annotations

from dataclasses import asdict, replace
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from reporting import CheckReport, run_report
from fixtures import six_node_workflow
from workflow.records import CachedRecord, RecordStore
from workflow.observability import current_operation_context
from workflow.reliability import ReliabilityConfig, ReliabilityTable, ScreeningMode
from workflow.runtime import GeoLeapRuntime, RuntimeConfig
from workflow.residency import ResidencyConfig
from workflow.spec import OperatorSpec, WorkflowSpec, canonical_json


EPOCH = "b2-runtime-feedback-check-v1"


def qualification_checks(report):
    _, (_, spec, _), initial, _ = six_node_workflow()
    record = CachedRecord.create(spec, spec.project(initial), {"v2": 2}, EPOCH,
                                 record_id="b2-qualification-record")
    now = [100.0]
    table = ReliabilityTable(ReliabilityConfig(min_groups=3, max_score=0.2,
                                               max_age_seconds=5), clock=lambda: now[0])
    before = table.export_record(record.record_id)
    empty = table.qualification_snapshot(spec, record, EPOCH)
    report.check("qualification_snapshot_includes_every_unobserved_bin",
                 len(empty) == 4 and all(not row["observed"] and row["groups"] == 0
                                        and not row["qualified"] for row in empty)
                 and [row["bin"] for row in empty] == [0, 1, 2, 3],
                 {"bins": list(empty)})
    report.check("qualification_snapshot_is_read_only",
                 table.export_record(record.record_id) == before)
    for index in range(3):
        table.observe(spec, record, EPOCH, 1.0, f"bad-{index}", 1.0)
    pending = table.qualification_snapshot(spec, record, EPOCH)[-1]
    report.check("unrefreshed_groups_do_not_authorize_reuse",
                 pending["groups"] == 3 and pending["enough_groups"]
                 and not pending["refreshed"] and not pending["qualified"]
                 and pending["rejection_reason"] == "not_refreshed", pending)
    table.refresh()
    rejected = table.qualification_snapshot(spec, record, EPOCH)[-1]
    report.check("minimum_support_and_full_qualification_are_separate",
                 rejected["enough_groups"] and not rejected["qualified"]
                 and rejected["score"] == 1.0
                 and rejected["rejection_reason"] == "disagreement_score_above_threshold", rejected)
    for index in range(3):
        table.observe(spec, record, EPOCH, 0.95, f"good-{index}", 0.0)
    table.refresh()
    qualified = table.qualification_snapshot(spec, record, EPOCH)[2]
    report.check("qualified_bin_reports_full_empirical_statistics",
                 qualified["groups"] == 3 and qualified["mean"] == 0.0
                 and qualified["margin"] == 0.0 and qualified["score"] == 0.0
                 and qualified["qualified"] and qualified["bin_lower"] == 0.9
                 and qualified["bin_upper"] == 0.97, qualified)
    qualified["qualified"] = False
    report.check("returned_status_rows_do_not_mutate_internal_statistics",
                 table.qualification_snapshot(spec, record, EPOCH)[2]["qualified"])
    changed = replace(record, output_json=canonical_json({"v2": 200}))
    altered = table.qualification_snapshot(spec, changed, EPOCH)[2]
    epoch_rows = table.qualification_snapshot(spec, record, "another-epoch")
    report.check("status_snapshot_rejects_changed_record_content_and_epoch",
                 not altered["record_valid"] and not altered["qualified"]
                 and altered["rejection_reason"] == "immutable_record_content_mismatch"
                 and all(not row["record_valid"] and not row["qualified"] for row in epoch_rows),
                 {"changed_record": altered, "wrong_epoch": list(epoch_rows)})
    now[0] += 6
    expired = table.qualification_snapshot(spec, record, EPOCH)[2]
    report.check("stale_support_remains_visible_but_is_not_qualified",
                 expired["groups"] == 3 and expired["enough_groups"] and expired["stale"]
                 and not expired["qualified"] and expired["rejection_reason"] == "stale_evidence", expired)
    similarity = ReliabilityTable(ReliabilityConfig(screening_mode=ScreeningMode.SIMILARITY))
    unobserved = similarity.qualification_snapshot(spec, record, EPOCH)
    report.check("similarity_policy_does_not_fabricate_historical_support",
                 all(not row["history_required"] and not row["observed"]
                     and not row["qualified"] and row["groups"] == 0 for row in unobserved))


def instrumented_workflow():
    original, _, initial, counters = six_node_workflow()
    contexts = []
    operators = []
    for operator in original.operators:
        def execute(inputs, wrapped=operator.function):
            contexts.append(dict(current_operation_context()))
            return wrapped(inputs)
        operators.append(replace(operator, function=execute))
    return WorkflowSpec("b2_instrumented_chain", operators), initial, counters, contexts


def runtime_checks(report):
    workflow, initial, counters, contexts = instrumented_workflow()
    whole = workflow.subgraph("whole_chain", workflow.topological_order)
    operations = []
    runtime = GeoLeapRuntime(
        workflow, (whole,),
        config=RuntimeConfig(epoch=EPOCH, allow_exact=False, feedback_on_normal_execution=True),
        reliability_config=ReliabilityConfig(min_groups=3),
        residency_config=ResidencyConfig(max_bytes=512000, audit_fraction=0.5),
        operation_observer=lambda event: operations.append(dict(event)),
    )
    traces = {}
    cold = runtime.run(initial, request_id="b2-cold")
    traces["cold"] = asdict(cold)
    runtime.maintenance_tick()
    snapshot = runtime.store.snapshot(include_audit=True)
    record = snapshot.records[0]
    statuses = runtime.qualification_snapshot()
    record_rows = [row for row in statuses if row["record_id"] == record.record_id]
    report.check("cold_request_creates_audit_record_without_self_label",
                 cold.executed_node_ids == workflow.topological_order and not cold.reused_subgraphs
                 and len(operations) == 6 and len(contexts) == 6
                 and not any(item.get("event") == "output_feedback" for item in cold.trace)
                 and record_rows and all(row["groups"] == 0 and not row["qualified"]
                                         and row["audit_only"] and not row["serving"] for row in record_rows),
                 {"record_id": record.record_id, "operation_count": len(operations),
                  "qualification_rows": record_rows})
    for index in range(1, 4):
        request_id = f"b2-natural-{index}"
        before_counts, before_operations, before_contexts = dict(counters), len(operations), len(contexts)
        result = runtime.run(initial, request_id=request_id, collect_records=False)
        traces[request_id] = asdict(result)
        added_operations, added_contexts = operations[before_operations:], contexts[before_contexts:]
        feedback = [item for item in result.trace if item.get("event") == "output_feedback"]
        evidence = runtime.reliability.lookup(whole, record, EPOCH, 1.0)
        report.check(f"normal_request_{index}_adds_feedback_without_extra_execution",
                     result.executed_node_ids == workflow.topological_order and not result.reused_subgraphs
                     and all(counters[key] - before_counts[key] == 1 for key in counters)
                     and len(added_operations) == 6 and len(added_contexts) == 6
                     and all(item["purpose"] == "serving" for item in added_operations)
                     and all(item["request_id"] == request_id and item["purpose"] == "serving"
                             for item in added_contexts)
                     and len(feedback) == 1 and feedback[0]["kind"] == "normal_execution"
                     and feedback[0]["record_id"] == record.record_id
                     and evidence is not None and evidence.groups == index and evidence.score == 0.0,
                     {"feedback": feedback, "normal_call_count": len(added_operations),
                      "groups": evidence.groups if evidence else None,
                      "operation_contexts": added_contexts})
        if index == 3:
            qualified_row = next(row for row in runtime.qualification_snapshot()
                                 if row["record_id"] == record.record_id and row["bin"] == 3)
            report.check("new_evidence_does_not_authorize_the_same_request_or_implicitly_promote",
                         not result.reused_subgraphs and len(result.executed_node_ids) == 6
                         and qualified_row["qualified"] and qualified_row["audit_only"]
                         and not qualified_row["serving"] and not runtime.store.snapshot().records,
                         qualified_row)
        maintenance = runtime.maintenance_tick()
        traces[f"{request_id}_maintenance"] = [asdict(item) for item in maintenance]
    report.check("maintenance_promotes_supported_record_separately_from_full_qualification",
                 len(runtime.store.snapshot().records) == 1
                 and record.record_id not in runtime.store.snapshot(include_audit=True).audit_ids)
    before_counts, before_operations = dict(counters), len(operations)
    warm = runtime.run(initial, request_id="b2-warm", collect_records=False)
    traces["warm"] = asdict(warm)
    report.check("next_request_reuses_after_natural_feedback_and_maintenance",
                 warm.reused_subgraphs == (whole.name,) and not warm.executed_node_ids
                 and warm.state["v6"] == 6 and counters == before_counts
                 and len(operations) == before_operations)

    before_counts, before_operations, before_contexts = dict(counters), len(operations), len(contexts)
    audited = runtime.run(initial, request_id="b2-audited-hit", collect_records=False,
                          audit_hits=True, audit_probability=1.0)
    traces["actual_hit_audit"] = asdict(audited)
    audit_operations, audit_contexts = operations[before_operations:], contexts[before_contexts:]
    audit_feedback = [item for item in audited.trace if item.get("event") == "output_feedback"
                      and item.get("kind") == "actual_hit"]
    report.check("actual_hit_audit_operations_are_separate_from_serving_calls",
                 audited.reused_subgraphs == (whole.name,) and not audited.executed_node_ids
                 and len(audit_operations) == 6 and len(audit_contexts) == 6
                 and all(counters[key] - before_counts[key] == 1 for key in counters)
                 and all(item["event"] == "operation_complete" and item["purpose"] == "extra_audit"
                         for item in audit_operations)
                 and all(item["purpose"] == "extra_audit" and item["request_id"] == "b2-audited-hit"
                         for item in audit_contexts)
                 and audited.audit_cost_usd > 0 and len(audit_feedback) == 1,
                 {"audit_operation_count": len(audit_operations), "audit_cost_usd": audited.audit_cost_usd,
                  "operation_contexts": audit_contexts, "feedback": audit_feedback})

    # The observer API can process an already available no-reuse result. It
    # must neither re-run operators nor promote records while collecting it.
    pre_request = runtime.store.snapshot(include_audit=True)
    manual = runtime.run(initial, request_id="b2-public-feedback", use_cache=False,
                         collect_records=False, collect_feedback=False)
    before_public = len(operations)
    returned_feedback = runtime.observe_reference_result(manual, pre_request)
    public_evidence = runtime.reliability.lookup(whole, record, EPOCH, 1.0)
    report.check("public_reference_feedback_reuses_available_outputs_without_rerunning",
                 len(operations) == before_public and len(returned_feedback) == 1
                 and public_evidence is not None and public_evidence.groups == 5,
                 {"returned_feedback": list(returned_feedback),
                  "groups": public_evidence.groups if public_evidence else None})
    before_baseline = len(operations)
    baseline = runtime.run(initial, request_id="b2-baseline-measurement", use_cache=False,
                           collect_records=False, collect_feedback=False,
                           execution_purpose="baseline_measurement")
    traces["baseline_measurement"] = asdict(baseline)
    baseline_operations = operations[before_baseline:]
    report.check("baseline_measurement_has_explicit_operation_role_and_no_calibration_leakage",
                 len(baseline_operations) == 6
                 and all(item["purpose"] == "baseline_measurement" for item in baseline_operations)
                 and runtime.reliability.lookup(whole, record, EPOCH, 1.0).groups == 5,
                 {"baseline_operation_count": len(baseline_operations), "groups_unchanged": 5})

    # A larger region containing an internally substituted block is not a fresh
    # reference. Comparing its stitched output would incorrectly self-support it.
    other_workflow, state, call_counts, other_contexts = instrumented_workflow()
    larger = other_workflow.subgraph("Sa", ("n1", "n2", "n3", "n4"))
    smaller = other_workflow.subgraph("Sb", ("n1", "n2"))
    large_record = CachedRecord.create(larger, {"v0": 0}, {"v4": 4}, EPOCH,
                                       record_id="b2-larger-audit")
    small_record = CachedRecord.create(smaller, {"v0": 0}, {"v2": 2}, EPOCH,
                                       record_id="b2-smaller-serving")
    other_store = RecordStore()
    assert other_store.publish((large_record, small_record), frozenset({large_record.record_id}),
                               other_store.version, 512000)
    other_operations = []
    partial_runtime = GeoLeapRuntime(
        other_workflow, (larger, smaller), store=other_store,
        config=RuntimeConfig(epoch=EPOCH, allow_exact=False, feedback_on_normal_execution=True),
        reliability_config=ReliabilityConfig(min_groups=3),
        residency_config=ResidencyConfig(max_bytes=512000, audit_fraction=0.5),
        operation_observer=lambda event: other_operations.append(dict(event)),
    )
    for index in range(3):
        partial_runtime.reliability.observe(smaller, small_record, EPOCH, 1.0, f"support-{index}", 0.0)
    partial_runtime.reliability.refresh()
    partial = partial_runtime.run(state, request_id="b2-partial-substitution", collect_records=False)
    traces["partial_substitution"] = asdict(partial)
    large_rows = partial_runtime.reliability.qualification_snapshot(larger, large_record, EPOCH)
    report.check("internally_substituted_region_never_supplies_fresh_reference_feedback",
                 partial.reused_subgraphs == ("Sb",)
                 and partial.executed_node_ids == ("n3", "n4", "n5", "n6")
                 and larger.identity not in partial.reference_outputs
                 and all(row["groups"] == 0 and not row["qualified"] for row in large_rows)
                 and not any(item.get("event") == "output_feedback" and item.get("subgraph") == "Sa"
                             for item in partial.trace)
                 and len(other_operations) == 4 and len(other_contexts) == 4,
                 {"executed_nodes": list(partial.executed_node_ids),
                  "reused_subgraphs": list(partial.reused_subgraphs),
                  "larger_region_qualification": list(large_rows), "call_counts": call_counts})

    # Disabling the exact shortcut does not prove that the input changed. This
    # separate text case checks actual unequal inputs in a supported lower bin.
    text_calls = []
    def respond(inputs):
        text_calls.append(inputs["query"])
        return {"answer": "workflow cache"}
    text_workflow = WorkflowSpec("b2_changed_input_workflow", (OperatorSpec(
        "respond", respond, read_fields=("query",), write_fields=("answer",),
        input_schema=(("query", "string"),), output_schema=(("answer", "string"),),
        cacheable=True, stable=False, effect_free=True, latency_ms=10, cost_usd=0.01,
    ),))
    text_spec = text_workflow.subgraph("text_answer", ("respond",))
    text_runtime = GeoLeapRuntime(
        text_workflow, (text_spec,),
        config=RuntimeConfig(epoch=EPOCH, allow_exact=False, feedback_on_normal_execution=True),
        reliability_config=ReliabilityConfig(min_groups=3),
        residency_config=ResidencyConfig(max_bytes=512000, audit_fraction=0.5),
    )
    original = "a long query about semantic cache matching a detailed workflow and function"
    live = original + " additional context "
    text_runtime.run({"query": original}, request_id="b2-text-cold")
    text_runtime.maintenance_tick()
    for index in range(3):
        text_runtime.run({"query": live}, request_id=f"b2-text-natural-{index}", collect_records=False)
        text_runtime.maintenance_tick()
    before_hit = len(text_calls)
    changed_input = text_runtime.run({"query": live}, request_id="b2-text-warm", collect_records=False)
    traces["changed_input_approximate_hit"] = asdict(changed_input)
    text_hit = next((item for item in changed_input.trace if item.get("event") == "reuse"), {})
    report.check("natural_feedback_enables_true_changed_input_approximate_hit",
                 changed_input.reused_subgraphs == (text_spec.name,)
                 and text_hit.get("exact") is False and text_hit.get("input_equal") is False
                 and text_hit.get("similarity_bin") == 2 and 0.9 <= text_hit.get("similarity", 0) < 0.97
                 and len(text_calls) == before_hit == 4 and changed_input.state["answer"] == "workflow cache",
                 {"original_input": original, "live_input": live,
                  "hit": text_hit, "call_count_before_hit": before_hit, "call_count_after_hit": len(text_calls)})

    # An optional reference failure must not discard the already restored
    # serving result, invent a label, or retain the selected record's pin.
    failed_operations = []
    def fail_only_in_audit(inputs):
        if current_operation_context()["purpose"] == "extra_audit":
            raise RuntimeError("intentional local reference failure")
        return {"v1": inputs["v0"] + 1}
    failing_workflow = WorkflowSpec("b2_failing_reference_workflow", (OperatorSpec(
        "step", fail_only_in_audit, read_fields=("v0",), write_fields=("v1",),
        cacheable=True, stable=True, effect_free=True, latency_ms=10, cost_usd=0.01,
    ),))
    failing_spec = failing_workflow.subgraph("cached_step", ("step",))
    failing_record = CachedRecord.create(failing_spec, {"v0": 0}, {"v1": 1}, EPOCH,
                                         record_id="b2-failing-reference-record")
    failing_store = RecordStore()
    assert failing_store.publish((failing_record,), frozenset(), 0, 512000)
    failing_runtime = GeoLeapRuntime(
        failing_workflow, (failing_spec,), store=failing_store,
        config=RuntimeConfig(epoch=EPOCH, allow_exact=False, feedback_on_normal_execution=True),
        reliability_config=ReliabilityConfig(min_groups=3),
        operation_observer=lambda event: failed_operations.append(dict(event)),
    )
    for index in range(3):
        failing_runtime.reliability.observe(failing_spec, failing_record, EPOCH, 1.0,
                                            f"failure-support-{index}", 0.0)
    failing_runtime.reliability.refresh()
    failed_reference = failing_runtime.run({"v0": 0}, request_id="b2-audit-failure",
                                           audit_hits=True, audit_probability=1.0, collect_records=False)
    traces["failed_optional_reference"] = asdict(failed_reference)
    report.check("failed_optional_audit_preserves_serving_output_and_releases_pin",
                 failed_reference.state["v1"] == 1 and not failed_reference.executed_node_ids
                 and failed_reference.reused_subgraphs == (failing_spec.name,)
                 and any(item.get("event") == "audit_failed" for item in failed_reference.trace)
                 and len(failed_operations) == 1 and failed_operations[0]["purpose"] == "extra_audit"
                 and failed_operations[0]["status"] == "error" and not failing_store.pinned_ids()
                 and failing_runtime.reliability.lookup(failing_spec, failing_record, EPOCH, 1.0).groups == 3,
                 {"served_state": failed_reference.state, "failed_operations": failed_operations,
                  "remaining_pins": sorted(failing_store.pinned_ids())})

    report.output_dir.mkdir(parents=True, exist_ok=True)
    trace_path = report.output_dir / "runtime_feedback_trace.json"
    trace_path.write_text(json.dumps({"execution_trace": traces, "operations": operations,
                                      "operator_contexts": contexts}, ensure_ascii=False, indent=2) + "\n",
                          encoding="utf-8")
    report.check("b2_feedback_and_operation_trace_is_saved", trace_path.is_file(),
                 {"path": str(trace_path.resolve())})


def checks(report: CheckReport) -> None:
    qualification_checks(report)
    runtime_checks(report)


if __name__ == "__main__":
    raise SystemExit(run_report("b2_runtime_feedback", "B2：自然反馈与冷启动运行接口检查", checks))
