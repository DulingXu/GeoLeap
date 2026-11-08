"""Check B1 policy switches locally, including lookup and runtime commit.

These are deterministic functional cases with declared integer operators.
They do not run an ablation experiment, a dataset, or an external service.
"""

from __future__ import annotations

import json
import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from reporting import CheckReport, run_report
from fixtures import six_node_workflow
from workflow.b1 import B1Config, add_b1_arguments, b1_config_from_args
from workflow.records import CacheSnapshot, CachedRecord, RecordStore
from workflow.reliability import Matcher, ReliabilityConfig, ReliabilityTable, ScreeningMode
from workflow.runtime import GeoLeapRuntime, RuntimeConfig


EPOCH = "b1-switch-check-v1"
MAX_SCORE = 0.25
MODES = (ScreeningMode.SIMILARITY, ScreeningMode.NO_MARGIN,
         ScreeningMode.MIN1, ScreeningMode.FULL)


def preset(mode):
    return B1Config(variant=mode.value, min_groups=3, max_score=MAX_SCORE,
                    similarity_threshold=0.8)


def seed(table, spec, record, labels, scenario):
    for index, disagreement in enumerate(labels):
        if not table.observe(spec, record, EPOCH, 1.0, f"{scenario}-{index}", disagreement):
            raise RuntimeError("The fixed functional history did not fit its metadata reserve")
    table.refresh()


def forbid_evidence_lookup(table):
    reads = {"count": 0}

    def fail_on_read(*args, **kwargs):
        reads["count"] += 1
        raise AssertionError("Similarity screening must not read reliability evidence")

    table.lookup = fail_on_read
    return reads


def decision_row(scenario, mode, config, accepted, descriptor=None, evidence=None, **details):
    return {
        "scenario": scenario,
        "variant": mode.value,
        "thresholds": config.to_dict(),
        "accepted": bool(accepted),
        "decision": "accept" if accepted else "reject",
        "score": descriptor.score if descriptor is not None else None,
        "score_rule": config.reliability_config().score_rule,
        "historical_evidence": None if evidence is None else {
            "groups": evidence.groups, "mean": evidence.mean,
            "margin": evidence.margin, "full_score": evidence.score,
        },
        **details,
    }


def checks(report: CheckReport) -> None:
    workflow, (_, spec, _), initial, counters = six_node_workflow()
    record = CachedRecord.create(spec, spec.project(initial), {"v2": 2}, EPOCH,
                                 record_id="b1-fixed-serving-record")
    store = RecordStore()
    published = store.publish((record,), frozenset(), store.version, record.byte_size)
    report.check("fixed_serving_record_pool_is_preseeded", published,
                 {"record_ids": [item.record_id for item in store.snapshot().records],
                  "record_bytes": record.byte_size})
    snapshot = store.snapshot()
    decisions = []
    report.check("variant_labels_and_aliases_are_normalized",
                 all(B1Config(variant=label).variant == expected for label, expected in (
                     ("Similarity", ScreeningMode.SIMILARITY), ("NoMargin", ScreeningMode.NO_MARGIN),
                     ("no-margin", ScreeningMode.NO_MARGIN), ("Min1", ScreeningMode.MIN1),
                     ("Full", ScreeningMode.FULL))))
    invalid_rejected = False
    try:
        B1Config(variant="unrecognized")
    except ValueError:
        invalid_rejected = True
    report.check("unknown_variant_is_rejected_instead_of_silently_using_full", invalid_rejected)
    parser = add_b1_arguments(argparse.ArgumentParser(add_help=False))
    args = parser.parse_args(["--b1-variant", "NoMargin", "--b1-min-groups", "5",
                              "--b1-max-score", "0.3", "--b1-sim-threshold", "0.9"])
    parsed = b1_config_from_args(args)
    report.check("command_line_and_python_api_resolve_the_same_switches",
                 parsed == B1Config("no_margin", 5, 0.3, 0.9), parsed.to_dict())
    factory_runtime = preset(ScreeningMode.NO_MARGIN).make_runtime(
        workflow, (spec,), config=RuntimeConfig(epoch=EPOCH, allow_exact=True, screening_budget=0.01), store=store)
    report.check("single_runtime_factory_applies_variant_exact_and_budget_together",
                 factory_runtime.matcher.config.screening_mode == ScreeningMode.NO_MARGIN
                 and factory_runtime.config.allow_exact is False and factory_runtime.matcher.allow_exact is False
                 and factory_runtime.config.screening_budget is None,
                 {"variant": factory_runtime.matcher.config.screening_mode.value,
                  "allow_exact": factory_runtime.config.allow_exact,
                  "screening_budget": factory_runtime.config.screening_budget})
    policies = [preset(mode) for mode in MODES]
    retrieval_orders = {policy.to_dict()["effective"]["retrieval_order"] for policy in policies}
    report.check("all_b1_variants_use_the_same_feedback_independent_retrieval_order",
                 all(not policy.reliability_config().rank_by_score for policy in policies)
                 and len(retrieval_orders) == 1,
                 {"retrieval_orders": sorted(retrieval_orders), "rank_by_score": False})

    # Keep both the record pool and the observed labels identical across all
    # policies. The mean passes while a positive bootstrap margin rejects.
    mixed_labels = (1.0, 0.0, 0.0, 0.0, 0.0)
    expected_mixed = {
        ScreeningMode.SIMILARITY: True, ScreeningMode.NO_MARGIN: True,
        ScreeningMode.MIN1: False, ScreeningMode.FULL: False,
    }
    for mode in MODES:
        config = preset(mode)
        table = ReliabilityTable(config.reliability_config())
        seed(table, spec, record, mixed_labels, "fixed-mixed-history")
        evidence = None
        reads = None
        if mode is ScreeningMode.SIMILARITY:
            reads = forbid_evidence_lookup(table)
        else:
            evidence = table.lookup(spec, record, EPOCH, 1.0)
        matcher = config.make_matcher(table)
        accepted = matcher.eligible(spec, initial, snapshot, EPOCH)
        descriptor = accepted[0] if accepted else None
        row = decision_row("same_mixed_feedback", mode, config, accepted, descriptor, evidence,
                           rejection_reasons=matcher.last_rejections,
                           exact_allowed=False,
                           evidence_reads=reads["count"] if reads else None)
        decisions.append(row)
        report.check(f"{mode.value}_uses_expected_screening_rule_on_fixed_feedback",
                     bool(accepted) == expected_mixed[mode], row)
        if descriptor is not None:
            report.check(f"{mode.value}_b1_preset_disables_exact_bypass", not descriptor.exact)
        if mode is ScreeningMode.SIMILARITY:
            report.check("similarity_has_no_empirical_score_and_does_not_read_evidence",
                         descriptor is not None and descriptor.score is None and reads["count"] == 0)
        if mode is ScreeningMode.NO_MARGIN:
            report.check("no_margin_returns_mean_as_actual_descriptor_score",
                         descriptor is not None and descriptor.score == evidence.mean
                         and evidence.mean <= MAX_SCORE < evidence.score)

    # All variants see one observed request group. Only Min1 lowers the support
    # threshold; the other historical rules retain the configured three groups.
    expected_one_group = {
        ScreeningMode.SIMILARITY: True, ScreeningMode.NO_MARGIN: False,
        ScreeningMode.MIN1: True, ScreeningMode.FULL: False,
    }
    for mode in MODES:
        config = preset(mode)
        table = ReliabilityTable(config.reliability_config())
        seed(table, spec, record, (0.0,), "fixed-one-group-history")
        evidence = None if mode is ScreeningMode.SIMILARITY else table.lookup(spec, record, EPOCH, 1.0)
        if mode is ScreeningMode.SIMILARITY:
            forbid_evidence_lookup(table)
        matcher = config.make_matcher(table)
        accepted = matcher.eligible(spec, initial, snapshot, EPOCH)
        row = decision_row("same_single_request_group", mode, config, accepted,
                           accepted[0] if accepted else None, evidence,
                           rejection_reasons=matcher.last_rejections)
        decisions.append(row)
        report.check(f"{mode.value}_uses_expected_minimum_group_threshold",
                     bool(accepted) == expected_one_group[mode], row)
        if mode is ScreeningMode.MIN1:
            report.check("min1_single_group_bootstrap_margin_is_zero",
                         evidence.groups == 1 and evidence.margin == 0.0
                         and config.reliability_config().effective_min_groups == 1)

    # Shared safeguards must stay active even in the similarity-only policy.
    guarded = workflow.subgraph("Sb_with_critical_v0", ("n1", "n2"), critical_fields=("v0",))
    guarded_record = CachedRecord.create(guarded, {"v0": 0}, {"v2": 2}, EPOCH,
                                         record_id="b1-guarded-record")
    for mode in MODES:
        config = preset(mode)
        table = ReliabilityTable(config.reliability_config())
        seed(table, spec, record, (0.0, 0.0, 0.0), "fixed-guard-history")
        if mode is ScreeningMode.SIMILARITY:
            forbid_evidence_lookup(table)
        matcher = config.make_matcher(table)
        schema_rejected = not matcher.eligible(spec, {"v0": "wrong-type"}, snapshot, EPOCH)
        epoch_rejected = not matcher.eligible(spec, initial, snapshot, "changed-epoch")
        critical_rejected = not matcher.eligible(
            guarded, {"v0": 1}, CacheSnapshot(snapshot.version, (guarded_record,)), EPOCH)
        critical_reason = list(matcher.last_rejections)
        critical_guard_checked_first = any(
            item.get("reason") == "incompatible_schema_epoch_or_critical_fields"
            for item in critical_reason
        )
        audit_rejected = not matcher.eligible(
            spec, initial, CacheSnapshot(snapshot.version, (record,), frozenset({record.record_id})), EPOCH)
        report.check(f"{mode.value}_retains_schema_epoch_critical_and_audit_guards",
                     schema_rejected and epoch_rejected and critical_rejected and audit_rejected
                     and critical_guard_checked_first,
                     {"schema_rejected": schema_rejected, "epoch_rejected": epoch_rejected,
                      "critical_field_rejected": critical_rejected, "audit_only_rejected": audit_rejected,
                      "critical_guard_precedes_similarity_and_evidence": critical_guard_checked_first,
                      "critical_field_rejection_reason": critical_reason})

    # This catches a subtle integration bug: a policy may pass lookup but fail
    # commit if the executor silently reverts to Full during revalidation.
    runtime_cases = (
        (ScreeningMode.NO_MARGIN, mixed_labels, True),
        (ScreeningMode.FULL, mixed_labels, False),
        (ScreeningMode.MIN1, (0.0,), True),
        (ScreeningMode.SIMILARITY, (), True),
        (ScreeningMode.FULL, (), False),
    )
    for mode, labels, expected_reuse in runtime_cases:
        config = preset(mode)
        runtime = GeoLeapRuntime(
            workflow, (spec,), config=config.runtime_config(RuntimeConfig(epoch=EPOCH)),
            reliability_config=config.reliability_config(), store=store,
        )
        if labels:
            seed(runtime.reliability, spec, record, labels, "fixed-runtime-history")
        runtime_evidence = runtime.reliability.lookup(spec, record, EPOCH, 1.0) if labels else None
        reads = forbid_evidence_lookup(runtime.reliability) if mode is ScreeningMode.SIMILARITY else None
        for key in counters:
            counters[key] = 0
        result = runtime.run(initial, request_id=f"b1-runtime-{mode.value}-{len(labels)}",
                             collect_records=False)
        actual_reuse = result.reused_subgraphs == (spec.name,)
        expected_nodes = ("n3", "n4", "n5", "n6") if expected_reuse else workflow.topological_order
        correct_calls = all(counters[key] == (0 if expected_reuse and key in ("n1", "n2") else 1)
                            for key in counters)
        reuse_events = [item for item in result.trace if item.get("event") == "reuse"]
        row = decision_row("runtime_lookup_and_commit", mode, config, actual_reuse,
                           evidence=runtime_evidence,
                           score=reuse_events[0]["score"] if reuse_events else None,
                           normal_execution_nodes=list(result.executed_node_ids),
                           reused_subgraphs=list(result.reused_subgraphs),
                           reused_record_ids=[item["record_id"] for item in reuse_events],
                           feedback_groups=len(labels), call_counts=dict(counters),
                           final_output=result.state["v6"],
                           commit_score=reuse_events[0]["score"] if reuse_events else None,
                           evidence_reads=reads["count"] if reads else None)
        decisions.append(row)
        suffix = "mixed_history" if len(labels) > 1 else ("single_group" if labels else "no_evidence")
        report.check(f"runtime_{mode.value}_{suffix}_uses_same_policy_at_lookup_and_commit",
                     actual_reuse == expected_reuse and tuple(result.executed_node_ids) == tuple(expected_nodes)
                     and correct_calls and result.state["v6"] == 6
                     and result.state["untouched"] == "keep", row)
        if mode is ScreeningMode.SIMILARITY:
            report.check("runtime_similarity_neither_fabricates_zero_score_nor_reads_evidence",
                         bool(reuse_events) and reuse_events[0]["score"] is None and reads["count"] == 0)
    report.check("functional_switch_checks_keep_original_serving_pool_unchanged",
                 store.snapshot() == snapshot)

    shared_table = ReliabilityTable(ReliabilityConfig())
    seed(shared_table, spec, record, mixed_labels, "shared-calibration")
    evidence_before = shared_table.export_record(record.record_id)
    shared_results = {mode.value: bool(preset(mode).make_matcher(shared_table).eligible(
        spec, initial, snapshot, EPOCH)) for mode in MODES}
    report.check("four_rules_can_read_one_history_without_rewriting_evidence",
                 shared_results == {mode.value: accepted for mode, accepted in expected_mixed.items()}
                 and shared_table.export_record(record.record_id) == evidence_before,
                 {"decisions": shared_results})
    good = CachedRecord.create(spec, spec.project(initial), {"v2": 2}, EPOCH, record_id="z-good")
    moderate = CachedRecord.create(spec, spec.project(initial), {"v2": 2}, EPOCH, record_id="a-moderate")
    seed(shared_table, spec, good, (0.0, 0.0, 0.0), "ranking-good")
    seed(shared_table, spec, moderate, (0.1, 0.1, 0.1), "ranking-moderate")
    tied_view = CacheSnapshot(2, (good, moderate))
    ordinary = Matcher(shared_table).eligible(spec, initial, tied_view, EPOCH, allow_exact=False)
    b1_tied = [preset(mode).make_matcher(shared_table).eligible(spec, initial, tied_view, EPOCH) for mode in MODES]
    report.check("ordinary_full_retains_score_ties_while_b1_fixes_common_retrieval_order",
                 [item.record.record_id for item in ordinary] == ["z-good", "a-moderate"]
                 and all([item.record.record_id for item in items] == ["a-moderate", "z-good"] for items in b1_tied),
                 {"ordinary_order": [item.record.record_id for item in ordinary],
                  "b1_order": [item.record.record_id for item in b1_tied[0]]})

    similarity_budget_rejected = False
    try:
        GeoLeapRuntime(workflow, (spec,),
                      config=RuntimeConfig(epoch=EPOCH, allow_exact=False, screening_budget=0.1),
                      reliability_config=preset(ScreeningMode.SIMILARITY).reliability_config(), store=store)
    except ValueError:
        similarity_budget_rejected = True
    report.check("similarity_cannot_apply_budget_without_empirical_scores", similarity_budget_rejected)

    report.output_dir.mkdir(parents=True, exist_ok=True)
    output = {
        "check_type": "local_functional_smoke", "experiment_run": False,
        "network_used": False, "exact_allowed": False,
        "fixed_record_ids": [record.record_id], "decisions": decisions,
    }
    path = report.output_dir / "switch_decisions.json"
    path.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    report.check("switch_decisions_are_written_for_later_inspection", path.is_file(),
                 {"path": str(path.resolve()), "decision_count": len(decisions)})


if __name__ == "__main__":
    raise SystemExit(run_report("b1_screening_switches", "B1：四种筛选开关的本地功能检查", checks))
