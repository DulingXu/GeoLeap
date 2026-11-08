"""Local smoke checks for paper IV-B; no service calls or experiments."""

from __future__ import annotations

import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from reporting import CheckReport, run_report
from fixtures import six_node_workflow
from workflow.records import CacheSnapshot, CachedRecord
from workflow.reliability import Matcher, ReliabilityConfig, ReliabilityTable


def checks(report: CheckReport) -> None:
    workflow, (_, spec, other_spec), initial, _ = six_node_workflow()
    epoch = "check-b-epoch-1"
    record = CachedRecord.create(spec, spec.project(initial), {"v2": 2}, epoch,
                                 record_id="b-good-record")
    snapshot = CacheSnapshot(1, (record,), frozenset())
    now = [1000.0]
    config = ReliabilityConfig(min_groups=3, max_score=0.2, max_age_seconds=60)
    table = ReliabilityTable(config, clock=lambda: now[0])
    matcher = Matcher(table)
    similarity = matcher.similarity(spec.project(initial), record)

    cold = matcher.eligible(spec, initial, snapshot, epoch, allow_exact=False)
    report.check("cold_approximate_record_is_rejected", not cold,
                 {"rejections": matcher.last_rejections})

    exact = matcher.eligible(spec, initial, snapshot, epoch)
    report.check("complete_equal_stable_input_uses_exact_path",
                 len(exact) == 1 and exact[0].exact and exact[0].score == 0.0)

    audit_snapshot = CacheSnapshot(1, (record,), frozenset({record.record_id}))
    audit = matcher.eligible(spec, initial, audit_snapshot, epoch)
    report.check("audit_only_record_cannot_serve_even_exact", not audit,
                 {"rejections": matcher.last_rejections})
    report.check("audit_comparison_can_check_compatible_record",
                 matcher.compatible(spec, record, spec.project(initial), epoch))

    for _ in range(5):
        table.observe(spec, record, epoch, similarity, "same-request", 0.0)
    table.refresh()
    evidence = table.lookup(spec, record, epoch, similarity)
    grouped = matcher.eligible(spec, initial, snapshot, epoch, allow_exact=False)
    report.check("repeated_labels_in_one_request_count_as_one_group",
                 evidence is not None and evidence.groups == 1 and not grouped,
                 {"groups": evidence.groups if evidence else None})

    for request in ("second-request", "third-request"):
        table.observe(spec, record, epoch, similarity, request, 0.0)
    before_refresh = matcher.eligible(spec, initial, snapshot, epoch, allow_exact=False)
    report.check("new_feedback_requires_offline_refresh", not before_refresh)
    table.refresh()
    supported = matcher.eligible(spec, initial, snapshot, epoch, allow_exact=False)
    evidence = table.lookup(spec, record, epoch, similarity)
    report.check("supported_low_disagreement_record_is_eligible",
                 len(supported) == 1 and not supported[0].exact and evidence.groups == 3
                 and evidence.mean == 0.0 and evidence.score == 0.0,
                 None if evidence is None else {
                     "groups": evidence.groups, "mean": evidence.mean,
                     "margin": evidence.margin, "score": evidence.score,
                 })

    bad = CachedRecord.create(spec, spec.project(initial), {"v2": 200}, epoch,
                              record_id="b-disagreeing-record")
    for request in ("bad-1", "bad-2", "bad-3"):
        table.observe(spec, bad, epoch, similarity, request, 1.0)
    table.refresh()
    high_disagreement = matcher.eligible(spec, initial, CacheSnapshot(2, (bad,), frozenset()),
                                         epoch, allow_exact=False)
    report.check("high_output_disagreement_record_is_rejected", not high_disagreement,
                 {"rejections": matcher.last_rejections,
                  "score": table.lookup(spec, bad, epoch, similarity).score})

    # Scores belong to the immutable revision, not merely the structural ID.
    replacement = CachedRecord.create(spec, spec.project(initial), {"v2": 2}, epoch,
                                      record_id=record.record_id, revision=2)
    report.check("new_revision_does_not_inherit_old_support",
                 table.lookup(spec, replacement, epoch, similarity) is None)
    report.check("different_subgraph_does_not_inherit_support",
                 table.lookup(other_spec, record, epoch, similarity) is None)

    changed_epoch = matcher.eligible(spec, initial, snapshot, "check-b-epoch-2")
    report.check("configuration_epoch_change_rejects_record", not changed_epoch)
    missing_input = matcher.eligible(spec, {}, snapshot, epoch)
    report.check("missing_declared_input_is_rejected", not missing_input)
    schema_mismatch = matcher.eligible(spec, {"v0": "zero"}, snapshot, epoch)
    report.check("input_schema_mismatch_is_rejected", not schema_mismatch,
                 {"rejections": matcher.last_rejections})
    report.check("different_similarity_bin_does_not_inherit_support",
                 table.lookup(spec, record, epoch, 0.95) is None)

    guarded = workflow.subgraph("Sb_with_critical_input", ("n1", "n2"), critical_fields=("v0",))
    guarded_record = CachedRecord.create(guarded, {"v0": 0}, {"v2": 2}, epoch,
                                         record_id="b-critical-input")
    permissive_matcher = Matcher(table, sim_threshold=-1.0)
    changed_input = {"v0": 1}
    changed_similarity = permissive_matcher.similarity(changed_input, guarded_record)
    for request in ("guard-1", "guard-2", "guard-3"):
        table.observe(guarded, guarded_record, epoch, changed_similarity, request, 0.0)
    table.refresh()
    guarded_hit = permissive_matcher.eligible(
        guarded, changed_input, CacheSnapshot(3, (guarded_record,), frozenset()), epoch)
    report.check("critical_field_guard_overrides_similarity_and_support", not guarded_hit,
                 {"similarity": changed_similarity, "retrieval_threshold": -1.0,
                  "rejections": permissive_matcher.last_rejections})

    averaged_table = ReliabilityTable(ReliabilityConfig(min_groups=2, bootstrap_samples=100))
    averaged_table.observe(spec, record, epoch, similarity, "group-a", 1.0)
    averaged_table.observe(spec, record, epoch, similarity, "group-a", 0.0)
    averaged_table.observe(spec, record, epoch, similarity, "group-b", 0.0)
    averaged_table.refresh()
    averaged = averaged_table.lookup(spec, record, epoch, similarity)
    report.check("disagreement_averages_within_request_before_across_requests",
                 averaged is not None and averaged.groups == 2 and averaged.mean == 0.25,
                 None if averaged is None else {"mean": averaged.mean, "groups": averaged.groups,
                                               "margin": averaged.margin, "score": averaged.score})

    now[0] += 61.0
    stale = matcher.eligible(spec, initial, snapshot, epoch, allow_exact=False)
    report.check("expired_calibration_is_rejected", not stale)

    bounded_table = ReliabilityTable(ReliabilityConfig(min_groups=1, max_groups=3,
                                                       bootstrap_samples=20))
    for index, disagreement in enumerate((0.0, 1.0, 0.0, 0.0, 0.0)):
        bounded_table.observe(spec, record, epoch, similarity, f"bounded-{index}", disagreement)
    bounded_table.refresh()
    bounded = bounded_table.lookup(spec, record, epoch, similarity)
    encoded = bounded_table.export_record(record.record_id)
    report.check("calibration_history_and_serialized_metadata_are_bounded",
                 bounded.groups == 3 and len(encoded) <= record.metadata_capacity,
                 {"groups": bounded.groups, "metadata_bytes": len(encoded),
                  "metadata_capacity_bytes": record.metadata_capacity,
                  "export_is_json": bool(json.loads(encoded))})

    # This separate record intentionally has no evidence allocation available.
    tiny = CachedRecord.create(spec, spec.project(initial), {"v2": 2}, epoch,
                               record_id="b-metadata-limit", metadata_capacity=1)
    accepted = bounded_table.observe(spec, tiny, epoch, similarity, "over-budget", 0.0)
    report.check("metadata_budget_prevents_unbounded_feedback", not accepted)
    bounded_table.discard_records({tiny.record_id})
    report.check("eviction_discards_record_feedback",
                 bounded_table.lookup(spec, record, epoch, similarity) is None)


if __name__ == "__main__":
    raise SystemExit(run_report("b_reliability_matching", "B：可靠性校准匹配", checks))
