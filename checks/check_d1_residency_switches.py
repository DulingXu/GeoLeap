"""Local D1 checks: four policies, equal byte budgets and lifecycle safety."""

from __future__ import annotations

import json
import math
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from check_d_opportunity_residency import make_record, preload
from fixtures import six_node_workflow
from reporting import CheckReport, run_report
from workflow.d1 import D1Config
from workflow.planner import PlannerConfig, PlanningMode
from workflow.records import RecordStore
from workflow.reliability import ReliabilityConfig
from workflow.residency import OpportunityCacheManager, ResidencyConfig, ResidencyMode


def checks(report: CheckReport):
    a, duplicate, b = (make_record(name) for name in ("r_a", "r_b", "r_c"))
    size = max(record.byte_size for record in (a, duplicate, b))
    budget = 2 * size

    def set_value(records):
        ids = {record.record_id for record in records}
        return (9.0 if ids & {a.record_id, duplicate.record_id} else 0.0) + (8.0 if b.record_id in ids else 0.0)

    profiles = {a.record_id: (900.0, 0.0), duplicate.record_id: (800.0, 0.0), b.record_id: (700.0, 0.0)}
    rows = []
    for mode in ResidencyMode:
        store = RecordStore()
        preload(store, (a, duplicate), budget)
        switches = D1Config(mode, budget / (1024 * 1024))
        manager = switches.make_manager(store, set_value, record_benefit_fn=lambda record: profiles[record.record_id])
        manager.advance_request()
        manager.record_access(a.record_id)
        result = manager.admit(b, now=1001)
        resident = {record.record_id for record in store.snapshot().records}
        expected_admission = mode != ResidencyMode.ADDITIVE_VALUE
        report.check(f"{mode.value}_has_expected_substitute_admission",
                     result.accepted == expected_admission
                     and (b.record_id in resident) == expected_admission
                     and store.byte_size <= budget,
                     {"resident_ids": sorted(resident), "reason": result.reason,
                      "utility_before": result.utility_before, "utility_after": result.utility_after})
        if mode == ResidencyMode.LRU:
            report.check("lru_uses_successful_access_not_record_creation_time",
                         resident == {a.record_id, b.record_id})
        if mode == ResidencyMode.FULL:
            report.check("full_keeps_one_substitute_and_new_opportunity",
                         len(resident & {a.record_id, duplicate.record_id}) == 1
                         and result.utility_before == 9.0 and result.utility_after == 17.0)
        if mode == ResidencyMode.ADDITIVE_VALUE:
            report.check("additive_value_uses_independent_sum_with_same_positive_gain_gate",
                         resident == {a.record_id, duplicate.record_id}
                         and result.utility_before == 18.0 and result.utility_after == 17.0
                         and result.reason == "utility_gain_below_transition_cost")
        stats = manager.maintenance_stats()
        report.check(f"{mode.value}_reports_physical_publication_and_maintenance_metrics",
                     stats["admitted_count"] == int(expected_admission)
                     and stats["evicted_count"] == int(expected_admission)
                     and stats["admitted_bytes"] == (b.byte_size if expected_admission else 0)
                     and stats["evicted_bytes"] == (size if expected_admission else 0)
                     and stats["peak_managed_bytes"] <= budget
                     and math.isfinite(stats["maintenance_cpu_ms"]) and stats["maintenance_cpu_ms"] >= 0
                     and math.isfinite(stats["maintenance_wall_ms"]) and stats["maintenance_wall_ms"] > 0,
                     stats)
        rows.append({"variant": mode.value, "accepted": result.accepted, "resident_ids": sorted(resident),
                     "managed_bytes": store.byte_size, "max_bytes": budget, "stats": stats})

    # A nonzero empty baseline is counted once; only singleton gains are added.
    additive = OpportunityCacheManager(RecordStore(), ResidencyConfig(policy="additive_value"),
                                      lambda records: 7.0 + set_value(records))
    report.check("additive_value_counts_empty_baseline_once",
                 additive._utility((a, duplicate), frozenset()) == 25.0
                 and additive._utility((), frozenset()) == 7.0
                 and additive._utility((a, b), frozenset((b.record_id,))) == 16.0)

    # Complementarity can cross the transition threshold only in set utility.
    def complement_value(records):
        ids = {record.record_id for record in records}
        if {a.record_id, b.record_id}.issubset(ids):
            return 10.0
        return sum({a.record_id: 2.0, duplicate.record_id: 1.0, b.record_id: 2.0}[key] for key in ids)

    complementary = {}
    for mode in (ResidencyMode.FULL, ResidencyMode.ADDITIVE_VALUE):
        store = RecordStore()
        preload(store, (a, duplicate), budget)
        manager = OpportunityCacheManager(store, ResidencyConfig(max_bytes=budget, policy=mode, kappa=3.0),
                                          complement_value)
        complementary[mode.value] = manager.admit(b, now=1001).accepted
    report.check("full_recognizes_complementarity_while_additive_holds_same_transition_threshold",
                 complementary == {"full": True, "additive_value": False}, complementary)

    legacy_store = RecordStore()
    preload(legacy_store, (a, duplicate), budget)
    legacy = OpportunityCacheManager(
        legacy_store, ResidencyConfig(max_bytes=budget, policy="legacy_density", legacy_alpha=2.0,
                                       legacy_beta=1.0, legacy_cost_reference=10.0,
                                       legacy_latency_reference_ms=100.0, legacy_half_life_requests=64.0,
                                       legacy_initial_reuse_weight=0.5),
        lambda records: (_ for _ in ()).throw(AssertionError("legacy must not run set planner")),
        record_benefit_fn=lambda record: (50.0, 2.0),
    )
    legacy.advance_request(1)
    legacy.record_access(a.record_id)
    legacy.record_access(a.record_id)
    density_before = legacy.legacy_density(a)
    legacy.advance_request(65)
    density_after = legacy.legacy_density(a)
    report.check("legacy_density_matches_old_normalized_weight_and_reuse_decay_formula",
                 math.isclose(density_before, 2.0 * 0.3 / a.byte_size, rel_tol=1e-12)
                 and math.isclose(density_after, 0.3 / a.byte_size, rel_tol=1e-12),
                 {"density_before": density_before, "after_one_half_life": density_after,
                  "source": "ByteCapacityCache._retention_score"})
    legacy_result = legacy.admit(b, now=1001)
    report.check("legacy_eviction_uses_independent_density_and_old_admission_rule",
                 legacy_result.accepted and legacy_store.get(a.record_id) is not None
                 and legacy_store.get(duplicate.record_id) is None)

    for mode in ResidencyMode:
        pinned_store = RecordStore()
        preload(pinned_store, (a,), size)
        manager = OpportunityCacheManager(pinned_store, ResidencyConfig(max_bytes=size, policy=mode), set_value,
                                          record_benefit_fn=lambda record: profiles[record.record_id])
        pinned_store.acquire_pin(a.record_id, a.revision)
        blocked = manager.admit(b, now=1001)
        report.check(f"{mode.value}_never_evicts_pinned_records",
                     not blocked.accepted and pinned_store.get(a.record_id) == a
                     and manager.maintenance_stats()["evicted_count"] == 0)
        pinned_store.release_pin(a.record_id, a.revision)

        audit_store = RecordStore()
        preload(audit_store, (a, duplicate), budget)
        audit_manager = OpportunityCacheManager(
            audit_store, ResidencyConfig(max_bytes=budget, policy=mode, audit_fraction=0.5,
                                         audit_ttl_seconds=10.0), set_value,
            record_benefit_fn=lambda record: profiles[record.record_id])
        audit_admission = audit_manager.admit(b, audit_only=True, now=1001)
        unavailable = audit_store.get(b.record_id) is None
        audit_touch = audit_manager.record_access(b.record_id)
        expiration = audit_manager.maintenance_tick(now=1011)
        stats = audit_manager.maintenance_stats()
        report.check(f"{mode.value}_keeps_common_audit_quota_and_counts_expiration_bytes",
                     audit_admission.accepted and unavailable and not audit_touch
                     and audit_store.get(b.record_id, include_audit=True) is None
                     and any(result.reason == "audit_expired" for result in expiration)
                     and stats["admitted_count"] == 1 and stats["evicted_count"] == 2
                     and stats["peak_managed_bytes"] <= budget,
                     stats)

    workflow, specs, _, _ = six_node_workflow()
    b_config = ReliabilityConfig(min_groups=7)
    c_config = PlannerConfig(mode=PlanningMode.GREEDY, depth=2, beam_width=3, alpha=2, beta=1,
                             cost_reference=10, latency_reference_ms=20)
    for mode in ResidencyMode:
        runtime = D1Config(mode).make_runtime(workflow, specs, reliability_config=b_config,
                                              planner_config=c_config)
        report.check(f"{mode.value}_preserves_shared_B_and_C_configuration",
                     runtime.reliability.config == b_config and runtime.planner.config == c_config
                     and runtime.manager.config.policy == mode and not runtime.config.fixed_cache
                     and not runtime.config.feedback_on_normal_execution
                     and runtime.manager.config.legacy_alpha == c_config.alpha
                     and runtime.manager.config.legacy_cost_reference == c_config.cost_reference)

    payload = {"check_type": "local_functional_smoke", "experiment_run": False, "network_used": False,
               "memory_accounting": "managed serialized bytes plus reserved metadata; not RSS",
               "variants": rows}
    report.output_dir.mkdir(parents=True, exist_ok=True)
    destination = report.output_dir / "residency_decisions.json"
    destination.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    report.check("d1_residency_decisions_are_saved", destination.is_file(), str(destination.resolve()))


if __name__ == "__main__":
    raise SystemExit(run_report("d1_residency_switches", "D1：驻留策略开关与字节预算检查", checks))
