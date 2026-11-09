"""Exercise manager heap ownership, queue bounds and rollback; no API calls."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import gc
import json
from pathlib import Path
import sys
import weakref

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from reporting import CheckReport, run_report
from workflow.memory import CacheMemoryBudget, CacheMemoryError, measure_object_graph
from workflow.records import CachedRecord, RecordStore
from workflow.residency import OpportunityCacheManager, ResidencyConfig
from workflow.spec import canonical_json, encode_input


def record(name, payload=32):
    inputs = {"question": "fixture " + name}
    return CachedRecord("manager-memory-fixture", name, 1, "fixture-v1", canonical_json(inputs),
        canonical_json({"answer": name + "x" * payload}), encode_input(inputs), 1000.0, 128)


def setup(*, queue_limit=8, queue_bytes=None, managed_bytes=1024*1024, value=None,
          resident_fraction=0.6):
    budget = CacheMemoryBudget(8*1024*1024, journal_limit=8)
    store = RecordStore(memory_budget=budget)
    manager = OpportunityCacheManager(store,
        ResidencyConfig(max_bytes=managed_bytes, policy="lru", queue_limit=queue_limit,
                        queue_max_bytes=queue_bytes, resident_heap_fraction=resident_fraction),
        value or (lambda records: float(len(records))))
    return budget, store, manager


def checks(report: CheckReport):
    legacy = OpportunityCacheManager(RecordStore(), ResidencyConfig(max_bytes=1024, queue_limit=2),
                                     lambda records: 0.0)
    oversized = record("legacy-oversized", 8192)
    report.check("legacy_mode_preserves_existing_queue_contract",
        legacy.enqueue(oversized) and legacy.queued_count == 1 and legacy.store.byte_size == 0)

    budget, store, manager = setup(queue_limit=1)
    a, b = record("queued-a"), record("queued-b")
    before = budget.check()["owned_heap_bytes"]
    accepted, rejected = manager.enqueue(a, False), not manager.enqueue(b, False)
    state = budget.check()
    report.check("heap_roots_charge_pending_before_store_admission_and_enforce_count",
        accepted and rejected and manager.last_enqueue_reason == "queue_count_limit"
        and store.byte_size == 0 and state["owned_heap_bytes"] > before
        and state["category_exclusive_bytes"].get("manager_pending", 0) > 0,
        {"queued": manager.queued_count, "managed_bytes": store.byte_size,
         "heap_before": before, "heap_after": state["owned_heap_bytes"]})

    byte_budget, byte_store, byte_manager = setup(queue_bytes=4096)
    report.check("queue_heap_bound_rejects_large_candidate_with_count_capacity_available",
        not byte_manager.enqueue(record("large-pending", 8192))
        and byte_manager.last_enqueue_reason == "queue_heap_limit" and byte_manager.queued_count == 0
        and byte_store.byte_size == 0)

    tight_budget, tight_store, tight_manager = setup()
    limit = tight_budget.limit_bytes
    tight_budget.limit_bytes = tight_budget.check()["total_accounted_bytes"] + 512
    denied = not tight_manager.enqueue(record("budget-denied"))
    tight_state = tight_budget.check()
    report.check("failed_queue_reservation_leaves_no_payload_or_operation_reservation",
        denied and tight_manager.last_enqueue_reason == "memory_budget_exhausted"
        and tight_manager.queued_count == 0 and tight_store.byte_size == 0
        and tight_state["operation_reserved_bytes"] == 0)
    tight_budget.limit_bytes = limit

    cancel_budget, cancel_store, cancel_manager = setup()
    cancelled_record = record("cancel-staged")
    cancel_manager.enqueue(cancelled_record, False)
    pending_reader = cancel_manager.queued_records
    cancelled_reference = weakref.ref(cancelled_record)
    del cancelled_record
    cancelled = cancel_manager.cancel_pending("cancel-staged")
    still_retained = cancel_budget.check()["category_exclusive_bytes"].get("manager_pending", 0)
    cancelled_tick = cancel_manager.maintenance_tick(now=1001)
    report.check("cancel_pending_rolls_back_scheduling_without_later_admitting_rejected_capture",
        cancelled and not cancel_manager.cancel_pending("cancel-staged")
        and cancel_manager.last_enqueue_reason == "pending_cancelled"
        and cancel_manager.queued_count == 0 and cancelled_tick == [] and cancel_store.version == 0
        and cancelled_reference() is pending_reader[0])
    del pending_reader
    gc.collect()
    after_cancel_reader = cancel_budget.check()["category_exclusive_bytes"].get("manager_pending", 0)
    report.check("cancelled_record_external_reader_remains_charged_until_reference_release",
        cancelled_reference() is None and after_cancel_reader < still_retained
        and cancel_budget.check()["operation_reserved_bytes"] == 0)

    batch_budget, batch_store, batch_manager = setup()
    seen = []
    # LRU skips utility callbacks, so observe at the actual admit boundary.
    original_admit = batch_manager.admit
    def observed_admit(item, **kwargs):
        pending = batch_manager._pending_memory_roots()
        seen.append({"queue_count": batch_manager.queued_count,
                     "batch_count": len(batch_manager._active_batch or ()),
                     "record_alive_in_batch": any(pair[0] is item for pair in pending[1]),
                     "accounted": batch_budget.check()["total_accounted_bytes"]})
        return original_admit(item, **kwargs)
    batch_manager.admit = observed_admit
    batch_manager.enqueue(record("batch-a"), False)
    batch_manager.enqueue(record("batch-b"), False)
    admitted = batch_manager.maintenance_tick(now=1001)
    report.check("queue_to_batch_transfer_preserves_all_pending_owners_during_admission",
        len(admitted) == 2 and all(item.accepted for item in admitted)
        and len(seen) == 2 and all(item["queue_count"] == 0 and item["batch_count"] == 2
            and item["record_alive_in_batch"] for item in seen)
        and batch_manager._active_batch is None and batch_manager.queued_count == 0,
        {"batch_observations": seen})
    report.check("successful_admission_releases_workspace_and_reports_truthful_scope",
        batch_budget.check()["operation_reserved_bytes"] == 0
        and batch_manager._memory_workspaces == [] and batch_manager._policy_workspace is None
        and batch_manager.maintenance_stats()["physical_budget_enforced"] is False
        and "not process RSS" in batch_manager.maintenance_stats()["accounted_heap"]["scope"])

    fail_budget, fail_store, fail_manager = setup()
    original_publish = fail_store.publish
    def reject_publish(*args, **kwargs):
        return False
    fail_store.publish = reject_publish
    failed = fail_manager.admit(record("publish-reject"), audit_only=False, now=1001)
    report.check("store_publication_rejection_does_not_install_policy_bookkeeping",
        not failed.accepted and fail_store.version == 0 and fail_store.byte_size == 0
        and fail_manager._last_access == {} and fail_manager._reuse_counts == {}
        and fail_manager.maintenance_stats()["admitted_count"] == 0
        and fail_budget.check()["operation_reserved_bytes"] == 0)
    fail_store.publish = original_publish

    spare = fail_budget.limit_bytes
    fail_budget.limit_bytes = fail_budget.check()["total_accounted_bytes"] + 1024
    failure = fail_manager.admit(record("workspace-reject"), audit_only=False, now=1001)
    fail_budget.limit_bytes = spare
    report.check("admission_workspace_exhaustion_is_explicit_and_atomic",
        not failure.accepted and failure.reason == "memory_budget_exhausted"
        and fail_store.version == 0 and fail_manager._last_access == {}
        and fail_budget.check()["operation_reserved_bytes"] == 0)

    callback_budget = CacheMemoryBudget(8*1024*1024, journal_limit=8)
    callback_store = RecordStore(callback_budget)
    def explode(records):
        raise RuntimeError("fixture utility failure")
    callback_manager = OpportunityCacheManager(callback_store,
        ResidencyConfig(max_bytes=1024*1024, policy="full"), explode)
    callback_manager.enqueue(record("callback-failure"), False)
    raised = False
    try:
        callback_manager.maintenance_tick(now=1001)
    except RuntimeError:
        raised = True
    report.check("callback_exception_cleans_batch_workspace_and_reservations",
        raised and callback_manager._active_batch is None and callback_manager._memory_workspaces == []
        and callback_manager._policy_workspace is None and callback_store.version == 0
        and callback_budget.check()["operation_reserved_bytes"] == 0)

    retain_budget, retain_store, retain_manager = setup(managed_bytes=512*1024)
    rejected_record = record("expired-pending", 2048)
    retain_manager.enqueue(rejected_record, True)
    exported = retain_manager.queued_records
    reference = weakref.ref(rejected_record)
    del rejected_record
    expired = retain_manager.maintenance_tick(now=10000)
    retained = retain_budget.check()["category_exclusive_bytes"].get("manager_pending", 0)
    del exported
    gc.collect()
    released = retain_budget.check()["category_exclusive_bytes"].get("manager_pending", 0)
    report.check("exported_pending_reader_keeps_rejected_record_charged_until_last_owner_drops",
        bool(expired) and not expired[0].accepted and retain_store.byte_size == 0
        and reference() is None and retained > released,
        {"pending_heap_while_reader_held": retained, "pending_heap_after_release": released})

    access_budget, access_store, access_manager = setup()
    live = record("access-live")
    access_manager.admit(live, False, now=1001)
    state_before = (dict(access_manager._last_access), dict(access_manager._reuse_counts),
                    dict(access_manager._reuse_positions), access_manager._access_clock)
    previous_limit = access_budget.limit_bytes
    access_budget.limit_bytes = access_budget.check()["total_accounted_bytes"] + 1024
    touched = access_manager.record_access(live.record_id)
    access_budget.limit_bytes = previous_limit
    state_after = (access_manager._last_access, access_manager._reuse_counts,
                   access_manager._reuse_positions, access_manager._access_clock)
    report.check("access_bookkeeping_reservation_failure_keeps_LRU_and_reuse_state",
        not touched and state_before == state_after and access_budget.check()["operation_reserved_bytes"] == 0)

    def traverse(index):
        for _ in range(8):
            if index:
                access_manager.queued_records
                access_manager.maintenance_stats()
            else:
                with access_budget.transaction():
                    access_store.snapshot()
        return True
    with ThreadPoolExecutor(max_workers=2) as executor:
        concurrent = list(executor.map(traverse, (0, 1)))
    report.check("shared_budget_then_component_lock_order_completes_cross_component_reads",
        all(concurrent) and access_budget.check()["operation_reserved_bytes"] == 0)

    shared_budget = CacheMemoryBudget(8*1024*1024, journal_limit=8)
    one_store, two_store = RecordStore(shared_budget), RecordStore(shared_budget)
    one = OpportunityCacheManager(one_store, ResidencyConfig(max_bytes=1024*1024), lambda rs:0.0)
    two = OpportunityCacheManager(two_store, ResidencyConfig(max_bytes=1024*1024), lambda rs:0.0)
    shared_record = record("real-shared")
    one.enqueue(shared_record)
    first = shared_budget.check()["owned_heap_bytes"]
    two.enqueue(shared_record)
    second = shared_budget.check()["owned_heap_bytes"]
    payload_heap = measure_object_graph({"record":shared_record})["owned_heap_bytes"]
    report.check("multiple_manager_roots_share_one_domain_and_deduplicate_the_same_payload_identity",
        one.store.memory_budget is two.store.memory_budget and 0 < second-first < payload_heap,
        {"second_queue_increment":second-first,"single_record_heap":payload_heap})

    pressure_budget, pressure_store, pressure_manager = setup(resident_fraction=0.003)
    pressure_results = [pressure_manager.admit(record("heap-pressure-%02d" % i), False, now=1001)
                        for i in range(12)]
    pressure_stats = pressure_manager.maintenance_stats()
    report.check("resident_heap_target_drives_real_eviction_before_managed_storage_is_full",
        all(item.accepted for item in pressure_results)
        and any(item.evicted_ids for item in pressure_results)
        and pressure_store.byte_size < pressure_manager.config.max_bytes
        and pressure_stats["projected_resident_heap_bytes"] <= pressure_manager.resident_heap_limit_bytes
        and pressure_budget.check()["total_accounted_bytes"] <= pressure_budget.limit_bytes,
        {"managed_bytes":pressure_store.byte_size,"managed_limit":pressure_manager.config.max_bytes,
         "resident_heap_target":pressure_manager.resident_heap_limit_bytes,
         "projected_resident_heap":pressure_stats["projected_resident_heap_bytes"],
         "eviction_count":sum(len(item.evicted_ids) for item in pressure_results)})

    retained_view = pressure_store.snapshot(include_audit=True)
    one_record = retained_view.records[0]
    projection_with = pressure_manager._bytes(retained_view.records)
    projection_without = pressure_manager._bytes(tuple(item for item in retained_view.records if item is not one_record))
    report.check("heap_density_uses_projected_marginal_without_claiming_reader_reclamation",
        projection_with > projection_without and pressure_stats["physical_budget_enforced"] is False
        and "not immediately reclaimable" in pressure_stats["density_space_scope"]
        and any(candidate is one_record for candidate in retained_view.records),
        {"projected_marginal":projection_with-projection_without,
         "density_scope":pressure_stats["density_space_scope"]})

    policy_results = {}
    for mode in ("lru", "legacy_density", "additive_value", "full"):
        policy_budget = CacheMemoryBudget(8*1024*1024, journal_limit=8)
        policy_store = RecordStore(policy_budget)
        def benefit(item):
            amount = int(item.record_id.rsplit("-", 1)[1]) + 1
            return float(amount), float(amount)
        policy_manager = OpportunityCacheManager(policy_store,
            ResidencyConfig(max_bytes=1024*1024, policy=mode, resident_heap_fraction=0.003),
            lambda rows:sum(benefit(item)[0] for item in rows), record_benefit_fn=benefit)
        outcomes = [policy_manager.admit(record("policy-%02d" % i), False, now=1001)
                    for i in range(12)]
        policy_results[mode] = {"accepted":sum(item.accepted for item in outcomes),
            "evicted":sum(len(item.evicted_ids) for item in outcomes),
            "heap_target":policy_manager.resident_heap_limit_bytes,
            "projected_heap":policy_manager._bytes(policy_store._records.values()),
            "managed_bytes":policy_store.byte_size,
            "global_within_limit":policy_budget.check()["total_accounted_bytes"] <= policy_budget.limit_bytes}
    report.check("all_four_D_policies_use_the_same_heap_target_and_preserve_global_peak_accounting",
        len({item["heap_target"] for item in policy_results.values()}) == 1
        and all(item["accepted"]==12 and item["evicted"]>0
                and item["projected_heap"]<=item["heap_target"] and item["global_within_limit"]
                and item["managed_bytes"]<1024*1024 for item in policy_results.values()), policy_results)


if __name__ == "__main__":
    raise SystemExit(run_report("cache_manager_memory", "Cache manager：有界堆所有权与准入", checks))
