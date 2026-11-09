"""Store/runtime accounted-heap integration checks; never certify cache RSS."""

from __future__ import annotations

import gc
import hashlib
import json
import threading
import weakref
from unittest.mock import patch
from contextlib import contextmanager

from reporting import CheckReport, run_report
from fixtures import six_node_workflow
from workflow.memory import CacheMemoryBudget, CacheMemoryError, measure_object_graph
from workflow.planner import PlannerConfig
from workflow.records import CachedRecord, RecordStore
from workflow.residency import OpportunityCacheManager, ResidencyConfig
from workflow.runtime import GeoLeapRuntime, RuntimeConfig
from workflow.spec import canonical_json, encode_input
import workflow.records as records_module


MANAGED_LIMIT = 8 * 1024 * 1024


def make_record(name, *, text="payload", metadata=4096):
    return CachedRecord("memory-fixture", name, 1, "memory-epoch",
                        canonical_json({"x": 1}), canonical_json({"y": text}),
                        encode_input({"x": 1}), 1000.0, metadata)


def fingerprint(store):
    return {"version": store.version,
            "records": {key: id(value) for key, value in store._records.items()},
            "buffers": {key: hashlib.sha256(value).hexdigest() for key, value in store._buffers.items()},
            "metadata": {key: hashlib.sha256(value).hexdigest() for key, value in store._metadata.items()},
            "audit_ids": sorted(store._audit_ids), "pins": dict(store._pins)}


def raises(kind, callback):
    try:
        callback()
    except kind:
        return True
    return False


def checks(report: CheckReport):
    observations = []
    def check(name, condition, details=None):
        report.check(name, condition, {"scope": "accounted_cache_owned_python_heap",
                     "physical_budget_enforced": False, **(details or {})})

    def memory(label, budget):
        value = budget.memory_snapshot()
        observations.append({"stage": label, **value})
        return value

    budget = CacheMemoryBudget(2 * 1024 * 1024, journal_limit=16)
    store = RecordStore(memory_budget=budget)
    record = make_record("initial")
    check("heap_store_publishes_and_distinguishes_managed_from_heap",
          store.publish((record,), frozenset(), store.version, MANAGED_LIMIT)
          and store.memory_snapshot()["enabled"] is True
          and store.memory_snapshot()["owned_heap_bytes"] > store.byte_size
          and store.memory_snapshot()["physical_budget_enforced"] is False)
    initial = fingerprint(store)
    replacement = make_record("replacement")
    check("stale_version_rejection_preserves_store_and_releases_reservations",
          not store.publish((replacement,), frozenset(), store.version-1, MANAGED_LIMIT)
          and fingerprint(store) == initial and budget.check()["operation_reserved_bytes"] == 0)
    pinned = store.acquire_pin(record.record_id, record.revision)
    pinned_fingerprint = fingerprint(store)
    check("pin_blocks_replacement_without_mutating_resident_revision",
          pinned is record and not store.publish((replacement,), frozenset(), store.version, MANAGED_LIMIT)
          and fingerprint(store) == pinned_fingerprint and store.get(record.record_id) is record)
    store.release_pin(record.record_id, record.revision)

    before = fingerprint(store)
    with patch.dict(records_module.__dict__, {"bytearray": lambda *args, **kwargs: (_ for _ in ()).throw(MemoryError("injected buffer allocation failure"))}):
        accepted = store.publish((replacement,), frozenset(), store.version, MANAGED_LIMIT)
    check("allocation_failure_rollback_keeps_original_store_and_no_workspace",
          not accepted and fingerprint(store) == before and store._publication_workspace is None
          and budget.check()["operation_reserved_bytes"] == 0)
    another = make_record("another")
    allocation_calls = []
    real_bytearray = bytearray
    def fail_second_buffer(*args, **kwargs):
        allocation_calls.append(True)
        if len(allocation_calls) == 2:
            raise MemoryError("injected second metadata allocation failure")
        return real_bytearray(*args, **kwargs)
    with patch.dict(records_module.__dict__, {"bytearray": fail_second_buffer}):
        accepted = store.publish((replacement, another), frozenset(), store.version, MANAGED_LIMIT)
    check("partial_multi_record_allocation_failure_removes_new_tracking_and_payloads",
          len(allocation_calls) == 2 and not accepted and fingerprint(store) == before
          and id(replacement) not in store._tracked_records and id(another) not in store._tracked_records
          and store._publication_workspace is None and budget.check()["operation_reserved_bytes"] == 0)

    original_check = budget.check
    publication_events = []
    def observe_publication():
        if store._publication_workspace is not None:
            workspace = store._publication_workspace
            old, proposed, new_buffers, new_metadata = workspace[:4]
            snapshot = budget.memory_snapshot()
            event = {"stage": "publication_old_new_live", **snapshot,
                     "old_record_ids": sorted(old[0]), "new_record_ids": sorted(proposed),
                     "old_buffer_ids": [id(value) for value in old[1].values()],
                     "new_buffer_ids": [id(value) for value in new_buffers.values()],
                     "old_buffer_bytes": sum(len(value) for value in old[1].values()),
                     "new_buffer_bytes": sum(len(value) for value in new_buffers.values())}
            publication_events.append(event)
        return original_check()
    with patch.object(budget, "check", observe_publication):
        published = store.publish((replacement,), frozenset(), store.version, MANAGED_LIMIT)
    observations.extend(publication_events)
    check("publication_actual_stage_retains_old_and_new_distinct_buffers",
          published and bool(publication_events)
          and any(event["old_buffer_bytes"] > 0 and event["new_buffer_bytes"] > 0
                  and not set(event["old_buffer_ids"]) & set(event["new_buffer_ids"])
                  and event["owned_heap_bytes"] >= event["old_buffer_bytes"]+event["new_buffer_bytes"]
                  for event in publication_events), {"observed_stages": len(publication_events)})
    check("publication_stage_peak_is_joint_observed_total_not_component_peak_sum",
          all(event["total_accounted_bytes"] <= event["peak_accounted_bytes"] <= event["limit_bytes"]
              and event["physical_budget_enforced"] is False for event in publication_events)
          and store._publication_workspace is None and budget.check()["operation_reserved_bytes"] == 0)

    before = fingerprint(store)
    failure_injected = []
    def fail_commit_once():
        if store._publication_workspace is not None and not failure_injected:
            failure_injected.append(True)
            raise CacheMemoryError("injected post-install reconciliation failure")
        return original_check()
    with patch.object(budget, "check", fail_commit_once):
        accepted = store.publish((record,), frozenset(), store.version, MANAGED_LIMIT)
    check("post_install_budget_failure_rolls_back_revision_and_payloads",
          failure_injected and not accepted and fingerprint(store) == before
          and store._publication_workspace is None and budget.check()["operation_reserved_bytes"] == 0)

    previous_metadata = store._metadata[replacement.record_id]
    previous_content = bytes(previous_metadata)
    metadata_events = []
    def observe_metadata():
        if store._metadata_workspace is not None:
            old, new, payload = store._metadata_workspace
            metadata_events.append({"stage": "metadata_cow", "old_id": id(old), "new_id": id(new),
                                    "old_unchanged": bytes(old) == previous_content,
                                    "new_bytes": len(new), **budget.memory_snapshot()})
        return original_check()
    with patch.object(budget, "check", observe_metadata):
        written = store.write_metadata(replacement.record_id, b"new evidence")
    observations.extend(metadata_events)
    check("metadata_write_is_cow_with_old_new_live_and_zero_padding",
          written and metadata_events and all(e["old_id"] != e["new_id"] and e["old_unchanged"] for e in metadata_events)
          and bytes(previous_metadata) == previous_content
          and bytes(store._metadata[replacement.record_id]) == b"new evidence" + b"\0"*(replacement.metadata_capacity-12))
    before = fingerprint(store)
    failure_injected.clear()
    def fail_metadata_once():
        if store._metadata_workspace is not None and not failure_injected:
            failure_injected.append(True)
            raise CacheMemoryError("injected metadata commit failure")
        return original_check()
    with patch.object(budget, "check", fail_metadata_once):
        written = store.write_metadata(replacement.record_id, b"rejected")
    check("metadata_commit_failure_restores_previous_bytes_and_cleans_workspace",
          failure_injected and not written and fingerprint(store) == before
          and store._metadata_workspace is None and budget.check()["operation_reserved_bytes"] == 0)
    check("oversized_metadata_rejects_without_changing_store",
          not store.write_metadata(replacement.record_id, b"x"*(replacement.metadata_capacity+1))
          and fingerprint(store) == before)

    # Two separate scopes avoid accidental test locals keeping a record alive.
    def retained_reader(kind):
        local_budget = CacheMemoryBudget(2 * 1024 * 1024, journal_limit=4)
        local_store = RecordStore(local_budget)
        source = make_record("reader", text="r" * 32768)
        assert local_store.publish((source,), frozenset(), 0, MANAGED_LIMIT)
        reader = local_store.snapshot() if kind == "snapshot" else local_store.get("reader")
        reference = weakref.ref(source)
        del source
        assert local_store.publish((), frozenset(), local_store.version, MANAGED_LIMIT)
        held = memory(kind + "_evicted_but_held", local_budget)
        alive_while_reading = reference() is not None
        del reader
        gc.collect()
        released = memory(kind + "_reader_released", local_budget)
        return alive_while_reading, reference() is None, held["owned_heap_bytes"]-released["owned_heap_bytes"], local_store.byte_size
    for kind in ("snapshot", "bare_record"):
        alive, freed, delta, serialized = retained_reader(kind)
        check(kind + "_evicted_object_remains_charged_until_reader_drops_it",
              alive and freed and delta >= 32768 and serialized == 0, {"released_heap_bytes": delta})

    shared_budget = CacheMemoryBudget(2 * 1024 * 1024, journal_limit=4)
    left, right = RecordStore(shared_budget), RecordStore(shared_budget)
    shared = make_record("shared", text="s" * 16384)
    assert left.publish((shared,), frozenset(), 0, MANAGED_LIMIT)
    assert right.publish((shared,), frozenset(), 0, MANAGED_LIMIT)
    resident_left = (left._records, left._buffers, left._metadata, left._audit_ids)
    resident_right = (right._records, right._buffers, right._metadata, right._audit_ids)
    union = measure_object_graph({"left": resident_left, "right": resident_right})["owned_heap_bytes"]
    separate = sum(measure_object_graph({"one": value})["owned_heap_bytes"] for value in (resident_left, resident_right))
    shared_snapshot = memory("two_store_shared_domain", shared_budget)
    check("multiple_stores_share_one_domain_and_deduplicate_same_record_identity",
          left.get("shared") is right.get("shared") is shared and union < separate
          and left.memory_snapshot()["budget_domain_id"] == right.memory_snapshot()["budget_domain_id"]
          and sum(shared_snapshot["category_exclusive_bytes"].values()) == shared_snapshot["owned_heap_bytes"],
          {"union_heap_bytes": union, "sum_separate_heap_bytes": separate})
    check("same_record_stored_twice_keeps_distinct_serialized_allocations_charged",
          left._buffers["shared"] == right._buffers["shared"]
          and left._buffers["shared"] is not right._buffers["shared"]
          and union > len(left._buffers["shared"]) + len(right._buffers["shared"]))

    tiny_budget = CacheMemoryBudget(128 * 1024, journal_limit=0)
    tiny_store = RecordStore(tiny_budget)
    oversized = make_record("oversized", text="z" * (128 * 1024))
    before = fingerprint(tiny_store)
    check("heap_capacity_rejects_payload_which_fits_managed_serialized_budget",
          oversized.byte_size < MANAGED_LIMIT
          and not tiny_store.publish((oversized,), frozenset(), 0, MANAGED_LIMIT)
          and fingerprint(tiny_store) == before and tiny_budget.check()["operation_reserved_bytes"] == 0)
    check("zero_heap_capacity_explicitly_rejects_initialization",
          raises(CacheMemoryError, lambda: CacheMemoryBudget(0)))
    legacy = RecordStore()
    assert legacy.publish((record,), frozenset(), 0, MANAGED_LIMIT)
    legacy_before = fingerprint(legacy)
    check("legacy_published_store_cannot_silently_rebind_to_new_heap_budget",
          raises(ValueError, lambda: legacy.bind_memory_budget(shared_budget))
          and legacy.memory_budget is None and fingerprint(legacy) == legacy_before)
    legacy_history = RecordStore()
    legacy_record = make_record("legacy_reader", text="legacy" * 4096)
    assert legacy_history.publish((legacy_record,), frozenset(), 0, MANAGED_LIMIT)
    legacy_reader = legacy_history.snapshot()
    assert legacy_history.publish((), frozenset(), legacy_history.version, MANAGED_LIMIT)
    check("previously_published_legacy_store_cannot_rebind_after_eviction_with_live_reader",
          raises(ValueError, lambda: legacy_history.bind_memory_budget(shared_budget))
          and legacy_history.memory_budget is None and legacy_reader.records[0] is legacy_record,
          {"legacy_version": legacy_history.version, "resident_count": len(legacy_history._records)})
    check("bound_store_cannot_change_global_budget_domain",
          raises(ValueError, lambda: left.bind_memory_budget(CacheMemoryBudget(2 * 1024 * 1024)))
          and left.memory_budget is shared_budget)
    gc_budget = CacheMemoryBudget(1024 * 1024, journal_limit=0)
    temporary_store = RecordStore(gc_budget)
    temporary_store_reference = weakref.ref(temporary_store)
    store_root_names = set(gc_budget.owned_roots())
    del temporary_store
    gc.collect()
    check("dead_store_owner_releases_all_bound_root_registrations",
          temporary_store_reference() is None and bool(store_root_names)
          and not store_root_names & set(gc_budget.owned_roots()))

    # Deterministic real concurrency: reader pin -> blocked maintenance ->
    # release -> successful maintenance; no sleeping or timing-based ordering.
    concurrent_budget = CacheMemoryBudget(4 * 1024 * 1024, journal_limit=8)
    concurrent_store = RecordStore(concurrent_budget)
    a, b = make_record("aaa", text="aaa"), make_record("bbb", text="bbb")
    managed = max(a.byte_size, b.byte_size)
    assert concurrent_store.publish((a,), frozenset(), 0, managed)
    manager = OpportunityCacheManager(concurrent_store,
        ResidencyConfig(max_bytes=managed, policy="lru", audit_fraction=0,
                        queue_max_bytes=256 * 1024), lambda records: float(len(records)))
    barrier, errors, results, batch_events = threading.Barrier(2), [], {}, []
    original_admit = manager.admit
    def observe_batch(*args, **kwargs):
        batch_events.append({"stage": "queue_transferred_to_active_batch", "queued_count": len(manager._queue),
                             "batch_count": len(manager._active_batch or ()), **concurrent_budget.memory_snapshot()})
        return original_admit(*args, **kwargs)
    def reader():
        try:
            selected = concurrent_store.acquire_pin(a.record_id, a.revision)
            results["selected"] = selected is a
            barrier.wait(timeout=5)
            barrier.wait(timeout=5)
            results["reader_valid"] = selected.outputs == {"y": "aaa"}
            concurrent_store.release_pin(a.record_id, a.revision)
            barrier.wait(timeout=5)
        except BaseException as error:
            errors.append({"thread": "reader", "type": type(error).__name__, "message": str(error)})
            barrier.abort()
    def maintenance():
        try:
            barrier.wait(timeout=5)
            if not manager.enqueue(b, audit_only=False):
                raise AssertionError("first enqueue rejected: " + manager.last_enqueue_reason)
            results["pinned_tick"] = manager.maintenance_tick(now=1001)
            barrier.wait(timeout=5)
            barrier.wait(timeout=5)
            if not manager.enqueue(b, audit_only=False):
                raise AssertionError("second enqueue rejected: " + manager.last_enqueue_reason)
            results["released_tick"] = manager.maintenance_tick(now=1001)
        except BaseException as error:
            errors.append({"thread": "maintenance", "type": type(error).__name__, "message": str(error)})
            barrier.abort()
    with patch.object(manager, "admit", observe_batch):
        threads = [threading.Thread(target=reader), threading.Thread(target=maintenance)]
        for thread in threads: thread.start()
        for thread in threads: thread.join(timeout=10)
    observations.extend(batch_events)
    check("real_concurrent_pin_blocks_then_release_allows_maintenance",
          not errors and all(not thread.is_alive() for thread in threads)
          and results.get("selected") and results.get("reader_valid")
          and any(not item.accepted for item in results.get("pinned_tick", ()))
          and any(item.accepted for item in results.get("released_tick", ()))
          and not concurrent_store.pinned_ids() and concurrent_store.get(b.record_id) is b,
          {"errors": errors})
    check("drained_queue_batch_payload_remains_in_accounted_roots",
          batch_events and all(event["queued_count"] == 0 and event["batch_count"] == 1
              and event["owned_heap_bytes"] >= measure_object_graph({"b": b})["owned_heap_bytes"]
              and event["total_accounted_bytes"] <= event["limit_bytes"] for event in batch_events),
          {"observed_stages": len(batch_events)})
    check("store_manager_logs_preserve_heap_scope_and_no_reservation_leaks",
          concurrent_budget.check()["operation_reserved_bytes"] == 0
          and concurrent_store.memory_snapshot()["physical_budget_enforced"] is False
          and manager.maintenance_stats()["physical_budget_enforced"] is False)

    def runtime_fixture(*, shared=None, limit=8 * 1024 * 1024, mode="full"):
        workflow, _, state, counts = six_node_workflow()
        spec = workflow.subgraph("whole_memory_chain", workflow.topological_order)
        runtime = GeoLeapRuntime(workflow, (spec,), config=RuntimeConfig(
            epoch="runtime-memory-v1", cache_heap_limit_bytes=limit),
            cache_memory_budget=shared,
            planner_config=PlannerConfig(mode=mode, depth=1, beam_width=2, scenario_limit=2),
            residency_config=ResidencyConfig(max_bytes=1024 * 1024, queue_max_bytes=256 * 1024,
                                            audit_fraction=1.0))
        return runtime, state, counts

    runtime, state, counts = runtime_fixture()
    cold = runtime.run(state, request_id="heap-runtime-cold")
    check("runtime_config_constructs_shared_domain_and_stages_completed_record",
          runtime.cache_memory_budget is runtime.store.memory_budget
          and runtime.cache_memory_budget.limit_bytes == 8 * 1024 * 1024
          and cold.state["v6"] == 6 and cold.state["untouched"] == "keep"
          and len(cold.executed_node_ids) == 6 and len(cold.new_record_ids) == 1
          and runtime.manager.queued_count == 1 and runtime._cache_memory_workspace is None
          and runtime.cache_memory_snapshot()["physical_budget_enforced"] is False)
    runtime.maintenance_tick()
    before_evidence = memory("runtime_audit_record_before_evidence", runtime.cache_memory_budget)
    for index in range(3):
        runtime.audit(state, request_id="heap-calibration-" + str(index))
    with_evidence = memory("runtime_after_three_reference_groups", runtime.cache_memory_budget)
    qualifications = runtime.qualification_snapshot()
    check("runtime_resident_evidence_forecast_and_pending_share_one_domain",
          runtime.reliability.memory_budget is runtime.cache_memory_budget
          and runtime.forecasts.memory_budget is runtime.cache_memory_budget
          and qualifications and any(item["groups"] == 3 for item in qualifications)
          and with_evidence["owned_heap_bytes"] > before_evidence["owned_heap_bytes"]
          and with_evidence["total_accounted_bytes"] <= with_evidence["limit_bytes"])
    runtime.maintenance_tick()
    counters_before_hit = counts.copy()
    hit = runtime.run(state, request_id="heap-runtime-served-hit")
    check("budgeted_runtime_reuses_and_restores_output_without_leaking_pin",
          hit.state["v6"] == 6 and hit.state["untouched"] == "keep"
          and len(hit.reused_subgraphs) == 1 and not hit.executed_node_ids and counts == counters_before_hit
          and not runtime.store.pinned_ids() and runtime.cache_memory_budget.check()["operation_reserved_bytes"] == 0)

    def rejecting_reservations(budget, categories, injected):
        original = budget.reservation
        @contextmanager
        def reserve(size, category="workspace"):
            if category in categories:
                injected.append(category)
                raise CacheMemoryError("injected reserve refusal for " + category)
            with original(size, category) as token:
                yield token
        return reserve

    for category in ("lookup", "planning"):
        pressured, initial_state, call_counts = runtime_fixture()
        refused = []
        with patch.object(pressured.cache_memory_budget, "reservation",
                          rejecting_reservations(pressured.cache_memory_budget, {category}, refused)):
            result = pressured.run(initial_state, request_id="refused-" + category,
                                   collect_records=False, collect_feedback=False)
        check(category + "_reservation_failure_executes_original_nodes_with_explicit_trace",
              refused and result.state["v6"] == 6 and result.state["untouched"] == "keep"
              and len(result.executed_node_ids) == 6 and all(value == 1 for value in call_counts.values())
              and not result.reused_subgraphs
              and any(event["event"] == "cache_planning_bypassed" for event in result.trace)
              and pressured._cache_memory_workspace is None
              and pressured.cache_memory_budget.check()["operation_reserved_bytes"] == 0,
              {"refused_reservations": len(refused)})

    capture_runtime, capture_state, _ = runtime_fixture()
    capture_refused = []
    with patch.object(capture_runtime.cache_memory_budget, "reservation",
                      rejecting_reservations(capture_runtime.cache_memory_budget, {"record_capture"}, capture_refused)):
        capture = capture_runtime.run(capture_state, request_id="capture-refused")
    check("record_capture_reservation_failure_keeps_serving_output_without_partial_queue",
          capture_refused and capture.state["v6"] == 6 and not capture.new_record_ids
          and capture_runtime.manager.queued_count == 0 and capture_runtime._cache_memory_workspace is None
          and any(event.get("reason") == "queue_count_or_heap_limit" for event in capture.trace))
    capture_after_enqueue, capture_state, _ = runtime_fixture()
    capture_commit_check = capture_after_enqueue.cache_memory_budget.check
    failed_commit = []
    def fail_after_manager_takes_record():
        workspace = capture_after_enqueue._cache_memory_workspace
        if (workspace and isinstance(workspace[0], CachedRecord)
                and capture_after_enqueue.manager._queue and not failed_commit):
            failed_commit.append(True)
            raise CacheMemoryError("injected capture commit failure after enqueue")
        return capture_commit_check()
    with patch.object(capture_after_enqueue.cache_memory_budget, "check", fail_after_manager_takes_record):
        capture = capture_after_enqueue.run(capture_state, request_id="capture-post-enqueue-refusal")
    check("post_enqueue_capture_failure_rolls_back_pending_handoff",
          failed_commit and capture.state["v6"] == 6 and not capture.new_record_ids
          and capture_after_enqueue.manager.queued_count == 0
          and capture_after_enqueue._cache_memory_workspace is None
          and capture_after_enqueue.cache_memory_budget.check()["operation_reserved_bytes"] == 0)

    baseline, baseline_state, baseline_counts = runtime_fixture(mode="no_cache")
    baseline_before = {"store_version": baseline.store.version, "queued": baseline.manager.queued_count,
                       "feedback": len(baseline.reliability._statistics), "forecasts": len(baseline.forecasts._counts),
                       "request_window": tuple(baseline._request_window),
                       "journal_sequence": baseline.cache_memory_budget.memory_snapshot()["sequence"]}
    def forbidden(*args, **kwargs):
        raise AssertionError("NoCache accessed a cache operation")
    with patch.object(baseline.store, "snapshot", forbidden), patch.object(baseline.planner, "choose", forbidden), \
            patch.object(baseline.manager, "enqueue", forbidden), patch.object(baseline.cache_memory_budget, "reservation", forbidden):
        no_cache = baseline.run(baseline_state, request_id="no-cache-heap")
    baseline_after = {"store_version": baseline.store.version, "queued": baseline.manager.queued_count,
                      "feedback": len(baseline.reliability._statistics), "forecasts": len(baseline.forecasts._counts),
                      "request_window": tuple(baseline._request_window),
                      "journal_sequence": baseline.cache_memory_budget.memory_snapshot()["sequence"]}
    check("no_cache_executes_all_nodes_without_lookup_planning_feedback_or_admission",
          no_cache.state["v6"] == 6 and len(no_cache.executed_node_ids) == 6
          and all(value == 1 for value in baseline_counts.values()) and no_cache.planning_elapsed_ms == 0
          and not no_cache.reused_subgraphs and not no_cache.new_record_ids and baseline_before == baseline_after)

    global_budget = CacheMemoryBudget(8 * 1024 * 1024, journal_limit=16)
    first_runtime, first_state, _ = runtime_fixture(shared=global_budget)
    second_runtime, second_state, _ = runtime_fixture(shared=global_budget)
    first_runtime.run(first_state, request_id="database-A")
    second_runtime.run(second_state, request_id="database-B")
    global_state = memory("two_runtime_one_global_domain", global_budget)
    check("multiple_runtime_injection_uses_one_total_limit_not_per_database_limits",
          first_runtime.cache_memory_budget is second_runtime.cache_memory_budget is global_budget
          and first_runtime.store.memory_budget is second_runtime.store.memory_budget is global_budget
          and first_runtime.cache_memory_snapshot()["budget_domain_id"] == second_runtime.cache_memory_snapshot()["budget_domain_id"]
          and global_state["limit_bytes"] == 8 * 1024 * 1024
          and global_state["total_accounted_bytes"] <= global_state["limit_bytes"]
          and first_runtime.manager.queued_count == second_runtime.manager.queued_count == 1)
    check("runtime_rejects_conflicting_injected_limit",
          raises(ValueError, lambda: GeoLeapRuntime(first_runtime.workflow, first_runtime.subgraphs,
                 config=RuntimeConfig(cache_heap_limit_bytes=1024 * 1024), cache_memory_budget=global_budget)))

    report.output_dir.mkdir(parents=True, exist_ok=True)
    (report.output_dir / "memory_events.jsonl").write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in observations))
    (report.output_dir / "scenario_summary.json").write_text(json.dumps({
        "scope": "accounted_cache_owned_python_heap", "physical_budget_enforced": False,
        "new_model_calls": 0, "not_covered": [
            "Independent raw field aliases retained outside a CachedRecord or CacheSnapshot are not proven tracked.",
            "Complete interpreter/native allocator/process RSS and kernel physical containment are separate scopes.",
            "Runtime maintenance-round complete rollback and returned-result/raw-field-alias lifetimes are not fully covered by this file.",
            "Independent semantic exit comparison and benchmark quality are outside this offline check."],
        "observed_memory_stages": len(observations), "formal_matrix_complete": False}, ensure_ascii=False, indent=2) + "\n")


if __name__ == "__main__":
    raise SystemExit(run_report("cache_memory_budget", "缓存堆预算集成：读者、发布、回滚与并发（非RSS上限）", checks))
