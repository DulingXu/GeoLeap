"""Regress cache-pressure failures with real local operators and no APIs.

Faults use ordinary functions, not Mock/MagicMock objects installed into owners.
The shared heap walker therefore sees normal controller state during each test.
"""
from __future__ import annotations

from contextlib import contextmanager
import gc
from pathlib import Path
import sys
import time
import weakref

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from reporting import CheckReport, run_report
from workflow.memory import CacheMemoryBudget, CacheMemoryError
from workflow.planner import ForecastTable, LookaheadPlanner
from workflow.records import CachedRecord, RecordStore
from workflow.reliability import ReliabilityConfig
from workflow.runtime import GeoLeapRuntime, RuntimeConfig
from workflow.spec import OperatorSpec, WorkflowSpec, canonical_json, encode_input


def fixture():
    calls = {"one": 0, "two": 0}
    def first(values):
        calls["one"] += 1
        return {"y": values["x"] + 1}
    def second(values):
        calls["two"] += 1
        return {"z": values["y"] * 2}
    workflow = WorkflowSpec("runtime-pressure-regression", (
        OperatorSpec("one", first, read_fields=("x",), write_fields=("y",),
            input_schema=(("x", "integer"),), output_schema=(("y", "integer"),),
            cacheable=True, effect_free=True, stable=False),
        OperatorSpec("two", second, dependencies=("one",), read_fields=("y",), write_fields=("z",),
            input_schema=(("y", "integer"),), output_schema=(("z", "integer"),),
            cacheable=True, effect_free=True, stable=False),
    ))
    spec = workflow.subgraph("whole", ("one", "two"))
    budget = CacheMemoryBudget(32*1024*1024, journal_limit=8)
    runtime = GeoLeapRuntime(workflow, (spec,),
        config=RuntimeConfig(allow_exact=False, metadata_capacity=4096),
        reliability_config=ReliabilityConfig(min_groups=1, bootstrap_samples=16),
        cache_memory_budget=budget)
    return runtime, spec, budget, calls


def completed_record(runtime, spec, value=1):
    return CachedRecord.create(spec, {"x": value}, {"z": (value+1)*2}, runtime.config.epoch,
                               metadata_capacity=4096)


def check_snapshot_pressure(report, ordinal, phase):
    runtime, spec, budget, calls = fixture()
    original = runtime.store.snapshot
    observed = {"count": 0, "faults": 0}
    def fail_selected_snapshot(*args, **kwargs):
        observed["count"] += 1
        if observed["count"] == ordinal:
            observed["faults"] += 1
            raise CacheMemoryError("fixture snapshot pressure: " + phase)
        return original(*args, **kwargs)
    runtime.store.snapshot = fail_selected_snapshot
    result, error = None, None
    try:
        result = runtime.run({"x": 1}, request_id="snapshot-"+phase)
    except Exception as failure:
        error = type(failure).__name__ + ": " + str(failure)
    finally:
        runtime.store.snapshot = original
    report.check("snapshot_pressure_"+phase+"_executes_each_original_node_once",
        observed["faults"] == 1 and result is not None and result.state["z"] == 4
        and calls == {"one": 1, "two": 1} and not result.served_hits
        and budget.check()["operation_reserved_bytes"] == 0,
        {"faults": observed["faults"], "snapshot_calls": observed["count"],
         "operator_calls": calls, "error": error,
         "final_z": result.state.get("z") if result is not None else None})


def checks(report: CheckReport):
    # Empty-cache two-node chronology: pre-request, first frontier, second
    # frontier, post-execution duplicate view. The middle fault is after node1.
    for ordinal, phase in ((1, "pre_request"), (3, "second_frontier"), (4, "post_execution")):
        check_snapshot_pressure(report, ordinal, phase)

    runtime, spec, budget, calls = fixture()
    source = ForecastTable()
    source.observe(spec.identity, "historical", 1, runtime.config.epoch, True)
    source_counts = dict(source._counts)
    runtime.set_demand_profiles({"A": source})
    installed = runtime._demand_profiles["A"]
    planner_domains = []
    original_choose = LookaheadPlanner.choose
    def observed_choose(planner, *args, **kwargs):
        planner_domains.append(planner.memory_budget is budget)
        return original_choose(planner, *args, **kwargs)
    LookaheadPlanner.choose = observed_choose
    try:
        context_result = runtime.run({"x": 1}, demand_context="A", collect_records=False,
                                     request_id="budgeted-context")
    finally:
        LookaheadPlanner.choose = original_choose
    report.check("context_profile_is_COW_bound_to_shared_budget_and_active_planner_is_budgeted",
        installed is not source and installed.memory_budget is budget
        and installed._counts == source_counts and source._counts == source_counts
        and source.memory_budget is None and planner_domains and all(planner_domains)
        and context_result.state["z"] == 4,
        {"installed_bound":installed.memory_budget is budget,"source_unmodified_domain":source.memory_budget is None,
         "active_planner_bound":planner_domains})

    old_profiles = runtime._demand_profiles
    original_register = budget.register_root
    registrations = {"count": 0}
    def fail_second_profile_root(name, getter, **kwargs):
        if name.startswith("forecasts:"):
            registrations["count"] += 1
            if registrations["count"] == 2:
                raise CacheMemoryError("fixture second context registration failure")
        return original_register(name, getter, **kwargs)
    budget.register_root = fail_second_profile_root
    rejected = False
    try:
        runtime.set_demand_profiles({"A": ForecastTable(), "B": ForecastTable()})
    except CacheMemoryError:
        rejected = True
    finally:
        budget.register_root = original_register
    report.check("failed_context_profile_bundle_preserves_previous_mapping_without_half_install",
        rejected and registrations["count"] == 2 and runtime._demand_profiles is old_profiles
        and runtime._demand_profiles["A"] is installed and budget.check()["operation_reserved_bytes"] == 0,
        {"rejected":rejected,"attempted_registrations":registrations["count"],
         "old_mapping_preserved":runtime._demand_profiles is old_profiles})

    tight, spec, small_budget, _ = fixture()
    large_source = ForecastTable()
    for index in range(50):
        large_source.observe(spec.identity, "source-%04d"%index, 1, tight.config.epoch, True)
    original_limit = small_budget.limit_bytes
    small_budget.limit_bytes = small_budget.check()["total_accounted_bytes"] + 2500
    rejected, within, error = False, False, None
    try:
        try:
            tight.set_demand_profiles({"large": large_source})
        except CacheMemoryError:
            rejected = True
        try:
            small_budget.check()
            within = True
        except CacheMemoryError as failure:
            error = str(failure)
        no_install = tight._demand_profiles == {}
    finally:
        small_budget.limit_bytes = original_limit
    report.check("oversized_context_install_rejects_before_publishing_over_limit_heap",
        rejected and within and no_install,
        {"rejected":rejected,"within_limit":within,"old_mapping_preserved":no_install,"error":error})

    republish_budget = CacheMemoryBudget(8*1024*1024, journal_limit=8)
    store = RecordStore(republish_budget)
    inputs = {"q": "x"*1024}
    item = CachedRecord("reader-subgraph", "raw-reader", 1, "epoch", canonical_json(inputs),
        canonical_json({"out":"v"*1024}), encode_input(inputs), time.time(), 128)
    assert store.publish((item,), frozenset(), store.version, 100000)
    raw = store.get(item.record_id)
    del item
    assert store.publish((), frozenset(), store.version, 100000)
    before = id(raw) in store._tracked_records
    original_check = republish_budget.check
    version = store.version
    def fail_republication_commit():
        if store._version == version+1 and store._publication_workspace is not None:
            raise CacheMemoryError("fixture republication commit failure")
        return original_check()
    republish_budget.check = fail_republication_commit
    try:
        accepted = store.publish((raw,), frozenset(), store.version, 100000)
    finally:
        republish_budget.check = original_check
    report.check("failed_republication_preserves_preexisting_raw_reader_tracking",
        before and not accepted and id(raw) in store._tracked_records and store.version == version
        and store.get(raw.record_id) is None and republish_budget.check()["operation_reserved_bytes"] == 0,
        {"tracked_before":before,"tracked_after":id(raw) in store._tracked_records,"accepted":accepted})

    planner_budget = CacheMemoryBudget(8*1024*1024, journal_limit=8)
    planner = LookaheadPlanner()
    register = planner_budget.register_root
    def fail_planner_registration(name, getter, **kwargs):
        if name.startswith("planner:"):
            raise CacheMemoryError("fixture planner registration failure")
        return register(name, getter, **kwargs)
    planner_budget.register_root = fail_planner_registration
    rejected = False
    try:
        planner.bind_memory_budget(planner_budget)
    except CacheMemoryError:
        rejected = True
    finally:
        planner_budget.register_root = register
    rolled_back = planner.memory_budget is None
    planner.bind_memory_budget(planner_budget)
    report.check("planner_bind_failure_rolls_back_and_retry_registers_controller_root",
        rejected and rolled_back and planner.memory_budget is planner_budget
        and any(name.startswith("planner:") for name in planner_budget.owned_roots()),
        {"rejected":rejected,"rolled_back_domain":rolled_back,
         "registered_on_retry":any(name.startswith("planner:") for name in planner_budget.owned_roots())})

    runtime, spec, budget, _ = fixture()
    expired_record = completed_record(runtime, spec)
    assert runtime.store.publish((expired_record,), frozenset((expired_record.record_id,)),
                                 runtime.store.version, 1000000)
    pending = completed_record(runtime, spec, 2)
    assert runtime.manager.enqueue(pending, audit_only=True)
    reservation = budget.reservation
    freeze_faults = {"count": 0}
    @contextmanager
    def fail_maintenance_freeze(size_bytes, category="workspace"):
        if category == "maintenance_profiles":
            freeze_faults["count"] += 1
            raise CacheMemoryError("fixture maintenance freeze failure")
        with reservation(size_bytes, category) as token:
            yield token
    def no_utility(records):
        raise AssertionError("failed freeze path must not use utility or admit pending work")
    runtime.manager.value_fn = no_utility
    budget.reservation = fail_maintenance_freeze
    try:
        maintenance = runtime.maintenance_tick(now=time.time()+4000)
    finally:
        budget.reservation = reservation
    remaining = runtime.store.snapshot(include_audit=True)
    report.check("failed_maintenance_freeze_still_expires_audits_without_draining_pending_or_utility",
        freeze_faults["count"] == 1 and not remaining.records and runtime.manager.queued_count == 1
        and any(item.reason == "memory_budget_exhausted" for item in maintenance)
        and budget.check()["operation_reserved_bytes"] == 0,
        {"freeze_faults":freeze_faults["count"],"reasons":[item.reason for item in maintenance],
         "remaining_records":len(remaining.records),"pending_records":runtime.manager.queued_count})

    runtime, spec, budget, _ = fixture()
    supported = completed_record(runtime, spec)
    assert runtime.store.publish((supported,), frozenset(), runtime.store.version, 1000000)
    assert runtime.reliability.observe(spec, supported, runtime.config.epoch, 1.0, "source-1", 0.0)
    runtime.reliability.refresh()
    before_support = runtime._has_support(spec, supported)
    write_metadata, discard_evidence, discard_forecast = (runtime.store.write_metadata,
        runtime.reliability.discard_records, runtime.forecasts.discard_records)
    def refuse_write(*unused):
        return False
    def refuse_discard(*unused):
        return False
    runtime.store.write_metadata = refuse_write
    runtime.reliability.discard_records = refuse_discard
    runtime.forecasts.discard_records = refuse_discard
    try:
        runtime._refresh_and_profile([(spec, supported, {"x":1})])
    finally:
        runtime.store.write_metadata = write_metadata
        runtime.reliability.discard_records = discard_evidence
        runtime.forecasts.discard_records = discard_forecast
    after_support = runtime._has_support(spec, supported)
    report.check("metadata_write_and_COW_cleanup_refusal_invalidates_support_fail_closed",
        before_support and not after_support
        and not any(key[1] == supported.record_id for key in runtime.reliability._statistics)
        and not any(key[1] == supported.record_id for key in runtime.forecasts._counts),
        {"support_before":before_support,"support_after_failure":after_support})

    runtime, spec, budget, _ = fixture()
    result = runtime.run({"x":1}, collect_records=False, request_id="retained-telemetry")
    identity = id(result)
    reference = weakref.ref(result)
    tracked = identity in runtime._retained_memory_results
    normal_accounting = result.cache_telemetry_accounting
    before = budget.memory_snapshot()["owned_heap_bytes"]
    del result
    gc.collect()
    after = budget.memory_snapshot()["owned_heap_bytes"]
    report.check("returned_trace_and_hits_remain_charged_until_last_RuntimeResult_reader_drops",
        tracked and normal_accounting == "shared_cache_heap_until_reader_release"
        and reference() is None and identity not in runtime._retained_memory_results and after < before,
        {"tracked_while_alive":tracked,"accounting":normal_accounting,
         "heap_with_result":before,"heap_after_release":after})

    runtime, spec, budget, calls = fixture()
    check = budget.check
    telemetry_faults = {"count":0}
    def fail_return_telemetry_commit():
        if runtime._retained_memory_results and telemetry_faults["count"] == 0:
            telemetry_faults["count"] += 1
            raise CacheMemoryError("fixture telemetry commit pressure after final output")
        return check()
    budget.check = fail_return_telemetry_commit
    value, error = None, None
    try:
        try:
            value = runtime.run({"x":1}, collect_records=False, request_id="late-telemetry-pressure")
        except Exception as failure:
            error = type(failure).__name__ + ": " + str(failure)
    finally:
        budget.check = check
    report.check("post_output_telemetry_commit_refusal_returns_existing_output_without_reexecution",
        telemetry_faults["count"] == 1 and value is not None and value.state["z"] == 4
        and calls == {"one":1,"two":1} and budget.check()["operation_reserved_bytes"] == 0
        and value.cache_telemetry_accounting == "caller_owned_after_cache_budget_rejection"
        and id(value) not in runtime._retained_memory_results,
        {"faults":telemetry_faults["count"],"operator_calls":calls,"error":error,
         "returned_output":value.state.get("z") if value is not None else None,
         "accounting":value.cache_telemetry_accounting if value is not None else None,
         "internally_retained":id(value) in runtime._retained_memory_results if value is not None else None})

    runtime, spec, budget, _ = fixture()
    staged = completed_record(runtime, spec)
    assert runtime.manager.enqueue(staged, audit_only=True)
    snapshot = runtime.store.snapshot
    post_maintenance = {"calls":0,"faults":0}
    def fail_after_manager_snapshot(*args, **kwargs):
        post_maintenance["calls"] += 1
        # freeze view, manager expiration view, manager admission view, then
        # the runtime's promotion/cleanup view after successful publication.
        if post_maintenance["calls"] == 4:
            post_maintenance["faults"] += 1
            raise CacheMemoryError("fixture post-manager snapshot pressure")
        return snapshot(*args, **kwargs)
    runtime.store.snapshot = fail_after_manager_snapshot
    maintenance, error = None, None
    try:
        try:
            maintenance = runtime.maintenance_tick()
        except Exception as failure:
            error = type(failure).__name__ + ": " + str(failure)
    finally:
        runtime.store.snapshot = snapshot
    report.check("post_manager_snapshot_refusal_preserves_committed_admission_receipt_and_reports_pressure",
        post_maintenance["faults"] == 1 and maintenance is not None
        and any(item.accepted and item.reason == "audit_admitted" for item in maintenance)
        and any(not item.accepted and item.reason == "memory_budget_exhausted" for item in maintenance)
        and runtime.store.get(staged.record_id, include_audit=True) is staged
        and runtime.manager.queued_count == 0 and runtime._cache_memory_workspace is None
        and budget.check()["operation_reserved_bytes"] == 0,
        {"faults":post_maintenance["faults"],"snapshot_calls":post_maintenance["calls"],"error":error,
         "reasons":[item.reason for item in maintenance] if maintenance is not None else None,
         "record_was_published":runtime.store.get(staged.record_id, include_audit=True) is staged})

    runtime, spec, budget, _ = fixture()
    original_publish = runtime.store.publish
    def reject_store_budget(*args, **kwargs):
        runtime.store._last_publish_failure_reason = "memory_budget_exhausted"
        return False
    runtime.store.publish = reject_store_budget
    try:
        refusal = runtime.manager.admit(completed_record(runtime, spec), audit_only=True)
    finally:
        runtime.store.publish = original_publish
    report.check("store_budget_refusal_is_not_reported_as_version_or_pin_conflict",
        not refusal.accepted and refusal.reason == "memory_budget_exhausted",
        {"accepted":refusal.accepted,"reason":refusal.reason})


if __name__ == "__main__":
    raise SystemExit(run_report("runtime_memory_failures", "Runtime：预算拒绝与生命周期失败回归", checks))
