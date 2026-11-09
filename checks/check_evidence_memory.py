"""Evidence/forecast heap admission, COW rollback and snapshot ownership.

All workloads below are deterministic local fixtures. The deliberate oversized
commit payload tests rejection/rollback, not a process-physical-memory claim.
"""
from __future__ import annotations

from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
import gc
from pathlib import Path
import sys
from unittest.mock import patch
import weakref

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from fixtures import six_node_workflow
from reporting import CheckReport, run_report
from workflow.memory import CacheMemoryBudget, CacheMemoryError
from workflow.planner import ForecastTable
from workflow.records import CachedRecord
from workflow.reliability import ReliabilityConfig, ReliabilityTable


def rejected(call):
    try:
        call()
    except CacheMemoryError:
        return True
    return False


@contextmanager
def inject_oversized_commit(budget, owner, observations):
    """Fault-inject an underestimated temporary graph at the real commit check."""
    reserve = budget.reservation

    @contextmanager
    def intercepted(size, category="workspace"):
        with reserve(size, category) as token:
            commit = token.commit

            def guarded_commit():
                work = owner._pending_workspace
                observations.append({"old_retained": "old" in work,
                                     "new_retained": "new" in work,
                                     "reserved_before_commit": budget.memory_snapshot()["operation_reserved_bytes"] > 0})
                work["oversized_fixture_allocation"] = bytearray(budget.limit_bytes + 1)
                commit()

            token.commit = guarded_commit
            yield token

    with patch.object(budget, "reservation", intercepted):
        yield


def checks(report: CheckReport):
    _, (_, spec, _), initial, _ = six_node_workflow()
    epoch, similarity = "evidence-heap-fixture-v1", 0.99
    record = CachedRecord.create(spec, spec.project(initial), {"v2": 2}, epoch,
                                 record_id="evidence-heap-fixture-record")
    config = ReliabilityConfig(min_groups=1, max_groups=4, bootstrap_samples=20)
    clock = lambda: 1000.0
    legacy = ReliabilityTable(config, clock=clock)
    bound = ReliabilityTable(config, clock=clock)
    forecasts = ForecastTable()
    budget = CacheMemoryBudget(4 * 1024**2, journal_limit=8)
    bound.bind_memory_budget(budget)
    forecasts.bind_memory_budget(budget)
    before = budget.memory_snapshot()["total_accounted_bytes"]
    accepted = []
    for group, disagreement in (("one", 0.0), ("one", 1.0), ("two", 0.0)):
        accepted.append(bound.observe(spec, record, epoch, similarity, group, disagreement))
        legacy.observe(spec, record, epoch, similarity, group, disagreement)
    accepted.append(forecasts.observe(spec.identity, record.record_id, record.revision, epoch, True))
    accepted.append(forecasts.observe(spec.identity, record.record_id, record.revision, epoch, False))
    report.check("shared_domain_counts_reliability_and_forecast_owned_heap",
                 all(accepted) and bound.memory_budget is budget and forecasts.memory_budget is budget
                 and budget.memory_snapshot()["total_accounted_bytes"] > before
                 and {"reliability", "forecasts"}.issubset(budget.memory_snapshot()["category_exclusive_bytes"]))
    report.check("budgeted_bootstrap_preserves_legacy_group_statistics_and_serialization",
                 legacy.refresh() == bound.refresh() == 1
                 and legacy.export_record(record.record_id) == bound.export_record(record.record_id)
                 and bound.lookup(spec, record, epoch, similarity).mean == 0.25
                 and forecasts.probability(spec, record, epoch) == 0.5)

    copied = bound.clone()
    forecast_copy = forecasts.snapshot()
    baseline_copy = copied.export_record(record.record_id)
    old_forecast = forecast_copy.export_record(record.record_id)
    report.check("clones_and_snapshots_register_their_own_roots_in_the_same_domain",
                 copied.memory_budget is budget and forecast_copy.memory_budget is budget
                 and copied._memory_prefix in budget.owned_roots() and forecast_copy._memory_prefix in budget.owned_roots()
                 and copied._statistics is not bound._statistics
                 and next(iter(copied._statistics.values())).groups is not next(iter(bound._statistics.values())).groups)
    bound.observe(spec, record, epoch, similarity, "three", 1.0)
    forecasts.observe(spec.identity, record.record_id, record.revision, epoch, True)
    report.check("later_mutation_does_not_change_snapshotted_history_or_forecast_counts",
                 copied.export_record(record.record_id) == baseline_copy
                 and forecast_copy.export_record(record.record_id) == old_forecast
                 and bound.lookup(spec, record, epoch, similarity) is None)

    old_stats, old_capacities = bound._statistics, bound._capacities
    old_encoded = bound.export_record(record.record_id)
    observed = []
    with inject_oversized_commit(budget, bound, observed):
        result = bound.observe(spec, record, epoch, similarity, "rejected-at-commit", 0.0)
    report.check("reliability_actual_heap_failure_rolls_back_all_maps_and_keeps_old_history_rooted_through_commit",
                 result is False and bound._statistics is old_stats and bound._capacities is old_capacities
                 and bound.export_record(record.record_id) == old_encoded
                 and observed == [{"old_retained": True, "new_retained": True, "reserved_before_commit": True}]
                 and bound._pending_workspace is None and budget.memory_snapshot()["operation_reserved_bytes"] == 0)
    assert bound.refresh() == 1
    previous = bound.lookup(spec, record, epoch, similarity)
    old_stats = bound._statistics
    old_encoded = bound.export_record(record.record_id)
    observed.clear()
    with inject_oversized_commit(budget, bound, observed):
        refreshed = bound.refresh()
    report.check("failed_bootstrap_commit_returns_zero_and_preserves_previous_evidence",
                 refreshed == 0 and bound._statistics is old_stats
                 and bound.lookup(spec, record, epoch, similarity) is previous
                 and bound.export_record(record.record_id) == old_encoded and observed[0]["old_retained"]
                 and bound._pending_workspace is None and budget.memory_snapshot()["operation_reserved_bytes"] == 0)

    old_counts = forecasts._counts
    old_encoded_forecast = forecasts.export_record(record.record_id)
    observed.clear()
    with inject_oversized_commit(budget, forecasts, observed):
        observed_forecast = forecasts.observe(spec.identity, record.record_id, record.revision, epoch, True)
    report.check("forecast_actual_heap_failure_rejects_counter_update_without_partial_counts",
                 observed_forecast is False and forecasts._counts is old_counts
                 and forecasts.export_record(record.record_id) == old_encoded_forecast
                 and observed[0]["old_retained"] and observed[0]["new_retained"]
                 and forecasts._pending_workspace is None and budget.memory_snapshot()["operation_reserved_bytes"] == 0)

    observed.clear()
    with inject_oversized_commit(budget, bound, observed):
        discarded = bound.discard_records([])
    with inject_oversized_commit(budget, forecasts, observed):
        discarded_forecast = forecasts.discard_records([])
    report.check("failed_discard_does_not_drop_existing_evidence_or_forecasts",
                 discarded is False and discarded_forecast is False
                 and bound.export_record(record.record_id) == old_encoded
                 and forecasts.export_record(record.record_id) == old_encoded_forecast
                 and bound._pending_workspace is None and forecasts._pending_workspace is None)

    old_root_names = set(budget.owned_roots())
    observed.clear()
    with inject_oversized_commit(budget, bound, observed):
        bad_clone = rejected(bound.clone)
    with inject_oversized_commit(budget, forecasts, observed):
        bad_snapshot = rejected(forecasts.snapshot)
    gc.collect()
    report.check("actual_heap_rejection_of_copies_raises_and_leaves_no_copy_root_or_reservation",
                 bad_clone and bad_snapshot and set(budget.owned_roots()) == old_root_names
                 and bound.export_record(record.record_id) == old_encoded
                 and forecasts.export_record(record.record_id) == old_encoded_forecast
                 and bound._pending_workspace is None and forecasts._pending_workspace is None
                 and budget.memory_snapshot()["operation_reserved_bytes"] == 0)

    normal_limit = budget.limit_bytes
    budget.limit_bytes = budget.check()["total_accounted_bytes"] + 2048
    old_stats, old_counts = bound._statistics, forecasts._counts
    old_root_names = set(budget.owned_roots())
    refused = [bound.observe(spec, record, epoch, similarity, "no-space", 0.0) is False,
               bound.refresh() == 0,
               forecasts.observe(spec.identity, record.record_id, record.revision, epoch, True) is False,
               bound.discard_records([]) is False, forecasts.discard_records([]) is False]
    clone_refused = rejected(bound.clone) and rejected(bound.snapshot) and rejected(forecasts.snapshot)
    report.check("insufficient_reservation_rejects_updates_and_raises_for_copies_without_unbudgeted_fallback",
                 all(refused) and clone_refused and old_root_names == set(budget.owned_roots())
                 and bound._statistics is old_stats and forecasts._counts is old_counts
                 and budget.memory_snapshot()["operation_reserved_bytes"] == 0)
    budget.limit_bytes = normal_limit

    tiny = CachedRecord.create(spec, spec.project(initial), {"v2": 2}, epoch,
                               record_id="metadata-too-small", metadata_capacity=1)
    report.check("serialized_metadata_capacity_remains_an_independent_limit",
                 bound.observe(spec, tiny, epoch, similarity, "fixture", 0.0) is False
                 and tiny.record_id not in bound._capacities)

    large_bootstrap = ReliabilityTable(ReliabilityConfig(min_groups=1, bootstrap_samples=1_000_000), clock=clock)
    large_bootstrap.bind_memory_budget(budget)
    assert large_bootstrap.observe(spec, record, epoch, similarity, "large-bootstrap", 0.0)
    original_large_history = large_bootstrap.export_record(record.record_id)
    report.check("bootstrap_sample_workspace_is_reserved_before_computation",
                 large_bootstrap.refresh() == 0
                 and large_bootstrap.export_record(record.record_id) == original_large_history
                 and large_bootstrap.lookup(spec, record, epoch, similarity) is None)

    clone_names = (copied._memory_prefix, forecast_copy._memory_prefix)
    cloned_weak = (weakref.ref(copied), weakref.ref(forecast_copy))
    del copied, forecast_copy
    gc.collect()
    report.check("weak_root_getters_and_finalizers_release_unreferenced_snapshot_owners",
                 all(reference() is None for reference in cloned_weak)
                 and all(name not in budget.owned_roots() for name in clone_names))

    parallel = ForecastTable()
    parallel.bind_memory_budget(budget)
    with ThreadPoolExecutor(max_workers=4) as pool:
        parallel_results = list(pool.map(lambda index: parallel.observe(spec.identity, "parallel-record", 1, epoch, index % 2 == 0), range(40)))
    report.check("shared_budget_transaction_serializes_counter_updates_without_lost_observations",
                 all(parallel_results) and parallel._counts[(spec.identity, "parallel-record", 1, epoch)] == (20, 40))

    assert bound.discard_records([]) is True
    assert forecasts.discard_records([]) is True
    report.check("successful_discard_clears_record_statistics_and_temporary_reservations",
                 not bound._statistics and not bound._capacities and not forecasts._counts
                 and budget.memory_snapshot()["operation_reserved_bytes"] == 0)
    report.check("heap_budget_scope_does_not_claim_process_physical_memory_enforcement",
                 budget.check()["total_accounted_bytes"] <= budget.limit_bytes
                 and budget.memory_snapshot()["physical_budget_enforced"] is False)
    report.check("changing_a_bound_tables_domain_is_explicitly_rejected",
                 _reject_domain_change(bound, CacheMemoryBudget(1024 * 1024, journal_limit=0))
                 and _reject_domain_change(forecasts, CacheMemoryBudget(1024 * 1024, journal_limit=0)))
    _pressure_clear_checks(report, spec, initial, epoch, config, clock)


def _pressure_clear_checks(report, spec, initial, epoch, config, clock):
    pressure_budget = CacheMemoryBudget(4 * 1024**2, journal_limit=8)
    reliability = ReliabilityTable(config, clock=clock)
    forecasts = ForecastTable()
    reliability.bind_memory_budget(pressure_budget)
    forecasts.bind_memory_budget(pressure_budget)
    records = [CachedRecord.create(spec, spec.project(initial), {"v2": 2}, epoch,
                                   record_id=f"pressure-fixture-{index}") for index in range(16)]
    for record in records:
        for group in range(4):
            assert reliability.observe(spec, record, epoch, 0.99, f"pressure-group-{group}", 0.0)
        assert forecasts.observe(spec.identity, record.record_id, record.revision, epoch, True)
    assert reliability.refresh() == len(records)
    reliability_copy = reliability.clone()
    forecast_copy = forecasts.snapshot()
    evidence_before = reliability_copy.export_record(records[0].record_id)
    forecast_before = forecast_copy.export_record(records[0].record_id)
    expected_evidence = len(reliability._statistics)
    expected_forecasts = len(forecasts._counts)
    before = pressure_budget.memory_snapshot()["owned_heap_bytes"]
    # Real low headroom: all live tables/snapshots fit, but a COW cleanup does
    # not. The pressure fallback must work without obtaining another reserve.
    pressure_budget.limit_bytes = pressure_budget.check()["total_accounted_bytes"] + 2048
    rejected_cleanup = reliability.discard_records(()) is False and forecasts.discard_records(()) is False
    def deny_reservation(*unused, **unused_keywords):
        raise CacheMemoryError("fixture denies every reservation")

    with patch.object(pressure_budget, "reservation", new=deny_reservation):
        removed_evidence = reliability.fail_closed_clear()
        removed_forecasts = forecasts.fail_closed_clear()
    report.check("pressure_reset_recovers_after_COW_discard_rejection_without_any_reservation",
                 rejected_cleanup and removed_evidence == expected_evidence == len(records)
                 and removed_forecasts == expected_forecasts == len(records)
                 and reliability.lookup(spec, records[0], epoch, 0.99) is None
                 and forecasts.probability(spec, records[0], epoch) == 0.0
                 and not reliability._statistics and not reliability._capacities and not forecasts._counts
                 and pressure_budget.memory_snapshot()["owned_heap_bytes"] < before
                 and pressure_budget.memory_snapshot()["operation_reserved_bytes"] == 0)
    report.check("pressure_reset_preserves_independent_evidence_and_forecast_snapshots",
                 reliability_copy.export_record(records[0].record_id) == evidence_before
                 and reliability_copy.lookup(spec, records[0], epoch, 0.99).groups == 4
                 and forecast_copy.export_record(records[0].record_id) == forecast_before
                 and forecast_copy.probability(spec, records[0], epoch) == 1.0
                 and reliability.fail_closed_clear() == forecasts.fail_closed_clear() == 0)

    unbound = ReliabilityTable(config, clock=clock)
    unbound_forecast = ForecastTable()
    unbound.observe(spec, records[0], epoch, 0.99, "unbound-pressure", 0.0)
    unbound_forecast.observe(spec.identity, records[0].record_id, records[0].revision, epoch, True)
    report.check("pressure_reset_also_forgets_unbudgeted_legacy_tables",
                 unbound.fail_closed_clear() == unbound_forecast.fail_closed_clear() == 1
                 and not unbound._statistics and not unbound._capacities and not unbound_forecast._counts)

    callback_budget = CacheMemoryBudget(1024**2, journal_limit=0)
    callback_table = ForecastTable()
    callback_table.bind_memory_budget(callback_budget)
    callback_table.observe(spec.identity, "callback-fixture", 1, epoch, True)
    propagated = False
    def failing_check():
        raise RuntimeError("fixture ledger callback failed")

    with patch.object(callback_budget, "check", new=failing_check):
        try:
            callback_table.fail_closed_clear()
        except RuntimeError as error:
            propagated = str(error) == "fixture ledger callback failed"
    report.check("pressure_reset_does_not_mask_a_ledger_callback_exception",
                 propagated and not callback_table._counts)


def _reject_domain_change(table, budget):
    try:
        table.bind_memory_budget(budget)
    except ValueError:
        return True
    return False


if __name__ == "__main__":
    raise SystemExit(run_report("evidence_memory", "B/C：证据与预测表共享堆预算及原子回滚", checks))
