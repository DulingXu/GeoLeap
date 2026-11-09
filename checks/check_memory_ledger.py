"""Independent offline checks of the accounted Python heap ledger, not RSS."""

from __future__ import annotations

from dataclasses import dataclass
import gc
import sys
import threading
import weakref

from reporting import CheckReport, run_report
from workflow.memory import CacheMemoryBudget, CacheMemoryError, measure_object_graph


def oracle(*roots):
    """Small independent oracle for the controlled builtin fixtures only."""
    seen, pending, total = set(), list(roots), 0
    while pending:
        item = pending.pop()
        if id(item) in seen:
            continue
        seen.add(id(item))
        total += sys.getsizeof(item)
        if isinstance(item, dict):
            pending.extend(item.keys())
            pending.extend(item.values())
        elif isinstance(item, (list, tuple, set, frozenset)):
            pending.extend(item)
    return total


def raises(error_type, callback):
    try:
        callback()
    except error_type:
        return True
    return False


def checks(report: CheckReport):
    payload = bytearray(8192)
    first, second = [payload], {"same": payload}
    result = measure_object_graph({"resident": first, "pending": second})
    report.check("shared_identity_matches_independent_union_oracle",
                 result["owned_heap_bytes"] == oracle(first, second))
    report.check("exclusive_categories_reconcile_to_total",
                 sum(result["category_exclusive_bytes"].values()) == result["owned_heap_bytes"])
    report.check("second_alias_does_not_charge_shared_buffer_twice",
                 result["category_exclusive_bytes"]["pending"]
                 == oracle(second) - sys.getsizeof(payload))
    duplicate = bytearray(payload)
    report.check("equal_distinct_buffers_are_separate_real_allocations",
                 duplicate == payload and duplicate is not payload
                 and measure_object_graph({"a": payload, "b": duplicate})["owned_heap_bytes"]
                 == sys.getsizeof(payload) + sys.getsizeof(duplicate))
    cyclic = []
    cyclic.append(cyclic)
    report.check("cycles_are_deduplicated_without_recursion",
                 measure_object_graph({"cycle": cyclic})["owned_heap_bytes"] == sys.getsizeof(cyclic))
    report.check("empty_roots_and_none_getter_result_are_supported",
                 measure_object_graph({})["owned_heap_bytes"] == 0
                 and measure_object_graph({"empty": None})["owned_heap_bytes"] == 0)

    @dataclass(slots=True)
    class Slot:
        value: object

    slot = Slot(payload)
    report.check("dataclass_slots_keep_the_owned_payload_in_scope",
                 measure_object_graph({"slot": slot})["owned_heap_bytes"]
                 == sys.getsizeof(slot) + sys.getsizeof(payload))

    @dataclass
    class DictionaryObject:
        value: object

    normal = DictionaryObject(payload)
    report.check("dataclass_dict_includes_container_and_referents",
                 measure_object_graph({"normal": normal})["owned_heap_bytes"]
                 == sys.getsizeof(normal) + oracle(vars(normal)))
    view = memoryview(payload)
    report.check("memoryview_reaches_underlying_buffer_once",
                 measure_object_graph({"view": view, "payload": payload})["owned_heap_bytes"]
                 == sys.getsizeof(view) + sys.getsizeof(payload))
    report.check("opaque_function_does_not_walk_captured_external_objects",
                 measure_object_graph({"getter": lambda: payload})["owned_heap_bytes"]
                 == sys.getsizeof(lambda: None))
    proxy_reads = []
    class CallableProxy:
        def __init__(self):
            self.hidden_payload = bytearray(16384)
        def __call__(self):
            raise AssertionError("heap measurement must not invoke a callable proxy")
        def __getattribute__(self, name):
            if name in {"__dict__", "__slots__", "hidden_payload"}:
                proxy_reads.append(name)
                raise AssertionError("heap measurement must not inspect callable proxy internals")
            return object.__getattribute__(self, name)
    proxy = CallableProxy()
    proxy_measurement = measure_object_graph({"proxy": proxy})
    report.check("callable_proxy_is_explicitly_opaque_without_reading_or_traversing_internals",
                 not proxy_reads and proxy_measurement["owned_heap_bytes"] == sys.getsizeof(proxy)
                 and type(proxy).__module__ + "." + type(proxy).__qualname__ in proxy_measurement["opaque_types"]
                 and any("callable" in exclusion for exclusion in proxy_measurement["exclusions"])
                 and proxy_measurement["physical_budget_enforced"] is False)
    report.check("heap_accounting_is_never_mislabeled_physical_rss",
                 result["physical_budget_enforced"] is False
                 and result["complete_physical_budget"] is False
                 and "not process RSS" in result["scope"] and bool(result["exclusions"]))

    budget = CacheMemoryBudget(256 * 1024, journal_limit=4)
    initial = budget.check()
    report.check("base_control_graph_and_control_reserve_are_charged",
                 initial["category_exclusive_bytes"]["__ledger__"] > 0
                 and initial["control_reserve_bytes"] > 0
                 and initial["total_accounted_bytes"] == initial["owned_heap_bytes"] + initial["reserved_bytes"])
    owned = []
    handle = budget.register_root("resident", lambda: owned, category="resident")
    report.check("registered_root_view_and_empty_getter_are_available",
                 budget.owned_roots()["resident"] is owned)
    empty = budget.register_root("empty", lambda: None)
    report.check("empty_registered_root_has_no_payload_charge",
                 budget.memory_snapshot()["root_exclusive_bytes"]["empty"] == 0)
    with budget.reservation(16384, "publication") as token:
        reserved = budget.memory_snapshot()
        report.check("reserve_is_charged_before_payload_allocation",
                     reserved["operation_reserved_bytes"] == 16384 and not owned)
        owned.append(bytearray(4096))
        token.commit()
    after = budget.check()
    report.check("commit_transfers_reservation_to_real_owned_roots",
                 after["operation_reserved_bytes"] == 0
                 and after["category_exclusive_bytes"]["resident"] == oracle(owned))
    before_ids = tuple(map(id, owned))
    def oversized():
        with budget.reservation(budget.limit_bytes, "must_reject"):
            owned.append(bytearray(1))
    report.check("oversized_reservation_rejects_before_body_and_keeps_roots",
                 raises(CacheMemoryError, oversized) and tuple(map(id, owned)) == before_ids
                 and budget.check()["operation_reserved_bytes"] == 0)
    def original_failure():
        with budget.reservation(4096, "rollback"):
            raise LookupError("deliberate caller error")
    report.check("exception_rolls_back_reservation_without_masking_original",
                 raises(LookupError, original_failure)
                 and budget.check()["operation_reserved_bytes"] == 0)
    with budget.reservation(1024, "manual_release") as token:
        token.release()
        token.release()
    report.check("release_is_idempotent_and_context_finally_does_not_leak",
                 budget.check()["operation_reserved_bytes"] == 0)
    for _ in range(20):
        with budget.reservation(0, "scalar_event"):
            pass
    journal = budget.memory_snapshot()
    report.check("journal_is_bounded_scalar_only_and_itself_measured",
                 len(journal["journal"]) <= 4
                 and all(all(isinstance(value, (str, int, float, bool, type(None))) for value in event)
                         for event in journal["journal"])
                 and journal["category_exclusive_bytes"]["__ledger__"] > initial["category_exclusive_bytes"]["__ledger__"])
    report.check("peak_is_observed_whole_total_and_stays_separate_from_rss",
                 journal["total_accounted_bytes"] <= journal["peak_accounted_bytes"] <= journal["limit_bytes"]
                 and journal["physical_budget_enforced"] is False
                 and journal["reclaimable_bytes"] is None)
    report.check("root_unregister_is_explicit_and_idempotent",
                 handle.unregister() and not handle.unregister() and "resident" not in budget.owned_roots())
    replacement = budget.register_root("resident", lambda: None)
    report.check("old_handle_cannot_unregister_a_new_registration_with_same_name",
                 not handle.unregister() and "resident" in budget.owned_roots())
    replacement.close()
    empty.close()

    class Owner:
        def __init__(self):
            self.payload = bytearray(4096)
        def root(self):
            return self.payload

    owner = Owner()
    weak = weakref.ref(owner)
    weak_handle = budget.register_root("weak_owner", owner.root)
    del owner
    gc.collect()
    report.check("bound_method_registration_does_not_keep_runtime_alive",
                 weak() is None and "weak_owner" not in budget.owned_roots())
    weak_handle.close()
    live_empty = Owner()
    live_empty.payload = None
    live_handle = budget.register_root("live_empty_owner", live_empty.root)
    report.check("live_weak_getter_returning_none_is_not_misclassified_dead",
                 "live_empty_owner" in budget.owned_roots()
                 and budget.owned_roots()["live_empty_owner"] is None)
    live_handle.close()
    for index in range(200):
        temporary_owner = Owner()
        budget.register_root("temporary_" + str(index), temporary_owner.root)
        del temporary_owner
        budget.memory_snapshot()
    report.check("repeated_dead_clone_roots_do_not_accumulate_registration_entries",
                 not any(name.startswith("temporary_") for name in budget.owned_roots())
                 and not any(name.startswith("temporary_") for name in budget._roots))
    report.check("zero_and_below_base_limits_explicitly_reject",
                 raises(CacheMemoryError, lambda: CacheMemoryBudget(0))
                 and raises(CacheMemoryError, lambda: CacheMemoryBudget(1)))
    report.check("invalid_limits_are_not_coerced_from_boolean_or_float",
                 all(raises(ValueError, lambda value=value: CacheMemoryBudget(value))
                     for value in (-1, True, 1.5)))
    report.check("invalid_reservations_are_rejected",
                 raises(ValueError, lambda: budget.reservation(-1).__enter__()))
    before = budget.owned_roots()
    incoming_oversized = bytearray(512 * 1024)
    report.check("oversized_root_registration_is_transactional",
                 raises(CacheMemoryError, lambda: budget.register_root("too_big", lambda: incoming_oversized))
                 and budget.owned_roots() == before)

    small = CacheMemoryBudget(64 * 1024, journal_limit=0)
    mutation = []
    small.register_root("mutable", lambda: mutation)
    try:
        with small.transaction():
            mutation.append(bytearray(128 * 1024))
    except CacheMemoryError:
        mutation.clear()  # Contract: caller rolls back its own root mutation.
        normal_exit_rejected = True
    else:
        normal_exit_rejected = False
    report.check("transaction_checks_normal_exit_and_caller_can_rollback",
                 normal_exit_rejected and small.check()["total_accounted_bytes"] <= small.limit_bytes)
    report.check("transaction_preserves_original_body_exception",
                 raises(ZeroDivisionError, lambda: transaction_failure(small)))

    threaded = CacheMemoryBudget(256 * 1024, journal_limit=8)
    current, errors = [], []
    threaded.register_root("thread_payload", lambda: current)
    barrier = threading.Barrier(3)
    def worker():
        try:
            barrier.wait(timeout=3)
            for _ in range(20):
                with threaded.transaction():
                    with threaded.reservation(4096, "thread") as token:
                        current.append(bytearray(512))
                        token.commit()
                    current.pop()
        except BaseException as error:
            errors.append(type(error).__name__)
    threads = [threading.Thread(target=worker) for _ in range(2)]
    for thread in threads:
        thread.start()
    barrier.wait(timeout=3)
    for thread in threads:
        thread.join(timeout=5)
    final = threaded.check()
    report.check("real_threads_serialize_mutations_without_deadlock_or_token_leak",
                 not errors and all(not t.is_alive() for t in threads)
                 and not current and final["operation_reserved_bytes"] == 0
                 and final["peak_accounted_bytes"] <= final["limit_bytes"], {"errors": errors})

    detached_budget = CacheMemoryBudget(256 * 1024, journal_limit=8)
    returned_payload = []
    detached_budget.register_root("returned_trace", lambda: returned_payload)
    detached = detached_budget.acquire_reservation(8192, "request_telemetry")
    ready, entered, finish = threading.Barrier(2), threading.Event(), threading.Event()
    detached_errors = []
    def competing_request():
        second = None
        try:
            ready.wait(timeout=3)
            second = detached_budget.acquire_reservation(4096, "other_request")
            entered.set()
            if not finish.wait(timeout=3):
                raise TimeoutError("test did not release competing request")
        except BaseException as error:
            detached_errors.append(type(error).__name__)
        finally:
            if second is not None:
                second.release()
    competing = threading.Thread(target=competing_request, daemon=True)
    competing.start()
    ready.wait(timeout=3)
    another_thread_entered = entered.wait(timeout=3)
    both_charged = (detached_budget.memory_snapshot()["operation_reserved_bytes"]
                    if another_thread_entered else None)
    finish.set()
    competing.join(timeout=3)
    report.check("detached_reservation_returns_without_holding_global_lock",
                 another_thread_entered and not competing.is_alive() and not detached_errors,
                 {"errors": detached_errors})
    remains_charged = detached_budget.memory_snapshot()["operation_reserved_bytes"]
    with detached_budget.transaction():
        returned_payload.append(bytearray(4096))
        detached.commit()
    detached_final = detached_budget.check()
    report.check("detached_token_stays_charged_across_requests_then_commits_to_roots",
                 both_charged == 8192+4096 and remains_charged == 8192
                 and detached_final["operation_reserved_bytes"] == 0
                 and detached_final["category_exclusive_bytes"]["returned_trace"] == oracle(returned_payload)
                 and detached_final["physical_budget_enforced"] is False)
    def failed_remote_request():
        token = detached_budget.acquire_reservation(1536, "failed_request")
        try:
            raise LookupError("simulated remote service failure")
        finally:
            token.release()
    report.check("detached_token_finally_release_preserves_original_exception",
                 raises(LookupError, failed_remote_request)
                 and detached_budget.check()["operation_reserved_bytes"] == 0)
    report.output_dir.mkdir(parents=True, exist_ok=True)
    import json
    (report.output_dir / "ledger_snapshot.json").write_text(json.dumps(final, indent=2) + "\n")


def transaction_failure(budget):
    with budget.transaction():
        raise ZeroDivisionError("original")


if __name__ == "__main__":
    raise SystemExit(run_report("memory_ledger", "缓存内存账本：Python堆归属、预留与事务（非RSS上限）", checks))
