"""Run local checks for D: set utility, audit quota, bytes, pins and versions."""

from __future__ import annotations

import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from reporting import CheckReport, run_report
from workflow.records import CachedRecord, RecordStore
from workflow.residency import OpportunityCacheManager, ResidencyConfig
from workflow.spec import encode_input


def make_record(name: str, *, identity: str = "subgraph", created_at: float = 1000.0) -> CachedRecord:
    """Equal-sized identifiers make the two-slot budget check unambiguous."""
    return CachedRecord(
        subgraph_id=identity,
        record_id=name,
        revision=1,
        epoch="check-epoch",
        input_json=json.dumps({"x": 1}, sort_keys=True),
        output_json=json.dumps({"y": 2}, sort_keys=True),
        embedding=encode_input({"x": 1}),
        created_at=created_at,
        metadata_capacity=128,
    )


def preload(store: RecordStore, records: tuple, budget: int, audit_ids: frozenset = frozenset()) -> None:
    if not store.publish(records, audit_ids, store.version, budget):
        raise AssertionError("could not prepare the local check store")


def checks(report: CheckReport) -> None:
    a, b, c = (make_record(name) for name in ("r_a", "r_b", "r_c"))
    record_size = max(a.byte_size, b.byte_size, c.byte_size)
    two_slots = 2 * record_size

    def opportunity_value(records: tuple) -> float:
        ids = {record.record_id for record in records}
        # a and b are substitutes: keeping both does not double their benefit.
        return (4.0 if ids & {"r_a", "r_b"} else 0.0) + (5.0 if "r_c" in ids else 0.0)

    store = RecordStore()
    preload(store, (a, b), two_slots)
    manager = OpportunityCacheManager(store, ResidencyConfig(max_bytes=two_slots), opportunity_value)
    admitted = manager.admit(c)
    resident = {record.record_id for record in store.snapshot().records}
    report.check(
        "集合机会收益保留替代记录之一并接纳新增机会",
        admitted.accepted and "r_c" in resident and len(resident & {"r_a", "r_b"}) == 1
        and admitted.utility_before == 4.0 and admitted.utility_after == 9.0,
        {"reason": admitted.reason, "resident_ids": sorted(resident),
         "evicted_ids": list(admitted.evicted_ids), "utility_before": admitted.utility_before,
         "utility_after": admitted.utility_after},
    )
    report.check(
        "序列化载荷及元数据预留计入同一字节预算",
        store.byte_size <= two_slots and a.byte_size == len(a.serialized()) + a.metadata_capacity,
        {"managed_bytes": store.byte_size, "max_bytes": two_slots,
         "one_record_bytes": a.byte_size, "metadata_reserved_bytes": a.metadata_capacity,
         "accounting": "managed serialized bytes, not Python RSS"},
    )

    oversized_preloaded_store = RecordStore()
    preload(oversized_preloaded_store, (a, b), two_slots)
    init_rejected = False
    init_message = ""
    try:
        OpportunityCacheManager(
            oversized_preloaded_store, ResidencyConfig(max_bytes=record_size), opportunity_value,
        )
    except ValueError as error:
        init_rejected = True
        init_message = str(error)
    report.check(
        "已有缓存超过新预算时初始化拒绝接入",
        init_rejected and oversized_preloaded_store.byte_size == two_slots,
        {"existing_bytes": oversized_preloaded_store.byte_size,
         "requested_max_bytes": record_size, "message": init_message},
    )

    smaller_value = lambda records: max(
        (4.0 if record.record_id == "r_a" else 3.0 for record in records), default=0.0,
    )
    rejecting_store = RecordStore()
    preload(rejecting_store, (a,), record_size)
    rejecting_manager = OpportunityCacheManager(
        rejecting_store, ResidencyConfig(max_bytes=record_size), smaller_value,
    )
    old_version = rejecting_store.version
    rejected = rejecting_manager.admit(c)
    report.check(
        "整体机会收益下降时拒绝准入且原缓存不变",
        not rejected.accepted and rejecting_store.version == old_version
        and {record.record_id for record in rejecting_store.snapshot().records} == {"r_a"},
        {"reason": rejected.reason, "utility_before": rejected.utility_before,
         "utility_after": rejected.utility_after},
    )

    threshold_store = RecordStore()
    preload(threshold_store, (a,), record_size)
    threshold_manager = OpportunityCacheManager(
        threshold_store, ResidencyConfig(max_bytes=record_size, kappa=1.0),
        lambda records: max((4.0 if item.record_id == "r_a" else 5.0 for item in records), default=0.0),
    )
    threshold = threshold_manager.admit(c)
    report.check(
        "净增益等于转移费用时不发布",
        not threshold.accepted and threshold.reason == "utility_gain_below_transition_cost"
        and threshold.utility_after - threshold.utility_before == 1.0,
        {"reason": threshold.reason, "kappa": 1.0},
    )

    audit_store = RecordStore()
    preload(audit_store, (a, b), two_slots)
    audit_calls = []

    def audit_value(records: tuple) -> float:
        ids = {record.record_id for record in records}
        audit_calls.append(sorted(ids))
        if "r_c" in ids:
            raise AssertionError("audit-only record reached the serving utility model")
        return float(len(ids))

    audit_manager = OpportunityCacheManager(
        audit_store,
        ResidencyConfig(max_bytes=two_slots, audit_fraction=0.5, audit_ttl_seconds=10000),
        audit_value,
    )
    audited = audit_manager.admit(c, audit_only=True, now=1001.0)
    audit_snapshot = audit_store.snapshot(include_audit=True)
    report.check(
        "审计配额可回收服务借用空间且审计记录不参与服务估值",
        audited.accepted and audit_snapshot.audit_ids == frozenset(("r_c",))
        and audit_store.get("r_c") is None and audit_store.get("r_c", include_audit=True) is not None
        and audited.utility_after < audited.utility_before
        and audit_snapshot.byte_size <= two_slots,
        {"reason": audited.reason, "audit_ids": sorted(audit_snapshot.audit_ids),
         "evicted_ids": list(audited.evicted_ids), "utility_model_inputs": audit_calls},
    )

    lifecycle_store = RecordStore()
    lifecycle_budget = 4 * record_size
    lifecycle_manager = OpportunityCacheManager(
        lifecycle_store,
        ResidencyConfig(max_bytes=lifecycle_budget, audit_fraction=1.0,
                        audit_ttl_seconds=10.0, audit_per_identity=1),
        lambda records: float(len(records)),
    )
    first = lifecycle_manager.admit(a, audit_only=True, now=1000.0)
    second = lifecycle_manager.admit(b, audit_only=True, now=1001.0)
    report.check(
        "每个子图身份的审计记录数量有界",
        first.accepted and second.accepted and lifecycle_store.get("r_a", include_audit=True) is None
        and lifecycle_store.get("r_b", include_audit=True) is not None,
        {"evicted_ids": list(second.evicted_ids), "audit_per_identity": 1},
    )
    expiration = lifecycle_manager.maintenance_tick(now=1011.0)
    report.check(
        "无新增请求时维护轮次仍清理过期审计记录",
        len(lifecycle_store.snapshot(include_audit=True).records) == 0
        and any(item.reason == "audit_expired" for item in expiration),
        {"maintenance_reasons": [item.reason for item in expiration],
         "managed_bytes_after_expiry": lifecycle_store.byte_size},
    )

    promotion_store = RecordStore()
    promotion_manager = OpportunityCacheManager(
        promotion_store,
        ResidencyConfig(max_bytes=two_slots, audit_fraction=1.0),
        lambda records: float(len(records)),
    )
    fresh = make_record("r_c", created_at=time.time())
    promotion_manager.admit(fresh, audit_only=True)
    bytes_before = promotion_store.byte_size
    promotion = promotion_manager.promote(fresh.record_id)
    report.check(
        "外部资格确认后审计记录经收益门槛晋升服务状态",
        promotion.accepted and promotion_store.get(fresh.record_id) is not None
        and fresh.record_id not in promotion_store.snapshot(include_audit=True).audit_ids
        and promotion_store.byte_size == bytes_before,
        {"reason": promotion.reason, "managed_bytes": promotion_store.byte_size,
         "note": "D receives the promotion request; B eligibility is checked by the runtime"},
    )

    pin_store = RecordStore()
    preload(pin_store, (a,), record_size)
    pin_manager = OpportunityCacheManager(
        pin_store, ResidencyConfig(max_bytes=record_size),
        lambda records: max((4.0 if item.record_id == "r_a" else 8.0 for item in records), default=0.0),
    )
    pinned = pin_store.acquire_pin(a.record_id, a.revision)
    blocked = pin_manager.admit(c)
    retained_while_pinned = pin_store.get(a.record_id) is not None
    pin_store.release_pin(a.record_id, a.revision)
    unblocked = pin_manager.admit(c)
    report.check(
        "pin中记录无法回收且释放pin后可重新准入",
        pinned is not None and not blocked.accepted and retained_while_pinned and unblocked.accepted,
        {"while_pinned": blocked.reason, "after_release": unblocked.reason},
    )

    race_store = RecordStore()
    preload(race_store, (a,), two_slots)
    version_changed = [False]

    def changing_version_value(records: tuple) -> float:
        if not version_changed[0]:
            version_changed[0] = True
            snapshot = race_store.snapshot(include_audit=True)
            if not race_store.publish(snapshot.records, snapshot.audit_ids, snapshot.version, two_slots):
                raise AssertionError("could not simulate a concurrent cache publication")
        return float(len(records))

    race_manager = OpportunityCacheManager(
        race_store, ResidencyConfig(max_bytes=two_slots), changing_version_value,
    )
    race = race_manager.admit(c)
    report.check(
        "试算过程中缓存版本变化阻止旧决策发布",
        not race.accepted and race.reason == "version_or_pin_conflict"
        and race_store.get(c.record_id) is None and race_store.get(a.record_id) is not None,
        {"reason": race.reason, "store_version": race_store.version},
    )

    tiny_store = RecordStore()
    tiny_manager = OpportunityCacheManager(
        tiny_store, ResidencyConfig(max_bytes=record_size - 1), lambda records: float(len(records)),
    )
    oversized = tiny_manager.admit(a)
    report.check(
        "超大单记录被拒绝且不会突破容量",
        not oversized.accepted and oversized.reason == "record_exceeds_budget" and tiny_store.byte_size == 0,
        {"reason": oversized.reason, "record_bytes": a.byte_size, "max_bytes": record_size - 1},
    )

    queue_store = RecordStore()
    queue_manager = OpportunityCacheManager(
        queue_store,
        ResidencyConfig(max_bytes=two_slots, audit_fraction=1.0, queue_limit=1,
                        audit_ttl_seconds=10000),
        lambda records: float(len(records)),
    )
    enqueued = queue_manager.enqueue(a)
    full = not queue_manager.enqueue(b)
    unchanged_before_tick = queue_store.byte_size == 0
    tick = queue_manager.maintenance_tick(now=1001.0)
    report.check(
        "有界队列只在显式后台维护轮次发布记录",
        enqueued and full and unchanged_before_tick and queue_manager.queued_count == 0
        and len(tick) == 1 and tick[0].accepted and queue_store.get(a.record_id, include_audit=True) is not None,
        {"queue_limit": 1, "maintenance_reasons": [item.reason for item in tick]},
    )


if __name__ == "__main__":
    raise SystemExit(run_report("d_opportunity_residency", "D：机会感知缓存驻留", checks))
