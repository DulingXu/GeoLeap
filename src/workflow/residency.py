"""Small, opportunity-aware cache manager for declared workflows.

The manager is driven by ``maintenance_tick`` rather than by every lookup.
Its caller supplies one fixed whole-workflow utility model for a maintenance
round. Only serving records enter that model; audit records use a separate
exploration quota inside the same managed-serialization byte budget.

This first implementation has no shared payload allocations. Consequently a
record's marginal reclaimable space is its ``byte_size``. These managed bytes
include reserved record metadata and are deliberately not a Python RSS limit.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from enum import Enum
from functools import wraps
import math
import threading
import time
from typing import Callable, Deque, FrozenSet, Iterable, List, Optional, Tuple

from .records import CachedRecord, RecordStore


class ResidencyMode(str, Enum):
    LRU = "lru"
    LEGACY_DENSITY = "legacy_density"
    ADDITIVE_VALUE = "additive_value"
    FULL = "full"

    @classmethod
    def parse(cls, value):
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            try:
                return cls(value.strip().lower().replace("-", "_"))
            except ValueError:
                pass
        raise ValueError("residency policy must be lru, legacy_density, additive_value or full")


def _measured_maintenance(function):
    """Measure outer maintenance calls, without counting nested admits twice."""
    @wraps(function)
    def measured(self, *args, **kwargs):
        with self._maintenance_lock:
            outer = self._measurement_depth == 0
            if outer:
                wall, cpu = time.perf_counter(), time.thread_time()
            self._measurement_depth += 1
            try:
                return function(self, *args, **kwargs)
            finally:
                self._measurement_depth -= 1
                if outer:
                    self._stats["maintenance_wall_ms"] += (time.perf_counter() - wall) * 1000
                    self._stats["maintenance_cpu_ms"] += (time.thread_time() - cpu) * 1000
    return measured


@dataclass(frozen=True)
class ResidencyConfig:
    max_bytes: int = 4 * 1024 * 1024
    audit_fraction: float = 0.25
    audit_ttl_seconds: float = 3600.0
    audit_per_identity: int = 2
    kappa: float = 0.0
    queue_limit: int = 64
    eviction_shortlist: int = 32
    policy: ResidencyMode = ResidencyMode.FULL
    legacy_alpha: float = 0.5
    legacy_beta: float = 0.5
    legacy_cost_reference: float = 1.0
    legacy_latency_reference_ms: float = 100.0
    legacy_half_life_requests: float = 64.0
    legacy_initial_reuse_weight: float = 1.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "policy", ResidencyMode.parse(self.policy))
        if self.max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        if not math.isfinite(self.audit_fraction) or not 0 <= self.audit_fraction <= 1:
            raise ValueError("audit_fraction must be between zero and one")
        if not math.isfinite(self.audit_ttl_seconds) or self.audit_ttl_seconds <= 0:
            raise ValueError("audit_ttl_seconds must be positive")
        if self.audit_per_identity <= 0 or self.queue_limit <= 0 or self.eviction_shortlist <= 0:
            raise ValueError("audit_per_identity, queue_limit and eviction_shortlist must be positive")
        if not math.isfinite(self.kappa) or self.kappa < 0:
            raise ValueError("kappa must be nonnegative")
        for name in ("legacy_alpha", "legacy_beta", "legacy_initial_reuse_weight"):
            if not math.isfinite(getattr(self, name)) or getattr(self, name) < 0:
                raise ValueError(f"{name} must be finite and nonnegative")
        if self.legacy_alpha + self.legacy_beta <= 0:
            raise ValueError("legacy objective needs a positive weight")
        for name in ("legacy_cost_reference", "legacy_latency_reference_ms", "legacy_half_life_requests"):
            if not math.isfinite(getattr(self, name)) or getattr(self, name) <= 0:
                raise ValueError(f"{name} must be finite and positive")


@dataclass(frozen=True)
class AdmissionResult:
    accepted: bool
    reason: str
    evicted_ids: Tuple[str, ...] = ()
    byte_size: int = 0
    utility_before: float = 0.0
    utility_after: float = 0.0


class OpportunityCacheManager:
    """Bounded virtual trials followed by a version-checked store publication.

    ``value_fn`` receives a tuple of *serving* records and returns their set
    utility under the caller's fixed planning policy, request window, profiles,
    and normalization. It must not execute remote operations. In particular it
    should account for alternatives and combinations instead of summing a
    record's isolated benefit. The manager itself does not decide B eligibility;
    callers submit a record to ``promote`` only after B has supplied support.

    Failed trials do not mutate the store. ``utility_after`` describes the
    evaluated trial even when its gain fails the transition-cost threshold;
    ``byte_size`` always describes the published store after the call.
    """

    def __init__(
        self,
        store: RecordStore,
        config: ResidencyConfig,
        value_fn: Callable[[Tuple[CachedRecord, ...]], float],
        *,
        record_benefit_fn: Optional[Callable[[CachedRecord], Tuple[float, float]]] = None,
    ) -> None:
        self.store = store
        self.config = config
        if store.byte_size > config.max_bytes:
            raise ValueError("Existing store exceeds the configured managed byte budget")
        self.value_fn = value_fn
        if config.policy == ResidencyMode.LEGACY_DENSITY and record_benefit_fn is None:
            raise ValueError("legacy_density requires record_benefit_fn returning (latency_ms, cost_usd)")
        self.record_benefit_fn = record_benefit_fn
        self._queue: Deque[Tuple[CachedRecord, bool]] = deque()
        self._queue_lock = threading.Lock()
        self._maintenance_lock = threading.RLock()
        self._measurement_depth = 0
        initial_records = store.snapshot(include_audit=True).records
        self._last_access = {record.record_id: i for i, record in enumerate(initial_records)}
        self._access_clock = len(initial_records)
        self._request_index = 0
        self._reuse_counts = {}
        self._reuse_positions = {}
        self._stats = {"peak_managed_bytes": store.byte_size, "maintenance_cpu_ms": 0.0,
                       "maintenance_wall_ms": 0.0, "admitted_count": 0, "evicted_count": 0,
                       "admitted_bytes": 0, "evicted_bytes": 0, "promoted_count": 0}

    def advance_request(self, request_index: Optional[int] = None) -> int:
        """Advance the logical stream position, including requests with no hits."""
        with self._maintenance_lock:
            position = self._request_index + 1 if request_index is None else request_index
            if isinstance(position, bool) or not isinstance(position, int) or position < self._request_index:
                raise ValueError("request_index must be an integer and cannot move backwards")
            self._request_index = position
            return position

    def _reuse_weight(self, record_id: str) -> float:
        count = self._reuse_counts.get(record_id, 0.0)
        elapsed = max(0, self._request_index - self._reuse_positions.get(record_id, self._request_index))
        return count * math.exp(-math.log(2.0) * elapsed / self.config.legacy_half_life_requests)

    def record_access(self, record_id: str, request_index: Optional[int] = None) -> bool:
        """Record a successful serving reuse; retrieval candidates do not touch LRU."""
        with self._maintenance_lock:
            if request_index is not None:
                self.advance_request(request_index)
            if self.store.get(record_id) is None:
                return False
            self._reuse_counts[record_id] = self._reuse_weight(record_id) + 1.0
            self._reuse_positions[record_id] = self._request_index
            self._access_clock += 1
            self._last_access[record_id] = self._access_clock
            return True

    def maintenance_stats(self) -> dict:
        with self._maintenance_lock:
            return dict(self._stats, managed_bytes=self.store.byte_size,
                        request_index=self._request_index, policy=self.config.policy.value,
                        memory_accounting="managed serialized bytes plus metadata reserve; not RSS",
                        cpu_clock="thread_time", wall_clock="perf_counter")

    def reset_maintenance_stats(self) -> None:
        """Start a measurement window without changing cache or access history."""
        with self._maintenance_lock:
            if self._measurement_depth:
                raise RuntimeError("Cannot reset counters during a maintenance operation")
            self._stats = {"peak_managed_bytes": self.store.byte_size, "maintenance_cpu_ms": 0.0,
                           "maintenance_wall_ms": 0.0, "admitted_count": 0, "evicted_count": 0,
                           "admitted_bytes": 0, "evicted_bytes": 0, "promoted_count": 0}

    def _publish(self, records, audit_ids, snapshot) -> bool:
        if not self.store.publish(records, audit_ids, snapshot.version, self.config.max_bytes):
            return False
        old = {record.record_id: record for record in snapshot.records}
        new = {record.record_id: record for record in records}
        added, removed = set(new) - set(old), set(old) - set(new)
        self._stats["admitted_count"] += len(added)
        self._stats["evicted_count"] += len(removed)
        self._stats["admitted_bytes"] += sum(new[key].byte_size for key in added)
        self._stats["evicted_bytes"] += sum(old[key].byte_size for key in removed)
        self._stats["promoted_count"] += len(snapshot.audit_ids - audit_ids - removed)
        self._stats["peak_managed_bytes"] = max(self._stats["peak_managed_bytes"], self.store.byte_size)
        for key in removed:
            self._last_access.pop(key, None)
            self._reuse_counts.pop(key, None)
            self._reuse_positions.pop(key, None)
        for key in sorted(added):
            self._access_clock += 1
            self._last_access[key] = self._access_clock
        return True

    @property
    def audit_quota_bytes(self) -> int:
        return int(self.config.max_bytes * self.config.audit_fraction)

    @property
    def queued_count(self) -> int:
        with self._queue_lock:
            return len(self._queue)

    @property
    def queued_records(self) -> Tuple[CachedRecord, ...]:
        """Read-only candidate view for freezing one maintenance round's B support."""
        with self._queue_lock:
            return tuple(record for record, _ in self._queue)

    def enqueue(self, record: CachedRecord, audit_only: bool = True) -> bool:
        """Queue a completed immutable record; return false if the queue is full."""
        with self._queue_lock:
            if len(self._queue) >= self.config.queue_limit:
                return False
            self._queue.append((record, audit_only))
            return True

    @_measured_maintenance
    def maintenance_tick(self, now: Optional[float] = None) -> List[AdmissionResult]:
        """Expire audit records and process at most one bounded queue batch.

        Applications can call this between requests or from a background worker.
        A planner never needs to call it before selecting an execution action.
        """
        current_time = time.time() if now is None else now
        if not math.isfinite(current_time):
            raise ValueError("now must be finite")
        with self._maintenance_lock:
            results: List[AdmissionResult] = []
            expiration = self._expire_audits(current_time)
            if expiration is not None:
                results.append(expiration)
            with self._queue_lock:
                batch = [self._queue.popleft() for _ in range(len(self._queue))]
            for record, audit_only in batch:
                results.append(self.admit(record, audit_only=audit_only, now=current_time))
            return results

    def _utility(self, records: Iterable[CachedRecord], audit_ids: FrozenSet[str]) -> float:
        if self.config.policy in (ResidencyMode.LRU, ResidencyMode.LEGACY_DENSITY):
            return 0.0  # These baselines do not invoke the whole-set planner.
        serving = tuple(sorted(
            (record for record in records if record.record_id not in audit_ids),
            key=lambda record: (record.record_id, record.revision),
        ))
        if self.config.policy == ResidencyMode.ADDITIVE_VALUE:
            empty = float(self.value_fn(()))
            result = empty + math.fsum(float(self.value_fn((record,))) - empty for record in serving)
        else:
            result = float(self.value_fn(serving))
        if not math.isfinite(result):
            raise ValueError("value_fn must return a finite utility")
        return result

    @staticmethod
    def _bytes(records: Iterable[CachedRecord]) -> int:
        return sum(record.byte_size for record in records)

    @staticmethod
    def _oldest(records: Iterable[CachedRecord]) -> List[CachedRecord]:
        return sorted(records, key=lambda record: (record.created_at, record.record_id, record.revision))

    def _expired_ids(
        self,
        records: Iterable[CachedRecord],
        audit_ids: FrozenSet[str],
        pinned_ids: FrozenSet[str],
        now: float,
    ) -> FrozenSet[str]:
        return frozenset(
            record.record_id for record in records
            if record.record_id in audit_ids
            and record.record_id not in pinned_ids
            and now - record.created_at >= self.config.audit_ttl_seconds
        )

    def _expire_audits(self, now: float) -> Optional[AdmissionResult]:
        snapshot = self.store.snapshot(include_audit=True)
        expired = self._expired_ids(snapshot.records, snapshot.audit_ids, self.store.pinned_ids(), now)
        if not expired:
            return None
        remaining = tuple(record for record in snapshot.records if record.record_id not in expired)
        published = self._publish(remaining, snapshot.audit_ids - expired, snapshot)
        return AdmissionResult(
            published,
            "audit_expired" if published else "version_or_pin_conflict",
            tuple(sorted(expired)) if published else (),
            self.store.byte_size,
        )

    def _lowest_density_victim(
        self,
        records: Tuple[CachedRecord, ...],
        audit_ids: FrozenSet[str],
        protected_id: str,
        pinned_ids: FrozenSet[str],
    ) -> Optional[CachedRecord]:
        eligible = self._oldest(
            record for record in records
            if record.record_id != protected_id
            and record.record_id not in audit_ids
            and record.record_id not in pinned_ids
        )
        if not eligible:
            return None
        if self.config.policy == ResidencyMode.LRU:
            return min(eligible, key=lambda record: (self._last_access.get(record.record_id, -1), record.record_id))
        if self.config.policy == ResidencyMode.LEGACY_DENSITY:
            return min(eligible, key=lambda record: (self.legacy_density(record),
                       self._last_access.get(record.record_id, -1), record.record_id))
        eligible = eligible[:self.config.eviction_shortlist]
        current_value = self._utility(records, audit_ids)
        choices = []
        for record in eligible:
            remaining = tuple(item for item in records if item.record_id != record.record_id)
            freed = self._bytes(records) - self._bytes(remaining)
            if freed > 0:
                density = (current_value - self._utility(remaining, audit_ids)) / freed
                choices.append((density, record.created_at, record.record_id, record))
        return min(choices, key=lambda item: item[:3])[3] if choices else None

    def legacy_density(self, record: CachedRecord) -> float:
        """Old independent reuse-weighted benefit density, with normalized weights."""
        if self.record_benefit_fn is None:
            raise ValueError("legacy_density requires record_benefit_fn")
        latency, cost = map(float, self.record_benefit_fn(record))
        if not math.isfinite(latency) or not math.isfinite(cost):
            raise ValueError("record benefit must be finite")
        weight_sum = self.config.legacy_alpha + self.config.legacy_beta
        benefit = (self.config.legacy_alpha * cost / self.config.legacy_cost_reference
                   + self.config.legacy_beta * latency / self.config.legacy_latency_reference_ms) / weight_sum
        reuse = max(self._reuse_weight(record.record_id), self.config.legacy_initial_reuse_weight)
        return reuse * benefit / record.byte_size

    @_measured_maintenance
    def admit(
        self,
        record: CachedRecord,
        audit_only: bool = False,
        now: Optional[float] = None,
    ) -> AdmissionResult:
        """Try an audit admission or a positive-whole-set-value serving admission.

        Audit admission is independent of the serving-value threshold. If
        serving records borrowed unused audit quota, they yield capacity using
        the same bounded marginal-loss comparison. Unexpired audit records are
        protected from serving admissions.
        """
        current_time = time.time() if now is None else now
        if not math.isfinite(current_time):
            raise ValueError("now must be finite")
        with self._maintenance_lock:
            snapshot = self.store.snapshot(include_audit=True)
            current_by_id = {item.record_id: item for item in snapshot.records}
            existing = current_by_id.get(record.record_id)
            if existing is not None:
                if existing.serialized() != record.serialized():
                    return AdmissionResult(False, "record_id_collision", byte_size=self.store.byte_size)
                if record.record_id not in snapshot.audit_ids:
                    return AdmissionResult(True, "already_serving", byte_size=self.store.byte_size)
                if current_time - existing.created_at >= self.config.audit_ttl_seconds:
                    return AdmissionResult(False, "audit_record_expired", byte_size=self.store.byte_size)
                if audit_only:
                    return AdmissionResult(True, "already_audit", byte_size=self.store.byte_size)
            if record.byte_size > self.config.max_bytes:
                return AdmissionResult(False, "record_exceeds_budget", byte_size=self.store.byte_size)
            if audit_only and record.byte_size > self.audit_quota_bytes:
                return AdmissionResult(False, "record_exceeds_audit_quota", byte_size=self.store.byte_size)
            if audit_only and current_time - record.created_at >= self.config.audit_ttl_seconds:
                return AdmissionResult(False, "incoming_audit_expired", byte_size=self.store.byte_size)

            pinned_ids = self.store.pinned_ids()
            expired = self._expired_ids(snapshot.records, snapshot.audit_ids, pinned_ids, current_time)
            working = tuple(item for item in snapshot.records if item.record_id not in expired)
            audit_ids = snapshot.audit_ids - expired
            evicted = set(expired)
            try:
                before = self._utility(snapshot.records, snapshot.audit_ids)
                # Moving an existing audit record to serving changes status but
                # does not create a second copy or a second byte charge.
                working = tuple(item for item in working if item.record_id != record.record_id) + (record,)
                if audit_only:
                    audit_ids = audit_ids | frozenset((record.record_id,))
                    same_identity = self._oldest(
                        item for item in working
                        if item.record_id in audit_ids and item.subgraph_id == record.subgraph_id
                        and item.record_id != record.record_id and item.record_id not in pinned_ids
                    )
                    identity_count = sum(
                        item.record_id in audit_ids and item.subgraph_id == record.subgraph_id
                        for item in working
                    )
                    while identity_count > self.config.audit_per_identity and same_identity:
                        victim = same_identity.pop(0)
                        working = tuple(item for item in working if item.record_id != victim.record_id)
                        audit_ids = audit_ids - frozenset((victim.record_id,))
                        evicted.add(victim.record_id)
                        identity_count -= 1
                    if identity_count > self.config.audit_per_identity:
                        return AdmissionResult(False, "audit_identity_pinned", byte_size=self.store.byte_size,
                                               utility_before=before, utility_after=before)
                    while self._bytes(item for item in working if item.record_id in audit_ids) > self.audit_quota_bytes:
                        removable = self._oldest(
                            item for item in working if item.record_id in audit_ids
                            and item.record_id != record.record_id and item.record_id not in pinned_ids
                        )
                        if not removable:
                            return AdmissionResult(False, "audit_quota_blocked", byte_size=self.store.byte_size,
                                                   utility_before=before, utility_after=before)
                        victim = removable[0]
                        working = tuple(item for item in working if item.record_id != victim.record_id)
                        audit_ids = audit_ids - frozenset((victim.record_id,))
                        evicted.add(victim.record_id)
                else:
                    audit_ids = audit_ids - frozenset((record.record_id,))

                while self._bytes(working) > self.config.max_bytes:
                    victim = self._lowest_density_victim(working, audit_ids, record.record_id, pinned_ids)
                    if victim is None:
                        return AdmissionResult(False, "capacity_blocked_by_pins_or_audit_reserve",
                                               byte_size=self.store.byte_size,
                                               utility_before=before, utility_after=before)
                    working = tuple(item for item in working if item.record_id != victim.record_id)
                    evicted.add(victim.record_id)
                after = self._utility(working, audit_ids)
            except (TypeError, ValueError, ArithmeticError) as error:
                return AdmissionResult(False, "invalid_utility:" + type(error).__name__,
                                       byte_size=self.store.byte_size)

            if (not audit_only and self.config.policy in (ResidencyMode.FULL, ResidencyMode.ADDITIVE_VALUE)
                    and after - before <= self.config.kappa):
                return AdmissionResult(False, "utility_gain_below_transition_cost", byte_size=self.store.byte_size,
                                       utility_before=before, utility_after=after)
            published = self._publish(working, audit_ids, snapshot)
            return AdmissionResult(
                published,
                ("audit_admitted" if audit_only else "serving_admitted") if published else "version_or_pin_conflict",
                tuple(sorted(evicted)) if published else (),
                self.store.byte_size,
                before,
                after,
            )

    @_measured_maintenance
    def promote(self, record_id: str) -> AdmissionResult:
        """Submit an externally B-qualified audit record to serving admission."""
        record = self.store.get(record_id, include_audit=True)
        if record is None:
            return AdmissionResult(False, "missing_record", byte_size=self.store.byte_size)
        return self.admit(record, audit_only=False)
