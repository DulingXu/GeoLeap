"""Small, opportunity-aware cache manager for declared workflows.

The manager is driven by ``maintenance_tick`` rather than by every lookup.
Its caller supplies one fixed whole-workflow utility model for a maintenance
round. Only serving records enter that model; audit records use a separate
exploration quota inside the same managed-serialization byte budget.

The legacy capacity and victim ranking use managed serialized storage, including
reserved metadata. A store with ``memory_budget`` uses a projected resident-heap
target for space trials and marginal density, retains the serialized cap, and
additionally enforces a shared accounted heap domain. Pending/batch/readers and
publication workspaces remain charged. Neither quantity is a process RSS limit;
projected resident marginal space is not immediately reclaimable heap.
"""

from __future__ import annotations

from collections import deque
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from enum import Enum
from functools import wraps
import itertools
import math
import sys
import threading
import time
import uuid
import weakref
from typing import Callable, Deque, FrozenSet, Iterable, List, Optional, Tuple

from .records import CachedRecord, RecordStore
from .memory import CacheMemoryError, measure_object_graph


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
        try:
            with self._memory_guard(), self._maintenance_lock:
                with self._memory_workspace(function.__name__, args):
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
        except CacheMemoryError:
            rejected = AdmissionResult(False, "memory_budget_exhausted", byte_size=self.store.byte_size)
            return [rejected] if function.__name__ == "maintenance_tick" else rejected
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
    queue_max_bytes: Optional[int] = None
    resident_heap_fraction: float = 0.6

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
        if self.queue_max_bytes is not None and (isinstance(self.queue_max_bytes, bool)
                or not isinstance(self.queue_max_bytes, int) or self.queue_max_bytes < 1):
            raise ValueError("queue_max_bytes must be a positive integer or None")
        if isinstance(self.resident_heap_fraction, bool) or not isinstance(self.resident_heap_fraction, (int, float)) \
                or not math.isfinite(self.resident_heap_fraction) or not 0 < self.resident_heap_fraction <= 1:
            raise ValueError("resident_heap_fraction must be finite and in (0, 1]")
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
        # Draining the queue transfers ownership here. It never makes candidate
        # payloads disappear from the heap domain before admission finishes.
        self._active_batch = None
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
        self._memory_prefix = "manager:" + uuid.uuid4().hex
        self._memory_handles = []
        self._memory_workspaces = []
        self._policy_workspace = None
        self._tracked_candidates = weakref.WeakValueDictionary()
        self._last_enqueue_reason = "not_attempted"
        self._bind_memory_roots()

    def _memory_guard(self):
        budget = self.store.memory_budget
        if budget is not None and not self._memory_handles:
            with budget.transaction():
                if not self._memory_handles:
                    self._bind_memory_roots()
        return budget.transaction() if budget is not None else nullcontext()

    def _bind_memory_roots(self):
        budget = self.store.memory_budget
        if budget is None:
            return
        handles = []
        try:
            for kind, getter in (("pending", self._pending_memory_roots),
                                 ("workspace", self._workspace_memory_roots),
                                 ("policy", self._policy_memory_roots)):
                handles.append(budget.register_root(self._memory_prefix + ":" + kind,
                    getter, category="manager_" + kind))
            self._memory_handles = handles
            budget.check()
        except BaseException:
            for handle in handles:
                handle.close()
            self._memory_handles = []
            raise

    def _pending_memory_roots(self):
        # Weak tracking exposes still-live pending records retained by a raw
        # queued_records reader, without keeping them alive solely for billing.
        return self._queue, self._active_batch, tuple(self._tracked_candidates.values())

    def _policy_memory_roots(self):
        # Include the controller object/header, __dict__, handles and any future
        # private state as well. Earlier roots classify pending/workspace; graph
        # identity deduplication prevents charging their payload a second time.
        return self

    def _workspace_memory_roots(self):
        return self._memory_workspaces, self._policy_workspace

    @contextmanager
    def _memory_workspace(self, operation, arguments=()):
        """Reserve manager containers and one bounded serialization workspace.

        The callback's planner/native workspace belongs to its own component;
        this reserve does not pretend to bound arbitrary user ``value_fn`` code.
        The conservative JSON model is separate from measured persistent heap.
        """
        budget = self.store.memory_budget
        if budget is None:
            yield
            return
        def records():
            return itertools.chain(self.store._records.values(),
                (record for record, _ in self._queue),
                (item for item in arguments if isinstance(item, CachedRecord)))
        chars = max((len(record.input_json) + len(record.output_json) + len(record.record_id)
                     + len(record.epoch) + len(record.subgraph_id) for record in records()), default=0)
        dimension = max((len(record.embedding) for record in records()), default=0)
        count = len(self.store._records) + len(self._queue) + len(arguments)
        amount = (8192 + 512 * (count + 1) if operation == "expire_audits_only" else
                  16384 + 2048 * (count + 1) + 128 * chars + 64 * dimension)
        with budget.reservation(amount, "manager_" + operation):
            self._memory_workspaces.append({"arguments": arguments, "serialized_lengths": {}})
            try:
                yield
            finally:
                self._memory_workspaces.pop()

    @property
    def pending_heap_limit_bytes(self):
        if self.store.memory_budget is None:
            return None
        return self.config.queue_max_bytes if self.config.queue_max_bytes is not None else min(
            self.config.max_bytes, self.store.memory_budget.limit_bytes)

    @property
    def resident_heap_limit_bytes(self):
        budget = self.store.memory_budget
        return None if budget is None else int(budget.limit_bytes * self.config.resident_heap_fraction)

    @property
    def _capacity_target_bytes(self):
        return self.config.max_bytes if self.store.memory_budget is None else self.resident_heap_limit_bytes

    def _resident_heap_projection(self, records):
        """Project the current generation, without crediting retired readers.

        Only storage for the proposed resident set is projected here. The global
        ledger independently retains pending, old snapshots and publication
        overlap, so this value never promises immediate physical reclamation.
        """
        by_id = {record.record_id: record for record in records}
        buffers, metadata = {}, {}
        additional = 0
        lengths = self._memory_workspaces[-1]["serialized_lengths"] if self._memory_workspaces else {}
        for key, record in by_id.items():
            if key in self.store._buffers:
                buffers[key] = self.store._buffers[key]
                metadata[key] = self.store._metadata[key]
            else:
                # Shared ID placeholders account for dict capacity without
                # materializing large fake payload/metadata allocations.
                buffers[key] = metadata[key] = key
                identity = (record.record_id, record.revision, id(record))
                if identity not in lengths:
                    lengths[identity] = len(record.serialized())
                additional += sys.getsizeof(b"") + lengths[identity]
                additional += sys.getsizeof(bytearray()) + record.metadata_capacity + int(record.metadata_capacity > 0)
        # All IDs is conservative for the audit membership set, and uses the
        # same identity-deduplicated representation for all four D strategies.
        graph = measure_object_graph({"records": by_id, "buffers": buffers,
            "metadata": metadata, "audit_ids": frozenset(by_id)})["owned_heap_bytes"]
        return graph + additional

    @property
    def last_enqueue_reason(self):
        return self._last_enqueue_reason

    def advance_request(self, request_index: Optional[int] = None) -> int:
        """Advance the logical stream position, including requests with no hits."""
        with self._memory_guard(), self._maintenance_lock:
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
        with self._memory_guard(), self._maintenance_lock:
            if request_index is not None:
                self.advance_request(request_index)
            if self.store.get(record_id) is None:
                return False
            if self.store.memory_budget is None:
                self._reuse_counts[record_id] = self._reuse_weight(record_id) + 1.0
                self._reuse_positions[record_id] = self._request_index
                self._access_clock += 1
                self._last_access[record_id] = self._access_clock
                return True
            old = self._reuse_counts, self._reuse_positions, self._last_access, self._access_clock
            try:
                with self.store.memory_budget.reservation(4096 + 512 * (len(self._last_access) + 1),
                                                          "manager_access") as token:
                    counts, positions, access = dict(old[0]), dict(old[1]), dict(old[2])
                    counts[record_id] = self._reuse_weight(record_id) + 1.0
                    positions[record_id] = self._request_index
                    access[record_id] = old[3] + 1
                    self._policy_workspace = (old, counts, positions, access)
                    self._reuse_counts, self._reuse_positions, self._last_access = counts, positions, access
                    self._access_clock = old[3] + 1
                    token.commit()
                return True
            except CacheMemoryError:
                self._reuse_counts, self._reuse_positions, self._last_access, self._access_clock = old
                return False
            finally:
                self._policy_workspace = None

    def maintenance_stats(self) -> dict:
        with self._memory_guard(), self._maintenance_lock:
            result = dict(self._stats, managed_bytes=self.store.byte_size,
                        request_index=self._request_index, policy=self.config.policy.value,
                        memory_accounting="managed serialized bytes plus metadata reserve; not RSS",
                        cpu_clock="thread_time", wall_clock="perf_counter")
            if self.store.memory_budget is not None:
                result.update(accounted_heap=self.store.memory_budget.memory_snapshot(),
                    queued_accounted_heap_bytes=measure_object_graph({"queue": self._queue,
                                                                     "batch": self._active_batch})["owned_heap_bytes"],
                    pending_heap_limit_bytes=self.pending_heap_limit_bytes,
                    managed_capacity_bytes=self.config.max_bytes,
                    managed_audit_quota_bytes=int(self.config.max_bytes * self.config.audit_fraction),
                    resident_heap_limit_bytes=self.resident_heap_limit_bytes,
                    resident_audit_heap_target_bytes=self.audit_quota_bytes,
                    resident_heap_fraction=self.config.resident_heap_fraction,
                    projected_resident_heap_bytes=self._resident_heap_projection(self.store._records.values()),
                    capacity_trial_scope="projected current-generation Python allocations; old readers remain globally charged",
                    density_space_scope="projected resident marginal heap, not immediately reclaimable physical bytes",
                    turnover_bytes_scope="admitted_bytes and evicted_bytes are managed serialized record sizes; not physical reclaim",
                    memory_accounting="shared cache-owned accounted Python heap; managed serialization reported separately",
                    physical_budget_enforced=False, os_memory_scope="external_envelope_reported_separately")
            return result

    def reset_maintenance_stats(self) -> None:
        """Start a measurement window without changing cache or access history."""
        with self._memory_guard(), self._maintenance_lock:
            if self._measurement_depth:
                raise RuntimeError("Cannot reset counters during a maintenance operation")
            self._stats = {"peak_managed_bytes": self.store.byte_size, "maintenance_cpu_ms": 0.0,
                           "maintenance_wall_ms": 0.0, "admitted_count": 0, "evicted_count": 0,
                           "admitted_bytes": 0, "evicted_bytes": 0, "promoted_count": 0}

    def _publish(self, records, audit_ids, snapshot) -> bool:
        if self.store.memory_budget is not None:
            return self._publish_with_heap_budget(records, audit_ids, snapshot)
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

    def _publish_with_heap_budget(self, records, audit_ids, snapshot):
        """Prepare policy bookkeeping before store publication can succeed."""
        budget = self.store.memory_budget
        with budget.reservation(8192 + 1024 * (len(records) + len(snapshot.records) + 1),
                                "manager_publication") as token:
            old = {record.record_id: record for record in snapshot.records}
            new = {record.record_id: record for record in records}
            added, removed = set(new) - set(old), set(old) - set(new)
            access, counts, positions, stats = (dict(self._last_access), dict(self._reuse_counts),
                                               dict(self._reuse_positions), dict(self._stats))
            clock = self._access_clock
            for key in removed:
                access.pop(key, None)
                counts.pop(key, None)
                positions.pop(key, None)
            for key in sorted(added):
                clock += 1
                access[key] = clock
            stats["admitted_count"] += len(added)
            stats["evicted_count"] += len(removed)
            stats["admitted_bytes"] += sum(self._managed_record_bytes(new[key]) for key in added)
            stats["evicted_bytes"] += sum(self._managed_record_bytes(old[key]) for key in removed)
            stats["promoted_count"] += len(snapshot.audit_ids - audit_ids - removed)
            stats["peak_managed_bytes"] = max(stats["peak_managed_bytes"], self._managed_bytes(records))
            self._policy_workspace = (old, new, added, removed, access, counts, positions, stats, clock)
            try:
                token.commit()
                # Match the store's preallocation check before it converts a
                # budget refusal to its compatibility boolean result.
                with budget.reservation(self.store.publication_reservation_bytes(records),
                                        "manager_store_publication_preflight"):
                    pass
                if not self.store.publish(records, audit_ids, snapshot.version, self.config.max_bytes):
                    return False
                self._last_access, self._reuse_counts, self._reuse_positions, self._stats = access, counts, positions, stats
                self._access_clock = clock
                return True
            finally:
                self._policy_workspace = None

    @property
    def audit_quota_bytes(self) -> int:
        return int(self._capacity_target_bytes * self.config.audit_fraction)

    @property
    def queued_count(self) -> int:
        with self._memory_guard(), self._queue_lock:
            return len(self._queue)

    @property
    def queued_records(self) -> Tuple[CachedRecord, ...]:
        """Read-only candidate view for freezing one maintenance round's B support."""
        with self._memory_guard(), self._queue_lock:
            return tuple(record for record, _ in self._queue)

    def enqueue(self, record: CachedRecord, audit_only: bool = True) -> bool:
        """Queue a completed immutable record; return false if the queue is full."""
        try:
            with self._memory_guard(), self._queue_lock:
                if len(self._queue) >= self.config.queue_limit:
                    self._last_enqueue_reason = "queue_count_limit"
                    return False
                budget = self.store.memory_budget
                if budget is None:
                    self._queue.append((record, audit_only))
                    self._last_enqueue_reason = "queued"
                    return True
                projected = measure_object_graph({"queue": self._queue, "batch": self._active_batch,
                                                   "incoming": record})["owned_heap_bytes"] + 1024
                if projected > self.pending_heap_limit_bytes:
                    self._last_enqueue_reason = "queue_heap_limit"
                    return False
                bound = measure_object_graph({"incoming": record})["owned_heap_bytes"] + 4096
                previous = len(self._queue)
                previously_tracked = id(record) in self._tracked_candidates
                try:
                    with budget.reservation(bound, "manager_pending") as token:
                        self._queue.append((record, audit_only))
                        self._tracked_candidates[id(record)] = record
                        self._last_enqueue_reason = "queued"
                        token.commit()
                    return True
                except CacheMemoryError:
                    while len(self._queue) > previous:
                        self._queue.pop()
                    if not previously_tracked:
                        self._tracked_candidates.pop(id(record), None)
                    self._last_enqueue_reason = "memory_budget_exhausted"
                    return False
        except CacheMemoryError:
            self._last_enqueue_reason = "memory_budget_exhausted"
            return False

    def cancel_pending(self, record_id: str) -> bool:
        """Cancel one not-yet-drained record after a failed capture commit.

        Cancellation removes scheduling ownership, not references held by a
        queued_records reader or the caller. Those remain weakly tracked and
        charged until their last live reference disappears.
        """
        if not isinstance(record_id, str) or not record_id:
            raise ValueError("record_id must be a nonempty string")
        with self._memory_guard(), self._queue_lock:
            for item in self._queue:
                if item[0].record_id == record_id:
                    self._queue.remove(item)
                    self._last_enqueue_reason = "pending_cancelled"
                    return True
            return False

    @_measured_maintenance
    def expire_audits_only(self, now: Optional[float] = None) -> Optional[AdmissionResult]:
        """Expire audits without processing pending work or evaluating utility.

        Used when fresh planning profiles could not be frozen. The existing
        snapshots and global publication peak remain accounted as usual.
        """
        current_time = time.time() if now is None else now
        if not math.isfinite(current_time):
            raise ValueError("now must be finite")
        with self._maintenance_lock:
            return self._expire_audits(current_time)

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
                # Keep the original deque as an accounted owner for the whole
                # batch. A drained queue is not evidence of reclaimed payload.
                fresh_queue = deque()
                self._active_batch = self._queue
                self._queue = fresh_queue
            try:
                for record, audit_only in self._active_batch:
                    results.append(self.admit(record, audit_only=audit_only, now=current_time))
                return results
            finally:
                self._active_batch = None

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

    def _bytes(self, records: Iterable[CachedRecord]) -> int:
        if self.store.memory_budget is not None:
            return self._resident_heap_projection(records)
        return self._managed_bytes(records)

    def _managed_record_bytes(self, record: CachedRecord) -> int:
        if self.store.memory_budget is not None and self.store._records.get(record.record_id) is record:
            return len(self.store._buffers[record.record_id]) + len(self.store._metadata[record.record_id])
        return record.byte_size

    def _managed_bytes(self, records: Iterable[CachedRecord]) -> int:
        return sum(self._managed_record_bytes(record) for record in records)

    def _publication_failure_reason(self):
        if self.store.memory_budget is not None:
            reason = self.store.last_publish_failure_reason
            if reason:
                return reason
        return "version_or_pin_conflict"

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
            "audit_expired" if published else self._publication_failure_reason(),
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
        return reuse * benefit / self._bytes((record,))

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
            if self.store.memory_budget is not None and self._bytes((record,)) > self._capacity_target_bytes:
                return AdmissionResult(False, "record_exceeds_resident_heap_target", byte_size=self.store.byte_size)
            if audit_only and (self._bytes((record,)) > self.audit_quota_bytes
                    or record.byte_size > int(self.config.max_bytes * self.config.audit_fraction)):
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
                    while (self._bytes(item for item in working if item.record_id in audit_ids) > self.audit_quota_bytes
                           or self._managed_bytes(item for item in working if item.record_id in audit_ids)
                              > int(self.config.max_bytes * self.config.audit_fraction)):
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

                while (self._bytes(working) > self._capacity_target_bytes
                       or self._managed_bytes(working) > self.config.max_bytes):
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
                ("audit_admitted" if audit_only else "serving_admitted") if published else self._publication_failure_reason(),
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
