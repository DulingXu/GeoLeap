"""Immutable records, serving snapshots, and pinned publication for B/C/D.

byte_size is managed serialized storage plus an allocated metadata reserve.
It does not claim to measure the Python process's RSS.
"""

from __future__ import annotations

import json
import math
from contextlib import nullcontext
import threading
import time
import uuid
import weakref
from dataclasses import dataclass
from typing import Any, Dict, FrozenSet, Optional, Sequence, Tuple

from .spec import SubgraphSpec, canonical_json, encode_similarity_input, LEGACY_INPUT_ENCODER
from .memory import CacheMemoryBudget, CacheMemoryError, measure_object_graph


@dataclass(frozen=True)
class CachedRecord:
    subgraph_id: str
    record_id: str
    revision: int
    epoch: str
    input_json: str
    output_json: str
    embedding: Tuple[float, ...]
    created_at: float
    metadata_capacity: int = 32768
    embedding_fields: Tuple[str, ...] = ()
    embedding_encoder: str = LEGACY_INPUT_ENCODER

    def __post_init__(self):
        if self.revision < 1 or self.metadata_capacity < 0 or not self.record_id:
            raise ValueError("Invalid immutable record version or metadata capacity")
        object.__setattr__(self, "embedding", tuple(self.embedding))
        inputs, outputs = json.loads(self.input_json), json.loads(self.output_json)
        if not isinstance(inputs, dict) or not isinstance(outputs, dict):
            raise ValueError("Record inputs and outputs must be objects")
        object.__setattr__(self, "input_json", canonical_json(inputs))
        object.__setattr__(self, "output_json", canonical_json(outputs))
        fields = tuple(self.embedding_fields)
        if (any(not isinstance(key, str) or key not in inputs for key in fields)
                or fields != tuple(sorted(set(fields)))):
            raise ValueError("Embedding fields must be sorted unique declared input keys")
        object.__setattr__(self, "embedding_fields", fields)
        expected = encode_similarity_input({key: inputs[key] for key in fields} if fields else inputs,
                                           self.embedding_encoder)
        if len(self.embedding) != len(expected) or any(
            not math.isfinite(value) or abs(value - target) > 1e-12
            for value, target in zip(self.embedding, expected)
        ):
            raise ValueError("Record embedding must match the stable input encoder")
        if not math.isfinite(self.created_at):
            raise ValueError("Record timestamp must be finite")

    @classmethod
    def create(cls, spec: SubgraphSpec, inputs: Dict[str, Any], outputs: Dict[str, Any], epoch: str,
               record_id: Optional[str] = None, revision: int = 1, metadata_capacity: int = 32768,
               embedding_encoder: str = LEGACY_INPUT_ENCODER):
        if not spec.accepts_input(inputs) or not spec.accepts_output(outputs):
            raise ValueError("Cached record does not satisfy its boundary schema")
        fields = spec.similarity_fields
        return cls(spec.identity, record_id or uuid.uuid4().hex, revision, epoch,
                   canonical_json(inputs), canonical_json(outputs),
                   encode_similarity_input({key: inputs[key] for key in fields} if fields else inputs, embedding_encoder),
                   time.time(), metadata_capacity, fields, embedding_encoder)

    @property
    def inputs(self) -> Dict[str, Any]:
        return json.loads(self.input_json)

    @property
    def outputs(self) -> Dict[str, Any]:
        return json.loads(self.output_json)

    def serialized(self) -> bytes:
        value = {
            "subgraph_id": self.subgraph_id, "record_id": self.record_id, "revision": self.revision,
            "epoch": self.epoch, "inputs": self.inputs, "outputs": self.outputs,
            "embedding": self.embedding, "created_at": self.created_at, "metadata_capacity": self.metadata_capacity,
        }
        if self.embedding_fields:
            value["embedding_fields"] = self.embedding_fields
        if self.embedding_encoder != LEGACY_INPUT_ENCODER:
            value["embedding_encoder"] = self.embedding_encoder
        return canonical_json(value).encode("utf-8")

    @property
    def byte_size(self) -> int:
        return len(self.serialized()) + self.metadata_capacity


@dataclass(frozen=True)
class CacheSnapshot:
    version: int
    records: Tuple[CachedRecord, ...] = ()
    audit_ids: FrozenSet[str] = frozenset()

    @property
    def byte_size(self) -> int:
        return sum(record.byte_size for record in self.records)


class RecordStore:
    def __init__(self, memory_budget: Optional[CacheMemoryBudget] = None):
        self._records = {}
        self._audit_ids = frozenset()
        self._buffers = {}
        self._metadata = {}
        self._pins = {}
        self._version = 0
        self._lock = threading.RLock()
        self.memory_budget = None
        self._memory_prefix = "store:" + uuid.uuid4().hex
        self._tracked_records = weakref.WeakValueDictionary()
        self._tracked_snapshots = weakref.WeakValueDictionary()
        self._publication_workspace = None
        self._metadata_workspace = None
        self._memory_handles = []
        self._last_publish_failure_reason = None
        if memory_budget is not None:
            self.bind_memory_budget(memory_budget)

    def bind_memory_budget(self, budget: CacheMemoryBudget) -> None:
        """Attach one shared heap domain; serialized byte_size remains separate.

        Weak tracking keeps objects charged while a snapshot or a raw reader
        still owns them, without itself preventing their eventual reclamation.
        This is CPython allocation accounting, not a process RSS hard limit.
        """
        if self.memory_budget is budget:
            return
        if self.memory_budget is not None:
            raise ValueError("A store cannot change its memory budget domain")
        if self._version or self._records or self._pins:
            raise ValueError("Bind the memory budget before loading records so all reader lifetimes are tracked")
        handles = []
        self.memory_budget = budget
        try:
            for kind in ("resident", "retained_readers", "publication", "control"):
                handles.append(budget.register_root(self._memory_prefix + ":" + kind,
                                                   getattr(self, "_memory_" + kind + "_roots")))
            self._memory_handles = handles
            budget.memory_snapshot()
        except Exception:
            for handle in handles:
                handle.close()
            self.memory_budget = None
            raise

    def _memory_resident_roots(self):
        return (self._records, self._buffers, self._metadata, self._audit_ids)

    def _memory_retained_readers_roots(self):
        return (tuple(self._tracked_records.values()), tuple(self._tracked_snapshots.values()))

    def _memory_publication_roots(self):
        return (self._publication_workspace, self._metadata_workspace)

    def _memory_control_roots(self):
        # Includes the actual object/dict/locks/weak registries/handles, not a
        # synthetic fixed-size stand-in for this store's control allocation.
        return self

    def _memory_guard(self):
        return self.memory_budget.transaction() if self.memory_budget is not None else nullcontext()

    def memory_snapshot(self) -> dict:
        if self.memory_budget is None:
            return {"enabled": False, "managed_serialized_bytes": self.byte_size,
                    "physical_budget_enforced": False, "scope": "legacy_serialized_storage_only"}
        return dict(self.memory_budget.memory_snapshot(), enabled=True, managed_serialized_bytes=self.byte_size)

    @staticmethod
    def _payload_bound(record: CachedRecord) -> int:
        # UTF-8 uses at most four bytes per code point. Embedding floats have
        # bounded repr length; JSON keys/scalars use the additional fixed room.
        chars = len(record.input_json) + len(record.output_json) + len(record.epoch) + len(record.record_id) + len(record.subgraph_id)
        return 4 * chars + 64 * len(record.embedding) + 4096

    def publication_reservation_bytes(self, records: Sequence[CachedRecord]) -> int:
        """Conservative *additional* heap for copy-on-write publication.

        The old generation stays charged until publication completes. Incoming
        records may already be owned by pending/reader roots; double reserving
        here is conservative and avoids freeing victims before a commit.
        """
        if self.memory_budget is None:
            return 0
        incoming = tuple(record for record in records if record.record_id not in self._records)
        object_bytes = measure_object_graph({"incoming": incoming})["owned_heap_bytes"]
        buffers = sum(self._payload_bound(record) + record.metadata_capacity + 128 for record in incoming)
        containers = 4096 + 512 * (len(records) + len(incoming) + 1)
        return object_bytes + buffers + containers

    @property
    def version(self) -> int:
        with self._lock:
            return self._version

    @property
    def byte_size(self) -> int:
        with self._lock:
            return sum(len(data) for data in self._buffers.values()) + sum(len(data) for data in self._metadata.values())

    def snapshot(self, include_audit: bool = False) -> CacheSnapshot:
        with self._memory_guard(), self._lock:
            reserve = (self.memory_budget.reservation(2048 + 32 * len(self._records), "snapshot")
                       if self.memory_budget is not None else nullcontext())
            with reserve as token:
                records = tuple(record for key, record in self._records.items() if include_audit or key not in self._audit_ids)
                result = CacheSnapshot(self._version, records, self._audit_ids if include_audit else frozenset())
                if self.memory_budget is not None:
                    self._tracked_snapshots[id(result)] = result
                    token.commit()
                return result

    def get(self, record_id: str, revision: Optional[int] = None, include_audit: bool = False) -> Optional[CachedRecord]:
        with self._lock:
            record = self._records.get(record_id)
            if record is None or (revision is not None and record.revision != revision):
                return None
            return record if include_audit or record_id not in self._audit_ids else None

    def acquire_pin(self, record_id: str, revision: int) -> Optional[CachedRecord]:
        with self._memory_guard(), self._lock:
            record = self.get(record_id, revision)
            if record:
                previous = self._pins.get(record_id, 0)
                try:
                    reserve = self.memory_budget.reservation(1024, "pin") if self.memory_budget is not None else nullcontext()
                    with reserve as token:
                        self._pins[record_id] = previous + 1
                        if token is not None:
                            token.commit()
                except CacheMemoryError:
                    if previous:
                        self._pins[record_id] = previous
                    else:
                        self._pins.pop(record_id, None)
                    return None
            return record

    def release_pin(self, record_id: str, revision: int) -> None:
        with self._memory_guard(), self._lock:
            record = self._records.get(record_id)
            if record is None or record.revision != revision or not self._pins.get(record_id):
                raise ValueError("Pin was not acquired for this record revision")
            self._pins[record_id] -= 1
            if not self._pins[record_id]:
                del self._pins[record_id]

    def pinned_ids(self) -> FrozenSet[str]:
        with self._lock:
            return frozenset(self._pins)

    def publish(self, records: Sequence[CachedRecord], audit_ids: FrozenSet[str], expected_version: int,
                max_bytes: int) -> bool:
        """Atomically publish a feasible set; pinned objects cannot be replaced or demoted."""
        self._last_publish_failure_reason = None
        if self.memory_budget is not None:
            return self._publish_with_heap_budget(records, audit_ids, expected_version, max_bytes)
        proposed = {record.record_id: record for record in records}
        if len(proposed) != len(records) or not set(audit_ids).issubset(proposed):
            raise ValueError("Duplicate records or unknown audit IDs")
        if sum(record.byte_size for record in records) > max_bytes:
            return False
        with self._lock:
            if expected_version != self._version:
                return False
            for key in self._pins:
                if proposed.get(key) != self._records[key] or key in audit_ids:
                    return False
            # A record ID/revision is immutable. Replace with a new ID/revision instead.
            for key, record in proposed.items():
                old = self._records.get(key)
                if old and old != record:
                    return False
            added = set(proposed) - set(self._records)
            # Allocate after feasibility/version/pin checks, while publication is locked.
            new_buffers = {key: proposed[key].serialized() for key in added}
            new_metadata = {key: bytearray(proposed[key].metadata_capacity) for key in added}
            self._buffers = {key: self._buffers[key] if key in self._buffers else new_buffers[key] for key in proposed}
            self._metadata = {key: self._metadata[key] if key in self._metadata else new_metadata[key] for key in proposed}
            self._records = proposed
            self._audit_ids = frozenset(audit_ids)
            self._version += 1
            return True

    def _publish_with_heap_budget(self, records, audit_ids, expected_version, max_bytes) -> bool:
        with self._memory_guard(), self._lock:
            if expected_version != self._version:
                self._last_publish_failure_reason = "version_conflict"
                return False
            old = (self._records, self._buffers, self._metadata, self._audit_ids, self._version)
            added = ()
            new_tracking = []
            try:
                with self.memory_budget.reservation(self.publication_reservation_bytes(records), "publication") as token:
                    managed = sum((len(self._buffers[record.record_id]) + len(self._metadata[record.record_id]))
                                  if self._records.get(record.record_id) == record else record.byte_size
                                  for record in records)
                    if managed > max_bytes:
                        self._last_publish_failure_reason = "managed_budget_exhausted"
                        return False
                    proposed = {record.record_id: record for record in records}
                    if len(proposed) != len(records) or not set(audit_ids).issubset(proposed):
                        raise ValueError("Duplicate records or unknown audit IDs")
                    if any(proposed.get(key) != self._records[key] or key in audit_ids for key in self._pins):
                        self._last_publish_failure_reason = "pin_conflict"
                        return False
                    if any(key in self._records and self._records[key] != record for key, record in proposed.items()):
                        self._last_publish_failure_reason = "immutable_record_conflict"
                        return False
                    added = tuple(key for key in proposed if key not in self._records)
                    # Retain both old and new allocations in the measured roots
                    # throughout the switch; retirement is never early credit.
                    new_buffers, new_metadata = {}, {}
                    self._publication_workspace = (old, proposed, new_buffers, new_metadata)
                    for key in added:
                        new_buffers[key] = proposed[key].serialized()
                        new_metadata[key] = bytearray(proposed[key].metadata_capacity)
                        identity = id(proposed[key])
                        if identity not in self._tracked_records:
                            new_tracking.append(identity)
                            self._tracked_records[identity] = proposed[key]
                    buffers = {key: self._buffers[key] if key in self._buffers else new_buffers[key] for key in proposed}
                    metadata = {key: self._metadata[key] if key in self._metadata else new_metadata[key] for key in proposed}
                    self._publication_workspace = (old, proposed, new_buffers, new_metadata, buffers, metadata)
                    self._records, self._buffers, self._metadata = proposed, buffers, metadata
                    self._audit_ids = frozenset(audit_ids)
                    self._version += 1
                    token.commit()
            except (CacheMemoryError, MemoryError) as error:
                self._records, self._buffers, self._metadata, self._audit_ids, self._version = old
                self._last_publish_failure_reason = "memory_budget_exhausted" if isinstance(error, CacheMemoryError) else "allocation_failed"
                for identity in new_tracking:
                    self._tracked_records.pop(identity, None)
                return False
            finally:
                # Do not report old allocations as reclaimed while this frame
                # still owns them through rollback or construction locals.
                old = proposed = new_buffers = new_metadata = buffers = metadata = None
                self._publication_workspace = None
            return True

    @property
    def last_publish_failure_reason(self):
        return self._last_publish_failure_reason

    def write_metadata(self, record_id: str, payload: bytes) -> bool:
        if self.memory_budget is not None:
            return self._write_metadata_with_heap_budget(record_id, payload)
        with self._memory_guard(), self._lock:
            target = self._metadata.get(record_id)
            if target is None or len(payload) > len(target):
                return False
            # Equal-size writes avoid reallocating the bytearray. No full-size
            # padding bytes object is constructed beside the live allocation.
            target[:len(payload)] = payload
            block = b"\x00" * min(4096, len(target) - len(payload))
            for offset in range(len(payload), len(target), 4096):
                size = min(4096, len(target) - offset)
                target[offset:offset+size] = block[:size]
            return True

    def _write_metadata_with_heap_budget(self, record_id, payload) -> bool:
        with self._memory_guard(), self._lock:
            previous = self._metadata.get(record_id)
            if previous is None or len(payload) > len(previous):
                return False
            try:
                with self.memory_budget.reservation(len(previous) + len(payload) + 4096, "metadata_write") as token:
                    target = bytearray(len(previous))
                    self._metadata_workspace = (previous, target, payload)
                    target[:len(payload)] = payload
                    self._metadata[record_id] = target
                    token.commit()
            except (CacheMemoryError, MemoryError):
                self._metadata[record_id] = previous
                return False
            finally:
                previous = target = None
                self._metadata_workspace = None
            return True
