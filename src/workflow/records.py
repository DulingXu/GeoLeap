"""Immutable records, serving snapshots, and pinned publication for B/C/D.

byte_size is managed serialized storage plus an allocated metadata reserve.
It does not claim to measure the Python process's RSS.
"""

from __future__ import annotations

import json
import math
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, FrozenSet, Optional, Sequence, Tuple

from .spec import SubgraphSpec, canonical_json, encode_input


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

    def __post_init__(self):
        if self.revision < 1 or self.metadata_capacity < 0 or not self.record_id:
            raise ValueError("Invalid immutable record version or metadata capacity")
        object.__setattr__(self, "embedding", tuple(self.embedding))
        inputs, outputs = json.loads(self.input_json), json.loads(self.output_json)
        if not isinstance(inputs, dict) or not isinstance(outputs, dict):
            raise ValueError("Record inputs and outputs must be objects")
        object.__setattr__(self, "input_json", canonical_json(inputs))
        object.__setattr__(self, "output_json", canonical_json(outputs))
        expected = encode_input(inputs)
        if len(self.embedding) != len(expected) or any(
            not math.isfinite(value) or abs(value - target) > 1e-12
            for value, target in zip(self.embedding, expected)
        ):
            raise ValueError("Record embedding must match the stable input encoder")
        if not math.isfinite(self.created_at):
            raise ValueError("Record timestamp must be finite")

    @classmethod
    def create(cls, spec: SubgraphSpec, inputs: Dict[str, Any], outputs: Dict[str, Any], epoch: str,
               record_id: Optional[str] = None, revision: int = 1, metadata_capacity: int = 32768):
        if not spec.accepts_input(inputs) or not spec.accepts_output(outputs):
            raise ValueError("Cached record does not satisfy its boundary schema")
        return cls(spec.identity, record_id or uuid.uuid4().hex, revision, epoch,
                   canonical_json(inputs), canonical_json(outputs), encode_input(inputs), time.time(), metadata_capacity)

    @property
    def inputs(self) -> Dict[str, Any]:
        return json.loads(self.input_json)

    @property
    def outputs(self) -> Dict[str, Any]:
        return json.loads(self.output_json)

    def serialized(self) -> bytes:
        return canonical_json({
            "subgraph_id": self.subgraph_id, "record_id": self.record_id, "revision": self.revision,
            "epoch": self.epoch, "inputs": self.inputs, "outputs": self.outputs,
            "embedding": self.embedding, "created_at": self.created_at, "metadata_capacity": self.metadata_capacity,
        }).encode("utf-8")

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
    def __init__(self):
        self._records = {}
        self._audit_ids = frozenset()
        self._buffers = {}
        self._metadata = {}
        self._pins = {}
        self._version = 0
        self._lock = threading.RLock()

    @property
    def version(self) -> int:
        with self._lock:
            return self._version

    @property
    def byte_size(self) -> int:
        with self._lock:
            return sum(len(data) for data in self._buffers.values()) + sum(len(data) for data in self._metadata.values())

    def snapshot(self, include_audit: bool = False) -> CacheSnapshot:
        with self._lock:
            records = tuple(record for key, record in self._records.items() if include_audit or key not in self._audit_ids)
            return CacheSnapshot(self._version, records, self._audit_ids if include_audit else frozenset())

    def get(self, record_id: str, revision: Optional[int] = None, include_audit: bool = False) -> Optional[CachedRecord]:
        with self._lock:
            record = self._records.get(record_id)
            if record is None or (revision is not None and record.revision != revision):
                return None
            return record if include_audit or record_id not in self._audit_ids else None

    def acquire_pin(self, record_id: str, revision: int) -> Optional[CachedRecord]:
        with self._lock:
            record = self.get(record_id, revision)
            if record:
                self._pins[record_id] = self._pins.get(record_id, 0) + 1
            return record

    def release_pin(self, record_id: str, revision: int) -> None:
        with self._lock:
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

    def write_metadata(self, record_id: str, payload: bytes) -> bool:
        with self._lock:
            target = self._metadata.get(record_id)
            if target is None or len(payload) > len(target):
                return False
            target[:] = payload + b"\x00" * (len(target) - len(payload))
            return True
