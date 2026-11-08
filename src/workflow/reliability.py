"""B: bounded, record-specific output feedback and online reuse screening.

Reference execution and :meth:`ReliabilityTable.refresh` run outside serving
lookup.  Scores are empirical screening statistics, not error guarantees.
"""

from __future__ import annotations

from bisect import bisect_right
from collections import OrderedDict
from dataclasses import dataclass, field, fields, replace
from enum import Enum
import hashlib
import math
import random
import threading
import time
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Tuple

from .records import CacheSnapshot, CachedRecord
from .spec import SubgraphSpec, canonical_json, cosine, encode_input


class ScreeningMode(str, Enum):
    """B1 changes only historical evidence screening; guards stay shared."""

    SIMILARITY = "similarity"
    NO_MARGIN = "no_margin"
    MIN1 = "min1"
    FULL = "full"

    @classmethod
    def parse(cls, value) -> "ScreeningMode":
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower().replace("-", "_")
            if normalized == "nomargin":
                normalized = "no_margin"
            try:
                return cls(normalized)
            except ValueError:
                pass
        raise ValueError("screening mode must be similarity, no_margin, min1, or full")

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class ReliabilityConfig:
    min_groups: int = 3
    max_score: float = 0.2
    similarity_bins: Tuple[float, ...] = (0.8, 0.9, 0.97)
    delta: float = 0.05
    bootstrap_samples: int = 200
    max_groups: int = 32
    max_age_seconds: float = 86400.0
    screening_mode: ScreeningMode = ScreeningMode.FULL
    rank_by_score: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "screening_mode", ScreeningMode.parse(self.screening_mode))
        if self.min_groups < 1 or self.max_groups < self.min_groups:
            raise ValueError("require 1 <= min_groups <= max_groups")
        if not 0.0 <= self.max_score <= 1.0:
            raise ValueError("max_score must lie in [0, 1]")
        if not 0.0 < self.delta < 1.0 or self.bootstrap_samples < 1:
            raise ValueError("require 0 < delta < 1 and bootstrap_samples >= 1")
        if not math.isfinite(self.max_age_seconds) or self.max_age_seconds <= 0:
            raise ValueError("max_age_seconds must be positive and finite")
        bins = tuple(self.similarity_bins)
        if bins != tuple(sorted(set(bins))) or any(
            not math.isfinite(value) or not -1.0 <= value <= 1.0 for value in bins
        ):
            raise ValueError("similarity_bins must be sorted unique finite cut points in [-1, 1]")
        object.__setattr__(self, "similarity_bins", bins)

    @property
    def effective_min_groups(self) -> int:
        if self.screening_mode == ScreeningMode.SIMILARITY:
            return 0
        return 1 if self.screening_mode == ScreeningMode.MIN1 else self.min_groups

    @property
    def score_rule(self) -> str:
        if self.screening_mode == ScreeningMode.SIMILARITY:
            return "none"
        return "mean" if self.screening_mode == ScreeningMode.NO_MARGIN else "mean_plus_margin"

    def screening_score(self, evidence: Optional["Evidence"]) -> Optional[float]:
        if self.screening_mode == ScreeningMode.SIMILARITY or evidence is None:
            return None
        return evidence.mean if self.screening_mode == ScreeningMode.NO_MARGIN else evidence.score

    def evidence_rejection(self, evidence: Optional["Evidence"]) -> Optional[str]:
        """One predicate for lookup, commit, and background qualification."""
        if self.screening_mode == ScreeningMode.SIMILARITY:
            return None
        if evidence is None:
            return "missing_or_stale_evidence"
        if evidence.groups < self.effective_min_groups:
            return "insufficient_request_groups"
        if self.screening_score(evidence) > self.max_score:
            return "disagreement_score_above_threshold"
        return None


@dataclass(frozen=True)
class Evidence:
    groups: int
    mean: float
    margin: float
    score: float
    updated_at: float


@dataclass
class _Statistics:
    # SHA-256 request identifiers keep request-local labels together without
    # retaining potentially unbounded task text or raw individual labels.
    groups: OrderedDict = field(default_factory=OrderedDict)
    updated_at: float = 0.0
    evidence: Optional[Evidence] = None
    record_digest: str = ""


class ReliabilityTable:
    """Bounded request-group history keyed by immutable record and score bin."""

    def __init__(
        self,
        config: Optional[ReliabilityConfig] = None,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.config = config or ReliabilityConfig()
        self._clock = clock
        self._statistics: Dict[Tuple[str, str, int, str, int], _Statistics] = {}
        self._capacities: Dict[str, int] = {}
        self._lock = threading.RLock()

    def clone(self, config: Optional[ReliabilityConfig] = None) -> "ReliabilityTable":
        """Copy one historical view without changing observations or their age.

        A B ablation may change only its screening/ranking parameters. Bin
        boundaries, bootstrap settings, history capacity and expiry remain the
        same, so copied evidence has precisely the original statistical meaning.
        The new table has its own lock and mutable history containers; subsequent
        feedback in either table cannot modify the other one.
        """
        target = self.config if config is None else config
        if not isinstance(target, ReliabilityConfig):
            raise ValueError("clone config must be a ReliabilityConfig")
        screening_fields = {"min_groups", "max_score", "screening_mode", "rank_by_score"}
        incompatible = [item.name for item in fields(self.config)
                        if item.name not in screening_fields
                        and getattr(target, item.name) != getattr(self.config, item.name)]
        if incompatible:
            raise ValueError("Evidence cloning cannot change statistical/history settings: "
                             + ", ".join(incompatible))
        copied = ReliabilityTable(target, clock=self._clock)
        with self._lock:
            copied._statistics = {
                key: _Statistics(groups=OrderedDict(history.groups), updated_at=history.updated_at,
                                 evidence=replace(history.evidence) if history.evidence is not None else None,
                                 record_digest=history.record_digest)
                for key, history in self._statistics.items()
            }
            copied._capacities = dict(self._capacities)
        return copied

    def snapshot(self, config: Optional[ReliabilityConfig] = None) -> "ReliabilityTable":
        """Return an independent historical snapshot; an alias for ``clone``."""
        return self.clone(config=config)

    def _key(self, spec: SubgraphSpec, record: CachedRecord, epoch: str, similarity: float) -> tuple:
        if not math.isfinite(similarity) or not -1.0 <= similarity <= 1.0:
            raise ValueError("similarity must be finite and lie in [-1, 1]")
        return (spec.identity, record.record_id, record.revision, epoch,
                bisect_right(self.config.similarity_bins, similarity))

    def observe(
        self,
        spec: SubgraphSpec,
        record: CachedRecord,
        epoch: str,
        similarity: float,
        request_id: str,
        disagreement: float,
    ) -> bool:
        """Add one reference label; return False if its metadata cannot fit.

        Labels in one request are averaged before computing across requests.
        Updated histories require a new explicit ``refresh`` before serving.
        """
        if record.subgraph_id != spec.identity or record.epoch != epoch:
            raise ValueError("feedback must identify the record's own subgraph and epoch")
        if not isinstance(request_id, str) or not request_id:
            raise ValueError("request_id must be a nonempty string")
        if not math.isfinite(disagreement) or not 0.0 <= disagreement <= 1.0:
            raise ValueError("disagreement must lie in [0, 1]")
        key = self._key(spec, record, epoch, similarity)
        group_id = hashlib.sha256(request_id.encode("utf-8")).hexdigest()
        with self._lock:
            previous = self._statistics.get(key)
            digest = hashlib.sha256(record.serialized()).hexdigest()
            if previous is not None and previous.record_digest != digest:
                return False
            candidate = _Statistics(
                groups=OrderedDict(previous.groups) if previous else OrderedDict(),
                updated_at=self._clock(),
                record_digest=digest,
            )
            total, count = candidate.groups.get(group_id, (0.0, 0))
            candidate.groups[group_id] = (total + disagreement, count + 1)
            while len(candidate.groups) > self.config.max_groups:
                candidate.groups.popitem(last=False)
            prospective = dict(self._statistics)
            prospective[key] = candidate
            # Leave room for refreshed float statistics for every active bin.
            # A record reserves this entire metadata allocation in the store.
            active_keys = sum(item[1] == record.record_id for item in prospective)
            encoded = self._serialize_record(record.record_id, prospective)
            if len(encoded) + 256 * active_keys > record.metadata_capacity:
                return False
            self._statistics[key] = candidate
            self._capacities[record.record_id] = record.metadata_capacity
            return True

    def refresh(self) -> int:
        """Compute deterministic group-bootstrap statistics outside lookup."""
        refreshed = 0
        with self._lock:
            for key, history in self._statistics.items():
                if not history.groups:
                    continue
                values = [total / count for total, count in history.groups.values()]
                mean = math.fsum(values) / len(values)
                seed_material = canonical_json([key, list(history.groups.items())])
                seed = int.from_bytes(hashlib.sha256(seed_material.encode("utf-8")).digest()[:8], "big")
                rng = random.Random(seed)
                bootstrap = sorted(
                    math.fsum(values[rng.randrange(len(values))] for _ in values) / len(values)
                    for _ in range(self.config.bootstrap_samples)
                )
                # Empirical quantile: inverse empirical CDF / nearest rank.
                index = max(0, min(len(bootstrap) - 1,
                                   math.ceil((1.0 - self.config.delta) * len(bootstrap)) - 1))
                margin = max(0.0, bootstrap[index] - mean)
                history.evidence = Evidence(len(values), mean, margin, min(1.0, mean + margin),
                                            history.updated_at)
                if len(self._serialize_record(key[1], self._statistics)) > self._capacities[key[1]]:
                    # Keep fail-closed behavior if unusual float encodings ever
                    # exceed the reserved metadata allocation.
                    history.evidence = None
                else:
                    refreshed += 1
        return refreshed

    def lookup(
        self, spec: SubgraphSpec, record: CachedRecord, epoch: str, similarity: float
    ) -> Optional[Evidence]:
        """Read only: no reference execution, labels, or bootstrap here."""
        if record.subgraph_id != spec.identity or record.epoch != epoch:
            return None
        key = self._key(spec, record, epoch, similarity)
        with self._lock:
            history = self._statistics.get(key)
            if history is None or history.evidence is None:
                return None
            if history.record_digest != hashlib.sha256(record.serialized()).hexdigest():
                return None
            if self._clock() - history.updated_at > self.config.max_age_seconds:
                return None
            return history.evidence

    def qualification_snapshot(
        self, spec: SubgraphSpec, record: CachedRecord, epoch: str
    ) -> Tuple[Dict[str, Any], ...]:
        """Return one public, read-only historical status row for every bin.

        ``enough_groups`` describes observed support even when its score fails
        or its evidence is stale. ``qualified`` additionally requires refreshed,
        live, content-bound evidence passing the configured screening policy.
        An unobserved bin is never historically qualified, including under the
        Similarity policy: ``history_required=False`` identifies that serving
        under that policy does not depend on this historical status at all.

        These rows do not authorize substitution and do not imply residency.
        Input similarity thresholds, guards, readiness and serving/audit-only
        status belong to the caller. Lower bounds are inclusive; upper bounds
        are exclusive except for the final bin, whose upper bound includes 1.
        No bootstrap, reference execution or reliability mutation occurs here.
        """
        cuts = self.config.similarity_bins
        identity_valid = record.subgraph_id == spec.identity
        epoch_valid = record.epoch == epoch
        digest = hashlib.sha256(record.serialized()).hexdigest()
        with self._lock:
            now = self._clock()
            rows = []
            for index in range(len(cuts) + 1):
                key = (spec.identity, record.record_id, record.revision, epoch, index)
                history = self._statistics.get(key)
                digest_valid = history is None or history.record_digest == digest
                record_valid = identity_valid and epoch_valid and digest_valid
                observed = bool(record_valid and history is not None and history.groups)
                evidence = history.evidence if observed else None
                groups = len(history.groups) if observed else 0
                mean = (math.fsum(total / count for total, count in history.groups.values()) / groups
                        if observed else None)
                stale = bool(observed and now - history.updated_at > self.config.max_age_seconds)
                enough_groups = bool(observed and groups >= self.config.effective_min_groups)
                if not identity_valid or not epoch_valid:
                    reason = "subgraph_or_epoch_mismatch"
                elif not digest_valid:
                    reason = "immutable_record_content_mismatch"
                elif not observed:
                    reason = "no_observations"
                elif evidence is None:
                    reason = "not_refreshed"
                elif stale:
                    reason = "stale_evidence"
                else:
                    reason = self.config.evidence_rejection(evidence)
                rows.append({
                    "subgraph_id": spec.identity, "record_id": record.record_id,
                    "revision": record.revision, "epoch": epoch, "bin": index,
                    "bin_lower": -1.0 if index == 0 else cuts[index - 1],
                    "bin_upper": 1.0 if index == len(cuts) else cuts[index],
                    "bin_upper_inclusive": index == len(cuts),
                    "observed": observed, "refreshed": evidence is not None,
                    "record_valid": record_valid, "stale": stale,
                    "groups": groups, "mean": mean,
                    "margin": evidence.margin if evidence else None,
                    "score": evidence.score if evidence else None,
                    "updated_at": history.updated_at if observed else None,
                    "enough_groups": enough_groups, "qualified": reason is None,
                    "rejection_reason": reason,
                    "configured_min_groups": self.config.min_groups,
                    "effective_min_groups": self.config.effective_min_groups,
                    "max_score": (None if self.config.screening_mode == ScreeningMode.SIMILARITY
                                  else self.config.max_score),
                    "screening_mode": self.config.screening_mode.value,
                    "score_rule": self.config.score_rule,
                    "screening_score": self.config.screening_score(evidence),
                    "history_required": self.config.screening_mode != ScreeningMode.SIMILARITY,
                })
            return tuple(rows)

    @staticmethod
    def _serialize_record(record_id: str, statistics: Mapping[tuple, _Statistics]) -> bytes:
        entries = []
        for key, history in sorted(statistics.items()):
            if key[1] != record_id:
                continue
            evidence = history.evidence
            entries.append({
                "subgraph_id": key[0], "revision": key[2], "epoch": key[3], "bin": key[4],
                "updated_at": history.updated_at,
                "record_digest": history.record_digest,
                "request_groups": [[request, total, count]
                                   for request, (total, count) in history.groups.items()],
                "evidence": None if evidence is None else {
                    "groups": evidence.groups, "mean": evidence.mean,
                    "margin": evidence.margin, "score": evidence.score,
                    "updated_at": evidence.updated_at,
                },
            })
        return canonical_json({"format_version": 1, "record_id": record_id, "entries": entries}).encode("utf-8")

    def export_record(self, record_id: str) -> bytes:
        with self._lock:
            return self._serialize_record(record_id, self._statistics)

    def discard_records(self, live_ids: Iterable[str]) -> None:
        live = set(live_ids)
        with self._lock:
            self._statistics = {key: value for key, value in self._statistics.items() if key[1] in live}
            self._capacities = {key: value for key, value in self._capacities.items() if key in live}


@dataclass(frozen=True)
class EligibleRecord:
    spec: SubgraphSpec
    record: CachedRecord
    similarity: float
    evidence: Optional[Evidence]
    epoch: str
    exact: bool = False
    screening_mode: ScreeningMode = ScreeningMode.FULL

    def __post_init__(self) -> None:
        object.__setattr__(self, "screening_mode", ScreeningMode.parse(self.screening_mode))

    @property
    def score(self) -> Optional[float]:
        if self.exact:
            return 0.0
        if self.screening_mode == ScreeningMode.SIMILARITY:
            return None  # Absence of evidence is not a zero disagreement score.
        if self.evidence is None:
            return 1.0
        return self.evidence.mean if self.screening_mode == ScreeningMode.NO_MARGIN else self.evidence.score

    @property
    def score_rule(self) -> str:
        if self.exact:
            return "exact"
        if self.screening_mode == ScreeningMode.SIMILARITY:
            return "none"
        return "mean" if self.screening_mode == ScreeningMode.NO_MARGIN else "mean_plus_margin"


class Matcher:
    """Screen compatible resident records and return descriptors to C."""

    def __init__(
        self,
        table: ReliabilityTable,
        config: Optional[ReliabilityConfig] = None,
        sim_threshold: float = 0.8,
        *,
        allow_exact: bool = True,
    ) -> None:
        self.table = table
        self.config = config or table.config
        if self.config.similarity_bins != table.config.similarity_bins:
            raise ValueError("matcher and table must use the same similarity bins")
        if not math.isfinite(sim_threshold) or not -1.0 <= sim_threshold <= 1.0:
            raise ValueError("sim_threshold must lie in [-1, 1]")
        self.sim_threshold = sim_threshold
        self.allow_exact = allow_exact
        self.last_rejections: list = []

    def compatible(self, spec: SubgraphSpec, record: CachedRecord,
                   inputs: Mapping[str, Any], epoch: str) -> bool:
        """Shared guard for serving and explicit audit-only comparisons."""
        if spec.identity != record.subgraph_id or record.epoch != epoch:
            return False
        try:
            cached_inputs, cached_outputs = record.inputs, record.outputs
            if not spec.accepts_input(inputs) or not spec.accepts_input(cached_inputs):
                return False
            if not spec.accepts_output(cached_outputs):
                return False
            return all(canonical_json(inputs[name]) == canonical_json(cached_inputs[name])
                       for name in spec.critical_fields)
        except (KeyError, TypeError, ValueError):
            return False

    def similarity(self, inputs: Mapping[str, Any], record: CachedRecord) -> float:
        return max(-1.0, min(1.0, cosine(encode_input(inputs), record.embedding)))

    def eligible(self, spec: SubgraphSpec, state: Mapping[str, Any], snapshot: CacheSnapshot,
                 epoch: str, allow_exact: bool = True) -> Tuple[EligibleRecord, ...]:
        self.last_rejections = []
        try:
            inputs = spec.project(state)
        except KeyError:
            self.last_rejections.append({"record_id": None, "reason": "missing_input_fields"})
            return ()
        accepted = []
        for record in snapshot.records:
            if record.subgraph_id != spec.identity:
                continue
            reason = None
            if record.record_id in snapshot.audit_ids:
                reason = "audit_only"
            elif not self.compatible(spec, record, inputs, epoch):
                reason = "incompatible_schema_epoch_or_critical_fields"
            if reason:
                self.last_rejections.append({"record_id": record.record_id, "reason": reason})
                continue
            exact = self.allow_exact and allow_exact and spec.exact_safe and canonical_json(inputs) == record.input_json
            similarity = 1.0 if exact else self.similarity(inputs, record)
            if exact:
                accepted.append(EligibleRecord(spec, record, similarity, None, epoch, True,
                                               self.config.screening_mode))
                continue
            if similarity < self.sim_threshold:
                reason = "similarity_below_threshold"
            elif self.config.screening_mode == ScreeningMode.SIMILARITY:
                # Do not even read the history: this ablation must also work
                # with absent, stale, or highly disagreeing calibration data.
                accepted.append(EligibleRecord(spec, record, similarity, None, epoch, False,
                                               self.config.screening_mode))
            else:
                evidence = self.table.lookup(spec, record, epoch, similarity)
                reason = self.config.evidence_rejection(evidence)
                if reason is None:
                    accepted.append(EligibleRecord(spec, record, similarity, evidence, epoch, False,
                                                   self.config.screening_mode))
            if reason:
                self.last_rejections.append({"record_id": record.record_id, "reason": reason})
        # The ordinary matcher retains its original score tie-break. B1 fixes
        # this switch to False so feedback cannot change retrieval ordering.
        return tuple(sorted(accepted, key=lambda item: (
            -item.similarity,
            (item.score if item.score is not None else 1.0) if self.config.rank_by_score else 0.0,
            item.record.record_id, item.record.revision,
        )))
