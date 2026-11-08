"""Incremental B -> C serving path, with explicitly scheduled D maintenance.

This executor controls declared operators, so a hit really bypasses every
covered callable. It is a single-request, serial implementation, independent
of the legacy tracing decorators and of external LLM/tool providers.
"""

from __future__ import annotations

import json
import hashlib
import math
import time
import uuid
from bisect import bisect_right
from collections import Counter, deque
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Tuple

from .planner import ForecastTable, LookaheadPlanner, PlannerConfig, PlanningMode
from .observability import current_operation_context, operation_scope
from .records import CacheSnapshot, CachedRecord, RecordStore
from .reliability import Matcher, ReliabilityConfig, ReliabilityTable, ScreeningMode
from .residency import AdmissionResult, OpportunityCacheManager, ResidencyConfig
from .spec import SubgraphSpec, WorkflowSpec, accepts_schema, canonical_json


@dataclass(frozen=True)
class RuntimeConfig:
    epoch: str = "default-v1"
    allow_exact: bool = True
    similarity_threshold: float = 0.8
    audit_top_k: int = 3
    request_window: int = 16
    metadata_capacity: int = 32768
    screening_budget: Optional[float] = None
    feedback_on_normal_execution: bool = False
    fixed_cache: bool = False

    def __post_init__(self):
        if not self.epoch or self.audit_top_k < 1 or self.request_window < 1 or self.metadata_capacity < 0:
            raise ValueError("Invalid runtime epoch or bounded resource configuration")
        if self.screening_budget is not None and (not math.isfinite(self.screening_budget) or self.screening_budget < 0):
            raise ValueError("Screening budget must be finite and nonnegative")


@dataclass
class RuntimeResult:
    request_id: str
    state: Dict[str, Any]
    executed_node_ids: Tuple[str, ...]
    reused_subgraphs: Tuple[str, ...]
    new_record_ids: Tuple[str, ...]
    elapsed_ms: float
    cost_usd: float
    audit_cost_usd: float = 0.0
    trace: list = field(default_factory=list)
    reference_inputs: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    reference_outputs: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    execution_purpose: str = "serving"
    planning_elapsed_ms: float = 0.0
    served_hits: list = field(default_factory=list)


class GeoLeapRuntime:
    def __init__(
        self, workflow: WorkflowSpec, subgraphs: Sequence[SubgraphSpec], *,
        config: Optional[RuntimeConfig] = None,
        reliability_config: Optional[ReliabilityConfig] = None,
        reliability_table: Optional[ReliabilityTable] = None,
        planner_config: Optional[PlannerConfig] = None,
        residency_config: Optional[ResidencyConfig] = None,
        store: Optional[RecordStore] = None,
        agreement: Optional[Callable[[SubgraphSpec, Mapping[str, Any], Mapping[str, Any]], float]] = None,
        operation_observer: Optional[Callable[[dict], None]] = None,
        hit_observer: Optional[Callable[[dict], None]] = None,
    ):
        self.workflow = workflow
        self.subgraphs = tuple(subgraphs)
        self.config = config or RuntimeConfig()
        if len({spec.identity for spec in self.subgraphs}) != len(self.subgraphs):
            raise ValueError("Subgraph identities must be unique")
        for spec in self.subgraphs:
            expected = workflow.subgraph(spec.name, spec.node_ids, spec.critical_fields)
            if expected != spec:
                raise ValueError("Subgraph declaration does not match this workflow")
        self.store = store if store is not None else RecordStore()
        if reliability_table is not None:
            if not isinstance(reliability_table, ReliabilityTable):
                raise ValueError("reliability_table must be a ReliabilityTable")
            if reliability_config is not None and reliability_config != reliability_table.config:
                raise ValueError("Injected reliability table and runtime configuration must match; "
                                 "use table.clone(config=...) for a screening variant")
            self.reliability = reliability_table
        else:
            self.reliability = ReliabilityTable(reliability_config)
        self.matcher = Matcher(self.reliability, sim_threshold=self.config.similarity_threshold,
                               allow_exact=self.config.allow_exact)
        if self.matcher.config.screening_mode == ScreeningMode.SIMILARITY and self.config.screening_budget is not None:
            raise ValueError("Similarity screening has no empirical score; screening_budget must be None")
        self.forecasts = ForecastTable()
        self.planner = LookaheadPlanner(planner_config, self.forecasts)
        if self.planner.config.max_workers != 1:
            raise ValueError("GeoLeapRuntime executes serially; parallel worker settings are planner diagnostics only")
        self.agreement = agreement or (lambda spec, cached, reference:
                                      float(canonical_json(cached) == canonical_json(reference)))
        self._request_window = deque(maxlen=self.config.request_window)
        self._maintenance_planner = LookaheadPlanner(self.planner.config, self.forecasts.snapshot())
        self._demand_profiles: Dict[str, ForecastTable] = {}
        self._maintenance_context_planners = {}
        self._maintenance_context_counts = Counter()
        self._cost_profile_revision = 0
        self._maintenance_cpu_ms = 0.0
        self._maintenance_wall_ms = 0.0
        self._window_weight = 0
        self.manager = OpportunityCacheManager(self.store, residency_config or ResidencyConfig(), self._utility,
                                               record_benefit_fn=self._record_benefit)
        self._running = False
        self.operation_observer = operation_observer
        # This observer receives only a detached commit record. Evaluation
        # reference execution belongs after the serving request, outside B.
        self.hit_observer = hit_observer
        self._active_request_id = None
        self._active_execution_purpose = "serving"
        self._audit_profile_cost = 0.0
        self._fixed_cache_snapshot = self.store.snapshot(include_audit=True) if self.config.fixed_cache else None
        self._fixed_planning_fingerprint = None

    def _check_fixed_planning_state(self) -> None:
        if not self.config.fixed_cache:
            return
        snapshot = self.store.snapshot(include_audit=True)
        if snapshot != self._fixed_cache_snapshot:
            raise RuntimeError("C1 fixed cache changed; construct a new runtime for a different cache view")
        material = b"".join(self.reliability.export_record(record.record_id) +
                            self.forecasts.export_record(record.record_id)
                            for record in sorted(snapshot.records, key=lambda item: item.record_id))
        fingerprint = hashlib.sha256(material).hexdigest()
        if self._fixed_planning_fingerprint is None:
            self._fixed_planning_fingerprint = fingerprint
        elif self._fixed_planning_fingerprint != fingerprint:
            raise RuntimeError("C1 matching evidence or forecasts changed during fixed-state execution")

    def _utility(self, records: Tuple[CachedRecord, ...]) -> float:
        # Each round freezes historical context forecasts and observed demand.
        # No future request inputs or realized outputs enter this comparison.
        if not self._window_weight:
            return 0.0
        counts = self._maintenance_context_counts or {None: self._window_weight}
        values = []
        for context, count in counts.items():
            planner = self._maintenance_context_planners.get(context, self._maintenance_planner)
            empty = planner.estimate(self.workflow, self.subgraphs, CacheSnapshot(0), self.config.epoch)
            cached = planner.estimate(self.workflow, self.subgraphs, CacheSnapshot(0, records), self.config.epoch)
            values.append(count / self._window_weight * (empty.objective - cached.objective))
        return math.fsum(values)

    def _record_benefit(self, record: CachedRecord) -> Tuple[float, float]:
        """Independent service-profile savings for the legacy density baseline."""
        spec = next((item for item in self.subgraphs if item.identity == record.subgraph_id), None)
        if spec is None or record.epoch != self.config.epoch:
            return 0.0, 0.0
        config = self.planner.config
        service_ms = math.fsum(self.workflow.nodes[node].latency_ms for node in spec.node_ids)
        overhead_ms = config.lookup_ms + config.validation_ms + config.restore_ms + config.planning_ms
        return (max(0.0, service_ms - overhead_ms),
                math.fsum(self.workflow.nodes[node].cost_usd for node in spec.node_ids))

    @property
    def cost_profile_revision(self) -> int:
        return self._cost_profile_revision

    @property
    def demand_contexts(self) -> Tuple[str, ...]:
        return tuple(sorted(self._demand_profiles))

    def set_demand_profiles(self, profiles: Mapping[str, ForecastTable]) -> None:
        """Install historical context-conditioned predictions between requests.

        Context labels identify known workload classes; these tables never
        authorize reuse. B still checks each live entry independently.
        """
        if self._running or self.config.fixed_cache:
            raise RuntimeError("Demand profiles can only change between non-fixed-cache requests")
        if not isinstance(profiles, Mapping):
            raise ValueError("Demand profiles must be a context mapping")
        if any(not isinstance(key, str) or not key or not isinstance(value, ForecastTable)
               for key, value in profiles.items()):
            raise ValueError("Demand profiles require nonempty context names and ForecastTable values")
        observed = {item.get("demand_context") for item in self._request_window} - {None}
        if not observed.issubset(profiles):
            raise ValueError("Demand profiles must retain contexts still present in the observed window")
        self._demand_profiles = {key: table.snapshot() for key, table in profiles.items()}

    def residency_demand_snapshot(self) -> dict:
        counts = Counter(item.get("demand_context") for item in self._request_window)
        return {"observed_requests": len(self._request_window),
                "context_counts": {key if key is not None else "<global>": count
                                   for key, count in counts.items()},
                "profile_revision": self.cost_profile_revision}

    def update_service_profiles(self, updates: Mapping[str, Mapping[str, float]]) -> int:
        """Update economic estimates without changing semantics or B evidence.

        Latency values are planner profiles, not injected sleeps or measured WAN
        changes. Real services/adapters supply their own timing and receipts.
        """
        if self._running or self.config.fixed_cache:
            raise RuntimeError("Service profiles can only change between non-fixed-cache requests")
        if not isinstance(updates, Mapping):
            raise ValueError("Service profile updates must be a node mapping")
        if not updates:
            return self.cost_profile_revision
        allowed = {"latency_ms", "cost_usd"}
        for node_id, values in updates.items():
            if node_id not in self.workflow.nodes or not isinstance(values, Mapping) or not values or set(values) - allowed:
                raise ValueError("Only known node latency_ms/cost_usd profiles may change")
            if any(isinstance(value, bool) or not isinstance(value, (int, float))
                   or not math.isfinite(value) or value < 0 for value in values.values()):
                raise ValueError("Service costs and latencies must be finite nonnegative numbers")
        operators = tuple(replace(operator, **updates[operator.node_id])
                          if operator.node_id in updates else operator for operator in self.workflow.operators)
        updated = WorkflowSpec(self.workflow.workflow_id, operators, version=self.workflow.version)
        if any(updated.subgraph(spec.name, spec.node_ids, spec.critical_fields) != spec for spec in self.subgraphs):
            raise ValueError("Economic profile update unexpectedly changes a subgraph identity")
        self.workflow = updated
        self._cost_profile_revision += 1
        return self.cost_profile_revision

    def _execute_operator(self, node_id: str, state: Dict[str, Any], *, purpose: Optional[str] = None,
                          subgraph_id: Optional[str] = None) -> Dict[str, Any]:
        operator = self.workflow.nodes[node_id]
        inputs = json.loads(canonical_json({key: state[key] for key in operator.read_fields}))
        if not accepts_schema(inputs, operator.input_schema):
            raise ValueError(f"Input schema mismatch: {node_id}")
        started, success = time.perf_counter(), False
        with operation_scope(request_id=self._active_request_id, node_id=node_id,
                             purpose=purpose or self._active_execution_purpose, subgraph_id=subgraph_id):
            try:
                output = operator.function(inputs)
                if not isinstance(output, Mapping) or not accepts_schema(output, operator.output_schema):
                    raise ValueError(f"Operator must return its declared output patch: {node_id}")
                output = json.loads(canonical_json(output))
                success = True
                if current_operation_context()["purpose"] == "extra_audit":
                    self._audit_profile_cost += operator.cost_usd
                return output
            finally:
                if self.operation_observer is not None:
                    self.operation_observer({"event": "operation_complete", **current_operation_context(),
                                             "status": "ok" if success else "error",
                                             "elapsed_ms": (time.perf_counter() - started) * 1000,
                                             "profile_cost_usd": operator.cost_usd,
                                             "cost_source": "operator_profile_not_provider_receipt"})

    def _reference_subgraph(self, spec: SubgraphSpec, inputs: Dict[str, Any]) -> Dict[str, Any]:
        local = json.loads(canonical_json(inputs))
        for node_id in spec.node_ids:
            local.update(self._execute_operator(node_id, local, purpose="extra_audit", subgraph_id=spec.identity))
        outputs = {key: local[key] for key in spec.output_fields}
        if not spec.accepts_output(outputs):
            raise ValueError("Reference output does not satisfy subgraph boundary")
        return outputs

    def _label(self, spec, record, inputs, outputs, request_id, kind, trace):
        if not self.matcher.compatible(spec, record, inputs, self.config.epoch):
            return False
        similarity = self.matcher.similarity(inputs, record)
        if similarity < self.matcher.sim_threshold:
            return False
        agreement = float(self.agreement(spec, record.outputs, outputs))
        if not math.isfinite(agreement) or not 0 <= agreement <= 1:
            raise ValueError("Agreement comparator must return a value in [0, 1]")
        stored = self.reliability.observe(spec, record, self.config.epoch, similarity, request_id, 1.0 - agreement)
        trace.append({"event": "output_feedback", "kind": kind, "subgraph": spec.name,
                      "record_id": record.record_id, "revision": record.revision,
                      "similarity": similarity, "disagreement": 1.0 - agreement, "stored": stored})
        return stored

    def _refresh_and_profile(self, observations):
        self.reliability.refresh()
        for spec, record, inputs in observations:
            snapshot = CacheSnapshot(self.store.version, (record,))
            # Audit-only records can be screened for graduation explicitly. They
            # are never exposed by the actual serving snapshot in run().
            supported = bool(self.matcher.eligible(spec, inputs, snapshot, self.config.epoch, allow_exact=False))
            self.forecasts.observe(spec.identity, record.record_id, record.revision, self.config.epoch, supported)
            payload = canonical_json({
                "reliability": json.loads(self.reliability.export_record(record.record_id)),
                "forecasts": json.loads(self.forecasts.export_record(record.record_id)),
            }).encode("utf-8")
            if not self.store.write_metadata(record.record_id, payload):
                # Fail closed instead of keeping support outside its allocation.
                other_ids = {item.record_id for item in self.store.snapshot(include_audit=True).records
                             if item.record_id != record.record_id}
                self.reliability.discard_records(other_ids)
                self.forecasts.discard_records(other_ids)

    def _has_support(self, spec: SubgraphSpec, record: CachedRecord) -> bool:
        if record.epoch != self.config.epoch:
            return False
        if self.matcher.config.screening_mode == ScreeningMode.SIMILARITY:
            return self.matcher.compatible(spec, record, record.inputs, self.config.epoch)
        for similarity in (-1.0,) + self.reliability.config.similarity_bins:
            evidence = self.reliability.lookup(spec, record, self.config.epoch, similarity)
            if self.matcher.config.evidence_rejection(evidence) is None:
                return True
        return False

    def observe_reference_result(self, result: RuntimeResult, snapshot: CacheSnapshot) -> tuple:
        """Reuse completed normal outputs to compare only pre-request records.

        No service is called. Internally substituted or incomplete regions are
        excluded. Call after the request; newly learned evidence never authorizes
        an earlier action in that request. The snapshot must precede execution.
        """
        if self.config.fixed_cache:
            raise RuntimeError("C1 fixed-state runtime does not update matching evidence")
        offset = len(result.trace)
        observations = []
        already_observed = {(event.get("record_id"), event.get("revision"), event.get("subgraph"))
                            for event in result.trace if event.get("event") == "output_feedback" and
                            event.get("kind") == "normal_execution" and event.get("stored")}
        fresh_nodes = set(result.executed_node_ids)
        for spec in self.subgraphs:
            if not set(spec.node_ids).issubset(fresh_nodes):
                continue
            if spec.identity not in result.reference_inputs or spec.identity not in result.reference_outputs:
                continue
            inputs, outputs = result.reference_inputs[spec.identity], result.reference_outputs[spec.identity]
            candidates = [record for record in snapshot.records
                          if record.record_id not in result.new_record_ids and
                          self.store.get(record.record_id, record.revision, include_audit=True) == record and
                          self.matcher.compatible(spec, record, inputs, self.config.epoch) and
                          self.matcher.similarity(inputs, record) >= self.matcher.sim_threshold]
            candidates.sort(key=lambda record: (-self.matcher.similarity(inputs, record), record.record_id))
            for record in candidates[:self.config.audit_top_k]:
                if (record.record_id, record.revision, spec.name) in already_observed:
                    continue
                if self._label(spec, record, inputs, outputs, result.request_id, "normal_execution", result.trace):
                    observations.append((spec, record, inputs))
        if observations:
            self._refresh_and_profile(observations)
        return tuple(result.trace[offset:])

    def qualification_snapshot(self) -> tuple:
        """Current per-record/per-bin history, distinct from serving residency."""
        view = self.store.snapshot(include_audit=True)
        specs = {spec.identity: spec for spec in self.subgraphs}
        rows = []
        for record in view.records:
            spec = specs.get(record.subgraph_id)
            if spec is None:
                continue
            for row in self.reliability.qualification_snapshot(spec, record, self.config.epoch):
                row = dict(row)
                row.update({"resident": True, "audit_only": record.record_id in view.audit_ids,
                            "serving": record.record_id not in view.audit_ids,
                            "snapshot_version": view.version})
                row.setdefault("bin_index", row["bin"])
                rows.append(row)
        return tuple(rows)

    def _run_without_cache(self, state: Dict[str, Any], request_id: str,
                           execution_purpose: str, started: float) -> RuntimeResult:
        """Execute the declared DAG without any cache access or planner call.

        This is the C1 baseline. The same operator validation and observer are
        used as in serving, but subgraph capture, evidence and cache bookkeeping
        are excluded from the measured request.
        """
        trace = [{"event": "execution_config", "planning_mode": PlanningMode.NO_CACHE.value,
                  "cache_enabled": False, "planning_enabled": False,
                  "execution_purpose": execution_purpose}]
        cost = 0.0
        for node_id in self.workflow.topological_order:
            op_started = time.perf_counter()
            state.update(self._execute_operator(node_id, state))
            duration = (time.perf_counter() - op_started) * 1000
            cost += self.workflow.nodes[node_id].cost_usd
            trace.append({"event": "execute", "node_id": node_id, "elapsed_ms": duration,
                          "profile_cost_usd": self.workflow.nodes[node_id].cost_usd})
        return RuntimeResult(
            request_id=request_id, state=state, executed_node_ids=self.workflow.topological_order,
            reused_subgraphs=(), new_record_ids=(), elapsed_ms=(time.perf_counter() - started) * 1000,
            cost_usd=cost, audit_cost_usd=cost if execution_purpose == "extra_audit" else 0.0,
            trace=trace, execution_purpose=execution_purpose, planning_elapsed_ms=0.0,
        )

    def run(self, initial_state: Mapping[str, Any], *, request_id: Optional[str] = None,
            use_cache: bool = True, collect_records: bool = True, audit_hits: bool = False,
            collect_feedback: Optional[bool] = None, execution_purpose: str = "serving",
            audit_selector: Optional[Callable[[dict], bool]] = None,
            audit_probability: Optional[float] = None,
            demand_context: Optional[str] = None) -> RuntimeResult:
        if self._running:
            raise RuntimeError("This incremental executor supports one request at a time")
        self._running = True
        started = time.perf_counter()
        try:
            no_cache = self.planner.config.mode == PlanningMode.NO_CACHE
            if no_cache and (collect_feedback is True or audit_hits or audit_selector is not None):
                raise ValueError("No-cache baseline disables audits and feedback updates")
            if self.config.fixed_cache:
                if collect_feedback is True or audit_hits or audit_selector is not None:
                    raise ValueError("C1 fixed-state execution disables audits and feedback updates")
                collect_records, collect_feedback = False, False
                if not no_cache:
                    self._check_fixed_planning_state()
            if execution_purpose not in ("serving", "extra_audit", "baseline_measurement"):
                raise ValueError("Unknown execution purpose")
            if audit_probability is not None and not 0 <= audit_probability <= 1:
                raise ValueError("Audit selection probability must lie in [0, 1]")
            if demand_context is not None and (not isinstance(demand_context, str)
                                                or demand_context not in self._demand_profiles):
                raise ValueError("Unknown demand context; install historical demand profiles first")
            state = json.loads(canonical_json(initial_state))
            if not isinstance(state, dict):
                raise ValueError("Initial workflow state must be an object")
            request_id = request_id or uuid.uuid4().hex
            self._active_request_id, self._active_execution_purpose = request_id, execution_purpose
            if no_cache:
                return self._run_without_cache(state, request_id, execution_purpose, started)
            if execution_purpose == "serving" and not self.config.fixed_cache:
                self.manager.advance_request()
            active_planner = (LookaheadPlanner(self.planner.config, self._demand_profiles[demand_context].snapshot())
                              if demand_context is not None else self.planner)
            pre_request_snapshot = self.store.snapshot(include_audit=True)
            feedback_enabled = self.config.feedback_on_normal_execution if collect_feedback is None else collect_feedback
            completed, normally_executed = set(), set()
            executed, reused = [], []
            served_hits = []
            screening = self.matcher.config
            trace = [{"event": "screening_config", "screening_mode": screening.screening_mode.value,
                      "score_rule": screening.score_rule, "configured_min_groups": screening.min_groups,
                      "effective_min_groups": screening.effective_min_groups,
                      "max_score": None if screening.screening_mode == ScreeningMode.SIMILARITY else screening.max_score,
                      "similarity_threshold": self.matcher.sim_threshold, "allow_exact": self.config.allow_exact,
                      "execution_purpose": execution_purpose},
                     {"event": "execution_config", "planning_mode": self.planner.config.mode.value,
                      "cache_enabled": bool(use_cache), "planning_enabled": bool(use_cache),
                      "demand_context": demand_context, "cost_profile_revision": self.cost_profile_revision,
                      "execution_purpose": execution_purpose}]
            captured_inputs = {}
            cost, audit_cost = 0.0, 0.0
            planning_elapsed_ms = 0.0
            allowance = self.config.screening_budget
            hit_observations = []
            while len(completed) < len(self.workflow.nodes):
                ready = self.workflow.ready(completed)
                if not ready:
                    raise RuntimeError("No ready operation in an unfinished workflow")
                for spec in self.subgraphs:
                    if spec.entry_node_id in ready and spec.identity not in captured_inputs:
                        captured_inputs[spec.identity] = spec.project(state)
                snapshot = self.store.snapshot()
                self._check_fixed_planning_state()
                eligible = []
                if use_cache:
                    for spec in self.subgraphs:
                        if spec.entry_node_id in ready and not set(spec.node_ids) & completed:
                            descriptors = self.matcher.eligible(spec, state, snapshot, self.config.epoch,
                                                                allow_exact=self.config.allow_exact)
                            eligible.extend(item for item in descriptors if allowance is None or (
                                item.score is not None and item.score <= allowance))
                    plan_started = time.perf_counter()
                    decision = active_planner.choose(self.workflow, self.subgraphs, completed, state,
                                                      snapshot, eligible, self.config.epoch)
                    planning_ms = (time.perf_counter() - plan_started) * 1000
                    planning_elapsed_ms += planning_ms
                    action = decision.action
                    trace.append({"event": "plan", "snapshot_version": snapshot.version,
                                  "objective": decision.objective, "details": dict(decision.trace),
                                  "screening_mode": screening.screening_mode.value,
                                  "planning_mode": self.planner.config.mode.value,
                                  "planning_elapsed_ms": planning_ms,
                                  "planner_reported_elapsed_ms": decision.planning_elapsed_ms})
                else:
                    action = None
                reused_now = False
                if action is not None and action.kind == "reuse":
                    candidate = action.candidate
                    spec, expected = candidate.spec, candidate.record
                    record = self.store.acquire_pin(expected.record_id, expected.revision)
                    if record is not None:
                        try:
                            fresh = self.matcher.eligible(spec, state, CacheSnapshot(self.store.version, (record,)),
                                                          self.config.epoch, allow_exact=self.config.allow_exact)
                            valid = next((item for item in fresh if item.record == record), None)
                            if valid and spec.entry_node_id in ready and not set(spec.node_ids) & completed and (
                                allowance is None or (valid.score is not None and valid.score <= allowance)
                            ):
                                live_inputs = spec.project(state)
                                state.update(record.outputs)
                                completed.update(spec.node_ids)
                                reused.append(spec.name)
                                if not self.config.fixed_cache:
                                    self.manager.record_access(record.record_id)
                                if allowance is not None:
                                    allowance -= valid.score
                                hit_index = len(served_hits) + 1
                                served_hit = {
                                    "hit_id": f"{request_id}:hit:{hit_index}",
                                    "request_id": request_id, "hit_index": hit_index,
                                    "subgraph_id": spec.identity, "subgraph_name": spec.name,
                                    "record_id": record.record_id, "revision": record.revision,
                                    "epoch": self.config.epoch, "covered_nodes": list(spec.node_ids),
                                    "record_digest": hashlib.sha256(record.serialized()).hexdigest(),
                                    "live_inputs": json.loads(canonical_json(live_inputs)),
                                    "cached_outputs": json.loads(canonical_json(record.outputs)),
                                    "screening_mode": valid.screening_mode.value,
                                    "score": valid.score, "score_rule": valid.score_rule,
                                    "groups": valid.evidence.groups if valid.evidence is not None else None,
                                    "mean": valid.evidence.mean if valid.evidence is not None else None,
                                    "margin": valid.evidence.margin if valid.evidence is not None else None,
                                    "empirical_score": valid.evidence.score if valid.evidence is not None else None,
                                    "evidence_updated_at": (valid.evidence.updated_at
                                                            if valid.evidence is not None else None),
                                    "similarity": valid.similarity,
                                    "similarity_bin": bisect_right(screening.similarity_bins, valid.similarity),
                                    "exact": valid.exact,
                                    "input_equal": canonical_json(live_inputs) == record.input_json,
                                }
                                served_hits.append(served_hit)
                                if self.hit_observer is not None:
                                    try:
                                        self.hit_observer(json.loads(canonical_json(served_hit)))
                                    except Exception as error:
                                        trace.append({"event": "hit_observer_failed", "hit_id": served_hit["hit_id"],
                                                      "error_type": type(error).__name__})
                                trace.append({"event": "reuse", "subgraph": spec.name,
                                              "record_id": record.record_id, "revision": record.revision,
                                              "covered_nodes": list(spec.node_ids), "exact": valid.exact,
                                              "score": valid.score, "score_rule": valid.score_rule,
                                              "screening_mode": valid.screening_mode.value,
                                              "empirical_score": valid.evidence.score if valid.evidence else None,
                                              "similarity": valid.similarity,
                                              "similarity_bin": bisect_right(screening.similarity_bins, valid.similarity),
                                              "input_equal": canonical_json(live_inputs) == record.input_json,
                                              "remaining_screening_budget": allowance})
                                reused_now = True
                                audit_context = {"request_id": request_id, "node_ids": spec.node_ids,
                                                 "subgraph_id": spec.identity, "record_id": record.record_id,
                                                 "revision": record.revision, "live_inputs": live_inputs,
                                                 "score": valid.score}
                                try:
                                    selected = bool(audit_selector(audit_context)) if audit_selector is not None else audit_hits
                                except Exception as error:
                                    selected = False
                                    trace.append({"event": "audit_decision_failed", "record_id": record.record_id,
                                                  "error_type": type(error).__name__})
                                trace.append({"event": "audit_decision", "record_id": record.record_id,
                                              "probability_basis": "caller_supplied_before_selector_budget_gate",
                                              "selected": bool(selected), "selection_probability": audit_probability
                                              if audit_probability is not None else (
                                                  None if audit_selector is not None else (1.0 if audit_hits else 0.0))})
                                if selected:
                                    before_audit_cost = self._audit_profile_cost
                                    try:
                                        reference = self._reference_subgraph(spec, live_inputs)
                                        if self._label(spec, record, live_inputs, reference, request_id, "actual_hit", trace):
                                            hit_observations.append((spec, record, live_inputs))
                                    except Exception as error:
                                        trace.append({"event": "audit_failed", "record_id": record.record_id,
                                                      "error_type": type(error).__name__})
                                    finally:
                                        audit_cost += self._audit_profile_cost - before_audit_cost
                        finally:
                            self.store.release_pin(expected.record_id, expected.revision)
                    if not reused_now:
                        trace.append({"event": "reuse_rejected_at_commit", "record_id": expected.record_id})
                if reused_now:
                    continue
                node_id = action.node_id if action is not None and action.kind == "execute" else ready[0]
                if node_id not in ready:
                    raise RuntimeError("Planner returned an operation that is not ready")
                op_started = time.perf_counter()
                state.update(self._execute_operator(node_id, state))
                duration = (time.perf_counter() - op_started) * 1000
                completed.add(node_id)
                normally_executed.add(node_id)
                executed.append(node_id)
                cost += self.workflow.nodes[node_id].cost_usd
                trace.append({"event": "execute", "node_id": node_id, "elapsed_ms": duration,
                              "profile_cost_usd": self.workflow.nodes[node_id].cost_usd})
            if hit_observations:
                self._refresh_and_profile(hit_observations)
            reference_outputs, new_ids = {}, []
            existing = self.store.snapshot(include_audit=True).records
            for spec in self.subgraphs:
                if not set(spec.node_ids).issubset(normally_executed):
                    continue  # An internally substituted region is never a fresh reference record.
                inputs = captured_inputs[spec.identity]
                outputs = {key: state[key] for key in spec.output_fields}
                reference_outputs[spec.identity] = outputs
                if collect_records and not any(
                    record.subgraph_id == spec.identity and record.epoch == self.config.epoch and
                    record.input_json == canonical_json(inputs) and record.output_json == canonical_json(outputs)
                    for record in existing
                ):
                    record = CachedRecord.create(spec, inputs, outputs, self.config.epoch,
                                                 metadata_capacity=self.config.metadata_capacity)
                    if self.manager.enqueue(record, audit_only=True):
                        new_ids.append(record.record_id)
                    else:
                        trace.append({"event": "audit_admission_queue_full", "subgraph": spec.name})
            if execution_purpose == "serving":
                self._request_window.append({"request_id": request_id, "epoch": self.config.epoch,
                                             "demand_context": demand_context})
            result = RuntimeResult(request_id, state, tuple(executed), tuple(reused), tuple(new_ids),
                                   (time.perf_counter() - started) * 1000, cost + audit_cost,
                                   cost + audit_cost if execution_purpose == "extra_audit" else audit_cost,
                                   trace, captured_inputs, reference_outputs, execution_purpose, planning_elapsed_ms,
                                   served_hits)
            if feedback_enabled:
                self.observe_reference_result(result, pre_request_snapshot)
            result.elapsed_ms = (time.perf_counter() - started) * 1000
            self._check_fixed_planning_state()
            return result
        finally:
            self._running = False
            self._active_request_id, self._active_execution_purpose = None, "serving"

    def audit(self, initial_state: Mapping[str, Any], *, request_id: Optional[str] = None) -> RuntimeResult:
        """Explicit no-reuse calibration pass, using a pre-run record snapshot.

        This is a functional API, not an experiment runner. Audit-only records
        receive their own output labels without ever serving a speculative hit.
        """
        if self.planner.config.mode == PlanningMode.NO_CACHE:
            raise RuntimeError("No-cache baseline disables reference audits")
        if self.config.fixed_cache:
            raise RuntimeError("C1 fixed-state execution disables reference audits")
        started = time.perf_counter()
        before = self.store.snapshot(include_audit=True)
        result = self.run(initial_state, request_id=request_id, use_cache=False, collect_records=False,
                          collect_feedback=False, execution_purpose="extra_audit")
        observations = []
        for spec in self.subgraphs:
            inputs = result.reference_inputs[spec.identity]
            outputs = result.reference_outputs[spec.identity]
            candidates = [record for record in before.records if
                          self.matcher.compatible(spec, record, inputs, self.config.epoch) and
                          self.matcher.similarity(inputs, record) >= self.matcher.sim_threshold]
            candidates.sort(key=lambda record: (-self.matcher.similarity(inputs, record), record.record_id))
            for record in candidates[:self.config.audit_top_k]:
                if self.store.get(record.record_id, record.revision, include_audit=True) is None:
                    continue
                if self._label(spec, record, inputs, outputs, result.request_id, "reference_only", result.trace):
                    observations.append((spec, record, inputs))
        self._refresh_and_profile(observations)
        result.elapsed_ms = (time.perf_counter() - started) * 1000
        return result

    def maintenance_tick(self, *, now: Optional[float] = None) -> list:
        """Freeze model, process D's bounded queue, and graduate supported records.

        Call between requests or from the application's maintenance scheduler.
        No serving lookup or planner action invokes this method implicitly.
        """
        if self.planner.config.mode == PlanningMode.NO_CACHE:
            raise RuntimeError("No-cache baseline disables cache maintenance")
        if self.config.fixed_cache:
            raise RuntimeError("C1 fixed cache disables admission, eviction, and evidence maintenance")
        if self._running:
            raise RuntimeError("Call maintenance between requests in this single-process version")
        wall_started, cpu_started = time.perf_counter(), time.thread_time()
        try:
            return self._maintenance_round(now)
        finally:
            self._maintenance_cpu_ms += (time.thread_time() - cpu_started) * 1000
            self._maintenance_wall_ms += (time.perf_counter() - wall_started) * 1000

    def maintenance_stats(self) -> dict:
        """Whole runtime maintenance, including profile freezing and promotion."""
        manager = self.manager.maintenance_stats()
        return dict(manager, manager_maintenance_cpu_ms=manager["maintenance_cpu_ms"],
                    manager_maintenance_wall_ms=manager["maintenance_wall_ms"],
                    maintenance_cpu_ms=self._maintenance_cpu_ms,
                    maintenance_wall_ms=self._maintenance_wall_ms,
                    maintenance_scope="runtime round including manager; do not add nested manager time")

    def _maintenance_round(self, now: Optional[float]) -> list:
        self._maintenance_planner = LookaheadPlanner(self.planner.config, self.forecasts.snapshot())
        current = self.store.snapshot(include_audit=True)
        declared = {spec.identity: spec for spec in self.subgraphs}
        # Historically qualified candidates can be queued before first residency.
        # Include them when freezing support so the admission trial can value
        # a new opportunity rather than treating every incoming forecast as zero.
        planning_records = current.records + self.manager.queued_records
        supported_ids = {
            record.record_id for record in planning_records if record.subgraph_id in declared and
            self._has_support(declared[record.subgraph_id], record)
        }
        self._maintenance_planner.forecasts.discard_records(supported_ids)
        self._maintenance_context_counts = Counter(item.get("demand_context") for item in self._request_window)
        self._maintenance_context_planners = {}
        for context in self._maintenance_context_counts:
            if context is None:
                continue
            planner = LookaheadPlanner(self.planner.config, self._demand_profiles[context].snapshot())
            planner.forecasts.discard_records(supported_ids)
            self._maintenance_context_planners[context] = planner
        self._window_weight = len(self._request_window)
        results = self.manager.maintenance_tick(now=now)
        snapshot = self.store.snapshot(include_audit=True)
        specs = {spec.identity: spec for spec in self.subgraphs}
        for record in snapshot.records:
            if record.record_id not in snapshot.audit_ids or record.subgraph_id not in specs:
                continue
            spec = specs[record.subgraph_id]
            # Support can be in a non-exact similarity bin. Graduation requires
            # at least one supported bin; serving still checks the actual bin.
            if self._has_support(spec, record):
                results.append(self.manager.promote(record.record_id))
        live_ids = {record.record_id for record in self.store.snapshot(include_audit=True).records}
        self.reliability.discard_records(live_ids)
        self.forecasts.discard_records(live_ids)
        return results
