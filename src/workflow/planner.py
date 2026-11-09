"""Bounded lookahead for a declared workflow and one cache snapshot.

The default resource model has one worker; configured worker limits preserve
the declared dependencies when forecasting each complete hit/miss scenario.
Future cache hits are forecasts, never authorizations to restore output.  A
forecast miss completes that planned block normally; the serving runtime still
commits only one checked action and plans again after its actual completion.
"""

from __future__ import annotations

import hashlib
import itertools
import math
import threading
import time
import uuid
import weakref
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Tuple, TYPE_CHECKING

from .records import CacheSnapshot, CachedRecord
from .memory import CacheMemoryBudget, CacheMemoryError, measure_object_graph
from .spec import SubgraphSpec, WorkflowSpec, canonical_json
from .scheduling import ScheduleTask, schedule_tasks

if TYPE_CHECKING:
    from .reliability import EligibleRecord


class PlanningMode(str, Enum):
    NO_CACHE = "no_cache"
    GREEDY = "greedy"
    FULL = "full"

    @classmethod
    def parse(cls, value) -> "PlanningMode":
        if isinstance(value, cls):
            return value
        if not isinstance(value, str):
            raise ValueError("planning mode must be no_cache, greedy or full")
        normalized = value.strip().lower().replace("-", "_")
        try:
            return cls(normalized)
        except ValueError:
            raise ValueError("planning mode must be no_cache, greedy or full") from None


class SearchStrategy(str, Enum):
    BOUNDED = "bounded"
    EXHAUSTIVE = "exhaustive"

    @classmethod
    def parse(cls, value) -> "SearchStrategy":
        if isinstance(value, cls):
            return value
        if not isinstance(value, str):
            raise ValueError("search strategy must be bounded or exhaustive")
        try:
            return cls(value.strip().lower())
        except ValueError:
            raise ValueError("search strategy must be bounded or exhaustive") from None


class ExhaustiveLimitExceeded(RuntimeError):
    """The small-DAG reference could not finish within its explicit guard.

    No partial result is returned or labelled as an exhaustive reference.
    """

    def __init__(self, message: str, statistics: Optional[Mapping[str, Any]] = None):
        super().__init__(message)
        self.statistics = dict(statistics or {})


@dataclass(frozen=True)
class PlannerConfig:
    depth: int = 3
    beam_width: int = 8
    alpha: float = 0.5
    beta: float = 0.5
    cost_reference: float = 1.0
    latency_reference_ms: float = 100.0
    lookup_ms: float = 0.1
    planning_ms: float = 0.1
    validation_ms: float = 0.1
    restore_ms: float = 0.1
    scenario_limit: int = 32
    mode: PlanningMode = PlanningMode.FULL
    max_workers: int = 1
    search_strategy: SearchStrategy = SearchStrategy.BOUNDED
    exhaustive_max_nodes: int = 10
    exhaustive_max_plans: int = 100000

    def __post_init__(self) -> None:
        object.__setattr__(self, "mode", PlanningMode.parse(self.mode))
        object.__setattr__(self, "search_strategy", SearchStrategy.parse(self.search_strategy))
        if self.search_strategy == SearchStrategy.EXHAUSTIVE and self.mode != PlanningMode.FULL:
            raise ValueError("exhaustive search requires full planning mode")
        for name in ("depth", "beam_width", "scenario_limit", "max_workers",
                     "exhaustive_max_nodes", "exhaustive_max_plans"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        for name in ("alpha", "beta", "lookup_ms", "planning_ms", "validation_ms", "restore_ms"):
            value = getattr(self, name)
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be finite and nonnegative")
        for name in ("cost_reference", "latency_reference_ms"):
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and positive")
        if self.alpha + self.beta <= 0:
            raise ValueError("at least one objective weight must be positive")


@dataclass(frozen=True)
class ForecastProfile:
    probability: float
    latency_ms: Optional[float] = None

    def __post_init__(self) -> None:
        if not math.isfinite(self.probability) or not 0 <= self.probability <= 1:
            raise ValueError("forecast probability must be in [0, 1]")
        if self.latency_ms is not None and (
            not math.isfinite(self.latency_ms) or self.latency_ms < 0
        ):
            raise ValueError("forecast latency must be finite and nonnegative")


ForecastKey = Tuple[str, str, int, str]


class ForecastTable:
    """Historical pass counts conditioned on record revision and epoch.

    This minimal profile has no context/correlation model.  An unobserved key
    has zero reuse probability.  A search copies the table before comparing
    plans so updates cannot change one side of the comparison.
    """

    def __init__(self) -> None:
        self._counts: Dict[ForecastKey, Tuple[int, int]] = {}
        self._lock = threading.RLock()
        self.memory_budget = None
        self._memory_prefix = "forecasts:" + uuid.uuid4().hex
        self._memory_handle = None
        self._memory_finalizer = None
        self._pending_workspace = None

    def _memory_root(self):
        return self

    def bind_memory_budget(self, budget: CacheMemoryBudget) -> None:
        """Measure this table and its COW workspaces in one shared heap domain."""
        if self.memory_budget is budget:
            return
        if self.memory_budget is not None:
            raise ValueError("A forecast table cannot change its memory budget domain")
        if not isinstance(budget, CacheMemoryBudget):
            raise TypeError("budget must be a CacheMemoryBudget")
        with budget.transaction(), self._lock:
            handle = None
            try:
                self.memory_budget = budget
                handle = budget.register_root(self._memory_prefix, self._memory_root, category="forecasts")
                self._memory_handle = handle
                self._memory_finalizer = weakref.finalize(self, handle.close)
                budget.check()
            except BaseException:
                if self._memory_finalizer is not None:
                    self._memory_finalizer.detach()
                self._memory_handle = self._memory_finalizer = None
                self.memory_budget = None
                if handle is not None:
                    handle.close()
                raise

    def _heap_reservation_bytes(self, key=None):
        current = measure_object_graph({"counts": self._counts})["owned_heap_bytes"]
        incoming = measure_object_graph({"key": key})["owned_heap_bytes"] if key is not None else 0
        return 16384 + 2 * current + 2 * incoming

    def observe(
        self, identity: str, record_id: str, revision: int, epoch: str, passed: bool
    ) -> bool:
        if not isinstance(passed, bool):
            raise TypeError("passed must be a bool")
        key = (identity, record_id, revision, epoch)
        if self.memory_budget is not None:
            budget = self.memory_budget
            with budget.transaction(), self._lock:
                old = self._counts
                try:
                    with budget.reservation(self._heap_reservation_bytes(key), "forecast_observe") as token:
                        self._pending_workspace = {"old": old, "key": key}
                        prospective = dict(old)
                        self._pending_workspace["new"] = prospective
                        successes, total = old.get(key, (0, 0))
                        prospective[key] = (successes + int(passed), total + 1)
                        self._counts = prospective
                        token.commit()
                    return True
                except MemoryError:
                    self._counts = old
                    return False
                finally:
                    self._pending_workspace = None
        with self._lock:
            successes, total = self._counts.get(key, (0, 0))
            self._counts[key] = (successes + int(passed), total + 1)
        return True

    def probability(self, spec: SubgraphSpec, record: CachedRecord, epoch: str) -> float:
        with self._lock:
            successes, total = self._counts.get(
                (spec.identity, record.record_id, record.revision, epoch), (0, 0)
            )
        return successes / total if total else 0.0

    def snapshot(self) -> "ForecastTable":
        if self.memory_budget is not None:
            budget = self.memory_budget
            with budget.transaction(), self._lock:
                result = None
                try:
                    with budget.reservation(self._heap_reservation_bytes(), "forecast_snapshot") as token:
                        self._pending_workspace = {"old": self._counts}
                        result = ForecastTable()
                        self._pending_workspace["copy"] = result
                        result._counts = self._counts.copy()
                        token.commit()
                        result.bind_memory_budget(budget)
                    return result
                except BaseException:
                    # Failure tracebacks may outlive the call. Do not leave a
                    # discarded, unreturned counter copy hidden in that frame.
                    self._pending_workspace = None
                    if result is not None:
                        result._counts = {}
                        if result._memory_finalizer is not None:
                            result._memory_finalizer()
                        result.memory_budget = None
                    raise
                finally:
                    self._pending_workspace = None
        result = ForecastTable()
        with self._lock:
            result._counts = self._counts.copy()
        return result

    def export_record(self, record_id: str) -> bytes:
        """Serialize this record's counters for its managed metadata reserve."""
        with self._lock:
            entries = [
                {"subgraph_id": key[0], "revision": key[2], "epoch": key[3],
                 "passed": successes, "total": total}
                for key, (successes, total) in sorted(self._counts.items())
                if key[1] == record_id
            ]
        return canonical_json({
            "format_version": 1, "record_id": record_id, "entries": entries,
        }).encode("utf-8")

    def discard_records(self, live_ids: Iterable[str]) -> None:
        """Drop profiles for evicted records after a maintenance round.

        Each immutable record's identity, revision, and epoch stay fixed while
        it is live.  Keeping only live IDs therefore bounds the normal runtime
        profile table to its allocated record set; snapshots remain independent.
        """
        if self.memory_budget is not None:
            budget = self.memory_budget
            with budget.transaction(), self._lock:
                old = self._counts
                try:
                    with budget.reservation(self._heap_reservation_bytes(), "forecast_discard") as token:
                        work = {"old": old}
                        self._pending_workspace = work
                        known = {key[1] for key in old}
                        live = {identity for identity in live_ids if identity in known}
                        work["sets"] = (known, live)
                        prospective = {key: counts for key, counts in old.items() if key[1] in live}
                        work["new"] = prospective
                        self._counts = prospective
                        token.commit()
                    return True
                except MemoryError:
                    self._counts = old
                    return False
                finally:
                    self._pending_workspace = None
        live = set(live_ids)
        with self._lock:
            self._counts = {key: counts for key, counts in self._counts.items() if key[1] in live}

    def fail_closed_clear(self) -> int:
        """Pressure fallback: forget all counters without a COW allocation.

        Snapshots have their own dictionaries and remain unchanged. No
        reservation is needed; errors from the shared ledger are not swallowed.
        The return value counts discarded forecast keys.
        """
        budget = self.memory_budget
        if budget is None:
            with self._lock:
                discarded = len(self._counts)
                self._counts.clear()
                return discarded
        with budget.transaction(), self._lock:
            discarded = len(self._counts)
            self._counts.clear()
            return discarded


@dataclass(frozen=True)
class CostEstimate:
    objective: float
    latency_ms: float
    cost_usd: float
    trace: Mapping[str, Any] = field(default_factory=dict)

    @property
    def estimated_latency_ms(self) -> float:
        return self.latency_ms

    @property
    def estimated_cost_usd(self) -> float:
        return self.cost_usd

    @property
    def predicted_makespan_ms(self) -> float:
        return float(self.trace.get("expected_makespan_ms", self.latency_ms))

    @property
    def ranking_time_ms(self) -> float:
        return float(self.trace.get("ranking_time_ms", self.latency_ms))


@dataclass(frozen=True)
class PlanAction:
    kind: str
    node_id: str
    candidate: Optional["EligibleRecord"] = None


@dataclass(frozen=True)
class PlanDecision:
    action: Optional[PlanAction]
    objective: float
    estimated_latency_ms: float
    estimated_cost_usd: float
    trace: Mapping[str, Any]
    planning_elapsed_ms: float = 0.0

    @property
    def predicted_makespan_ms(self) -> float:
        return float(self.trace.get("expected_makespan_ms", self.estimated_latency_ms))

    @property
    def ranking_time_ms(self) -> float:
        return float(self.trace.get("ranking_time_ms", self.estimated_latency_ms))


@dataclass(frozen=True)
class _Step:
    kind: str
    node_id: str
    nodes: Tuple[str, ...]
    hit_probability: float
    hit_latency_ms: float
    miss_latency_ms: float
    miss_cost_usd: float
    spec: Optional[SubgraphSpec] = None
    record: Optional[CachedRecord] = None
    candidate: Optional["EligibleRecord"] = None

    @property
    def key(self) -> str:
        if self.record is None:
            return self.node_id
        return f"{self.spec.identity}:{self.record.record_id}:{self.record.revision}"


@dataclass(frozen=True)
class _Plan:
    covered: frozenset
    steps: Tuple[_Step, ...] = ()


class LookaheadPlanner:
    def __init__(
        self,
        config: Optional[PlannerConfig] = None,
        forecasts: Optional[ForecastTable] = None,
    ) -> None:
        self.config = config or PlannerConfig()
        self.forecasts = forecasts if forecasts is not None else ForecastTable()
        self.memory_budget = None
        self._memory_workspace = None
        self._memory_returned = None
        if getattr(self.forecasts, "memory_budget", None) is not None:
            self.bind_memory_budget(self.forecasts.memory_budget)

    def bind_memory_budget(self, budget):
        """Charge planning copies/workspace in the same cache heap domain."""
        import weakref
        if self.memory_budget is budget:
            return
        if self.memory_budget is not None:
            raise ValueError("Planner cannot change its cache memory domain")
        self.forecasts.bind_memory_budget(budget)
        self._memory_returned = weakref.WeakValueDictionary()
        try:
            self._memory_handle = budget.register_root("planner:" + str(id(self)), self._memory_roots,
                                                       category="planner_workspace")
            self.memory_budget = budget
        except BaseException:
            self._memory_returned = None
            self.memory_budget = None
            raise

    def _memory_roots(self):
        return (self, tuple(self._memory_returned.values()) if self._memory_returned is not None else ())

    def _memory_call(self, function, workflow, subgraphs, snapshot, *args, **kwargs):
        if self.memory_budget is None and getattr(self.forecasts, "memory_budget", None) is not None:
            self.bind_memory_budget(self.forecasts.memory_budget)
        if self.memory_budget is None:
            return function(*args, **kwargs)
        nodes, records = len(workflow.nodes), len(snapshot.records)
        branch = nodes + records + 1
        if self.config.search_strategy == SearchStrategy.EXHAUSTIVE:
            plans = self.config.exhaustive_max_plans
        else:
            beam = 1 if self.config.mode == PlanningMode.GREEDY else self.config.beam_width
            plans = beam * branch * min(nodes, self.config.depth)
        scenarios = min(self.config.scenario_limit, 2 ** min(nodes, self.config.depth))
        # Reserve before allocating a frontier. This conservative finite
        # workspace is not a measured RSS value; actual retained graph bytes
        # and the reservation are reported independently by the shared ledger.
        scratch = 65536 + records * 256 + plans * (1024 + nodes * 512) * max(1, scenarios)
        try:
            with self.memory_budget.transaction(), self.memory_budget.reservation(scratch, "planning") as token:
                self._memory_workspace = {}
                result = function(*args, **kwargs)
                self._memory_returned[id(result)] = result
                # Drop construction workspaces before converting the reserved
                # capacity to the returned, still-live decision/estimate.
                self._memory_workspace = None
                token.commit()
                return result
        finally:
            self._memory_workspace = None

    def choose(
        self, workflow, subgraphs, completed, state, snapshot, eligible, epoch,
    ):
        return self._memory_call(self._choose_unbudgeted, workflow, subgraphs, snapshot,
                                 workflow, subgraphs, completed, state, snapshot, eligible, epoch)

    def _choose_unbudgeted(
        self,
        workflow: WorkflowSpec,
        subgraphs: Sequence[SubgraphSpec],
        completed: set,
        state: Mapping[str, Any],
        snapshot: CacheSnapshot,
        eligible: Sequence["EligibleRecord"],
        epoch: str,
    ) -> PlanDecision:
        """Choose a normal action or a currently checked substitution.

        ``state`` is intentionally not used to infer future values.  B supplies
        the descriptors checked against that state; runtime checks them again
        immediately before restoring a pinned record.
        """
        started = time.perf_counter()
        del state
        if not set(completed).issubset(workflow.nodes):
            raise ValueError("completed contains unknown workflow nodes")
        if any(not set(workflow.nodes[n].dependencies).issubset(completed) for n in completed):
            raise ValueError("completed must include all dependencies of completed nodes")
        if len(completed) == len(workflow.nodes):
            elapsed = (time.perf_counter() - started) * 1000
            return PlanDecision(None, 0.0, 0.0, 0.0, {
                "snapshot_version": snapshot.version, "steps": [],
                "planning_mode": self.config.mode.value,
                "resource_model": self._resource_model,
                "max_workers": self.config.max_workers,
                "planning_elapsed_ms": elapsed,
            }, elapsed)
        if self.config.mode == PlanningMode.NO_CACHE:
            estimate = self._normal_estimate(workflow, completed, snapshot.version)
            node_id = workflow.ready(completed)[0]
            elapsed = (time.perf_counter() - started) * 1000
            trace = dict(estimate.trace, planning_elapsed_ms=elapsed,
                         steps=[{"kind": "execute", "node_id": node_id,
                                 "covered_nodes": [node_id], "checked": False}],
                         static_baseline_estimate=True)
            return PlanDecision(PlanAction("execute", node_id), estimate.objective,
                                estimate.latency_ms, estimate.cost_usd, trace, elapsed)
        plans, scores = self._search(
            workflow, tuple(subgraphs), frozenset(completed), snapshot, tuple(eligible), epoch,
            require_checked_first=True,
        )
        best, estimate = plans[0], scores[0]
        first = best.steps[0]
        if first.kind == "reuse" and first.candidate is None:
            raise RuntimeError("a forecast must never become a serving action")
        action = PlanAction(first.kind, first.node_id, first.candidate)
        elapsed = (time.perf_counter() - started) * 1000
        trace = dict(estimate.trace, planning_elapsed_ms=elapsed)
        return PlanDecision(
            action, estimate.objective, estimate.latency_ms, estimate.cost_usd, trace, elapsed
        )

    def estimate(
        self, workflow, subgraphs, snapshot, epoch,
    ):
        return self._memory_call(self._estimate_unbudgeted, workflow, subgraphs, snapshot,
                                 workflow, subgraphs, snapshot, epoch)

    def _estimate_unbudgeted(
        self,
        workflow: WorkflowSpec,
        subgraphs: Sequence[SubgraphSpec],
        snapshot: CacheSnapshot,
        epoch: str,
    ) -> CostEstimate:
        """Forecast a complete request without reading its future inputs.

        This is the same bounded planning policy used by ``choose``, with all
        candidate matches forecast rather than live-checked.  The no-cache
        counterfactual is the original dependency/resource-aware execution
        without cache overhead.
        Callers comparing cache sets should freeze ``forecasts`` beforehand.
        """
        if self.config.mode == PlanningMode.NO_CACHE:
            return self._normal_estimate(workflow, set(), snapshot.version)
        serving = [r for r in snapshot.records if r.record_id not in snapshot.audit_ids and r.epoch == epoch]
        if not serving:
            return self._normal_estimate(workflow, set(), snapshot.version)
        _, scores = self._search(
            workflow, tuple(subgraphs), frozenset(), snapshot, (), epoch,
            require_checked_first=False,
        )
        return scores[0]

    def _normal_estimate(self, workflow: WorkflowSpec, completed: set,
                         snapshot_version: int) -> CostEstimate:
        """Static cost of remaining original nodes, with no reuse overhead.

        This diagnostic never reads cache contents, eligibility or forecasts.
        The serving no-cache path bypasses the planner entirely.
        """
        remaining = [n for n in workflow.topological_order if n not in completed]
        scheduled = schedule_tasks([
            ScheduleTask(n, tuple(parent for parent in workflow.nodes[n].dependencies
                                  if parent not in completed), workflow.nodes[n].latency_ms, i)
            for i, n in enumerate(remaining)
        ], self.config.max_workers)
        latency = scheduled.makespan_ms
        cost = math.fsum(workflow.nodes[n].cost_usd for n in remaining)
        return CostEstimate(self._objective(latency, cost), latency, cost, {
            "snapshot_version": snapshot_version,
            "resource_model": self._resource_model,
            "planning_mode": self.config.mode.value,
            "max_workers": self.config.max_workers,
            "expected_makespan_ms": latency,
            "expected_additive_time_ms": scheduled.additive_time_ms,
            "ranking_time_ms": latency,
            "selection_rule": "normal_execution",
            "cache_enabled": False,
            "steps": [],
            "normal_tail_nodes": remaining,
            "no_cache_baseline": True,
        })

    @property
    def _resource_model(self) -> str:
        return "serial" if self.config.max_workers == 1 else "bounded_workers"

    def _objective(self, latency: float, cost: float) -> float:
        return (
            self.config.alpha * cost / self.config.cost_reference
            + self.config.beta * latency / self.config.latency_reference_ms
        )

    def _search(
        self,
        workflow: WorkflowSpec,
        subgraphs: Tuple[SubgraphSpec, ...],
        completed: frozenset,
        snapshot: CacheSnapshot,
        eligible: Tuple["EligibleRecord", ...],
        epoch: str,
        *,
        require_checked_first: bool,
    ) -> Tuple[Tuple[_Plan, ...], Tuple[CostEstimate, ...]]:
        forecasts = self.forecasts.snapshot()
        records, checked, initial_ready, entries_with_records = self._prepare(
            workflow, subgraphs, completed, snapshot, eligible, epoch
        )
        if self._memory_workspace is not None:
            self._memory_workspace.update(forecasts=forecasts, records=records, checked=checked,
                                          ready=initial_ready, entries=entries_with_records)
        if self.config.search_strategy == SearchStrategy.EXHAUSTIVE:
            return self._exhaustive_search(
                workflow, subgraphs, completed, snapshot, records, checked, initial_ready,
                entries_with_records, forecasts, epoch, require_checked_first=require_checked_first,
            )
        if self.config.mode == PlanningMode.GREEDY:
            plan = _Plan(completed)
            first_comparisons = []
            while len(plan.covered) != len(workflow.nodes):
                options = tuple(self._steps(
                    workflow, subgraphs, plan, records, checked, initial_ready, forecasts, epoch,
                    require_checked_first=require_checked_first,
                ))
                step, comparisons = self._greedy_step(workflow, options, entries_with_records)
                if not plan.steps:
                    first_comparisons = comparisons
                plan = _Plan(plan.covered.union(step.nodes), plan.steps + (step,))
                if require_checked_first:
                    break  # Future opportunities cannot influence a current greedy action.
            score = self._score(workflow, plan, entries_with_records, snapshot.version)
            trace = dict(score.trace, selection_rule="positive_immediate_J_savings",
                         immediate_candidates=first_comparisons,
                         configured_depth=self.config.depth, configured_beam_width=self.config.beam_width,
                         search_depth=1, beam_width=1)
            score = CostEstimate(score.objective, score.latency_ms, score.cost_usd, trace)
            return (plan,), (score,)
        plans = (_Plan(completed),)
        statistics = {"expanded_plans": 0, "scored_plans": 0, "pruned_plans": 0,
                      "complete_plans": 0, "retained_plans": 0,
                      "search_depth_reached": 0, "exhaustive_complete": False}
        for depth in range(self.config.depth):
            expanded = []
            for plan in plans:
                if len(plan.covered) == len(workflow.nodes):
                    expanded.append(plan)
                    continue
                for step in self._steps(
                    workflow, subgraphs, plan, records, checked, initial_ready, forecasts, epoch,
                    require_checked_first=require_checked_first,
                ):
                    expanded.append(_Plan(plan.covered.union(step.nodes), plan.steps + (step,)))
                    statistics["expanded_plans"] += 1
            if self._memory_workspace is not None:
                self._memory_workspace.update(plans=plans, expanded=expanded, statistics=statistics)
            if not expanded:
                raise ValueError("workflow has no executable action; check DAG dependencies")
            scored = [(self._score(workflow, p, entries_with_records, snapshot.version), p) for p in expanded]
            if self._memory_workspace is not None:
                self._memory_workspace["scored"] = scored
            statistics["scored_plans"] += len(scored)
            statistics["pruned_plans"] += max(0, len(scored) - self.config.beam_width)
            statistics["search_depth_reached"] = depth + 1
            scored.sort(key=lambda item: (
                item[0].objective, item[0].latency_ms, item[0].cost_usd,
                tuple((s.kind, s.key) for s in item[1].steps),
            ))
            # Pruning is over whole plans, never over individual miss scenarios.
            plans = tuple(p for _, p in scored[: self.config.beam_width])
        scored = [(self._score(workflow, p, entries_with_records, snapshot.version), p) for p in plans]
        statistics["scored_plans"] += len(scored)
        statistics["retained_plans"] = len(plans)
        statistics["complete_plans"] = sum(len(p.covered) == len(workflow.nodes) for p in plans)
        scored.sort(key=lambda item: (item[0].objective, item[0].latency_ms, item[0].cost_usd))
        return tuple(p for _, p in scored), tuple(
            self._with_search_statistics(score, statistics) for score, _ in scored
        )

    def _exhaustive_search(
        self, workflow, subgraphs, completed, snapshot, records, checked,
        initial_ready, entries_with_records, forecasts, epoch, *, require_checked_first,
    ):
        """Enumerate complete action plans with the same information and scorer.

        This is an exact search over this implementation's action model and
        scenario objective, including its forecast-miss simplification. It is
        not an oracle for future values, realized latency, or adaptive policies.
        """
        statistics = {"expanded_plans": 0, "scored_plans": 0, "pruned_plans": 0,
                      "complete_plans": 0, "retained_plans": 0,
                      "search_depth_reached": 0, "exhaustive_complete": False}
        remaining = len(workflow.nodes) - len(completed)
        if remaining > self.config.exhaustive_max_nodes:
            raise ExhaustiveLimitExceeded(
                f"exhaustive search has {remaining} remaining nodes; limit is {self.config.exhaustive_max_nodes}",
                statistics,
            )
        frontier = [_Plan(completed)]
        terminal = []
        while frontier:
            next_frontier = []
            for plan in frontier:
                if len(plan.covered) == len(workflow.nodes):
                    terminal.append(plan)
                    continue
                options = self._steps(
                    workflow, subgraphs, plan, records, checked, initial_ready, forecasts, epoch,
                    require_checked_first=require_checked_first,
                )
                found = False
                for step in options:
                    found = True
                    statistics["expanded_plans"] += 1
                    if statistics["expanded_plans"] > self.config.exhaustive_max_plans:
                        raise ExhaustiveLimitExceeded(
                            f"exhaustive search exceeded {self.config.exhaustive_max_plans} generated plans",
                            statistics,
                        )
                    next_frontier.append(_Plan(plan.covered.union(step.nodes), plan.steps + (step,)))
                    statistics["search_depth_reached"] = max(
                        statistics["search_depth_reached"], len(plan.steps) + 1,
                    )
                if not found:
                    raise ValueError("workflow has no executable action; check DAG dependencies")
            frontier = next_frontier
        scored = [(self._score(workflow, plan, entries_with_records, snapshot.version), plan)
                  for plan in terminal]
        statistics.update(scored_plans=len(scored), complete_plans=len(scored),
                          retained_plans=len(scored), exhaustive_complete=True)
        scored.sort(key=lambda item: (
            item[0].objective, item[0].latency_ms, item[0].cost_usd,
            tuple((s.kind, s.key) for s in item[1].steps),
        ))
        return tuple(plan for _, plan in scored), tuple(
            self._with_search_statistics(score, statistics) for score, _ in scored
        )

    def _with_search_statistics(self, score, statistics):
        trace = dict(score.trace, search_strategy=self.config.search_strategy.value,
                     search_statistics=dict(statistics), configured_depth=self.config.depth,
                     configured_beam_width=self.config.beam_width,
                     search_depth=statistics["search_depth_reached"])
        if self.config.search_strategy == SearchStrategy.EXHAUSTIVE:
            trace.update(beam_width=None, depth_limit_active=False, beam_pruning_enabled=False,
                         exhaustive_reference_scope="complete action plans under the same forecast/scenario model")
        return CostEstimate(score.objective, score.latency_ms, score.cost_usd, trace)

    def _prepare(self, workflow, subgraphs, completed, snapshot, eligible, epoch):
        """Apply one common record/descriptor validator to every selection mode."""
        identities = {s.identity: s for s in subgraphs}
        if any(not set(s.node_ids).issubset(workflow.nodes) for s in subgraphs):
            raise ValueError("subgraph contains nodes outside this workflow")
        if any(workflow.subgraph(s.name, s.node_ids, s.critical_fields, similarity_fields=s.similarity_fields).identity != s.identity for s in subgraphs):
            raise ValueError("subgraph identity does not belong to this declared workflow")
        records: Dict[str, list] = {}
        for record in snapshot.records:
            if record.record_id not in snapshot.audit_ids and record.epoch == epoch and record.subgraph_id in identities:
                records.setdefault(record.subgraph_id, []).append(record)
        entries_with_records = {s.entry_node_id for s in subgraphs if records.get(s.identity)}
        # Descriptors must refer to the exact immutable record in this view.
        resident = {(r.record_id, r.revision): r for rs in records.values() for r in rs}
        checked = {}
        for candidate in eligible:
            key = (candidate.record.record_id, candidate.record.revision)
            if candidate.epoch == epoch and key in resident and candidate.spec.identity in identities:
                if (resident[key] == candidate.record
                        and candidate.spec == identities[candidate.spec.identity]
                        and candidate.record.subgraph_id == candidate.spec.identity):
                    checked[(candidate.spec.identity, *key)] = candidate
        initial_ready = set(workflow.ready(set(completed)))
        return records, checked, initial_ready, entries_with_records

    def _greedy_step(self, workflow, options, entries_with_records):
        normal = [step for step in options if step.kind == "execute"]
        if not normal:
            raise ValueError("workflow has no ready normal operation")
        comparisons = []
        positive = []
        for step in options:
            if step.kind != "reuse":
                continue
            region = set(step.nodes)
            native = schedule_tasks([
                ScheduleTask(n, tuple(p for p in workflow.nodes[n].dependencies if p in region),
                             workflow.nodes[n].latency_ms + self.config.planning_ms
                             + (self.config.lookup_ms if n in entries_with_records else 0), i)
                for i, n in enumerate(step.nodes)
            ], self.config.max_workers)
            # Only this current block is compared. No future plan/tail, later
            # forecast, or configured beam depth enters the greedy choice.
            miss = schedule_tasks([
                ScheduleTask(n, tuple(p for p in workflow.nodes[n].dependencies if p in region),
                             workflow.nodes[n].latency_ms + (
                                 self.config.planning_ms + self.config.lookup_ms + self.config.validation_ms
                                 if n == step.node_id else 0), i)
                for i, n in enumerate(step.nodes)
            ], self.config.max_workers)
            p = step.hit_probability
            reused_time = p * step.hit_latency_ms + (1 - p) * miss.makespan_ms
            reused_cost = (1 - p) * step.miss_cost_usd
            gain = (self._objective(native.makespan_ms, step.miss_cost_usd)
                    - self._objective(reused_time, reused_cost))
            comparisons.append({"node_id": step.node_id, "record_id": step.record.record_id,
                                "immediate_J_gain": gain, "normal_region_ms": native.makespan_ms,
                                "reuse_region_expected_ms": reused_time,
                                "checked": step.candidate is not None})
            if gain > 0:
                positive.append((gain, step))
        if positive:
            positive.sort(key=lambda item: (-item[0], item[1].key))
            return positive[0][1], comparisons
        return normal[0], comparisons

    def _steps(
        self,
        workflow: WorkflowSpec,
        subgraphs: Tuple[SubgraphSpec, ...],
        plan: _Plan,
        records: Mapping[str, Sequence[CachedRecord]],
        checked: Mapping[Tuple[str, str, int], "EligibleRecord"],
        initial_ready: set,
        forecasts: ForecastTable,
        epoch: str,
        *,
        require_checked_first: bool,
    ) -> Iterable[_Step]:
        ready = set(workflow.ready(set(plan.covered)))
        entries_with_records = {s.entry_node_id for s in subgraphs if records.get(s.identity)}
        for node_id in workflow.topological_order:
            if node_id not in ready:
                continue
            node = workflow.nodes[node_id]
            overhead = self.config.planning_ms
            if node_id in entries_with_records:
                overhead += self.config.lookup_ms
            latency = node.latency_ms + overhead
            yield _Step("execute", node_id, (node_id,), 0.0, latency, latency, node.cost_usd)
        for spec in subgraphs:
            if spec.entry_node_id not in ready or plan.covered.intersection(spec.node_ids):
                continue
            for record in records.get(spec.identity, ()):
                candidate = checked.get((spec.identity, record.record_id, record.revision))
                # Only descriptors checked at an actually ready entry are live.
                if spec.entry_node_id not in initial_ready:
                    candidate = None
                if not plan.steps and require_checked_first and candidate is None:
                    continue
                probability = 1.0 if candidate is not None else forecasts.probability(spec, record, epoch)
                if probability <= 0:
                    continue
                attempt_overhead = self.config.planning_ms + self.config.lookup_ms + self.config.validation_ms
                hit_latency = attempt_overhead + self.config.restore_ms
                miss_latency = attempt_overhead + sum(workflow.nodes[n].latency_ms for n in spec.node_ids)
                miss_cost = sum(workflow.nodes[n].cost_usd for n in spec.node_ids)
                yield _Step(
                    "reuse", spec.entry_node_id, tuple(spec.node_ids), probability,
                    hit_latency, miss_latency, miss_cost, spec, record, candidate,
                )

    def _score(
        self,
        workflow: WorkflowSpec,
        plan: _Plan,
        entries_with_records: set,
        version: int,
    ) -> CostEstimate:
        uncertain = [i for i, step in enumerate(plan.steps) if 0 < step.hit_probability < 1]
        scenario_count = 2 ** len(uncertain)
        totals = []
        if scenario_count <= self.config.scenario_limit:
            for outcomes in itertools.product((False, True), repeat=len(uncertain)):
                probability = 1.0
                selected = dict(zip(uncertain, outcomes))
                for i, hit in selected.items():
                    p = plan.steps[i].hit_probability
                    probability *= p if hit else 1 - p
                totals.append((probability, selected))
        else:
            # Stable event-specific pseudo-random numbers are shared across
            # cache-set comparisons.  No realized future output is inspected.
            for sample in range(self.config.scenario_limit):
                selected = {}
                for i in uncertain:
                    digest = hashlib.sha256(f"{sample}:{plan.steps[i].key}".encode()).digest()
                    uniform = int.from_bytes(digest[:8], "big") / 2 ** 64
                    selected[i] = uniform < plan.steps[i].hit_probability
                totals.append((1.0 / self.config.scenario_limit, selected))
        expected_makespan = 0.0
        expected_additive = 0.0
        expected_cost = 0.0
        scenario_details = []
        for weight, selected in totals:
            schedule, cost, hit_records = self._scenario_schedule(workflow, plan, entries_with_records, selected)
            # Compute each complete resource-constrained schedule first. Taking
            # a critical path after averaging uncertain node times is different.
            expected_makespan += weight * schedule.makespan_ms
            expected_additive += weight * schedule.additive_time_ms
            expected_cost += weight * cost
            scenario_details.append({"weight": weight, "makespan_ms": schedule.makespan_ms,
                                     "additive_time_ms": schedule.additive_time_ms,
                                     "cost_usd": cost, "hit_records": hit_records})
        ranking_time = expected_makespan
        trace = {
            "snapshot_version": version,
            "resource_model": self._resource_model,
            "planning_mode": self.config.mode.value,
            "max_workers": self.config.max_workers,
            "time_model": "scenario_makespan",
            "expected_makespan_ms": expected_makespan,
            "expected_additive_time_ms": expected_additive,
            "ranking_time_ms": ranking_time,
            "scheduling_priority": "beam order breaks ready-task ties; it creates no dependency edges",
            "forecast_assumption": "independent record outcomes",
            "forecast_miss_model": "complete the selected block normally",
            "scenario_count": len(totals),
            "scenario_method": "exact" if scenario_count <= self.config.scenario_limit else "bounded_sample",
            "search_depth": self.config.depth,
            "beam_width": self.config.beam_width,
            "scenarios": scenario_details,
            "steps": [{
                "kind": s.kind,
                "node_id": s.node_id,
                "covered_nodes": list(s.nodes),
                "subgraph_id": s.spec.identity if s.spec else None,
                "record_id": s.record.record_id if s.record else None,
                "revision": s.record.revision if s.record else None,
                "hit_probability": s.hit_probability,
                "checked": s.candidate is not None,
            } for s in plan.steps],
            "normal_tail_nodes": [n for n in workflow.topological_order if n not in plan.covered],
        }
        return CostEstimate(
            self._objective(ranking_time, expected_cost), ranking_time, expected_cost, trace
        )

    def _scenario_schedule(self, workflow, plan, entries_with_records, selected):
        """Materialize one hit/miss DAG and schedule it under the worker limit.

        A successful SESE substitution becomes one restore task. A forecast
        miss preserves all of the region's native dependencies and adds one
        attempt overhead at its entry. Ordinary work beyond the search horizon
        stays in the DAG. The plan order is a ready-queue priority, not a chain.
        """
        planned_nodes = {n for step in plan.steps for n in step.nodes}
        completed = set(plan.covered) - planned_nodes
        position = {n: i for i, n in enumerate(workflow.topological_order)}
        stride = len(workflow.nodes) + 1
        representatives = {n: n for n in workflow.nodes}
        durations = {
            n: workflow.nodes[n].latency_ms + self.config.planning_ms
               + (self.config.lookup_ms if n in entries_with_records else 0)
            for n in workflow.topological_order if n not in completed
        }
        priorities = {n: len(plan.steps) * stride + position[n] for n in durations}
        hits = []
        hit_records = []
        used_ids = set(workflow.nodes)
        for i, step in enumerate(plan.steps):
            if step.kind == "execute":
                durations[step.node_id] = step.miss_latency_ms
                priorities[step.node_id] = i * stride + position[step.node_id]
                continue
            hit = selected.get(i, step.hit_probability >= 1)
            if hit:
                task_id = f"__geoleap_reuse_{i}__"
                while task_id in used_ids:
                    task_id += "_"
                used_ids.add(task_id)
                for node_id in step.nodes:
                    representatives[node_id] = task_id
                    durations.pop(node_id)
                    priorities.pop(node_id)
                hits.append((task_id, i, step))
                hit_records.append(step.record.record_id)
            else:
                for node_id in step.nodes:
                    durations[node_id] = workflow.nodes[node_id].latency_ms
                    if node_id == step.node_id:
                        durations[node_id] += self.config.planning_ms + self.config.lookup_ms + self.config.validation_ms
                    priorities[node_id] = i * stride + position[node_id]
        tasks = []
        for node_id, duration in durations.items():
            dependencies = tuple(sorted({representatives[parent]
                                         for parent in workflow.nodes[node_id].dependencies
                                         if parent not in completed}))
            tasks.append(ScheduleTask(node_id, dependencies, duration, priorities[node_id]))
        for task_id, i, step in hits:
            region = set(step.nodes)
            dependencies = tuple(sorted({representatives[parent]
                                         for n in step.nodes for parent in workflow.nodes[n].dependencies
                                         if parent not in completed and parent not in region}))
            tasks.append(ScheduleTask(task_id, dependencies, step.hit_latency_ms, i * stride))
        schedule = schedule_tasks(tasks, self.config.max_workers)
        cost = math.fsum(workflow.nodes[node_id].cost_usd for node_id in durations)
        return schedule, cost, hit_records
