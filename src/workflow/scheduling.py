"""Deterministic dependency-aware schedule estimates on one shared worker pool.

This is a pure cost-model simulator, not an executor of operator callables.
Priorities break ties between ready tasks; they never add dependency edges.
"""

from __future__ import annotations

from dataclasses import dataclass
import heapq
import math
from typing import Mapping, Sequence, Tuple


@dataclass(frozen=True)
class ScheduleTask:
    task_id: str
    dependencies: Tuple[str, ...]
    duration_ms: float
    priority: int = 0

    def __post_init__(self):
        object.__setattr__(self, "dependencies", tuple(self.dependencies))
        if not self.task_id or len(set(self.dependencies)) != len(self.dependencies):
            raise ValueError("Schedule tasks need a unique ID and distinct dependencies")
        if not math.isfinite(self.duration_ms) or self.duration_ms < 0:
            raise ValueError("Task duration must be finite and nonnegative")
        if isinstance(self.priority, bool) or not isinstance(self.priority, int):
            raise ValueError("Task priority must be an integer")


@dataclass(frozen=True)
class ScheduleResult:
    makespan_ms: float
    additive_time_ms: float
    start_times: Mapping[str, float]
    end_times: Mapping[str, float]
    trace: tuple


def schedule_tasks(tasks: Sequence[ScheduleTask], max_workers: int = 1) -> ScheduleResult:
    if isinstance(max_workers, bool) or not isinstance(max_workers, int) or max_workers < 1:
        raise ValueError("max_workers must be a positive integer")
    tasks = tuple(tasks)
    by_id = {task.task_id: task for task in tasks}
    if len(by_id) != len(tasks):
        raise ValueError("Duplicate task IDs")
    children = {task_id: [] for task_id in by_id}
    remaining = {}
    for task in tasks:
        if any(parent not in by_id for parent in task.dependencies):
            raise ValueError("Unknown schedule dependency")
        remaining[task.task_id] = len(task.dependencies)
        for parent in task.dependencies:
            children[parent].append(task.task_id)
    ready = [(task.priority, task.task_id) for task in tasks if not task.dependencies]
    heapq.heapify(ready)
    running, start_times, end_times, trace = [], {}, {}, []
    now = 0.0
    while len(end_times) < len(tasks):
        while ready and len(running) < max_workers:
            priority, key = heapq.heappop(ready)
            task = by_id[key]
            start_times[key] = now
            finish = now + task.duration_ms
            heapq.heappush(running, (finish, priority, key))
            trace.append({"task_id": key, "start_ms": now, "end_ms": finish,
                          "dependencies": list(task.dependencies), "priority": priority})
        if not running:
            raise ValueError("Schedule contains a dependency cycle")
        now = running[0][0]
        finished = []
        while running and running[0][0] == now:
            finish, _, key = heapq.heappop(running)
            end_times[key] = finish
            finished.append(key)
        for key in finished:
            for child in children[key]:
                remaining[child] -= 1
                if remaining[child] == 0:
                    heapq.heappush(ready, (by_id[child].priority, child))
    return ScheduleResult(max(end_times.values(), default=0.0),
                          math.fsum(task.duration_ms for task in tasks), start_times, end_times, tuple(trace))
