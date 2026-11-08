"""Run local C-module checks: lookahead, forecasts, and checked first actions."""

from __future__ import annotations

import math

from reporting import CheckReport, run_report
from fixtures import six_node_workflow
from workflow.planner import ForecastTable, LookaheadPlanner, PlannerConfig
from workflow.records import CacheSnapshot, CachedRecord
from workflow.reliability import Matcher, ReliabilityTable


def checks(report: CheckReport) -> None:
    workflow, (sa, sb, sc), state, counters = six_node_workflow()
    epoch = "check-c-epoch"
    records = tuple(
        CachedRecord.create(spec, inputs, outputs, epoch, record_id=spec.name, metadata_capacity=0)
        for spec, inputs, outputs in (
            (sa, {"v0": 0}, {"v4": 4}),
            (sb, {"v0": 0}, {"v2": 2}),
            (sc, {"v2": 2}, {"v6": 6}),
        )
    )
    snapshot = CacheSnapshot(1, records)
    matcher = Matcher(ReliabilityTable())
    eligible = matcher.eligible(sa, state, snapshot, epoch) + matcher.eligible(sb, state, snapshot, epoch)
    forecasts = ForecastTable()
    forecasts.observe(sc.identity, records[2].record_id, records[2].revision, epoch, True)
    planner = LookaheadPlanner(PlannerConfig(depth=3, beam_width=8), forecasts)
    decision = planner.choose(workflow, (sa, sb, sc), set(), state, snapshot, eligible, epoch)
    report.check(
        "前瞻选择 Sb，为后续 Sc 保留入口",
        decision.action is not None and decision.action.kind == "reuse"
        and decision.action.candidate.record.record_id == "Sb",
        {"selected": decision.action.candidate.spec.name if decision.action.candidate else decision.action.node_id,
         "estimated_latency_ms": decision.estimated_latency_ms,
         "estimated_cost_usd": decision.estimated_cost_usd,
         "objective": decision.objective, "plan": decision.trace["steps"]},
    )
    forecast_steps = [step for step in decision.trace["steps"] if step["record_id"] == "Sc"]
    report.check(
        "未来 Sc 仅为预测，不需要读取尚未产生的 v2",
        len(forecast_steps) == 1 and not forecast_steps[0]["checked"]
        and "v2" not in state and sum(counters.values()) == 0,
        {"future_step": forecast_steps, "executed_callable_count": sum(counters.values()),
         "initial_state_fields": sorted(state)},
    )
    greedy_horizon = LookaheadPlanner(PlannerConfig(depth=1), forecasts).choose(
        workflow, (sa, sb, sc), set(), state, snapshot, eligible, epoch
    )
    report.check(
        "单步视野选择 Sa，较深视野识别更好的兼容组合",
        greedy_horizon.action.candidate.record.record_id == "Sa"
        and decision.estimated_latency_ms < greedy_horizon.estimated_latency_ms,
        {"depth_1_selected": greedy_horizon.action.candidate.spec.name,
         "depth_1_estimated_latency_ms": greedy_horizon.estimated_latency_ms,
         "depth_3_estimated_latency_ms": decision.estimated_latency_ms},
    )
    no_profile = LookaheadPlanner(PlannerConfig(), ForecastTable()).choose(
        workflow, (sa, sb, sc), set(), state, snapshot, eligible, epoch
    )
    report.check(
        "没有历史支持时，不假定未来 Sc 命中",
        no_profile.action.kind == "reuse" and no_profile.action.candidate.record.record_id == "Sa",
        {"selected": no_profile.action.candidate.spec.name,
         "unobserved_probability": ForecastTable().probability(sc, records[2], epoch)},
    )
    at_sc = planner.choose(
        workflow, (sa, sb, sc), {"n1", "n2"}, {"v2": 2}, snapshot, (), epoch
    )
    report.check(
        "到达实际入口后，仅有预测而无 B 合格描述符时执行正常节点",
        at_sc.action.kind == "execute" and at_sc.action.node_id == "n3",
        {"action": at_sc.action.kind, "node_id": at_sc.action.node_id},
    )
    stale = planner.choose(
        workflow, (sa, sb, sc), set(), state, CacheSnapshot(2, (records[2],)), eligible, epoch
    )
    report.check(
        "当前快照中缺失的记录不能成为提交动作",
        stale.action.kind == "execute" and stale.action.node_id == "n1",
        {"snapshot_version": stale.trace["snapshot_version"], "action": stale.action.kind},
    )
    overlap = planner.choose(workflow, (sa, sb, sc), {"n1"}, {"v1": 1}, snapshot, eligible, epoch)
    report.check(
        "复用候选不能覆盖已经完成的节点",
        overlap.action.kind == "execute" and overlap.action.node_id == "n2",
        {"completed": ["n1"], "action": overlap.action.kind, "node_id": overlap.action.node_id},
    )
    audit_snapshot = CacheSnapshot(3, records, frozenset({"Sc"}))
    audit_decision = planner.choose(workflow, (sa, sb, sc), set(), state, audit_snapshot, eligible, epoch)
    report.check(
        "审计保留区记录不参与 serving 组合预测",
        audit_decision.action.candidate.record.record_id == "Sa"
        and all(step["record_id"] != "Sc" for step in audit_decision.trace["steps"]),
        {"selected": audit_decision.action.candidate.spec.name, "audit_ids": ["Sc"]},
    )
    full = workflow.subgraph("whole", workflow.topological_order)
    full_record = CachedRecord.create(
        full, {"v0": 0}, {"v6": 6}, epoch, record_id="whole", metadata_capacity=0
    )
    half = ForecastTable()
    half.observe(full.identity, full_record.record_id, 1, epoch, True)
    half.observe(full.identity, full_record.record_id, 1, epoch, False)
    half_planner = LookaheadPlanner(PlannerConfig(), half)
    estimate = half_planner.estimate(workflow, (full,), CacheSnapshot(4, (full_record,)), epoch)
    # One attempt: 0.3 ms overhead; hit restores in 0.1 ms, miss runs 60 ms.
    expected_latency = 0.5 * 0.4 + 0.5 * 60.3
    report.check(
        "命中与失配情景全部计入同一完整计划的成本",
        estimate.trace["scenario_count"] == 2
        and math.isclose(estimate.latency_ms, expected_latency, abs_tol=1e-9)
        and math.isclose(estimate.cost_usd, 0.03, abs_tol=1e-9),
        {"scenario_count": estimate.trace["scenario_count"],
         "forecast_probability": 0.5, "estimated_latency_ms": estimate.latency_ms,
         "expected_latency_ms": expected_latency, "estimated_cost_usd": estimate.cost_usd,
         "resource_model": estimate.trace["resource_model"]},
    )
    empty = half_planner.estimate(workflow, (full,), CacheSnapshot(5), epoch)
    report.check(
        "提供 D 可复用的缓存集合估计接口与无缓存基准",
        math.isclose(empty.latency_ms, 60.0) and math.isclose(empty.cost_usd, 0.06)
        and estimate.objective < empty.objective,
        {"no_cache_latency_ms": empty.latency_ms, "no_cache_cost_usd": empty.cost_usd,
         "no_cache_objective": empty.objective, "cache_objective": estimate.objective},
    )
    frozen = half.snapshot()
    half.observe(full.identity, full_record.record_id, 1, epoch, False)
    report.check(
        "画像快照在一次集合比较期间保持不变",
        frozen.probability(full, full_record, epoch) == 0.5
        and math.isclose(half.probability(full, full_record, epoch), 1 / 3),
        {"frozen_probability": frozen.probability(full, full_record, epoch),
         "updated_probability": half.probability(full, full_record, epoch)},
    )


if __name__ == "__main__":
    raise SystemExit(run_report("c_lookahead_planning", "C：前瞻复用规划", checks))
