"""Local functional checks of the declared runtime with B/C/D connected."""

from dataclasses import asdict, replace
import json

from reporting import CheckReport, run_report
from fixtures import six_node_workflow
from workflow.records import CachedRecord, RecordStore
from workflow.runtime import GeoLeapRuntime, RuntimeConfig
from workflow.reliability import ReliabilityConfig
from workflow.residency import ResidencyConfig
from workflow.spec import OperatorSpec, WorkflowSpec


def _text_runtime(metadata_capacity=32768):
    calls = []

    def respond(inputs):
        calls.append(inputs["query"])
        return {"answer": "workflow cache"}

    workflow = WorkflowSpec("text_functional_check", [OperatorSpec(
        "respond", respond, read_fields=("query",), write_fields=("answer",),
        cacheable=True, stable=False, effect_free=True, latency_ms=10,
        input_schema=(("query", "string"),), output_schema=(("answer", "string"),),
    )])
    spec = workflow.subgraph("text_answer", ("respond",))
    runtime = GeoLeapRuntime(workflow, (spec,), config=RuntimeConfig(
        epoch="text-check-v1", allow_exact=False, metadata_capacity=metadata_capacity),
        reliability_config=ReliabilityConfig(min_groups=3),
        residency_config=ResidencyConfig(max_bytes=512000, audit_fraction=0.5))
    return runtime, spec, calls


def checks(report: CheckReport):
    workflow, _, state, counters = six_node_workflow()
    whole = workflow.subgraph("whole_chain", workflow.topological_order)
    runtime = GeoLeapRuntime(workflow, (whole,), config=RuntimeConfig(epoch="integration-v1"))
    cold = runtime.run(state, request_id="cold")
    report.check("冷启动真实执行完整工作流并保留无关状态", cold.state["v6"] == 6
                 and cold.state["untouched"] == "keep" and len(cold.executed_node_ids) == 6
                 and all(value == 1 for value in counters.values()),
                 {"executed_nodes": cold.executed_node_ids, "reused_subgraphs": cold.reused_subgraphs,
                  "final_output": cold.state["v6"], "call_counts": counters.copy()})
    report.check("正常执行仅排队新增记录，服务路径不隐式运行 D", runtime.store.byte_size == 0
                 and runtime.manager.queued_count == 1,
                 {"queued_records": runtime.manager.queued_count, "resident_bytes": runtime.store.byte_size})
    admissions = runtime.maintenance_tick()
    all_records = runtime.store.snapshot(include_audit=True)
    report.check("后台维护将冷记录准入 audit-only，B 不允许直接服务", len(all_records.records) == 1
                 and len(all_records.audit_ids) == 1 and not runtime.store.snapshot().records,
                 {"admissions": [asdict(item) for item in admissions],
                  "audit_records": len(all_records.audit_ids), "serving_records": 0})
    for index in range(3):
        runtime.audit(state, request_id=f"calibration-{index}")
    record = runtime.store.snapshot(include_audit=True).records[0]
    evidence = runtime.reliability.lookup(whole, record, runtime.config.epoch, 1.0)
    report.check("无复用参考执行产生独立请求组反馈", evidence is not None and evidence.groups == 3
                 and evidence.score == 0 and len(runtime.store.snapshot().records) == 0,
                 {"groups": evidence.groups if evidence else None, "score": evidence.score if evidence else None,
                  "serving_records_before_maintenance": len(runtime.store.snapshot().records)})
    promoted = runtime.maintenance_tick()
    report.check("D 使用冻结的 C 估值，支持记录经正收益检查后晋升", len(runtime.store.snapshot().records) == 1
                 and any(item.accepted and item.reason == "serving_admitted" for item in promoted),
                 {"maintenance": [asdict(item) for item in promoted]})
    before_warm = counters.copy()
    warm = runtime.run(state, request_id="warm")
    report.check("真实复用整段跳过六个 callable，并恢复声明出口", warm.state["v6"] == 6
                 and warm.state["untouched"] == "keep" and counters == before_warm
                 and warm.executed_node_ids == () and warm.reused_subgraphs == ("whole_chain",),
                 {"executed_nodes": warm.executed_node_ids, "reused_subgraphs": warm.reused_subgraphs,
                  "call_counts_before": before_warm, "call_counts_after": counters.copy(),
                  "output_state": warm.state})
    report.check("缓存替换过的子图不会产生新的参考记录", warm.new_record_ids == ()
                 and runtime.manager.queued_count == 0,
                 {"new_record_ids": warm.new_record_ids, "queued_records": runtime.manager.queued_count})
    audited_hit = runtime.run(state, request_id="hit-audit", audit_hits=True)
    feedback = [item for item in audited_hit.trace if item["event"] == "output_feedback"]
    report.check("实际命中审计绑定被选记录与原始入口", len(feedback) == 1
                 and feedback[0]["kind"] == "actual_hit" and feedback[0]["record_id"] == record.record_id
                 and feedback[0]["disagreement"] == 0 and audited_hit.audit_cost_usd > 0,
                 {"feedback": feedback, "audit_profile_cost_usd": audited_hit.audit_cost_usd})
    report.check("B/C 元数据使用已分配预留区，维护后预算与 pin 一致",
                 runtime.store.byte_size == runtime.store.snapshot(include_audit=True).byte_size
                 and runtime.store.byte_size <= runtime.manager.config.max_bytes and not runtime.store.pinned_ids(),
                 {"managed_bytes": runtime.store.byte_size, "budget_bytes": runtime.manager.config.max_bytes,
                  "remaining_pins": sorted(runtime.store.pinned_ids())})

    text_runtime, spec, calls = _text_runtime()
    original = "a long query about semantic cache matching a detailed workflow and function"
    live = original + " new "
    text_runtime.run({"query": original}, request_id="text-cold")
    text_runtime.maintenance_tick()
    for index in range(3):
        text_runtime.audit({"query": live}, request_id=f"text-audit-{index}")
    text_runtime.maintenance_tick()
    call_count = len(calls)
    approximate = text_runtime.run({"query": live}, request_id="text-warm")
    hit = next((item for item in approximate.trace if item["event"] == "reuse"), {})
    report.check("不同输入在对应分箱获得支持后可晋升并近似复用",
                 approximate.reused_subgraphs == (spec.name,) and len(calls) == call_count
                 and hit.get("exact") is False and approximate.state["answer"] == "workflow cache",
                 {"original_input": original, "live_input": live, "hit": hit,
                  "call_count_before": call_count, "call_count_after": len(calls)})

    small_runtime, _, _ = _text_runtime(metadata_capacity=0)
    small_runtime.run({"query": original}, request_id="small-cold")
    small_runtime.maintenance_tick()
    small_audit = small_runtime.audit({"query": live}, request_id="small-audit")
    small_runtime.maintenance_tick()
    report.check("元数据配额不足时拒绝反馈并继续正常执行",
                 not small_runtime.store.snapshot().records
                 and any(item.get("stored") is False for item in small_audit.trace if item["event"] == "output_feedback"),
                 {"serving_records": len(small_runtime.store.snapshot().records),
                  "metadata_capacity": 0})

    # Sc belongs to a past input producing v2=0; this request produces v2=20.
    # Its historical forecast must be rechecked against the actual boundary.
    chain, (sa, sb, sc), initial, calls_count = six_node_workflow()
    def multiply(inputs):
        calls_count["n2"] += 1
        return {"v2": inputs["v1"] * 20}

    chain = WorkflowSpec("recheck_chain", tuple(
        replace(operator, function=multiply, semantic_version="multiply-20") if operator.node_id == "n2" else operator
        for operator in chain.operators
    ))
    sa = chain.subgraph("Sa", sa.node_ids)
    sb = chain.subgraph("Sb", sb.node_ids)
    sc = chain.subgraph("Sc", sc.node_ids, critical_fields=("v2",))
    stored = tuple(CachedRecord.create(spec, inputs, outputs, "recheck-v1", record_id=spec.name)
                   for spec, inputs, outputs in ((sa, {"v0": 0}, {"v4": 22}),
                                                (sb, {"v0": 0}, {"v2": 20}),
                                                (sc, {"v2": 0}, {"v6": 4})))
    store = RecordStore()
    assert store.publish(stored, frozenset(), 0, 1_000_000)
    checked_runtime = GeoLeapRuntime(chain, (sa, sb, sc), config=RuntimeConfig(epoch="recheck-v1"), store=store)
    checked_runtime.forecasts.observe(sc.identity, stored[2].record_id, 1, "recheck-v1", True)
    changed = checked_runtime.run(initial, request_id="later-input-recheck", collect_records=False)
    report.check("未来入口真实关键字段不匹配时回退正常执行", changed.reused_subgraphs == ("Sb",)
                 and changed.executed_node_ids == ("n3", "n4", "n5", "n6") and changed.state["v6"] == 24,
                 {"reused_subgraphs": changed.reused_subgraphs, "executed_nodes": changed.executed_node_ids,
                  "actual_v2": changed.state["v2"], "final_output": changed.state["v6"]})

    # A record can disappear between planning and commit. The runtime must
    # execute normal operations after pin acquisition fails.
    baseline_choose = checked_runtime.planner.choose
    invalidated = [False]

    def choose_then_evict(*args, **kwargs):
        decision = baseline_choose(*args, **kwargs)
        if not invalidated[0] and decision.action.kind == "reuse":
            invalidated[0] = True
            view = store.snapshot(include_audit=True)
            assert store.publish((), frozenset(), view.version, 1_000_000)
        return decision

    checked_runtime.planner.choose = choose_then_evict
    fallback = checked_runtime.run(initial, request_id="commit-miss", collect_records=False)
    report.check("提交前记录消失时安全回退，结果仍与正常执行一致",
                 fallback.state["v6"] == 24 and len(fallback.executed_node_ids) == 6
                 and not fallback.reused_subgraphs
                 and any(item["event"] == "reuse_rejected_at_commit" for item in fallback.trace),
                 {"executed_nodes": fallback.executed_node_ids, "output": fallback.state["v6"]})

    trace_path = report.output_dir / "execution_trace.json"
    trace_path.parent.mkdir(parents=True, exist_ok=True)
    trace_path.write_text(json.dumps({
        "cold": asdict(cold), "warm": asdict(warm), "actual_hit_audit": asdict(audited_hit),
        "approximate": asdict(approximate), "later_entry_recheck": asdict(changed),
        "commit_fallback": asdict(fallback),
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    report.check("保存可检查的完整动作与反馈轨迹", trace_path.is_file(), {"path": str(trace_path.resolve())})


if __name__ == "__main__":
    raise SystemExit(run_report("core_integration", "B/C/D：执行与生命周期联通检查", checks))
