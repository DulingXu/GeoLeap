"""Offline checks of declared fields for the 128d character-gram encoder."""
from dataclasses import replace
import hashlib
import json

from reporting import CheckReport, run_report
from workflow.d2 import D2Runner
from workflow.planner import LookaheadPlanner, PlannerConfig
from workflow.records import CacheSnapshot, CachedRecord, RecordStore
from workflow.reliability import Matcher, ReliabilityConfig, ReliabilityTable
from workflow.residency import ResidencyConfig
from workflow.runtime import GeoLeapRuntime, RuntimeConfig
from workflow.spec import OperatorSpec, WorkflowSpec, canonical_json, encode_input


# Generated once using the frozen pre-projection spec.py snapshot, not the
# current implementation. The check itself does not depend on old paid runs.
LEGACY_ID = "860dd87ec1ef63e745b020ca9c6ab94bd71a5bddc9e6e21075dda11d50e97a5c"
LEGACY_RECORD_SHA = "4c4a5d7b766af4b872ef87a446c3a3f3b0e597183e66834ef11488dd0e0d1dc1"
EPOCH = "projection-epoch"


def fixture():
    counts = {"analyze": 0, "answer": 0}
    def analyze(inputs):
        counts["analyze"] += 1
        return {"analysis": "shared facts"}
    def answer(inputs):
        counts["answer"] += 1
        return {"answer": inputs["question"] + " | " + inputs["analysis"]}
    workflow = WorkflowSpec("projection_check", [
        OperatorSpec("analyze", analyze, read_fields=("question", "schema", "guard"),
            write_fields=("analysis",), cacheable=True, effect_free=True, stable=False,
            latency_ms=100, cost_usd=.1,
            input_schema=(("question", "string"), ("schema", "string"), ("guard", "string")),
            output_schema=(("analysis", "string"),)),
        OperatorSpec("answer", answer, dependencies=("analyze",),
            read_fields=("question", "analysis"), write_fields=("answer",),
            input_schema=(("question", "string"), ("analysis", "string")),
            output_schema=(("answer", "string"),)),
    ])
    legacy = workflow.subgraph("analysis", ("analyze",), critical_fields=("schema", "guard"))
    projected = workflow.subgraph("analysis", ("analyze",), critical_fields=("schema", "guard"),
                                 similarity_fields=("question",))
    return workflow, legacy, projected, counts


def raises(kind, function):
    try:
        function()
    except kind:
        return True
    return False


def record(spec, inputs, name="record", revision=1):
    return replace(CachedRecord.create(spec, inputs, {"analysis": "shared facts"}, EPOCH,
                                      record_id=name, revision=revision, metadata_capacity=4096),
                   created_at=1000.0)


def checks(report: CheckReport):
    workflow, legacy, projected, counts = fixture()
    tiny = {"question": "Who founded Acme robotics?", "schema": "tiny fixed schema", "guard": "repo-rev-1"}
    legacy_record = record(legacy, tiny, "legacy-record")
    explicit_empty = workflow.subgraph("analysis", ("analyze",), critical_fields=("schema", "guard"), similarity_fields=())
    report.check("empty_projection_preserves_prechange_subgraph_identity",
                 legacy.identity == explicit_empty.identity == LEGACY_ID)
    report.check("legacy_embedding_still_uses_full_input_and_has_128_dimensions",
                 legacy_record.embedding_fields == () and len(legacy_record.embedding) == 128
                 and legacy_record.embedding == encode_input(tiny))
    report.check("legacy_serialization_omits_new_key_and_preserves_golden_digest",
                 "embedding_fields" not in json.loads(legacy_record.serialized())
                 and hashlib.sha256(legacy_record.serialized()).hexdigest() == LEGACY_RECORD_SHA)
    report.check("declared_projection_has_distinct_identity_and_serialized_contract",
                 projected.identity != legacy.identity and projected.similarity_fields == ("question",)
                 and json.loads(record(projected, tiny).serialized())["embedding_fields"] == ["question"])
    all_fields = workflow.subgraph("analysis", ("analyze",), critical_fields=("schema", "guard"),
                                  similarity_fields=("schema", "guard", "question", "question"))
    report.check("projection_order_is_canonical_and_explicit_all_fields_has_new_identity",
                 all_fields.similarity_fields == ("guard", "question", "schema")
                 and all_fields.identity != legacy.identity
                 and record(all_fields, tiny).embedding == legacy_record.embedding)

    schema = " ".join(f"table_{index}.column_name_{index} TEXT fixed database schema" for index in range(400))
    first = {**tiny, "schema": schema}
    second = {**first, "question": "What is the boiling point of mercury?"}
    full_record, soft_record = record(legacy, first), record(projected, first)
    table = ReliabilityTable(ReliabilityConfig(screening_mode="similarity"))
    matcher = Matcher(table, sim_threshold=.8, allow_exact=False)
    full_score = matcher.similarity(second, full_record)
    question_score = matcher.similarity(second, soft_record)
    report.check("large_common_schema_does_not_dominate_question_projection",
                 full_score > .99 and full_score > question_score + .2,
                 {"full_input_cosine": full_score, "question_only_cosine": question_score,
                  "encoder": "128d hashed character3gram, not a semantic embedding model"})
    report.check("projection_vector_is_exactly_declared_question_encoding",
                 soft_record.embedding_fields == ("question",)
                 and soft_record.embedding == encode_input({"question": first["question"]})
                 and soft_record.embedding != encode_input(first))
    changed_hard = {**first, "schema": "another schema", "guard": "repo-rev-2"}
    report.check("soft_similarity_ignores_excluded_content_but_hard_guards_still_reject",
                 abs(matcher.similarity(changed_hard, soft_record)-1) < 1e-12
                 and not matcher.compatible(projected, soft_record, changed_hard, EPOCH))
    report.check("all_schema_epoch_and_critical_checks_survive_projection",
                 matcher.compatible(projected, soft_record, first, EPOCH)
                 and all(not matcher.compatible(projected, soft_record, value, EPOCH) for value in (
                     {**first, "guard": "repo-rev-2"}, {**first, "schema": "changed schema"},
                     {key: value for key, value in first.items() if key != "guard"},
                     {**first, "question": 123}, {**first, "extra": "undeclared"}))
                 and not matcher.compatible(projected, soft_record, first, "different-epoch"))
    report.check("capture_projection_keeps_all_boundary_fields_for_guards",
                 projected.project({**first, "unrelated": "preserved"}) == first
                 and soft_record.inputs == first and set(soft_record.inputs) == {"question", "schema", "guard"})
    report.check("same_hard_checked_pair_has_different_soft_acceptance",
                 bool(matcher.eligible(legacy, second, CacheSnapshot(1, (full_record,)), EPOCH, allow_exact=False))
                 and not matcher.eligible(projected, second, CacheSnapshot(1, (soft_record,)), EPOCH, allow_exact=False))
    report.check("unknown_similarity_field_is_rejected_at_workflow_declaration",
                 raises(ValueError, lambda: workflow.subgraph("bad", ("analyze",), similarity_fields=("missing",))))
    report.check("unknown_record_embedding_field_is_rejected",
                 raises(ValueError, lambda: replace(soft_record, embedding_fields=("missing",))))
    report.check("forged_projection_vector_and_mismatched_field_declaration_are_rejected",
                 raises(ValueError, lambda: replace(soft_record, embedding=(0.0,)*128))
                 and raises(ValueError, lambda: replace(soft_record, embedding_fields=("guard",))))
    spoofed = replace(soft_record, embedding_fields=("guard",), embedding=encode_input({"guard": first["guard"]}))
    report.check("self_consistent_wrong_projection_cannot_spoof_subgraph_contract",
                 not matcher.compatible(projected, spoofed, first, EPOCH))

    evidence_table = ReliabilityTable(ReliabilityConfig(min_groups=3))
    old = record(legacy, tiny, "same-record-id")
    for index in range(3):
        assert evidence_table.observe(legacy, old, EPOCH, 1, "source-"+str(index), 0)
    evidence_table.refresh()
    changed = record(projected, tiny, "same-record-id")
    report.check("new_projection_identity_does_not_inherit_old_record_evidence",
                 evidence_table.lookup(legacy, old, EPOCH, 1).groups == 3
                 and evidence_table.lookup(projected, changed, EPOCH, 1) is None
                 and not Matcher(evidence_table, allow_exact=False).compatible(projected, old, tiny, EPOCH))

    # Runtime construction and downstream execution must use the declared
    # similarity field list while preserving the full current state.
    config = RuntimeConfig(epoch=EPOCH, allow_exact=False, similarity_threshold=.4)
    runtime = GeoLeapRuntime(workflow, (projected,), config=config,
        reliability_config=ReliabilityConfig(screening_mode="similarity"),
        planner_config=PlannerConfig(mode="greedy"),
        residency_config=ResidencyConfig(policy="lru", max_bytes=1024*1024))
    cold = runtime.run({**tiny, "untouched": "keep"}, request_id="capture-projection", collect_feedback=False)
    captured = runtime.manager.queued_records[0]
    report.check("runtime_capture_carries_projection_and_complete_boundary_input",
                 cold.state["answer"] == tiny["question"] + " | shared facts"
                 and captured.embedding_fields == ("question",) and captured.inputs == tiny)
    store = RecordStore()
    assert store.publish((captured,), frozenset(), 0, 1024*1024)
    warm_runtime = GeoLeapRuntime(workflow, (projected,), config=config, store=store,
        reliability_config=ReliabilityConfig(screening_mode="similarity"),
        planner_config=PlannerConfig(mode="greedy"),
        residency_config=ResidencyConfig(policy="lru", max_bytes=1024*1024))
    live = {**tiny, "question": tiny["question"] + " Please.", "untouched": "keep"}
    before = counts.copy()
    warm = warm_runtime.run(live, request_id="reuse-projection", collect_records=False, collect_feedback=False)
    report.check("projected_reuse_restores_only_exit_and_runs_real_downstream_current_question",
                 warm.reused_subgraphs == ("analysis",) and warm.executed_node_ids == ("answer",)
                 and counts["analyze"] == before["analyze"] and counts["answer"] == before["answer"]+1
                 and warm.state["question"] == live["question"] and warm.state["untouched"] == "keep"
                 and warm.state["answer"] == live["question"] + " | shared facts")
    eligible = warm_runtime.matcher.eligible(projected, live, store.snapshot(), EPOCH, allow_exact=False)
    decision = LookaheadPlanner(PlannerConfig(mode="full", depth=2, beam_width=4)).choose(
        workflow, (projected,), set(), live, store.snapshot(), eligible, EPOCH)
    report.check("full_planner_revalidates_projected_subgraph_identity",
                 decision.action is not None and decision.action.kind == "reuse"
                 and decision.action.candidate.spec.identity == projected.identity)
    revision = warm_runtime.update_service_profiles({"analyze": {"latency_ms": 50}})
    report.check("economic_profile_rebuild_preserves_projection_contract",
                 revision == 1 and warm_runtime.subgraphs[0].similarity_fields == ("question",)
                 and warm_runtime.subgraphs[0].identity == projected.identity)
    controls = D2Runner._controls(warm_runtime)
    report.check("d2_control_signature_rebuild_preserves_projection_identity",
                 controls[3] == (projected.identity,))
    report.output_dir.mkdir(parents=True, exist_ok=True)
    (report.output_dir / "projection_decisions.json").write_text(json.dumps({
        "legacy_identity": legacy.identity, "projected_identity": projected.identity,
        "legacy_record_sha256": hashlib.sha256(legacy_record.serialized()).hexdigest(),
        "encoder": "128d character3gram hashing", "semantic_model_claim": False,
        "large_schema_full_cosine": full_score, "question_projection_cosine": question_score,
        "hard_fields_retained": list(projected.critical_fields), "new_model_calls": 0,
        "runtime_executed_nodes": list(warm.executed_node_ids),
        "runtime_reused_subgraphs": list(warm.reused_subgraphs)}, indent=2) + "\n")


if __name__ == "__main__":
    raise SystemExit(run_report("similarity_projection", "相似度字段投影：硬检查保留、旧版本兼容与真实续算", checks))
