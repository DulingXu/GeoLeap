"""Versioned input encoders: isolation, validation and actual downstream use."""
from dataclasses import asdict, replace
import json

from reporting import CheckReport, run_report
from check_similarity_projection import fixture, raises
from workflow.spec import canonical_json, register_input_encoder, LEGACY_INPUT_ENCODER
from workflow.records import CachedRecord, CacheSnapshot
from workflow.reliability import Matcher, ReliabilityTable, ReliabilityConfig
from workflow.runtime import GeoLeapRuntime, RuntimeConfig
from workflow.planner import PlannerConfig
from workflow.residency import ResidencyConfig
from workflow.memory import CacheMemoryBudget, measure_object_graph


def checks(report):
    wf, _, spec, counts = fixture()
    identity = 'fixture-semantic-input-v1'
    def encoder(value):
        return (1., 0.) if 'robot' in value['question'].lower() else (0., 1.)
    register_input_encoder(identity, encoder)
    report.check('encoder_identity_cannot_be_rebound', raises(ValueError, lambda: register_input_encoder(identity, lambda _: (1., 0.))))
    a = {'question': 'Who founded Acme robotics?', 'schema': 's', 'guard': 'v1'}
    b = {**a, 'question': 'Name the creator of the robot company'}
    old = CachedRecord.create(spec, a, {'analysis': 'shared facts'}, 'epoch', record_id='r')
    new = CachedRecord.create(spec, a, {'analysis': 'shared facts'}, 'epoch', record_id='r', embedding_encoder=identity)
    report.check('legacy_serialization_unchanged_new_encoder_is_explicit', 'embedding_encoder' not in json.loads(old.serialized()) and json.loads(new.serialized())['embedding_encoder'] == identity)
    report.check('forged_vector_rejected', raises(ValueError, lambda: replace(new, embedding=(0., 1.))))
    report.check('unknown_encoder_rejected', raises(ValueError, lambda: replace(new, embedding_encoder='missing')))
    restored = CachedRecord(**json.loads(json.dumps(asdict(new))))
    report.check('versioned_record_json_roundtrip_preserves_digest_payload', restored.serialized() == new.serialized())
    table = ReliabilityTable(ReliabilityConfig(screening_mode='similarity'))
    semantic = Matcher(table, sim_threshold=.95, allow_exact=False, input_encoder_id=identity)
    lexical = Matcher(table, sim_threshold=.95, allow_exact=False)
    report.check('encoder_spaces_cannot_mix', not lexical.compatible(spec, new, a, 'epoch') and not semantic.compatible(spec, old, a, 'epoch') and raises(ValueError, lambda: lexical.similarity(a, new)))
    report.check('same_projection_semantic_match', semantic.similarity(b, new) == 1. and bool(semantic.eligible(spec, b, CacheSnapshot(1, (new,)), 'epoch')))
    report.check('hard_guard_remains_mandatory', not semantic.compatible(spec, new, {**b, 'guard': 'v2'}, 'epoch'))
    exact_spec = replace(spec, exact_safe=True)
    exact_matcher = Matcher(table, allow_exact=True, input_encoder_id=identity)
    own_exact = exact_matcher.eligible(exact_spec, a, CacheSnapshot(1, (new,)), 'epoch')
    report.check('exact_path_rejects_cross_encoder_namespace', bool(own_exact) and own_exact[0].exact
        and not exact_matcher.eligible(exact_spec, a, CacheSnapshot(1, (old,)), 'epoch'))
    proof = ReliabilityTable(ReliabilityConfig(min_groups=1))
    proof.observe(spec, old, 'epoch', .97, 'source', 0.)
    proof.refresh()
    report.check('encoder_change_invalidates_old_evidence_digest', proof.lookup(spec, old, 'epoch', .97) is not None and proof.lookup(spec, new, 'epoch', .97) is None)
    runtime = GeoLeapRuntime(wf, (spec,), input_encoder_id=identity,
        config=RuntimeConfig(epoch='epoch', allow_exact=False, similarity_threshold=.95),
        reliability_config=ReliabilityConfig(screening_mode='similarity'),
        planner_config=PlannerConfig(mode='greedy'), residency_config=ResidencyConfig(policy='lru', max_bytes=1024**2))
    runtime.run(a, request_id='first', collect_feedback=False)
    captured = runtime.manager.queued_records[0]
    report.check('runtime_capture_uses_injected_encoder', captured.embedding_encoder == identity and captured.embedding == (1., 0.))
    runtime.store.publish((captured,), frozenset(), runtime.store.version, 1024**2)
    before = counts.copy()
    result = runtime.run(b, request_id='second', collect_records=False, collect_feedback=False)
    report.check('actual_substitution_runs_current_downstream', bool(result.served_hits) and counts['analyze'] == before['analyze'] and counts['answer'] == before['answer']+1 and result.state['answer'] == b['question']+' | shared facts')
    report.check('default_runtime_still_legacy', GeoLeapRuntime(wf, (spec,)).matcher.input_encoder_id == LEGACY_INPUT_ENCODER)

    memo_calls = []
    def memo_encoder(value):
        memo_calls.append(value['question'])
        return (1., 0.)
    memo_identity = 'fixture-semantic-bounded-memo-v1'
    register_input_encoder(memo_identity, memo_encoder)
    memo_record = CachedRecord.create(spec, a, {'analysis': 'shared facts'}, 'epoch',
        embedding_encoder=memo_identity)
    memo = Matcher(table, input_encoder_id=memo_identity)
    def memo_input(index, payload=''):
        return {**a, 'question': f'robot {index} {payload}'}
    def memo_key(value):
        return canonical_json({'question': value['question']})
    memo_calls.clear()
    for index in range(64):
        memo.similarity(memo_input(index), memo_record)
    initial_count = len(memo_calls)
    memo.similarity(memo_input(0), memo_record)
    report.check('semantic_lru_hit_avoids_encoder_and_refreshes_recency',
        len(memo_calls) == initial_count and next(reversed(memo._semantic_vectors)) == memo_key(memo_input(0)))
    memo.similarity(memo_input(64), memo_record)
    report.check('semantic_lru_64_entry_cap_evicts_least_recent',
        len(memo._semantic_vectors) == 64
        and memo_key(memo_input(0)) in memo._semantic_vectors
        and memo_key(memo_input(1)) not in memo._semantic_vectors
        and memo_key(memo_input(64)) in memo._semantic_vectors
        and memo._semantic_vector_bytes == sum(item[1] for item in memo._semantic_vectors.values()))
    byte_capped = Matcher(table, input_encoder_id=memo_identity)
    for index in range(40):
        byte_capped.similarity(memo_input(index, 'a' * 50000), memo_record)
    report.check('semantic_lru_4MiB_charge_cap_evicts_before_entry_cap',
        0 < len(byte_capped._semantic_vectors) < 64
        and 0 < byte_capped._semantic_vector_bytes <= 4 * 1024**2
        and byte_capped._semantic_vector_bytes == sum(item[1] for item in byte_capped._semantic_vectors.values())
        and measure_object_graph({'memo': byte_capped._semantic_vectors})['owned_heap_bytes'] <= 4 * 1024**2
        and memo_key(memo_input(0, 'a' * 50000)) not in byte_capped._semantic_vectors)
    oversized_input = memo_input('oversized', 'z' * 1100000)
    before_keys = tuple(byte_capped._semantic_vectors)
    before_bytes, before_calls = byte_capped._semantic_vector_bytes, len(memo_calls)
    byte_capped.similarity(oversized_input, memo_record)
    byte_capped.similarity(oversized_input, memo_record)
    report.check('oversized_semantic_vector_entry_bypasses_memo_without_eviction',
        tuple(byte_capped._semantic_vectors) == before_keys
        and byte_capped._semantic_vector_bytes == before_bytes and len(memo_calls) == before_calls + 2)
    lexical.similarity(a, old)
    report.check('legacy_character_path_does_not_populate_semantic_memo',
        not lexical._semantic_vectors and lexical._semantic_vector_bytes == 0)
    heap_budget = CacheMemoryBudget(8 * 1024**2)
    budget_runtime = GeoLeapRuntime(wf, (spec,), input_encoder_id=memo_identity,
        cache_memory_budget=heap_budget)
    before_heap = heap_budget.check()['owned_heap_bytes']
    for index in range(20):
        budget_runtime.matcher.similarity(memo_input(index, 'b' * 10000), memo_record)
    after_heap = heap_budget.check()['owned_heap_bytes']
    key_bytes = sum(len(key.encode('utf-8')) for key in budget_runtime.matcher._semantic_vectors)
    report.check('runtime_root_exposes_semantic_lru_to_shared_cache_heap_ledger',
        len(budget_runtime.matcher._semantic_vectors) == 20 and key_bytes > 200000
        and after_heap - before_heap >= key_bytes
        and heap_budget.memory_snapshot()['physical_budget_enforced'] is False,
        {'before_owned_heap_bytes': before_heap, 'after_owned_heap_bytes': after_heap,
         'memo_key_bytes': key_bytes, 'scope': 'reachable Python heap, not native model or complete physical cache memory'})


if __name__ == '__main__':
    run_report('input_encoder', '输入编码器：版本隔离、硬检查和实际下游续算', checks)
