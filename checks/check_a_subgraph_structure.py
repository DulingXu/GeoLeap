"""Local structural smoke checks for the legacy SESE extractor foundation."""

from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from reporting import CheckReport, run_report
from workflow.dag import CacheableSubgraph, WorkflowDAG
from workflow.determinism import DeterminismChecker
from workflow.subgraph_extractor import SubgraphExtractor


def candidate(nodes, entry, exit_node):
    return CacheableSubgraph("structural-check", entry, exit_node, list(nodes), "structural-check")


def contains(extracted, nodes):
    return any(set(item.node_ids) == set(nodes) for item in extracted)


def checks(report: CheckReport) -> None:
    extractor = SubgraphExtractor(DeterminismChecker())
    dynamic = WorkflowDAG()
    first = dynamic.add_node("tool", tool_name="random", is_deterministic=False)
    second = dynamic.add_node("tool", tool_name="weather", dependencies=[first], is_deterministic=False)
    report.check("all_noncacheable_workflow_produces_no_candidates",
                 extractor.extract_subgraphs(dynamic) == [])
    report.check("direct_validation_rejects_noncacheable_nodes",
                 not extractor._validate_subgraph(candidate((first, second), first, second), dynamic))

    boundary_graph = WorkflowDAG()
    left = boundary_graph.add_node("tool", tool_name="left_source")
    right = boundary_graph.add_node("tool", tool_name="right_source")
    entry = boundary_graph.add_node("tool", tool_name="entry", dependencies=[left, right])
    exit_node = boundary_graph.add_node("tool", tool_name="exit", dependencies=[entry])
    boundary_graph.add_node("tool", tool_name="left_consumer", dependencies=[exit_node])
    boundary_graph.add_node("tool", tool_name="right_consumer", dependencies=[exit_node])
    region = (entry, exit_node)
    valid = candidate(region, entry, exit_node)
    report.check("multiple_incoming_edges_to_one_entry_are_valid",
                 extractor._has_single_entry(boundary_graph, set(region), entry))
    report.check("multiple_outgoing_edges_from_one_exit_are_valid",
                 extractor._has_single_exit(boundary_graph, set(region), exit_node))
    extracted = extractor.extract_subgraphs(boundary_graph)
    report.check("multi_edge_single_entry_exit_region_is_extracted", contains(extracted, region),
                 {"region": list(region), "candidate_count": len(extracted)})
    report.check("separate_entry_nodes_are_rejected",
                 not extractor._validate_subgraph(candidate((left, right, entry), left, entry), boundary_graph))

    disconnected = WorkflowDAG()
    chain_start = disconnected.add_node("tool", tool_name="chain_start")
    chain_end = disconnected.add_node("tool", tool_name="chain_end", dependencies=[chain_start])
    isolated = disconnected.add_node("tool", tool_name="isolated")
    malformed = candidate((chain_start, chain_end, isolated), chain_start, chain_end)
    report.check("reachable_exit_does_not_hide_isolated_selected_node",
                 not extractor._is_connected_subgraph(disconnected, set(malformed.node_ids),
                                                       chain_start, chain_end))
    report.check("disconnected_candidate_is_rejected",
                 not extractor._validate_subgraph(malformed, disconnected))

    diamond = WorkflowDAG()
    root = diamond.add_node("tool", tool_name="root")
    branch_a = diamond.add_node("tool", tool_name="branch_a", dependencies=[root])
    branch_b = diamond.add_node("tool", tool_name="branch_b", dependencies=[root])
    merge = diamond.add_node("tool", tool_name="merge", dependencies=[branch_a, branch_b])
    whole = (root, branch_a, branch_b, merge)
    report.check("branch_merge_sese_region_is_extracted",
                 contains(extractor.extract_subgraphs(diamond), whole))
    report.check("virtual_start_and_end_allow_workflow_root_to_sink",
                 extractor._validate_subgraph(candidate(whole, root, merge), diamond))
    report.check("two_sinks_are_two_exit_nodes",
                 not extractor._validate_subgraph(candidate((root, branch_a, branch_b), root, branch_a), diamond))

    llm_graph = WorkflowDAG()
    llm_a = llm_graph.add_node("llm", input_data="prompt-a", is_deterministic=True)
    llm_b = llm_graph.add_node("llm", input_data="prompt-b", dependencies=[llm_a], is_deterministic=True)
    llm_candidates = extractor.extract_subgraphs(llm_graph)
    report.check("stable_llm_only_chain_does_not_require_tool_node",
                 contains(llm_candidates, (llm_a, llm_b)),
                 {"candidate_count": len(llm_candidates)})

    mixed = WorkflowDAG()
    before = mixed.add_node("tool", tool_name="before")
    boundary = mixed.add_node("tool", tool_name="random", dependencies=[before], is_deterministic=False)
    after = mixed.add_node("tool", tool_name="after", dependencies=[boundary])
    mixed_candidates = extractor.extract_subgraphs(mixed)
    report.check("candidate_extraction_respects_noncacheable_boundary",
                 contains(mixed_candidates, (before,)) and contains(mixed_candidates, (after,))
                 and all(boundary not in item.node_ids for item in mixed_candidates))

    bounded = SubgraphExtractor(DeterminismChecker(), max_subset_nodes=2)
    report.check("large_region_skips_exponential_subset_enumeration",
                 bounded._enumerate_node_combinations(set(whole)) == []
                 and contains(bounded.extract_subgraphs(diamond), whole))


if __name__ == "__main__":
    raise SystemExit(run_report("a_subgraph_structure", "A：子图结构与缓存边界基础", checks))
