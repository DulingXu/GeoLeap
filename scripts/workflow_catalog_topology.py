"""Topology checks for the generated workflow catalog, using only the stdlib.

Node IDs are opaque, unique, hashable values.  Edges describe a simple directed
graph; duplicate edges, unknown endpoints, and cycles are rejected.  Operator
names and all other workflow attributes are deliberately outside this module.

``graph_signature`` is an isomorphism-invariant *bucket key*, not a canonical
label or an exact uniqueness certificate.  Use ``is_isomorphic`` to resolve
equal bucket keys before accepting a topology as new.  Exact matching uses
joint color refinement and exhaustive constrained individualization when color
refinement alone is inconclusive.  It has no approximate or timeout fallback.
Like general graph-isomorphism searches, its worst case can be expensive; the
intended inputs are small (at most 50 nodes), structured workflow DAGs.
"""

from __future__ import annotations

from collections import Counter, deque
from dataclasses import dataclass
import hashlib
import json
from typing import Any, Hashable, Iterable, Sequence


Node = Hashable
Edge = tuple[Node, Node]


@dataclass(frozen=True)
class _Graph:
    nodes: tuple[Node, ...]
    predecessors: tuple[tuple[int, ...], ...]
    successors: tuple[tuple[int, ...], ...]
    predecessor_masks: tuple[int, ...]
    successor_masks: tuple[int, ...]
    topological_order: tuple[int, ...]
    root_depth: tuple[int, ...]
    sink_depth: tuple[int, ...]
    ancestor_counts: tuple[int, ...]
    descendant_counts: tuple[int, ...]
    edge_count: int

    @property
    def size(self) -> int:
        return len(self.nodes)

    def initial_features(self) -> list[tuple[int, ...]]:
        return [
            (
                len(self.predecessors[node]),
                len(self.successors[node]),
                self.root_depth[node],
                self.sink_depth[node],
                self.ancestor_counts[node],
                self.descendant_counts[node],
            )
            for node in range(self.size)
        ]


def _build_graph(nodes: Iterable[Node], edges: Iterable[Edge]) -> _Graph:
    node_ids = tuple(nodes)
    index: dict[Node, int] = {}
    for position, node in enumerate(node_ids):
        try:
            if node in index:
                raise ValueError(f"Duplicate node ID: {node!r}")
            index[node] = position
        except TypeError as error:
            raise ValueError("Node IDs must be hashable") from error

    predecessors: list[set[int]] = [set() for _ in node_ids]
    successors: list[set[int]] = [set() for _ in node_ids]
    edge_count = 0
    for edge in edges:
        if isinstance(edge, (str, bytes)):
            raise ValueError("Each edge must be a (source, target) pair")
        try:
            source, target = edge
        except (TypeError, ValueError) as error:
            raise ValueError("Each edge must be a (source, target) pair") from error
        try:
            source_index = index[source]
            target_index = index[target]
        except (KeyError, TypeError) as error:
            raise ValueError(f"Edge references an unknown node: {edge!r}") from error
        if source_index == target_index:
            raise ValueError(f"Self-loop is not a DAG edge: {edge!r}")
        if target_index in successors[source_index]:
            raise ValueError(f"Duplicate edge: {edge!r}")
        successors[source_index].add(target_index)
        predecessors[target_index].add(source_index)
        edge_count += 1

    # Sorting integer indices makes results repeatable for a given input order.
    pred = tuple(tuple(sorted(values)) for values in predecessors)
    succ = tuple(tuple(sorted(values)) for values in successors)
    remaining_indegree = [len(values) for values in pred]
    ready = deque(node for node, degree in enumerate(remaining_indegree) if degree == 0)
    topological_order: list[int] = []
    root_depth = [0] * len(node_ids)
    ancestor_masks = [0] * len(node_ids)
    while ready:
        node = ready.popleft()
        topological_order.append(node)
        for target in succ[node]:
            root_depth[target] = max(root_depth[target], root_depth[node] + 1)
            ancestor_masks[target] |= ancestor_masks[node] | (1 << node)
            remaining_indegree[target] -= 1
            if remaining_indegree[target] == 0:
                ready.append(target)
    if len(topological_order) != len(node_ids):
        raise ValueError("Graph contains a directed cycle")

    sink_depth = [0] * len(node_ids)
    descendant_masks = [0] * len(node_ids)
    for node in reversed(topological_order):
        for target in succ[node]:
            sink_depth[node] = max(sink_depth[node], sink_depth[target] + 1)
            descendant_masks[node] |= descendant_masks[target] | (1 << target)

    return _Graph(
        nodes=node_ids,
        predecessors=pred,
        successors=succ,
        predecessor_masks=tuple(sum(1 << value for value in values) for values in pred),
        successor_masks=tuple(sum(1 << value for value in values) for values in succ),
        topological_order=tuple(topological_order),
        root_depth=tuple(root_depth),
        sink_depth=tuple(sink_depth),
        ancestor_counts=tuple(mask.bit_count() for mask in ancestor_masks),
        descendant_counts=tuple(mask.bit_count() for mask in descendant_masks),
        edge_count=edge_count,
    )


def _maximum_antichain_width(graph: _Graph) -> int:
    """Apply Dilworth's theorem via a matching on transitive reachability.

    Each original vertex appears once on each side of a bipartite graph.  A
    left vertex connects to every strict descendant on the right, so a maximum
    matching yields width = vertex count - matching size.  Using only original
    DAG edges here would compute a different statistic on some DAGs.
    """
    descendants = [0] * graph.size
    for source in reversed(graph.topological_order):
        for target in graph.successors[source]:
            descendants[source] |= descendants[target] | (1 << target)
    reachable = [
        tuple(target for target in range(graph.size) if mask & (1 << target))
        for mask in descendants
    ]
    matched_left = [-1] * graph.size

    def augment(source: int, seen_right: set[int]) -> bool:
        for target in reachable[source]:
            if target in seen_right:
                continue
            seen_right.add(target)
            if matched_left[target] == -1 or augment(matched_left[target], seen_right):
                matched_left[target] = source
                return True
        return False

    matching_size = sum(augment(source, set()) for source in range(graph.size))
    return graph.size - matching_size


def graph_metrics(nodes: Iterable[Node], edges: Iterable[Edge]) -> dict[str, Any]:
    """Validate a DAG and return size, depth, and structural width statistics.

    ``longest_path_nodes`` includes both endpoints (a singleton has depth 1).
    ``max_generation_width`` is the largest generation in a Kahn layering,
    equivalently grouping nodes by their longest distance from a root.  It is
    not a claim about actual parallel execution or the maximum antichain.
    ``maximum_antichain_width`` is the exact maximum number of pairwise
    unreachable vertices, computed by transitive closure and bipartite matching.
    ``roots``, ``sinks``, and ``topological_order`` contain original node IDs.
    An empty DAG is accepted and has zero nodes, depth, and generation width.
    """
    graph = _build_graph(nodes, edges)
    generation_counts = Counter(graph.root_depth)
    generation_sizes = [generation_counts[level] for level in sorted(generation_counts)]
    roots = [graph.nodes[node] for node in range(graph.size) if not graph.predecessors[node]]
    sinks = [graph.nodes[node] for node in range(graph.size) if not graph.successors[node]]

    unseen = set(range(graph.size))
    component_count = 0
    while unseen:
        component_count += 1
        start = unseen.pop()
        pending = [start]
        while pending:
            node = pending.pop()
            for neighbor in graph.predecessors[node] + graph.successors[node]:
                if neighbor in unseen:
                    unseen.remove(neighbor)
                    pending.append(neighbor)

    longest_path_nodes = max(graph.root_depth, default=-1) + 1
    return {
        "node_count": graph.size,
        "edge_count": graph.edge_count,
        "longest_path_nodes": longest_path_nodes,
        "longest_path_edges": max(0, longest_path_nodes - 1),
        "max_generation_width": max(generation_sizes, default=0),
        "maximum_antichain_width": _maximum_antichain_width(graph),
        "generation_sizes": generation_sizes,
        "roots": roots,
        "sinks": sinks,
        "root_count": len(roots),
        "sink_count": len(sinks),
        "max_in_degree": max((len(values) for values in graph.predecessors), default=0),
        "max_out_degree": max((len(values) for values in graph.successors), default=0),
        "weak_component_count": component_count,
        "topological_order": [graph.nodes[node] for node in graph.topological_order],
    }


def _colorize(features: Sequence[tuple[Any, ...]]) -> list[int]:
    feature_colors = {feature: color for color, feature in enumerate(sorted(set(features)))}
    return [feature_colors[feature] for feature in features]


def _node_color_features(graph: _Graph, colors: Sequence[int]) -> list[tuple[Any, ...]]:
    return [
        (
            colors[node],
            tuple(sorted(colors[source] for source in graph.predecessors[node])),
            tuple(sorted(colors[target] for target in graph.successors[node])),
        )
        for node in range(graph.size)
    ]


def _refine(graphs: Sequence[_Graph], colors: Sequence[Sequence[int]]) -> list[list[int]]:
    """Refine all graphs in one color namespace, without ever merging cells."""
    current = [list(values) for values in colors]
    while True:
        previous_color_count = len({color for values in current for color in values})
        features = [
            _node_color_features(graph, values)
            for graph, values in zip(graphs, current)
        ]
        flat_colors = _colorize([feature for values in features for feature in values])
        refined: list[list[int]] = []
        offset = 0
        for graph in graphs:
            refined.append(flat_colors[offset : offset + graph.size])
            offset += graph.size
        current = refined
        if len(set(flat_colors)) == previous_color_count:
            return current


def _signature(graph: _Graph) -> str:
    initial_features = graph.initial_features()
    initial_colors = _colorize(initial_features)
    colors = _refine([graph], [initial_colors])[0]
    refined_features = _node_color_features(graph, colors)
    # Preserve the actual initial invariants as well as relative color IDs:
    # locally generated color number 0 can mean different things in two graphs.
    description = {
        "version": 1,
        "node_count": graph.size,
        "edge_count": graph.edge_count,
        "features": sorted(zip(initial_features, refined_features)),
    }
    serialized = json.dumps(description, separators=(",", ":"), sort_keys=True)
    return "dag-wl-v1:" + hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def graph_signature(nodes: Iterable[Node], edges: Iterable[Edge]) -> str:
    """Return a fast, deterministic invariant for bucketing possible isomorphs.

    The result ignores node IDs, node order, and edge order.  Equal signatures
    are *not* proof of isomorphism: call ``is_isomorphic`` inside each bucket.
    All graph validation rules described in the module docstring apply.
    """
    return _signature(_build_graph(nodes, edges))


def _classes(colors: Sequence[int]) -> dict[int, list[int]]:
    result: dict[int, list[int]] = {}
    for node, color in enumerate(colors):
        result.setdefault(color, []).append(node)
    return result


def _verify_mapping(graph_a: _Graph, graph_b: _Graph, mapping: Sequence[int]) -> bool:
    """Verify every directed adjacency under a proposed complete bijection."""
    for source in range(graph_a.size):
        mapped_successors = sum(1 << mapping[target] for target in graph_a.successors[source])
        if mapped_successors != graph_b.successor_masks[mapping[source]]:
            return False
    return True


def _match_refined(
    graph_a: _Graph,
    graph_b: _Graph,
    colors_a: Sequence[int],
    colors_b: Sequence[int],
) -> bool:
    refined_a, refined_b = _refine([graph_a, graph_b], [colors_a, colors_b])
    if Counter(refined_a) != Counter(refined_b):
        return False
    classes_a = _classes(refined_a)
    classes_b = _classes(refined_b)

    # This also finishes large twin cells without needless individualization.
    # A failed arbitrary bijection is only inconclusive; it never rejects a DAG.
    mapping = [0] * graph_a.size
    for color, members in classes_a.items():
        for source, target in zip(members, classes_b[color]):
            mapping[source] = target
    if _verify_mapping(graph_a, graph_b, mapping):
        return True

    # Every singleton is already forced by refinement.  Individualize one node
    # from the smallest remaining cell, trying every inequivalent target.
    unresolved = [members for members in classes_a.values() if len(members) > 1]
    if not unresolved:
        return False
    selected_class = min(
        unresolved,
        key=lambda members: (
            len(members),
            -len(graph_a.predecessors[members[0]]) - len(graph_a.successors[members[0]]),
            refined_a[members[0]],
        ),
    )
    source = selected_class[0]
    targets = classes_b[refined_a[source]]
    individualized_color = max(refined_a, default=-1) + 1
    tried_twins: set[tuple[int, int]] = set()
    for target in targets:
        # Exact twins have identical incoming/outgoing neighbor sets.  Swapping
        # them is an automorphism, so one representative suffices.  Similar
        # degree or color alone is deliberately NOT enough to skip a branch.
        twin_key = (graph_b.predecessor_masks[target], graph_b.successor_masks[target])
        if twin_key in tried_twins:
            continue
        tried_twins.add(twin_key)
        branch_a = refined_a.copy()
        branch_b = refined_b.copy()
        branch_a[source] = individualized_color
        branch_b[target] = individualized_color
        if _match_refined(graph_a, graph_b, branch_a, branch_b):
            return True
    return False


def is_isomorphic(
    nodes_a: Iterable[Node],
    edges_a: Iterable[Edge],
    nodes_b: Iterable[Node],
    edges_b: Iterable[Edge],
) -> bool:
    """Exactly test unlabeled directed-DAG isomorphism, ignoring operator names.

    Both inputs are validated, even when sizes differ.  Refinement provides
    necessary conditions; successful matching always verifies a full bijection
    against every edge, and inconclusive cells are searched exhaustively.
    """
    graph_a = _build_graph(nodes_a, edges_a)
    graph_b = _build_graph(nodes_b, edges_b)
    if graph_a.size != graph_b.size or graph_a.edge_count != graph_b.edge_count:
        return False
    features_a = graph_a.initial_features()
    features_b = graph_b.initial_features()
    if Counter(features_a) != Counter(features_b):
        return False
    joint_colors = _colorize(features_a + features_b)
    return _match_refined(
        graph_a,
        graph_b,
        joint_colors[: graph_a.size],
        joint_colors[graph_a.size :],
    )
