"""Offline, reproducible regression checks for catalog topology algorithms.

Run from any directory with::

    python3 /path/to/GeoCAT/checks/check_workflow_catalog_topology.py

Only the Python standard library is needed.  The permutation and subset
oracles are intentionally independent of the implementation being checked.
These algorithm checks do not execute, or count as execution fixtures for,
the generated workflow catalog.  No model or external service is called.
"""

from __future__ import annotations

import itertools
from pathlib import Path
import random
import sys
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from workflow_catalog_topology import graph_metrics, graph_signature, is_isomorphic


def permutation_isomorphism_oracle(
    size: int, edges_a: list[tuple[int, int]], edges_b: list[tuple[int, int]]
) -> bool:
    """Check every possible vertex bijection, with no coloring assumptions."""
    targets = set(edges_b)
    return any(
        {(permutation[a], permutation[b]) for a, b in edges_a} == targets
        for permutation in itertools.permutations(range(size))
    )


def subset_antichain_oracle(size: int, edges: list[tuple[int, int]]) -> int:
    """Compute reachability, then enumerate every candidate antichain subset."""
    reach = [[False] * size for _ in range(size)]
    for source, target in edges:
        reach[source][target] = True
    for middle in range(size):
        for source in range(size):
            for target in range(size):
                reach[source][target] = reach[source][target] or (
                    reach[source][middle] and reach[middle][target]
                )
    for width in range(size, -1, -1):
        for candidate in itertools.combinations(range(size), width):
            if all(
                not reach[a][b] and not reach[b][a]
                for a, b in itertools.combinations(candidate, 2)
            ):
                return width
    raise AssertionError("The empty subset must always be an antichain")


class WorkflowCatalogTopologyChecks(unittest.TestCase):
    def test_120_small_dag_pairs_against_permutation_oracle(self) -> None:
        rng = random.Random(811)
        checked = positive = negative = 0
        for size in range(2, 7):
            potential = list(itertools.combinations(range(size), 2))
            for repeat in range(24):
                # Equal edge counts avoid a mostly trivial size-only test.
                edge_count = rng.randrange(len(potential) + 1)
                edges_a = rng.sample(potential, edge_count)
                if repeat % 3 == 0:
                    permutation = list(range(size))
                    rng.shuffle(permutation)
                    edges_b = [(permutation[a], permutation[b]) for a, b in edges_a]
                else:
                    edges_b = rng.sample(potential, edge_count)
                expected = permutation_isomorphism_oracle(size, edges_a, edges_b)
                with self.subTest(size=size, repeat=repeat):
                    self.assertEqual(
                        is_isomorphic(range(size), edges_a, range(size), edges_b),
                        expected,
                    )
                checked += 1
                positive += expected
                negative += not expected
        self.assertEqual(checked, 120)
        self.assertGreater(positive, 0)
        self.assertGreater(negative, 0)

    def test_128_small_dags_against_subset_antichain_oracle(self) -> None:
        rng = random.Random(945)
        checked = 0
        for size in range(1, 9):
            potential = list(itertools.combinations(range(size), 2))
            for repeat in range(16):
                edges = [edge for edge in potential if rng.random() < 0.28]
                with self.subTest(size=size, repeat=repeat):
                    self.assertEqual(
                        graph_metrics(range(size), edges)["maximum_antichain_width"],
                        subset_antichain_oracle(size, edges),
                    )
                checked += 1
        self.assertEqual(checked, 128)

    def test_100_random_relabelings_preserve_signature_and_isomorphism(self) -> None:
        rng = random.Random(20261111)
        checked = 0
        for size in range(1, 51):
            for repeat in range(2):
                nodes = list(range(size))
                edges = [
                    (source, target)
                    for source, target in itertools.combinations(nodes, 2)
                    if rng.random() < 0.13
                ]
                permutation = nodes.copy()
                rng.shuffle(permutation)
                renamed = {node: f"renamed_{permutation[node]}" for node in nodes}
                renamed_nodes = [renamed[node] for node in reversed(nodes)]
                renamed_edges = [(renamed[a], renamed[b]) for a, b in reversed(edges)]
                with self.subTest(size=size, repeat=repeat):
                    self.assertEqual(
                        graph_signature(nodes, edges),
                        graph_signature(renamed_nodes, renamed_edges),
                    )
                    self.assertTrue(is_isomorphic(nodes, edges, renamed_nodes, renamed_edges))
                checked += 1
        self.assertEqual(checked, 100)

    def test_equal_wl_bucket_does_not_prove_isomorphism(self) -> None:
        # Both graphs have four sources and four sinks, all degree two.  The
        # underlying undirected graph is respectively an 8-cycle or two 4-cycles.
        nodes = list(range(8))
        one_cycle = [(i, 4 + i) for i in range(4)] + [
            (i, 4 + (i + 1) % 4) for i in range(4)
        ]
        two_cycles = [(i, j) for i in (0, 1) for j in (4, 5)] + [
            (i, j) for i in (2, 3) for j in (6, 7)
        ]
        self.assertEqual(graph_signature(nodes, one_cycle), graph_signature(nodes, two_cycles))
        self.assertFalse(is_isomorphic(nodes, one_cycle, nodes, two_cycles))

    def test_known_depth_and_width_metrics(self) -> None:
        cases = [
            ("empty", [], [], 0, 0, 0),
            ("chain", list(range(5)), list(zip(range(4), range(1, 5))), 5, 1, 1),
            (
                "diamond",
                ["s", "a", "b", "t"],
                [("s", "a"), ("s", "b"), ("a", "t"), ("b", "t")],
                3,
                2,
                2,
            ),
            (
                "uneven_branch",
                ["s", "a", "b", "c", "x", "y", "t"],
                [
                    ("s", "a"), ("a", "b"), ("b", "c"), ("c", "t"),
                    ("s", "x"), ("x", "t"), ("b", "y"), ("y", "t"),
                ],
                5,
                2,
                3,
            ),
        ]
        for name, nodes, edges, depth, generation_width, antichain_width in cases:
            with self.subTest(case=name):
                metrics = graph_metrics(nodes, edges)
                self.assertEqual(metrics["node_count"], len(nodes))
                self.assertEqual(metrics["edge_count"], len(edges))
                self.assertEqual(metrics["longest_path_nodes"], depth)
                self.assertEqual(metrics["max_generation_width"], generation_width)
                self.assertEqual(metrics["maximum_antichain_width"], antichain_width)

    def test_invalid_graphs_are_rejected_by_all_public_interfaces(self) -> None:
        cases = [
            ("cycle", ["a", "b"], [("a", "b"), ("b", "a")]),
            ("duplicate_edge", ["a", "b"], [("a", "b"), ("a", "b")]),
            ("duplicate_node", ["a", "a"], []),
            ("self_loop", ["a"], [("a", "a")]),
            ("unknown_endpoint", ["a"], [("a", "missing")]),
        ]
        for name, nodes, edges in cases:
            for operation, check in [
                ("metrics", lambda: graph_metrics(nodes, edges)),
                ("signature", lambda: graph_signature(nodes, edges)),
                ("isomorphism_left", lambda: is_isomorphic(nodes, edges, [], [])),
                ("isomorphism_right", lambda: is_isomorphic([], [], nodes, edges)),
            ]:
                with self.subTest(case=name, operation=operation):
                    with self.assertRaises(ValueError):
                        check()


if __name__ == "__main__":
    unittest.main(verbosity=2)
