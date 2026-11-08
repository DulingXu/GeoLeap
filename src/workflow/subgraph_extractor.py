# workflow/subgraph_extractor.py
"""
Cacheable subgraph extractor.

Identifies subgraphs that satisfy: weak connectivity, single entry, single exit.
"""

from __future__ import annotations
from typing import List, Set, Tuple, Optional
from workflow.dag import WorkflowDAG, ExecutionNode, CacheableSubgraph
from workflow.determinism import DeterminismChecker


class SubgraphExtractor:
    """
    Extract cacheable subgraphs: identify non-deterministic boundaries,
    find deterministic regions between them, validate single entry/exit.
    """
    
    def __init__(self, determinism_checker: DeterminismChecker, max_subset_nodes: int = 12):
        self.determinism_checker = determinism_checker
        if isinstance(max_subset_nodes, bool) or not isinstance(max_subset_nodes, int) or max_subset_nodes < 1:
            raise ValueError("max_subset_nodes must be a positive integer")
        self.max_subset_nodes = max_subset_nodes
    
    def extract_subgraphs(self, dag: WorkflowDAG) -> List[CacheableSubgraph]:
        """
        Extract validated cacheable subgraphs (including nested). Steps:
        1) Identify non-deterministic boundaries
        2) Find deterministic regions between boundaries
        3) Enumerate valid subgraphs per region
        4) Deduplicate and sort by node count descending

        Subset enumeration is exhaustive only up to max_subset_nodes. Larger
        regions use validated topological windows, avoiding exponential search.
        """
        if not dag.nodes:
            return []
        if len(dag.get_topological_order()) != len(dag.nodes):
            raise ValueError("Subgraph extraction requires a valid acyclic workflow")
        boundaries = set(self._identify_boundaries(dag))
        deterministic_regions = self._find_deterministic_regions(dag, boundaries)
        all_subgraphs = []
        for region_nodes in deterministic_regions:
            region_subgraphs = self._enumerate_all_subgraphs_in_region(dag, region_nodes, boundaries)
            all_subgraphs.extend(region_subgraphs)
        unique_subgraphs = self._deduplicate_subgraphs(all_subgraphs)
        unique_subgraphs.sort(key=lambda sg: len(sg.node_ids), reverse=True)
        return unique_subgraphs
    
    def _find_deterministic_regions(
        self,
        dag: WorkflowDAG,
        boundaries: Set[str]
    ) -> List[Set[str]]:
        """Find connected regions of deterministic nodes between boundaries."""
        regions = []
        visited = set()
        

        for node_id, node in dag.nodes.items():
            if node_id in visited or node_id in boundaries:
                continue
            
            if not self.determinism_checker.is_deterministic(node):
                continue
            

            region = set()
            queue = [node_id]
            
            while queue:
                current_id = queue.pop(0)
                if current_id in visited or current_id in boundaries:
                    continue
                
                current_node = dag.get_node(current_id)
                if not current_node:
                    continue
                
                if not self.determinism_checker.is_deterministic(current_node):
                    continue
                
                visited.add(current_id)
                region.add(current_id)
                

                for dep_id in current_node.dependencies:
                    if dep_id not in visited and dep_id not in boundaries:
                        dep_node = dag.get_node(dep_id)
                        if dep_node and self.determinism_checker.is_deterministic(dep_node):
                            queue.append(dep_id)
                

                for dep_id in current_node.dependents:
                    if dep_id not in visited and dep_id not in boundaries:
                        dep_node = dag.get_node(dep_id)
                        if dep_node and self.determinism_checker.is_deterministic(dep_node):
                            queue.append(dep_id)
            
            if region:
                regions.append(region)
        
        return regions
    
    def _enumerate_all_subgraphs_in_region(
        self,
        dag: WorkflowDAG,
        region_nodes: Set[str],
        boundaries: Set[str]
    ) -> List[CacheableSubgraph]:
        """Enumerate all subgraphs in a deterministic region with single entry/exit. Chain: sliding window; else enumerate node subsets and validate."""
        if not region_nodes:
            return []
        
        subgraphs = []
        


        chain_subgraphs = self._enumerate_chain_subgraphs(dag, region_nodes, boundaries)
        subgraphs.extend(chain_subgraphs)
        


        all_combinations = self._enumerate_node_combinations(region_nodes)
        
        for node_set in all_combinations:
            if len(node_set) == 0:
                continue
            

            node_set_tuple = tuple(sorted(node_set))
            if any(tuple(sorted(sg.node_ids)) == node_set_tuple for sg in chain_subgraphs):
                continue
            

            candidate_subgraphs = self._try_build_subgraphs_from_nodes(dag, node_set, boundaries)
            subgraphs.extend(candidate_subgraphs)
        
        return subgraphs
    
    def _enumerate_chain_subgraphs(
        self,
        dag: WorkflowDAG,
        region_nodes: Set[str],
        boundaries: Set[str]
    ) -> List[CacheableSubgraph]:
        """Enumerate all contiguous chain subgraphs. Returns list of valid chain subgraphs."""
        subgraphs = []
        

        topo_order = dag.get_topological_order()
        region_chain = [node_id for node_id in topo_order if node_id in region_nodes]
        
        if not region_chain:
            return []
        

        for start in range(len(region_chain)):
            for end in range(start + 1, len(region_chain) + 1):
                chain_subset = set(region_chain[start:end])
                


                entry_node_id = region_chain[start]
                

                candidate_subgraphs = self._try_build_subgraphs_from_nodes(
                    dag, chain_subset, boundaries, entry_node_id
                )
                subgraphs.extend(candidate_subgraphs)
        
        return subgraphs
    
    def _enumerate_node_combinations(self, node_set: Set[str]) -> List[Set[str]]:
        """
        Enumerate all non-empty subsets only for small regions.

        Topological windows are generated separately for larger regions.
        """
        nodes_list = sorted(list(node_set))
        combinations = []
        

        n = len(nodes_list)
        if n > self.max_subset_nodes:
            return []
        for mask in range(1, 1 << n):  # skip empty set
            subset = {nodes_list[i] for i in range(n) if (mask >> i) & 1}
            combinations.append(subset)
        
        return combinations
    
    def _try_build_subgraphs_from_nodes(
        self,
        dag: WorkflowDAG,
        node_set: Set[str],
        boundaries: Set[str],
        suggested_entry: Optional[str] = None
    ) -> List[CacheableSubgraph]:
        """
        Try to build valid subgraph(s) from node_set; single entry/exit. Returns list (may be empty).
        """
        if not node_set or not node_set.issubset(dag.nodes) or node_set.intersection(boundaries):
            return []
        if any(not self.determinism_checker.is_deterministic(dag.nodes[key]) for key in node_set):
            return []
        # Virtual Start has one edge to every workflow root. Multiple outside
        # edges may enter the same entry node; only the entry-node count matters.
        entries = self._entry_nodes(dag, node_set)
        if len(entries) != 1:
            return []
        entry_node_id = next(iter(entries))
        subgraph = self._build_subgraph_from_nodes(dag, node_set, entry_node_id)
        return [subgraph] if subgraph and self._validate_subgraph(subgraph, dag) else []
    
    def _identify_boundaries(self, dag: WorkflowDAG) -> List[str]:
        """
        Identify non-deterministic nodes as cacheability boundaries. Returns list of boundary node ids.
        """
        boundaries = []
        for node_id, node in dag.nodes.items():
            if not self.determinism_checker.is_deterministic(node):
                boundaries.append(node_id)
        return boundaries
    
    def _extract_from_root(
        self,
        dag: WorkflowDAG,
        root_id: str,
        boundaries: List[str]
    ) -> List[CacheableSubgraph]:
        """Extract subgraphs from root until boundaries. Returns list of cacheable subgraphs."""
        subgraphs = []
        visited = set()
        

        queue = [root_id]
        candidate_nodes = set()
        
        while queue:
            node_id = queue.pop(0)
            if node_id in visited or node_id in boundaries:
                continue
            visited.add(node_id)
            
            node = dag.get_node(node_id)
            if node and self.determinism_checker.is_deterministic(node):
                candidate_nodes.add(node_id)
                

                for dependent_id in node.dependents:
                    if dependent_id not in visited and dependent_id not in boundaries:
                        queue.append(dependent_id)
        

        if candidate_nodes:

            if len(candidate_nodes) == 1:

                single_node_id = list(candidate_nodes)[0]
                single_node = dag.get_node(single_node_id)
                if single_node:

                    subgraph = self._build_single_node_subgraph(dag, single_node_id)
                    if subgraph and self._validate_subgraph(subgraph, dag):
                        subgraphs.append(subgraph)
            else:

                subgraph = self._build_subgraph_from_nodes(dag, candidate_nodes, root_id)
                if subgraph and self._validate_subgraph(subgraph, dag):
                    subgraphs.append(subgraph)
        
        return subgraphs
    
    def _build_single_node_subgraph(
        self,
        dag: WorkflowDAG,
        node_id: str
    ) -> Optional[CacheableSubgraph]:
        """
        Build single-node subgraph (special case). Returns cacheable subgraph or None.
        """
        node = dag.get_node(node_id)
        if not node:
            return None
        

        subgraph_id = f"subgraph_single_{node_id}"
        entry_key = self._generate_entry_key(node)
        
        subgraph = CacheableSubgraph(
            subgraph_id=subgraph_id,
            entry_node_id=node_id,
            exit_node_id=node_id,
            node_ids=[node_id],
            entry_key=entry_key,
            entry_input=node.input_data,
        )
        
        return subgraph
    
    def _extract_between_boundaries(
        self,
        dag: WorkflowDAG,
        start_boundary: str,
        end_boundary: str
    ) -> Optional[CacheableSubgraph]:
        """
        Extract subgraph between two boundaries: BFS from start_boundary, collect deterministic nodes until end_boundary; build and validate single entry/exit. Returns cacheable subgraph or None.
        """
        start_node = dag.get_node(start_boundary)
        end_node = dag.get_node(end_boundary)
        
        if not start_node or not end_node:
            return None
        


        visited = set()
        queue = []
        candidate_nodes = set()
        

        for dependent_id in start_node.dependents:
            if dependent_id != end_boundary:
                queue.append(dependent_id)
        

        while queue:
            node_id = queue.pop(0)
            if node_id in visited:
                continue
            

            if node_id == end_boundary:
                continue
            
            visited.add(node_id)
            node = dag.get_node(node_id)
            
            if not node:
                continue
            

            if self.determinism_checker.is_deterministic(node):
                candidate_nodes.add(node_id)
                

                for dependent_id in node.dependents:
                    if dependent_id not in visited and dependent_id != end_boundary:
                        queue.append(dependent_id)
            else:

                continue
        

        if not candidate_nodes:
            return None
        

        entry_candidates = [
            node_id for node_id in candidate_nodes
            if node_id in start_node.dependents
        ]
        
        if not entry_candidates:

            topo_order = dag.get_topological_order()
            for node_id in topo_order:
                if node_id in candidate_nodes:
                    entry_candidates = [node_id]
                    break
        
        if not entry_candidates:
            return None
        
        entry_node_id = entry_candidates[0]  # if multiple, take first
        

        exit_candidates = []
        for node_id in candidate_nodes:
            node = dag.get_node(node_id)
            if not node:
                continue
            

            if end_boundary in node.dependents:
                exit_candidates.append(node_id)

            elif not node.dependents:
                exit_candidates.append(node_id)
        
        if not exit_candidates:

            topo_order = dag.get_topological_order()
            for node_id in reversed(topo_order):
                if node_id in candidate_nodes:
                    exit_candidates = [node_id]
                    break
        
        if not exit_candidates:
            return None
        
        exit_node_id = exit_candidates[0]  # if multiple, take first
        


        subgraph = self._build_subgraph_from_nodes(
            dag, candidate_nodes, entry_node_id
        )
        
        if not subgraph:
            return None
        


        if exit_node_id in candidate_nodes:

            if self._has_single_exit(dag, candidate_nodes, exit_node_id):
                subgraph.exit_node_id = exit_node_id
        

        if self._validate_subgraph(subgraph, dag):
            return subgraph
        
        return None
    
    def _build_subgraph_from_nodes(
        self,
        dag: WorkflowDAG,
        node_ids: Set[str],
        entry_node_id: str
    ) -> Optional[CacheableSubgraph]:
        """Build subgraph from node set; single entry/exit. Returns cacheable subgraph or None."""
        if not node_ids or entry_node_id not in node_ids:
            return None
        

        exit_node_id = self._find_exit_node(dag, node_ids)
        if not exit_node_id:
            return None
        

        if not self._has_single_entry(dag, node_ids, entry_node_id):
            return None
        if not self._has_single_exit(dag, node_ids, exit_node_id):
            return None
        

        subgraph_id = f"subgraph_{entry_node_id}_{exit_node_id}"
        entry_node = dag.get_node(entry_node_id)
        

        entry_key = self._generate_entry_key(entry_node)
        
        subgraph = CacheableSubgraph(
            subgraph_id=subgraph_id,
            entry_node_id=entry_node_id,
            exit_node_id=exit_node_id,
            node_ids=sorted(list(node_ids)),  # sort for consistency
            entry_key=entry_key,
            entry_input=entry_node.input_data if entry_node else None,
        )
        
        return subgraph
    
    def _find_exit_node(self, dag: WorkflowDAG, node_ids: Set[str]) -> Optional[str]:
        """Return the unique exit node, allowing multiple edges from that node."""
        exits = self._exit_nodes(dag, node_ids)
        return next(iter(exits)) if len(exits) == 1 else None

    @staticmethod
    def _entry_nodes(dag: WorkflowDAG, node_ids: Set[str]) -> Set[str]:
        if not node_ids or not node_ids.issubset(dag.nodes):
            return set()
        return {
            key for key in node_ids
            if not dag.nodes[key].dependencies or
            any(parent not in node_ids for parent in dag.nodes[key].dependencies)
        }

    @staticmethod
    def _exit_nodes(dag: WorkflowDAG, node_ids: Set[str]) -> Set[str]:
        if not node_ids or not node_ids.issubset(dag.nodes):
            return set()
        # Virtual End receives one edge from every workflow sink.
        return {
            key for key in node_ids
            if not dag.nodes[key].dependents or
            any(child not in node_ids for child in dag.nodes[key].dependents)
        }
    
    def _has_single_entry(
        self,
        dag: WorkflowDAG,
        node_ids: Set[str],
        entry_node_id: str
    ) -> bool:
        """Every outside incoming edge (including Start) targets one node."""
        return self._entry_nodes(dag, node_ids) == {entry_node_id}

    def _has_single_exit(
        self,
        dag: WorkflowDAG,
        node_ids: Set[str],
        exit_node_id: str
    ) -> bool:
        """Every outside outgoing edge (including End) originates at one node."""
        return self._exit_nodes(dag, node_ids) == {exit_node_id}
    
    def _validate_subgraph(
        self,
        subgraph: CacheableSubgraph,
        dag: WorkflowDAG
    ) -> bool:
        """Validate: weak connectivity, single entry, single exit, no other external edges. Returns True if all hold."""
        node_ids = set(subgraph.node_ids)
        if not node_ids or not node_ids.issubset(dag.nodes):
            return False
        if any(not self.determinism_checker.is_deterministic(dag.nodes[key]) for key in node_ids):
            return False
        

        if not self._is_connected_subgraph(dag, node_ids, subgraph.entry_node_id, subgraph.exit_node_id):
            return False
        

        if not self._has_single_entry(dag, node_ids, subgraph.entry_node_id):
            return False
        

        if not self._has_single_exit(dag, node_ids, subgraph.exit_node_id):
            return False
        


        for node_id in node_ids:
            if node_id == subgraph.entry_node_id:
                continue  # skip entry

            node = dag.get_node(node_id)
            if not node:
                continue

            external_deps = [dep_id for dep_id in node.dependencies if dep_id not in node_ids]
            if external_deps:

                return False
        

        for node_id in node_ids:
            if node_id == subgraph.exit_node_id:
                continue  # skip exit
            
            node = dag.get_node(node_id)
            if not node:
                continue
            

            external_deps = [dep_id for dep_id in node.dependents if dep_id not in node_ids]
            if external_deps:

                return False
        
        return True
    
    def _is_connected_subgraph(
        self,
        dag: WorkflowDAG,
        node_ids: Set[str],
        entry_node_id: str,
        exit_node_id: str
    ) -> bool:
        """Visit every selected node using internal edges in either direction."""
        if not node_ids.issubset(dag.nodes) or entry_node_id not in node_ids or exit_node_id not in node_ids:
            return False
        

        if len(node_ids) == 1:
            return entry_node_id == exit_node_id
        

        visited = set()
        queue = [entry_node_id]
        visited.add(entry_node_id)
        
        while queue:
            current = queue.pop(0)
            

            node = dag.get_node(current)
            if not node:
                return False
            for neighbor_id in set(node.dependencies) | set(node.dependents):
                if neighbor_id in node_ids and neighbor_id not in visited:
                    visited.add(neighbor_id)
                    queue.append(neighbor_id)
        return visited == node_ids
    
    def _generate_entry_key(self, entry_node: Optional[ExecutionNode]) -> str:
        """Generate semantic entry key for cache matching."""
        if not entry_node:
            return "unknown"
        

        if entry_node.node_type == "tool" and entry_node.tool_name:

            input_str = str(entry_node.input_data) if entry_node.input_data else ""
            return f"{entry_node.tool_name}:{input_str}"
        elif entry_node.node_type == "llm":

            input_str = str(entry_node.input_data) if entry_node.input_data else ""
            return f"llm:{input_str}"
        else:
            return f"{entry_node.node_type}:{entry_node.node_id}"
    
    def _deduplicate_subgraphs(
        self,
        subgraphs: List[CacheableSubgraph]
    ) -> List[CacheableSubgraph]:
        """Deduplicate subgraphs by node_ids set. Returns unique list."""
        seen = set()
        unique = []
        
        for subgraph in subgraphs:

            node_ids_tuple = tuple(sorted(subgraph.node_ids))
            if node_ids_tuple not in seen:
                seen.add(node_ids_tuple)
                unique.append(subgraph)
        
        return unique
