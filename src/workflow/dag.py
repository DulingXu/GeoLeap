# workflow/dag.py
"""
Workflow DAG: model agent execution as DAG for cacheable subgraph extraction.
"""

from __future__ import annotations
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Set


@dataclass
class ExecutionNode:
    """Single node: LLM or tool call."""
    node_id: str
    node_type: str  # "llm" | "tool"
    tool_name: Optional[str] = None
    input_data: Any = None
    output_data: Any = None
    is_deterministic: bool = True
    cost: float = 0.0
    latency: float = 0.0
    timestamp: float = field(default_factory=time.time)
    dependencies: List[str] = field(default_factory=list)
    dependents: List[str] = field(default_factory=list)
    
    def add_dependency(self, node_id: str):
        if node_id not in self.dependencies:
            self.dependencies.append(node_id)
    
    def add_dependent(self, node_id: str):
        if node_id not in self.dependents:
            self.dependents.append(node_id)


@dataclass
class CacheableSubgraph:
    """Cacheable subgraph: weak connectivity, single entry, single exit."""
    subgraph_id: str
    entry_node_id: str
    exit_node_id: str
    node_ids: List[str]
    entry_key: str
    entry_embedding: Optional[List[float]] = None
    entry_input: Any = None
    exit_value: Any = None
    total_cost: float = 0.0
    total_latency: float = 0.0
    staticity: int = 5
    frequency: int = 0
    timestamp: float = field(default_factory=time.time)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "subgraph_id": self.subgraph_id,
            "entry_node_id": self.entry_node_id,
            "exit_node_id": self.exit_node_id,
            "node_ids": self.node_ids,
            "entry_key": self.entry_key,
            "entry_embedding": self.entry_embedding,
            "entry_input": self.entry_input,
            "exit_value": self.exit_value,
            "total_cost": self.total_cost,
            "total_latency": self.total_latency,
            "staticity": self.staticity,
            "frequency": self.frequency,
            "timestamp": self.timestamp,
        }


class WorkflowDAG:
    """DAG of execution nodes: record run, extract cacheable subgraphs."""
    
    def __init__(self):
        self.nodes: Dict[str, ExecutionNode] = {}
        self.root_nodes: List[str] = []
        self._node_counter = 0
    
    def add_node(
        self,
        node_type: str,
        tool_name: Optional[str] = None,
        input_data: Any = None,
        dependencies: Optional[List[str]] = None,
        is_deterministic: bool = True,
    ) -> str:
        """Add node to DAG; return new node_id."""
        node_id = f"node_{self._node_counter}"
        self._node_counter += 1
        node = ExecutionNode(
            node_id=node_id,
            node_type=node_type,
            tool_name=tool_name,
            input_data=input_data,
            is_deterministic=is_deterministic,
        )
        if dependencies:
            for dep_id in dependencies:
                node.add_dependency(dep_id)
                if dep_id in self.nodes:
                    self.nodes[dep_id].add_dependent(node_id)
        
        self.nodes[node_id] = node
        if not dependencies:
            self.root_nodes.append(node_id)
        
        return node_id
    
    def get_node(self, node_id: str) -> Optional[ExecutionNode]:
        return self.nodes.get(node_id)
    
    def get_all_nodes(self) -> List[ExecutionNode]:
        return list(self.nodes.values())
    
    def get_topological_order(self) -> List[str]:
        """Topological order of node ids."""
        in_degree = {node_id: len(node.dependencies) for node_id, node in self.nodes.items()}
        queue = [node_id for node_id, degree in in_degree.items() if degree == 0]
        result = []
        
        while queue:
            node_id = queue.pop(0)
            result.append(node_id)
            
            for dependent_id in self.nodes[node_id].dependents:
                in_degree[dependent_id] -= 1
                if in_degree[dependent_id] == 0:
                    queue.append(dependent_id)
        
        return result
    
    def get_reachable_nodes(self, start_node_id: str) -> Set[str]:
        """BFS from start_node_id; used for subgraph identification."""
        visited = set()
        queue = [start_node_id]
        
        while queue:
            node_id = queue.pop(0)
            if node_id in visited:
                continue
            visited.add(node_id)
            
            node = self.nodes.get(node_id)
            if node:
                for dependent_id in node.dependents:
                    if dependent_id not in visited:
                        queue.append(dependent_id)
        
        return visited
    
    def clear(self):
        self.nodes.clear()
        self.root_nodes.clear()
        self._node_counter = 0
