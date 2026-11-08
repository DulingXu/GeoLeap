# workflow/determinism.py
"""
Determinism checker: identify non-deterministic ops as cacheability boundaries.
"""

from __future__ import annotations
from typing import Set, Optional
from workflow.dag import ExecutionNode


class DeterminismChecker:
    """Non-deterministic: time/date, random/uuid, live data (weather/stock/news), LLM with temperature > 0."""
    
    NON_DETERMINISTIC_TOOLS: Set[str] = {
        "weather",
        "stock",
        "time",
        "date",
        "random",
        "uuid",
        "news",
        "current_time",
        "get_time",
    }
    
    def __init__(self):
        pass
    
    def is_deterministic(self, node: ExecutionNode) -> bool:
        """Return True if node is deterministic."""
        if node.node_type == "tool" and node.tool_name:
            if node.tool_name.lower() in self.NON_DETERMINISTIC_TOOLS:
                return False
        
        if not node.is_deterministic:
            return False
        return True
    
    def check_and_mark(self, node: ExecutionNode) -> bool:
        """Check and set node.is_deterministic; return whether deterministic."""
        is_det = self.is_deterministic(node)
        node.is_deterministic = is_det
        return is_det
    
    def add_non_deterministic_tool(self, tool_name: str):
        self.NON_DETERMINISTIC_TOOLS.add(tool_name.lower())
    
    def remove_non_deterministic_tool(self, tool_name: str):
        self.NON_DETERMINISTIC_TOOLS.discard(tool_name.lower())
