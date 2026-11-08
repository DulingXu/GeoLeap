"""Small declared workflows shared by the module checks."""

from workflow.spec import OperatorSpec, WorkflowSpec


def six_node_workflow():
    counters = {f"n{index}": 0 for index in range(1, 7)}
    operators = []
    for index in range(1, 7):
        node_id = f"n{index}"
        source, target = f"v{index - 1}", f"v{index}"

        def execute(inputs, key=node_id, read=source, write=target):
            counters[key] += 1
            return {write: inputs[read] + 1}

        operators.append(OperatorSpec(
            node_id, execute, dependencies=(f"n{index - 1}",) if index > 1 else (),
            read_fields=(source,), write_fields=(target,), cacheable=True, stable=True,
            effect_free=True, latency_ms=10.0, cost_usd=0.01,
            input_schema=((source, "integer"),), output_schema=((target, "integer"),),
        ))
    workflow = WorkflowSpec("six_node_check", operators)
    sa = workflow.subgraph("Sa", ("n1", "n2", "n3", "n4"))
    sb = workflow.subgraph("Sb", ("n1", "n2"))
    sc = workflow.subgraph("Sc", ("n3", "n4", "n5", "n6"))
    return workflow, (sa, sb, sc), {"v0": 0, "untouched": "keep"}, counters
