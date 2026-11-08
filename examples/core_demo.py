"""Run the GeoLeap core without network calls or external dependencies."""
from workflow.runtime import GeoLeapRuntime
from workflow.spec import OperatorSpec, WorkflowSpec

workflow = WorkflowSpec("geoleap_demo", (
    OperatorSpec("increment", lambda state: {"y": state["x"] + 1},
                 read_fields=("x",), write_fields=("y",)),
))
result = GeoLeapRuntime(workflow, ()).run({"x": 1}, use_cache=False, collect_records=False)
assert result.state["y"] == 2
print({"result": result.state["y"], "executed_nodes": result.executed_node_ids})
