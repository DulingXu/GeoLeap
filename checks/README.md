# Offline functional checks

Run `python3 checks/run_core_checks.py` from the repository root. The suite uses the Python standard library, declared local workflows, and synthetic fixtures. It does not invoke external APIs or run benchmark experiments.

| Script | Coverage |
| --- | --- |
| `check_a_subgraph_structure.py` | DAG structure, single-entry/single-exit regions, and boundaries |
| `check_b_reliability_matching.py` | Hard guards, grouped feedback, and revision/epoch/bin isolation |
| `check_b1_screening_switches.py` | Four qualification rules and lookup/commit consistency |
| `check_b_experiment_interfaces.py` | Frozen evidence, independent clones, hit capture, and post-service learning |
| `check_b2_runtime_feedback.py` | Reference feedback, support accumulation, and later-request eligibility |
| `check_c_lookahead_planning.py` | Reuse combinations, forecast misses, and actual-entrance revalidation |
| `check_c1_planning_switches.py` | NoCache, Greedy, and Full execution paths |
| `check_c2_search_cost.py` | Depth/beam controls, same-information exhaustive search, and size limits |
| `check_d_opportunity_residency.py` | Set utility, byte budgets, audit quotas, pins, and versions |
| `check_d1_residency_switches.py` | Four residency policies, reuse access updates, and maintenance statistics |
| `check_d2_change_response.py` | Demand/cost-profile changes, sequential execution, and window accounting |
| `check_core_integration.py` | B/C/D integration, actual replacement, and exit-state restoration |
| `check_memory_ledger.py` | Shared objects, conservative reservations, and transactions |
| `check_evidence_memory.py` | Shared evidence/forecast budgets and rollback |
| `check_cache_manager_memory.py` | Pending queues, maintenance, and residency budgets |
| `check_cache_memory_budget.py` | Publication, retained readers, budget rejection, and fallback |
| `check_runtime_memory_failures.py` | Runtime failure paths, rollback, and retained result lifetimes |
| `check_similarity_projection.py` | Field projections, guards, legacy compatibility, and downstream execution |
| `check_input_encoder.py` | Encoder identities, vector validation, namespace isolation, bounded LRU, and ownership |

Use `--module c1_planning_switches` to select one module, or `--output-dir /tmp/geoleap-checks` to choose the report location. Each module writes `report.txt`, `report.json`, and relevant details; the suite writes `summary.txt` and `summary.json`. A failed check produces a nonzero exit status.

Reports describe functional behavior under the fixtures. They are not benchmark measurements of performance, task quality, or cost savings. The encoder checks use local function fixtures and do not load external semantic models. Memory checks cover the Python-object ledger and failure handling; they do not certify a complete cache-only physical-memory limit.
