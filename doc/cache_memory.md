# Cache memory interface

`CacheMemoryBudget` accounts for reachable cache-owned Python objects by identity,
plus conservative control and operation reservations. It is separate from
serialized record size and does not measure process RSS or every native allocation.

```python
from workflow.memory import CacheMemoryBudget
from workflow.runtime import GeoLeapRuntime, RuntimeConfig

domain = CacheMemoryBudget(8 * 1024**2)
runtime = GeoLeapRuntime(
    workflow, subgraphs,
    config=RuntimeConfig(cache_heap_limit_bytes=domain.limit_bytes),
    cache_memory_budget=domain,
)
print(runtime.cache_memory_snapshot())
```

Share one domain across runtimes using the same cache. Bind record stores before
their first publication. Independent methods or caches need independent domains.

The domain covers resident records, allocated serialized buffers, metadata,
pending candidates, evidence, forecasts, planner workspaces and bounded query
vector memoization. Snapshots and readers keep their reachable allocations charged
after eviction. Publication reserves construction space before replacing the old
generation. Refused captures and copy-on-write updates roll back; refused cache
lookups or planning workspaces can fall back to executing the original operators.

`owned_heap_bytes` reports the reachable object graph; `total_accounted_bytes`
also includes reservations. The reported peak is a joint observed total, not the
sum of separate component peaks. Repeated object graph inspection and maintenance
have CPU costs that depend on the workload and object layout.

Native model allocations, allocator arenas and fragmentation, callable/lock
internals, unregistered transient objects and caller-owned copies are not fully
attributed. External encoder resources require their own process/resource policy.
The API therefore keeps `physical_budget_enforced=false` and does not establish a
complete cache-only physical-memory guarantee. Whole-process OS isolation is a
separate deployment responsibility.

Local checks are named by module in `checks/README.md`. They exercise accounting,
publication, retained readers, memory pressure and rollback using local fixtures.
