# GeoLeap constrained workflow template catalog v1

This is a reproducible **generated derivative workload**, not 200 published
methods, benchmark traces, or evidence of benchmark quality. There are exactly
100 QA and 100 SQL templates, with topology uniqueness checked **within each
scenario**. Across scenarios, identical bare topology is allowed.

## Generation

Typed graph rewrites expand a minimal functional pipeline using real stage
insertions, complementary strategy branches with consumed joins, and bounded
next-hop/execute-repair expansions. No empty nodes, input/output sentinels or
transitive redundant edges are inserted to inflate size or diversity. Branch
roles are declared in node parameters and must be honored by real bindings.
Budgets per scenario: 70 × 3–10, 20 × 11–19, 7 × 20–29, 3 × 30–50 nodes.
The seed, rewrite history, operator contracts, bounds, code and artifact checksums
are frozen in JSON. No benchmark labels or task outcomes guide selection.

`graph_signature` is only an invariant bucket key. Exact directed graph
isomorphism resolves every potential collision, ignoring names, operator
labels and parameters. Templates within a scenario therefore cannot differ
only in prompts, strategies, IDs or random seeds.

## Files and reproduction

- `qa/` and `sql/`: standalone DAG declarations with ports, parameters,
  provenance, topology metrics and core-validated cache regions.
- `manifest.json`: frozen generation settings and SHA-256 of all 200 files.
- `operators.json`: stage contracts and method references.
- `summary.json`: actual size/width counts and generation rejection counts.
- `METHODS.zh.md`: paper-ready description and scope.
- `EXAMPLES.md`: small reference and representative fork/join diagrams.
- `validation.json`: independent structural/core fixture checks, created by
  the validation command below; reports are generated locally and are not
  included in the published catalog.

From the repository root:

```sh
python3 scripts/generate_workflow_catalog.py --output /tmp/geoleap-catalog-reproduction
python3 checks/check_workflow_catalog.py --catalog /tmp/geoleap-catalog-reproduction --output /tmp/geoleap-catalog-reproduction/validation.json
```

Generation refuses to overwrite an existing folder. The files have no clock
timestamps, so the same seed and code reproduce byte-identical declarations.

## Execution boundary

The graph compiler is `scripts/workflow_catalog_runtime.py`. In real mode,
`compile_template(template, bindings=...)` requires an explicit callable for
**every used operator** and fails before execution if any is absent. A callable
receives `(node_declaration, declared_input_values)` and returns
`{"artifact_type": node["output_type"], "data": {...}}`, satisfying
`required_data`. Bindings must implement the declared branch role, consume all
parents, preserve necessary context/provenance, use the **current** question,
resolve versioned resources, and enforce read-only tools and finite JSON.
Question, resource version and model version are current initial inputs to
every node; parent payload fields have one writer each. Reference SQL, gold
answers and benchmark quality checks stay outside the service DAG.

QA query-plan artifacts carry `documents` as well as `queries`: the initial
list is empty, next-hop expansion preserves earlier evidence, and retrieval
merges new passages with the carried set. Answer artifacts retain supporting
documents for candidate selection. SQL `schema_context` must retain the
immutable base schema, keys and annotations; pruning records a selected view
rather than deleting schema information needed by other branches. Required
field types are checked; faithful evidence/schema preservation must also be
tested when real bindings are attached.

`fixture_execute(template)` explicitly uses type-only synthetic lineage
artifacts, core `GeoLeapRuntime`, no-cache mode, no feedback and no record
collection. It verifies every node executes once and every branch reaches the
terminal payload. It performs **zero model, retrieval or database calls** and
does not generate answers or SQL. Fixture success is neither task quality nor
real service end-to-end validation. Real benchmark execution, quality,
latency, cost and concurrency measurements remain **not_run**.

The current runtime is serial even for fork/join DAGs. Zero latency/cost
defaults in the compiler mean *unprofiled*, not free real services. Actual
experiments must inject measured profiles/accounting before comparing plans.
Cache identity remains scoped to template ID and version; this catalog does
not enable cross-template cache equivalence. Resource/model versions are
critical guards. Valid single-entry/single-exit regions are checked by the
actual core; a whole fork with multiple boundary entries is not automatically
cacheable.

## Provenance and scope

QA stage organization is inspired by [DSPy §7 BasicMultiHop](https://arxiv.org/html/2310.03714#S7)
and SQL by [DIN-SQL](https://arxiv.org/abs/2304.11015), with pinned author
code references in `operators.json`. These sources do not supply this catalog.
The 3-node QA case uses one retrieval hop; the 3-node SQL case omits
classification. Fork/join, candidate selection and execution-guided repairs
are our declared adaptations. HotpotQA/BIRD task attachment and resource
snapshots are a separate future experimental step; no benchmark records are
bundled or silently substituted here.

`qa_004` preserves the two-hop BasicMultiHop **stage topology** and `sql_002`
the four-stage DIN-SQL topology as reference anchors within the 100. They do
not reproduce the authors' original prompts, retrievers or optimized programs.
The few long DAGs are structure/scale stress templates; there is no evidence
yet that extra analysis branches improve answer or SQL quality. Total retrieval
stages across branches differ from sequential hop depth (bounded at four).

The published catalog contains generated DAG declarations and generation
metadata. Benchmark records, service execution logs, credentials and local
validation reports are excluded.
