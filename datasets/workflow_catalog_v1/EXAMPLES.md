# Representative generated DAGs

Actual declarations from this catalog; diagrams indicate data dependencies, not concurrent execution.

## qa_004: 5 nodes / 4 edges

```mermaid
flowchart TD
  n001["n001: qa.query_plan<br/>default"]
  n002["n002: qa.retrieve<br/>entity_anchor"]
  n003["n003: qa.expand_query<br/>bridge_entity"]
  n004["n004: qa.retrieve<br/>entity_anchor"]
  n005["n005: qa.answer<br/>direct_evidence"]
  n001 --> n002
  n002 --> n003
  n003 --> n004
  n004 --> n005
```

## qa_025: 8 nodes / 10 edges

```mermaid
flowchart TD
  n001["n001: qa.query_plan<br/>default"]
  n002["n002: qa.retrieve<br/>entity_anchor"]
  n003["n003: qa.retrieve<br/>relation_bridge"]
  n004["n004: qa.retrieve<br/>constraint_detail"]
  n005["n005: qa.retrieve<br/>independent_verification"]
  n006["n006: qa.rerank<br/>entity_relevance"]
  n007["n007: qa.summarize<br/>entity_profile"]
  n008["n008: qa.answer<br/>direct_evidence"]
  n001 --> n002
  n001 --> n003
  n001 --> n004
  n001 --> n005
  n002 --> n006
  n004 --> n007
  n003 --> n008
  n005 --> n008
  n006 --> n008
  n007 --> n008
```

## qa_091: 26 nodes / 35 edges

```mermaid
flowchart TD
  n001["n001: qa.query_plan<br/>default"]
  n002["n002: qa.retrieve<br/>relation_bridge"]
  n003["n003: qa.rewrite_query<br/>entity_disambiguation"]
  n004["n004: qa.rewrite_query<br/>relation_expansion"]
  n005["n005: qa.retrieve<br/>relation_bridge"]
  n006["n006: qa.retrieve<br/>entity_anchor"]
  n007["n007: qa.retrieve<br/>constraint_detail"]
  n008["n008: qa.retrieve<br/>independent_verification"]
  n009["n009: qa.retrieve<br/>constraint_detail"]
  n010["n010: qa.merge_evidence<br/>default"]
  n011["n011: qa.rerank<br/>entity_relevance"]
  n012["n012: qa.expand_query<br/>bridge_entity"]
  n013["n013: qa.retrieve<br/>entity_anchor"]
  n014["n014: qa.merge_evidence<br/>default"]
  n015["n015: qa.answer<br/>direct_evidence"]
  n016["n016: qa.verify_evidence<br/>entity_identity"]
  n017["n017: qa.answer<br/>decomposed_reasoning"]
  n018["n018: qa.answer<br/>constraint_first"]
  n019["n019: qa.answer<br/>decomposed_reasoning"]
  n020["n020: qa.answer<br/>constraint_first"]
  n021["n021: qa.expand_query<br/>bridge_entity"]
  n022["n022: qa.rewrite_query<br/>entity_disambiguation"]
  n023["n023: qa.retrieve<br/>entity_anchor"]
  n024["n024: qa.answer<br/>direct_evidence"]
  n025["n025: qa.select_answer<br/>default"]
  n026["n026: qa.select_answer<br/>default"]
  n001 --> n002
  n001 --> n003
  n001 --> n004
  n003 --> n005
  n003 --> n006
  n003 --> n007
  n003 --> n008
  n004 --> n009
  n005 --> n010
  n006 --> n010
  n007 --> n010
  n008 --> n010
  n009 --> n011
  n010 --> n012
  n012 --> n013
  n002 --> n014
  n011 --> n014
  n013 --> n014
  n014 --> n015
  n014 --> n016
  n014 --> n017
  n014 --> n018
  n016 --> n019
  n016 --> n020
  n016 --> n021
  n021 --> n022
  n022 --> n023
  n023 --> n024
  n019 --> n025
  n020 --> n025
  n024 --> n025
  n015 --> n026
  n017 --> n026
  n018 --> n026
  n025 --> n026
```

## sql_002: 4 nodes / 3 edges

```mermaid
flowchart TD
  n001["n001: sql.schema_linking<br/>default"]
  n002["n002: sql.classify<br/>full_query_classification"]
  n003["n003: sql.generate<br/>direct"]
  n004["n004: sql.self_correct<br/>default"]
  n001 --> n002
  n002 --> n003
  n003 --> n004
```

## sql_025: 8 nodes / 10 edges

```mermaid
flowchart TD
  n001["n001: sql.schema_linking<br/>default"]
  n002["n002: sql.generate<br/>direct"]
  n003["n003: sql.generate<br/>join_first"]
  n004["n004: sql.generate<br/>aggregation_first"]
  n005["n005: sql.decompose<br/>projection"]
  n006["n006: sql.generate<br/>decomposition_guided"]
  n007["n007: sql.select_candidate<br/>default"]
  n008["n008: sql.self_correct<br/>default"]
  n001 --> n002
  n001 --> n003
  n001 --> n004
  n001 --> n005
  n005 --> n006
  n002 --> n007
  n003 --> n007
  n004 --> n007
  n006 --> n007
  n007 --> n008
```

## sql_091: 24 nodes / 30 edges

```mermaid
flowchart TD
  n001["n001: sql.schema_linking<br/>default"]
  n002["n002: sql.decompose<br/>projection"]
  n003["n003: sql.retrieve_values<br/>entity_values"]
  n004["n004: sql.classify<br/>join_complexity"]
  n005["n005: sql.classify<br/>aggregation_complexity"]
  n006["n006: sql.prune_schema<br/>projection_columns"]
  n007["n007: sql.join_analysis<br/>key_cardinality"]
  n008["n008: sql.merge_context<br/>default"]
  n009["n009: sql.join_analysis<br/>foreign_key_paths"]
  n010["n010: sql.prune_schema<br/>projection_columns"]
  n011["n011: sql.generate<br/>direct"]
  n012["n012: sql.generate<br/>decomposition_guided"]
  n013["n013: sql.generate<br/>join_first"]
  n014["n014: sql.generate<br/>aggregation_first"]
  n015["n015: sql.decompose<br/>projection"]
  n016["n016: sql.retrieve_values<br/>entity_values"]
  n017["n017: sql.select_candidate<br/>default"]
  n018["n018: sql.generate<br/>decomposition_guided"]
  n019["n019: sql.generate<br/>decomposition_guided"]
  n020["n020: sql.prune_schema<br/>projection_columns"]
  n021["n021: sql.execute<br/>default"]
  n022["n022: sql.generate<br/>direct"]
  n023["n023: sql.select_candidate<br/>default"]
  n024["n024: sql.select_and_correct<br/>default"]
  n001 --> n002
  n001 --> n003
  n001 --> n004
  n001 --> n005
  n002 --> n006
  n003 --> n007
  n004 --> n008
  n005 --> n008
  n006 --> n009
  n007 --> n010
  n008 --> n011
  n008 --> n012
  n008 --> n013
  n008 --> n014
  n009 --> n015
  n010 --> n016
  n011 --> n017
  n012 --> n017
  n013 --> n017
  n014 --> n017
  n015 --> n018
  n016 --> n019
  n016 --> n020
  n018 --> n021
  n020 --> n022
  n019 --> n023
  n022 --> n023
  n017 --> n024
  n021 --> n024
  n023 --> n024
```
