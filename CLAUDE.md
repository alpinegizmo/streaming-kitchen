# Design patterns for stream processing with Apache Flink SQL

## Objective

Build a single page website with animated examples of each basic design pattern. For each example, illustrate

* an english-language summary of the use case being illustrated
* the SQL query 
* the job graph, including a kafka source, a kafka sink, and if needed, changelog normalization and sink upsert materialization
* at each stage of the job graph, the stream type: append-only, upsert, or retract
* the state used by each stateful operator
* the role (if any) that watermarks play

## Patterns

* filtering and projection
* deduplication
  - event time
  - processing time
* non-windowed aggregation
* windows
  - tumble
  - hop
  - cumulate
  - session
* event correlation (interval join)
* enrichment
  - lookup join
  - temporal join
  - lateral snapshot join
* complex event processing

## Resources

Use the examples about a chain of pizza shops -- in /Users/david/stuff/flink-intro-talk/flink-intro.html -- as a starting point.
