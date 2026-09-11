# Kernel plan execution

`kernel-exec` is the shared local executor for Kernel Java and Spark. It owns the passive plan
language, cache, hash state, and operator loops. Hosts own conversion, admission, cache population,
scans, expression evaluation, and physical data retention.

## Public API

```java
PlanExecutor(Engine engine, PlanResultCache cache)
PlanExecutor(PlanEngine engine, PlanResultCache cache)
CloseableIterator<ColumnarBatch> PlanExecutor.execute(PlanNode root)

interface PlanEngine {
  BatchEvaluator bind(...);
  ColumnarBatch filter(...);
  Row retainRow(...);
  Row retainValue(...);
  ColumnarBatch appendColumns(...);
  CloseableIterator<ColumnarBatch> scan(PlanNode.FileScan scan);
}

PlanResultCache(int maxEntries, long maxBytes)
CloseableIterator<ColumnarBatch> PlanResultCache.get(FileScan scan)
boolean PlanResultCache.prefetch(
    FileScan scan, CloseableIterator<ColumnarBatch> result, long approxBytes)
boolean PlanResultCache.prefetch(
    FileScan scan,
    CompletableFuture<CloseableIterator<ColumnarBatch>> result,
    long approxBytes)
```

`PlanEngine` is the complete host boundary. Spark implements it with Catalyst evaluation, Spark
JSON and Parquet readers, and `InternalRow` retention. The public `Engine` constructor uses Kernel's
existing expression, JSON, Parquet, filesystem, and deletion-vector implementations through a
private adapter.

## Data

Operators exchange only `CloseableIterator<ColumnarBatch>`. A batch is `BORROWED` or `OWNED`.
Borrowed data is valid until its producer advances or closes; owned data remains valid while
reachable. Selection vectors never cross the operator boundary: predicate evaluation returns a
filtered batch. Project evaluates one struct expression and returns one batch.

`RowBackedColumnarBatch` is a zero-copy view over independently addressable `Row` objects; its
column vectors are private views. Spark rows implement the Kernel row contract, so scan,
expression, and aggregate results remain `InternalRow`-backed. The engine copies only values that
must outlive a borrowed input. Aggregate state retains group keys on insertion and only winning
values, then asks the engine to append result columns to the key batch.

## Plans and semantics

All node kinds are nested in `PlanNode`: `Values`, `FileScan`, `Filter`, `Project`, `UnionAll`,
`Aggregate`, and `SemiJoin`. Nodes carry output and operand types. Plans are trees; repeated
Catalyst inputs are copied during Spark conversion. Only `FileScan` has structural equality because
it is the only cache key. A `FileScan` contains at most one entry for each file path.

Group and join keys use null-safe equality. `MIN` and `MAX` ignore null operands. For
`*_NON_NULL_BY(value, sentinel, key)`, a row qualifies when sentinel and key are non-null; the
winning value may be null. Ties are unspecified. Hash keys may contain scalars and nested structs;
collection-bearing keys fall back to the host executor.

## Cache and execution

Cache lookup happens during execution; cache population is explicit host policy. `prefetch` stores
a single-use scan iterator or future, claimed by one consumer. Eviction is FIFO.

Execution recursively opens the plan tree. Static scans are opened early so host readers may
overlap I/O with plan preparation and other branches.

V1 intentionally excludes internal plan rewriting, admission, distributed fallback, protobuf
conversion, parallel pulls, and unboxed state.
