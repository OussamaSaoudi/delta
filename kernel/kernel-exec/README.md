# Kernel plan execution

Status: design of record for the unified Kernel Java and Spark local executor.

## Goal

`kernel-exec` executes one passive plan language for both Kernel Java and Spark. Kernel owns the
relational semantics. Each host keeps its scan implementation, expression implementation,
admission policy, prefetch policy, and conversion into the shared plan language.

The implementation should stay small. Reuse Kernel data, expression, scan, and row interfaces;
reuse Spark Catalyst expressions and readers. Do not copy the old Java or Spark operator stacks.

## Modules

- `kernel-api` contains the existing data and engine contracts, passive plan values, structural
  expression equality, and per-batch lifetime metadata.
- `kernel-exec` contains `PlanExecutor`, caching, identity sharing, and the common operators.
- `kernel-defaults` implements the Kernel Java expression and scan hooks using its existing
  readers, deletion-vector code, and row implementations.
- Spark implements the same engine hooks with Catalyst expressions and Spark readers. Admission,
  plan conversion, prefetch scheduling, and distributed fallback remain Spark policy.

`kernel-exec` depends only on `kernel-api`. Plan nodes contain no execution methods.

## Main API

```java
public final class PlanExecutor {
  public PlanExecutor(Engine engine, PlanResultCache cache);

  public CloseableIterator<FilteredColumnarBatch> execute(PlanNode root);
}

public final class PlanResultCache implements AutoCloseable {
  public PlanResultCache(int maxEntries, long maxBytes);
  public static PlanResultCache disabled();

  public CloseableIterator<FilteredColumnarBatch> get(PlanNode plan); // null on miss
  public void put(
      PlanNode plan, CloseableIterable<FilteredColumnarBatch> owned, long approxBytes);
  public boolean prefetch(
      PlanNode plan, CloseableIterator<FilteredColumnarBatch> result, long approxBytes);
  public boolean prefetch(
      PlanNode plan,
      CompletableFuture<CloseableIterator<FilteredColumnarBatch>> result,
      long approxBytes);
  public long weightBytes();
  public void invalidateAll();
  public void close();
}
```

The executor obtains the expression handler from `Engine`. A host that needs Catalyst evaluation
provides an engine whose expression handler composes Spark evaluation with the default handler.

Cache lookup is part of execution. Population is explicit host policy. `put` installs a replayable
owned iterable. `prefetch` installs an iterator, or a future of one, that one consumer may claim and
stream. Entries use structural plan keys and FIFO eviction. A future is claimed as a future but its
iterator is not exposed until completion; unfinished rows are never streamed. Owned results are
ordinary reachable Java objects, so v1 uses GC rather than a materialized-result abstraction.

## Data and lifetime

Operators exchange `CloseableIterator<FilteredColumnarBatch>`. Every returned batch declares one
of these lifetimes:

- `BORROWED`: data is valid only until its producing iterator or evaluator advances or closes.
- `OWNED`: immutable data remains valid while reachable and may be read concurrently.

Lifetime is batch-specific because a single source may sometimes return owned data and sometimes
return a borrowed view. Union therefore needs no special lifetime rule: it forwards each batch's
own declaration.

Retention is allowed only at identity sharing, cache fill, dynamic-scan metadata collection,
aggregate/join state insertion, and owned output assembly. Retaining a borrowed batch copies its
selected rows. Retaining an owned batch reuses it.

`RowBackedColumnarBatch` is a view over independently addressable Kernel `Row` objects. Spark's
adapter should likewise be a zero-copy view over independently addressable `InternalRow` values or
Spark column vectors. A Spark source whose iterator reuses one mutable row must copy at that source
boundary; the shared runtime does not hide that copy.

## Expressions

Project is one struct-valued expression, not a list of fields assembled by the operator.

```java
ExpressionEvaluator getEvaluator(
    StructType inputSchema, Expression expression, StructType outputSchema);

FilteredColumnarBatch ExpressionEvaluator.eval(FilteredColumnarBatch input);
FilteredColumnarBatch PredicateEvaluator.eval(FilteredColumnarBatch input);
```

An evaluator returns a batch so it can declare its result lifetime. It must preserve row count and
row order. A predicate also preserves the input data and only narrows selection. Operators bind an
evaluator once, call it once per batch, and consume the result row by row.

Kernel expressions use the default handler. Engine-native expressions use opaque Kernel carriers.
Spark opaque equality is Catalyst semantic equality and hashing. Spark can initially evaluate a
single struct-valued Catalyst expression even if that creates a larger expression tree; optimize
only after measurement shows it matters.

The plan records expression result types. Aggregate grouping types are the leading fields of its
output schema; aggregate operand types live in `Agg`; semi-join key types live in `SemiJoin`.
Spark conversion must populate these types from analyzed Catalyst expressions.

## Passive plan language

The closed leaf set is `Values`, `ScanJson`, and `ScanParquet`. Operators are `Filter`, `Project`,
`UnionAll`, `Aggregate`, `SemiJoin`, and `DynamicScan`. Structural equality includes node kind,
payload, expressions, types, children, and scan file identity.

Execution sharing is intentionally different from equality. Reusing the same `PlanNode` object
shares execution. Structurally equal but distinct objects execute independently unless the cache
contains their structural key.

Each scan file contains an absolute path, positive size, modification time, owned constant row,
and optional `DeletionVectorDescriptor`. The table root is stored once on the scan, not once per
deletion vector. JSON and Parquet engine handlers execute a complete scan request, including file
constants, row-index handling, and deletion-vector selection. This lets Kernel Java reuse its scan
stack and Spark reuse its file-index/reader stack.

`DynamicScan` materializes selected metadata rows into scan files, then invokes exactly the same
static scan hook. It has no separate read loop.

## Operator semantics

- Filter narrows selection without rewriting data.
- Project evaluates its single struct expression.
- Union concatenates child iterators and preserves each batch's lifetime.
- Aggregate and semi/anti join use one `StateTable` implementation.
- Aggregate output appends state-backed result vectors to the owned group-key batch.
- Group and join keys are null-safe. Null is a value and participates in equality and hashing.
- Aggregate ties are unspecified. Tests must be tie-insensitive.
- `MIN` and `MAX` ignore null operands. Global aggregation emits one row for empty input.
- `*_NON_NULL_BY(value, sentinel, key)` qualifies a row only when sentinel and key are non-null.
  The winning value may itself be null.
- Expression binding validates output types. Aggregate output fields are also checked against the
  aggregate result types and nullability carried by the plan.

`StateTable` uses Kernel `Row` keys. A probe is a zero-copy row view; the table copies only on key
insertion. Hash and equality logic lives in `RowKernels`, using existing Kernel value/hash behavior
where possible. Values retained in state are owned Kernel values.

## Execution and sharing

`PlanExecutor.execute` performs one identity pre-walk. A node with one consumer opens directly. A
node with multiple consumers uses one lazy pull-through iterable and one cursor per consumer. The
shared iterable retains each borrowed batch once and gives all cursors the owned result.

Execution opens every static scan handler before returning the root iterator. This lets host
readers initiate I/O for independent branches as early as possible and overlap it with earlier
branch computation. Row production remains pull-driven. Dynamic scans open only after their
metadata input has produced the files to scan.

V1 is single-threaded. The lazy shared iterable is the only mutable object shared between plan
branches and is the future synchronization point. Close is idempotent; every resource is attempted,
the first failure wins, and later close failures are suppressed.

## Host usage

Kernel Java:

```java
try (PlanResultCache cache = new PlanResultCache(64, 256L << 20)) {
  PlanExecutor executor = new PlanExecutor(defaultEngine, cache);
  try (CloseableIterator<FilteredColumnarBatch> batches = executor.execute(plan)) {
    while (batches.hasNext()) {
      consume(batches.next());
    }
  }
}
```

Spark, schematically:

```scala
val converted = SparkPlans.convert(optimizedSubtree)
if (converted.isPresent) {
  val kernelPlan = converted.get()
  val executor = new PlanExecutor(sparkKernelEngine, sharedCache)
  val rows = collectInternalRows(executor.execute(kernelPlan))
  LocalRelation(optimizedSubtree.output, rows)
}
```

The Spark planning rule tries maximal eligible subtrees top-down. Conversion is all-or-nothing for
each attempted subtree; unsupported nodes stay in normal Spark execution. A worker may prefetch a
subtree by passing its execution iterator future to `prefetch`.

## V1 exclusions

No runtime plan rewriting, admission, distributed fallback, implicit cache fill, streaming cache
fills, intra-execution parallelism, unboxed state table, named relation context, or vectorized fast
path is required for v1. These can be added without changing the plan algebra.
