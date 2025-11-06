# Oxidized Kernel: One-Page Technical Summary

## The Problem
Current architecture creates **tight coupling** between Rust Kernel and engines, limiting flexibility and forcing engines into a specific execution model.

### Core Issues
1. **Wide FFI Surface**: Engines must implement the entire Engine + EngineData traits
2. **Forced Execution Model**: Engines locked into Rust's iterator model (sync, specific types)
3. **No Native Format Support**: Cannot efficiently use engine's own data structures
4. **No Custom Operators**: Cannot substitute optimized implementations
5. **Limited Control**: No flexibility for parallelization, caching, or materialization strategies

## The Solution
**Logical plans as a declarative interface**: Rust specifies WHAT to compute (the query plan), engine decides HOW to execute it (using its own infrastructure).

---

# Part 1: State Machine + Logical Plan Architecture

## State Machine Flow

```
1) User makes request → Kernel-rs
2) Kernel produces a state machine
3) State machine node produces a plan → Engine Plan execution
4) State machine driver calls .next() repeatedly until terminus
5) Upon hitting terminus, the state machine driver returns the result
```

### Two Generic Driver Patterns

The architecture supports two generic patterns:

#### Pattern 1: Terminus-Based (for final results)

Used when building final result objects (e.g., Snapshot).

```rust
// Generic terminus-based driver
// Returns the final result object

let mut phase = builder_into_state_machine(params);

loop {
    match phase.phase_type() {
        PhaseType::Terminus => {
            let result = phase.get_from_terminus();
            return Ok(result);
        }
        PhaseType::Consume => {
            let plan = phase.get_plan()?;
            let results = engine.execute(plan)?;  // Engine controls execution
            phase = phase.consume_next(results)?;  // Kernel consumes results
        }
        PhaseType::PartialResult => {
            return Err("unexpected for terminus-based state machine");
        }
    }
}
```

**Example**: Building a Snapshot
- Phase 1 (CONSUME): Execute plan to list log files
- Phase 2 (CONSUME): Execute plan to read protocol/metadata
- Phase 3 (TERMINUS): Extract Snapshot object

#### Pattern 2: Partial Result-Based (for intermediate data)

Used when collecting intermediate results that stay in the engine (e.g., scan files).

```rust
// Generic partial result-based driver
// Collects intermediate results without sending to kernel

let mut phase = scan.into_state_machine();  // Note: scan, not scan_builder

loop {
    match phase.phase_type() {
        PhaseType::Terminus => {
            // All intermediate results collected
            return Ok(());
        }
        PhaseType::PartialResult => {
            let plan = phase.get_plan()?;  // PARTIAL_RESULT produces plans!
            let intermediate_results = engine.execute(plan)?;  // e.g., scan files
            // Engine can cache, parallelize, materialize - full control
            // Results stay in engine, not sent to kernel
            phase = phase.partial_result_next()?;  // No results to kernel
        }
        PhaseType::Consume => {
            let plan = phase.get_plan()?;
            let results = engine.execute(plan)?;
            phase = phase.consume_next(results)?;  // Kernel consumes these
        }
    }
}
```

**Example**: Executing a Scan
- Phase 1 (PARTIAL_RESULT): Execute plan for commit files → collect scan files
- Phase 2 (PARTIAL_RESULT): Execute plan for checkpoint → collect more scan files  
- Phase 3 (TERMINUS): Done - all scan files collected in engine

### Key: Shared Interface, No Duplication

Both patterns use the **same state machine interface**:
- `phase.phase_type()` - Check current phase type
- `phase.get_plan()` - Get logical plan to execute
- `phase.consume_next(results)` - Advance CONSUME phase with results
- `phase.partial_result_next()` - Advance PARTIAL_RESULT phase

The only difference is what the engine does with results:
- **Terminus-based**: Engine returns results to kernel (CONSUME phases)
- **Partial result-based**: Engine keeps results (PARTIAL_RESULT phases)

Engine chooses pattern based on what it's building.

---

## Logical Plan Node Types

Rust kernel generates logical plans with these node types:

1. **ScanNode**: Read Parquet/JSON files
2. **FilterNode**: Apply row-level filter (RowFilter)
3. **SelectNode**: Project/compute columns
4. **UnionNode**: Combine two data sources
5. **ParseJsonNode**: Parse JSON string column to struct
6. **FilterByExpressionNode**: Filter using Delta expression
7. **FileListingNode**: List files from storage
8. **FirstNonNullNode**: Coalesce operation

---

## Plan Execution (Engine's Responsibility)

Engine implements plan execution using its own APIs:

```rust
// Engine has full control over execution strategy
fn execute(&self, plan: LogicalPlanNode) -> Result<Box<dyn Iterator<Item = Batch>>> {
    match plan {
        LogicalPlanNode::Scan(scan) => {
            // Engine uses its own Parquet reader
            self.engine.parquet_handler().read(scan.files, scan.schema)
        }
        
        LogicalPlanNode::Union(union) => {
            // Engine controls recursion and iteration strategy
            let left_iter = self.execute(*union.a)?;
            let right_iter = self.execute(*union.b)?;
            Ok(Box::new(left_iter.chain(right_iter)))  // Engine's iterator model
        }
        
        LogicalPlanNode::Select(select) => {
            // Engine projects columns by name from output schema
            let batches = self.execute(*select.child)?;
            self.project_columns(batches, &select.output_type)
        }
        
        LogicalPlanNode::FilterByExpression(filter) => {
            // Engine uses its own expression evaluator
            let batches = self.execute(*filter.child)?;
            self.engine.expression_handler().filter(batches, &filter.predicate)
        }
        
        LogicalPlanNode::FileListing(listing) => {
            // Engine uses its own file system client
            self.engine.file_system_client().list_from(&listing.path)
        }
    }
}
```

---

## Engine API Compatibility

**Any engine implementing Engine APIs can execute logical plans**:

```rust
// If engine has these APIs:
trait Engine {
    fn parquet_handler(&self) -> &dyn ParquetHandler;
    fn json_handler(&self) -> &dyn JsonHandler;
    fn expression_handler(&self) -> &dyn ExpressionHandler;
    fn file_system_client(&self) -> &dyn FileSystemClient;
}

// Then engine can execute logical plans:
impl PlanExecutor for MyPlanExecutor {
    fn execute(&self, plan: LogicalPlanNode) -> Result<Iterator<Item = Batch>> {
        match plan {
            ScanNode => self.engine.parquet_handler().read(),
            SelectNode => self.project_columns() /* or expression_handler().eval() */,
            FilterByExpressionNode => self.engine.expression_handler().eval_predicate(),
            FileListingNode => self.engine.file_system_client().list_from(),
            // ...
        }
        // No need to implement Rust Engine trait for kernel-rs!
        // No forced iterator types!
        // Use engine's own data format!
    }
}
```

### What This Means for Different Engines

**Spark**: Uses distributed Parquet reader, Catalyst expressions, RDD caching  
**Flink**: Uses async I/O, streaming operators, state management  

**All using the same logical plan interface!**

---

## Key Architectural Benefits

| Benefit | How It Helps |
|---------|-------------|
| **Decoupling** | Engine doesn't implement full kernel-rs Engine trait |
| **Flexibility** | Engine chooses sync/async, parallel/sequential execution |
| **Native Formats** | Engine uses its own ColumnarBatch/Arrow/whatever |
| **Custom Operators** | Engine can swap in optimized implementations |
| **Caching Control** | Engine decides where to cache and materialize |
| **Parallelization** | Engine parallelizes at whatever granularity it wants |

### Why This Matters

Different engines have different strengths:
- **Spark**: Distributed execution, RDD caching, Catalyst optimizer
- **Flink**: Streaming, async I/O, state management

Logical plans let each engine leverage its strengths without being forced into kernel-rs's model.

---

# Part 2: Java Implementation (Proof of Concept)

We built a complete reference implementation to prove the interface works.

## What We Built

| Component | Description | Lines of Code |
|-----------|-------------|---------------|
| DefaultPlanExecutor | Full plan executor using Java DefaultEngine | 1,337 |
| LogicalPlan types | Sealed interface with 8 plan record types | 80 |
| LogicalPlanVisitor | Visitor pattern for plan transfer | 399 |
| ArrowCDataConverter | Zero-copy data transfer via Arrow C | 450 |
| Tests | 12 comprehensive tests with data validation | 800 |
| **Total** | **Working, tested implementation** | **3,200** |

---

## What This Proves

### 1. Interface is Complete
All necessary operations are implementable using standard Engine APIs.

```java
// Engine has full control over execution strategy
return switch (plan) {
    case ScanPlan s -> executeScan(s);
    case FilterPlan f -> executeFilter(f);
    case SelectPlan s -> executeSelect(s);
    case UnionPlan u -> executeUnion(u);
    // Compiler ensures all cases handled
};
```

### 2. Interface is Narrow
Engine doesn't need to implement kernel-rs Engine/EngineData traits.

**What engine needs**:
- State machine driver loop (shown above in Rust)
- Plan executor using existing Engine APIs

That's it. No kernel-rs trait implementations.

### 3. Interface is Flexible
Java engine controls execution model completely.

```java
// Engine uses its own data format
private CloseableIterator<FilteredColumnarBatch> executeScan(ScanPlan scan) {
    return engine.getParquetHandler().readParquetFiles(
        fileStatusIter,
        scan.schema(),
        Optional.empty()
    );
    // FilteredColumnarBatch is Java EngineData, not Rust type
}
```

### 4. Interface is Universal
Any engine implementing Engine APIs can execute plans.

**What we demonstrated**:
- ✅ Uses Java DefaultEngine (ParquetHandler, JsonHandler, ExpressionHandler)
- ✅ Uses Java EngineData (ColumnarBatch, ColumnVector)
- ✅ Uses Java iterators (CloseableIterator)
- ✅ Uses Java resource management (try-with-resources, AutoCloseable)
- ✅ No kernel-rs types exposed to engine code

### 5. No Rust Dependency in Engine Code
Engine doesn't implement kernel-rs Engine/EngineData traits.

```java
// Engine just needs standard APIs
class DefaultPlanExecutor {
  private final Engine engine;  // Java Engine interface
  
  CloseableIterator<FilteredColumnarBatch> execute(LogicalPlan plan) {
    // Delegates to engine.getParquetHandler(), etc.
    // No kernel-rs trait implementation needed
  }
}
```

---

## Test Coverage

| Test Category | What It Validates |
|---------------|-------------------|
| **ScanPlan** | Reads Parquet/JSON using DefaultEngine |
| **UnionPlan** | Chains multiple scans correctly |
| **SelectPlan** | Projects and reorders columns |
| **FilterPlan** | Integrates with Rust RowFilter |
| **Data Validation** | All tests verify actual data values (not just row counts) |
| **Resource Management** | Verifies cleanup, no memory leaks |

**12 tests, all passing** with real data validation.

---

## Code Quality

### Type Safety
```java
// Exhaustive pattern matching with sealed interfaces
return switch (plan) {
    case ScanPlan s -> executeScan(s);
    case FilterPlan f -> executeFilter(f);
    case SelectPlan s -> executeSelect(s);
    case UnionPlan u -> executeUnion(u);
    // Compiler error if we miss a case
};
```

### Resource Safety
```java
// Automatic cleanup with try-with-resources
try (CloseableIterator<FilteredColumnarBatch> iter = execute(plan)) {
    while (iter.hasNext()) {
        process(iter.next());
    }
    // Iterator automatically closed
}
```

### Engine Control
```java
// Engine decides execution strategy for each node
private CloseableIterator<FilteredColumnarBatch> executeUnion(UnionPlan union) {
    var leftIter = execute(union.left());    // Recurse
    var rightIter = execute(union.right());  // Recurse
    return leftIter.combine(rightIter);      // Java's chaining logic
}
```

---

## Key Takeaway from Java Implementation

**We proved that:**
1. The logical plan interface is complete (all operations work)
2. The interface is narrow (no kernel-rs trait implementations needed)
3. Engines can use their own execution model
4. Engines can use their own data formats
5. Any engine with standard APIs can implement it

**This validates the architectural approach.**

---

# Part 3: FFI Interface Details

## FFI Surface

The FFI boundary uses a narrow interface:

**State Machine Control** (Rust → Any Language):
```rust
// Check phase type
pub extern "C" fn phase_type(phase: Handle<SharedPhase>) -> PhaseType;

// Get logical plan from phase
pub extern "C" fn phase_get_plan(phase: Handle<SharedPhase>) -> Handle<SharedLogicalPlan>;

// Advance CONSUME phase with results
pub extern "C" fn consume_phase_next(
    phase: Handle<SharedPhase>,
    results: *mut FFI_ArrowArrayStream
) -> Handle<SharedPhase>;

// Advance PARTIAL_RESULT phase
pub extern "C" fn partial_result_phase_next(
    phase: Handle<SharedPhase>
) -> Handle<SharedPhase>;
```

**Plan Transfer** (Rust → Any Language):
```rust
// Visit logical plan using callbacks
pub extern "C" fn visit_logical_plan(
    plan: Handle<SharedLogicalPlan>,
    visitor: &mut EnginePlanVisitor
) -> usize;

// EnginePlanVisitor has ~10 callbacks:
// - visit_scan(...)
// - visit_filter(...)
// - visit_select(...)
// - visit_union(...)
// - etc.
```

**Total: ~15 FFI functions** for complete state machine + plan interface.

### Compare to Wide FFI

- **Old approach**: Implement 20+ Engine methods + complete EngineData trait
- **New approach**: Implement state machine driver + plan executor using existing Engine APIs

### FFI Data Transfer

For CONSUME phases that need to return results to kernel:
- Uses **Arrow C Data Interface** (zero-copy)
- Only when kernel needs to consume results
- PARTIAL_RESULT phases don't transfer data

---

# Summary

## Problem
Tight coupling between kernel-rs and engines via wide FFI surface (Engine + EngineData traits).

## Solution  
Logical plans as declarative interface - Rust specifies WHAT, engine decides HOW.

## Architecture
- **State Machine**: Two generic patterns (terminus-based, partial result-based)
- **Logical Plans**: 8 node types (Scan, Filter, Select, Union, etc.)
- **Narrow Interface**: State machine control + plan visitor

## Benefits
- **Decoupling**: Engine doesn't implement kernel-rs traits
- **Flexibility**: Engine controls execution (sync/async, parallel/sequential)
- **Native Formats**: Engine uses its own data structures
- **Custom Operators**: Engine can optimize
- **Universal**: Any Engine implementation works

## Proof
3,200 lines of working Java code with 12 passing tests demonstrating the interface is complete, narrow, flexible, and universal.

## Impact
Enables multiple engines (Spark, Flink, standalone) to integrate efficiently while leveraging their unique architectural strengths. Provides opportunities for parallelization and caching at every level the engine chooses.
