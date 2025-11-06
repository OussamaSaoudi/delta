# Oxidized Kernel Architecture Diagrams

## State Machine Flow (Overview)

```
┌───────────────────────────────────────────────────────────────────────┐
│                   1) User makes request                                │
│                                                                         │
│   Java/Scala: snapshot.scanBuilder().withPredicate(pred).build()      │
└───────────────────────────────────┬───────────────────────────────────┘
                                    │
                                    ▼
┌───────────────────────────────────────────────────────────────────────┐
│                      KERNEL-RS (Rust)                                  │
│                                                                         │
│        2) Kernel produces a state machine                              │
│                                                                         │
│   ┌─────────────────────────────────────────────────────────┐        │
│   │  State Machine                                           │        │
│   │  • Manages phases (CONSUME, PARTIAL_RESULT, TERMINUS)   │        │
│   │  • Generates logical plans for each phase               │        │
│   │  • Declarative: Specifies WHAT to compute               │        │
│   └─────────────────────────────────────────────────────────┘        │
│                                                                         │
└───────────────────────────────────┬───────────────────────────────────┘
                                    │
                         NARROW INTERFACE
                              (boundary)
                                    │
                                    ▼
┌───────────────────────────────────────────────────────────────────────┐
│               ENGINE PLAN EXECUTION (Java/Scala)                       │
│                                                                         │
│   3) State machine node produces a plan → Engine executes              │
│                                                                         │
│   ┌─────────────────────────────────────────────────────────┐        │
│   │  Plan Executor                                           │        │
│   │  • Receives logical plan from Rust                       │        │
│   │  • Executes using engine's own APIs                     │        │
│   │  • Imperative: Controls HOW to compute                  │        │
│   │  • Uses engine's data format                            │        │
│   └─────────────────────────────────────────────────────────┘        │
│                                                                         │
└───────────────────────────────────┬───────────────────────────────────┘
                                    │
              ┌─────────────────────┼─────────────────────┐
              │                     │                     │
        .next() loop         3.5) Based on state      Returns
     until terminus          machine type, may        results
                            return results back
                            to kernel
              │                     │                     │
              └─────────────────────┼─────────────────────┘
                                    │
                                    ▼
┌───────────────────────────────────────────────────────────────────────┐
│                 STATE MACHINE DRIVER (Java/Scala)                      │
│                                                                         │
│   4) State machine driver calls .next() repeatedly until terminus      │
│                                                                         │
│   ┌─────────────────────────────────────────────────────────┐        │
│   │  loop {                                                  │        │
│   │    match phase.phase_type() {                            │        │
│   │      TERMINUS => break,                                  │        │
│   │      _ => { plan = phase.get_plan(); ... }              │        │
│   │    }                                                     │        │
│   │  }                                                       │        │
│   └─────────────────────────────────────────────────────────┘        │
│                                                                         │
└───────────────────────────────────┬───────────────────────────────────┘
                                    │
                                    ▼
┌───────────────────────────────────────────────────────────────────────┐
│                         TERMINUS                                       │
│                                                                         │
│   5) Upon hitting terminus, the state machine driver returns result    │
│                                                                         │
│   result = phase.get_from_terminus()                                  │
│   return result  // Snapshot, Scan, or collected data                 │
└───────────────────────────────────────────────────────────────────────┘
```

**Key Boundary**: The line shows where Rust ends and Engine/Driver begins.
- **Above**: Rust controls (generates state machine, produces plans)
- **Below**: Engine controls (executes plans, drives state machine, collects results)

---

## State Machine Driver Patterns

### Pattern 1: Terminus-Based (Final Result)

Used when building final result objects like Snapshot.

```
┌─────────────────────────────────────────────────────────────────┐
│  Generic Terminus-Based Driver                                   │
│  Returns: Final result object (Snapshot, etc.)                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  let mut phase = builder_into_state_machine(params);            │
│                                                                   │
│  loop {                                                          │
│    match phase.phase_type() {                                   │
│                                                                   │
│      PhaseType::Terminus => {                                    │
│        ┌────────────────────────────────────┐                  │
│        │ Extract final result                │                  │
│        │ let result = phase.get_from_terminus(); │             │
│        │ return Ok(result);                  │                  │
│        └────────────────────────────────────┘                  │
│      }                                                           │
│                                                                   │
│      PhaseType::Consume => {                                     │
│        ┌────────────────────────────────────┐                  │
│        │ 1. Get plan from kernel            │                  │
│        │ let plan = phase.get_plan()?;      │                  │
│        │                                     │                  │
│        │ 2. Engine executes plan            │                  │
│        │ let results = engine.execute(plan)?;│                 │
│        │                                     │                  │
│        │ 3. Return results to kernel        │                  │
│        │ phase = phase.consume_next(results)?;│                │
│        └────────────────────────────────────┘                  │
│      }                                                           │
│                                                                   │
│      PhaseType::PartialResult => {                               │
│        return Err("unexpected for terminus-based");             │
│      }                                                           │
│    }                                                             │
│  }                                                               │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘

Example: Snapshot Construction
  Phase 1 (CONSUME): Read protocol/metadata → send to kernel
  Phase 2 (CONSUME): List checkpoint files → send to kernel  
  Phase 3 (TERMINUS): Extract Snapshot object
```

### Pattern 2: Partial Result-Based (Intermediate Data)

Used when collecting intermediate results that stay in engine.

```
┌─────────────────────────────────────────────────────────────────┐
│  Generic Partial Result-Based Driver                             │
│  Returns: Collected intermediate results                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  let mut phase = scan.into_state_machine();                     │
│  // Note: scan, not scan_builder                                │
│                                                                   │
│  loop {                                                          │
│    match phase.phase_type() {                                   │
│                                                                   │
│      PhaseType::Terminus => {                                    │
│        ┌────────────────────────────────────┐                  │
│        │ All intermediate results collected  │                  │
│        │ return Ok(());                      │                  │
│        └────────────────────────────────────┘                  │
│      }                                                           │
│                                                                   │
│      PhaseType::PartialResult => {                               │
│        ┌────────────────────────────────────┐                  │
│        │ 1. Get plan from kernel            │                  │
│        │ let plan = phase.get_plan()?;      │                  │
│        │                                     │                  │
│        │ 2. Engine executes plan            │                  │
│        │ let data = engine.execute(plan)?;  │                  │
│        │                                     │                  │
│        │ 3. Engine keeps results            │                  │
│        │ // Cache, materialize, stream, etc │                  │
│        │                                     │                  │
│        │ 4. Advance (no results to kernel)  │                  │
│        │ phase = phase.partial_result_next()?;│                │
│        └────────────────────────────────────┘                  │
│      }                                                           │
│                                                                   │
│      PhaseType::Consume => {                                     │
│        ┌────────────────────────────────────┐                  │
│        │ 1. Get plan from kernel            │                  │
│        │ let plan = phase.get_plan()?;      │                  │
│        │                                     │                  │
│        │ 2. Engine executes plan            │                  │
│        │ let results = engine.execute(plan)?;│                 │
│        │                                     │                  │
│        │ 3. Return results to kernel        │                  │
│        │ phase = phase.consume_next(results)?;│                │
│        └────────────────────────────────────┘                  │
│      }                                                           │
│    }                                                             │
│  }                                                               │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘

Example: Scan Execution
  Phase 1 (PARTIAL_RESULT): Commit files → collect scan files in engine
  Phase 2 (PARTIAL_RESULT): Checkpoint → collect more scan files
  Phase 3 (TERMINUS): Done - engine has all scan files cached
```

### Shared Interface - No Duplication

```
┌─────────────────────────────────────────────────────────────────┐
│  Both Patterns Use Same State Machine Interface                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  trait StateMachinePhase {                                       │
│    // Check phase type                                           │
│    fn phase_type(&self) -> PhaseType;                           │
│                                                                   │
│    // Get plan to execute (works for all phase types)           │
│    fn get_plan(&self) -> DeltaResult<LogicalPlanNode>;         │
│                                                                   │
│    // Advance CONSUME phase, providing results to kernel        │
│    fn consume_next(self, results: ArrowStream)                  │
│        -> DeltaResult<StateMachinePhase>;                       │
│                                                                   │
│    // Advance PARTIAL_RESULT phase, no results to kernel        │
│    fn partial_result_next(self) -> DeltaResult<StateMachinePhase>;│
│  }                                                               │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘

Key Difference:
  • Terminus-based: Engine sends results back (CONSUME)
  • Partial result-based: Engine keeps results (PARTIAL_RESULT)
```

---

## Plan Execution Flow

### Engine Executes Plan Using Its Own APIs

```
┌───────────────────────────────────────────────────────────────────┐
│  Engine Plan Executor                                              │
│  (Engine has full control)                                         │
├───────────────────────────────────────────────────────────────────┤
│                                                                     │
│  fn execute(&self, plan: LogicalPlanNode)                          │
│      -> DeltaResult<Box<dyn Iterator<Item = Batch>>> {            │
│                                                                     │
│    match plan {                                                    │
│                                                                     │
│      ┌─────────────────────────────────────────────────┐         │
│      │ LogicalPlanNode::Scan(scan) => {                │         │
│      │   // Engine uses its own Parquet reader         │         │
│      │   self.engine.parquet_handler()                 │         │
│      │       .read(scan.files, scan.schema)            │         │
│      │ }                                                │         │
│      │                                                   │         │
│      │ Engine controls:                                 │         │
│      │ • Which reader implementation                    │         │
│      │ • Batch sizes                                    │         │
│      │ • Parallelization                                │         │
│      │ • Data format (ColumnarBatch, Arrow, etc.)      │         │
│      └─────────────────────────────────────────────────┘         │
│                                                                     │
│      ┌─────────────────────────────────────────────────┐         │
│      │ LogicalPlanNode::Union(union) => {               │         │
│      │   let left = self.execute(*union.a)?;           │         │
│      │   let right = self.execute(*union.b)?;          │         │
│      │   Ok(Box::new(left.chain(right)))               │         │
│      │ }                                                 │         │
│      │                                                   │         │
│      │ Engine controls:                                 │         │
│      │ • Iterator chaining strategy                     │         │
│      │ • Lazy vs eager evaluation                       │         │
│      │ • Memory management                              │         │
│      └─────────────────────────────────────────────────┘         │
│                                                                     │
│      ┌─────────────────────────────────────────────────┐         │
│      │ LogicalPlanNode::Select(select) => {             │         │
│      │   let batches = self.execute(*select.child)?;   │         │
│      │   self.project_columns(batches,                 │         │
│      │                        &select.output_type)      │         │
│      │ }                                                 │         │
│      │                                                   │         │
│      │ Engine controls:                                 │         │
│      │ • Projection implementation                      │         │
│      │ • Column reordering strategy                     │         │
│      │ • Could use ExpressionHandler for computed cols  │         │
│      └─────────────────────────────────────────────────┘         │
│                                                                     │
│      ┌─────────────────────────────────────────────────┐         │
│      │ LogicalPlanNode::FilterByExpression(filter) => { │         │
│      │   let batches = self.execute(*filter.child)?;   │         │
│      │   self.engine.expression_handler()              │         │
│      │       .filter(batches, &filter.predicate)       │         │
│      │ }                                                 │         │
│      │                                                   │         │
│      │ Engine controls:                                 │         │
│      │ • Expression evaluation strategy                 │         │
│      │ • Predicate pushdown opportunities               │         │
│      │ • Vectorization                                  │         │
│      └─────────────────────────────────────────────────┘         │
│    }                                                               │
│  }                                                                 │
│                                                                     │
└───────────────────────────────────────────────────────────────────┘
```

---

## Engine API to Plan Mapping

Shows how any engine with standard APIs can execute plans.

```
┌─────────────────────────────────────────────────────────────────┐
│  Standard Engine APIs                                            │
│  (What engines typically have)                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  trait Engine {                                                  │
│    fn parquet_handler(&self) -> &dyn ParquetHandler;           │
│    fn json_handler(&self) -> &dyn JsonHandler;                 │
│    fn expression_handler(&self) -> &dyn ExpressionHandler;     │
│    fn file_system_client(&self) -> &dyn FileSystemClient;      │
│  }                                                               │
│                                                                   │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      │ Maps to
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│  Logical Plan Execution                                          │
│  (How to execute each plan type)                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ScanNode(files, schema) =>                                     │
│    engine.parquet_handler().read(files, schema)                 │
│                                                                   │
│  SelectNode(child, schema) =>                                   │
│    project_columns(execute(child), schema)                      │
│    // OR engine.expression_handler().eval(...)                  │
│                                                                   │
│  FilterByExpressionNode(child, predicate) =>                    │
│    engine.expression_handler().filter(                          │
│        execute(child), predicate)                               │
│                                                                   │
│  FileListingNode(path) =>                                       │
│    engine.file_system_client().list_from(path)                  │
│                                                                   │
│  UnionNode(left, right) =>                                      │
│    execute(left).chain(execute(right))                          │
│                                                                   │
│  ParseJsonNode(child, json_col, schema, out_col) =>            │
│    engine.json_handler().parse_json(...)                        │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘

Key: No need to implement kernel-rs Engine trait!
     Just map plans to existing Engine APIs.
```

---

## Different Engines, Same Interface

```
┌────────────────────────────────────────────────────────────────────┐
│                        Logical Plan                                 │
│                  (Same for all engines)                             │
│                                                                      │
│  ScanNode { files: [f1, f2], schema: struct<id:int, name:string> }│
└──────────────────────────────┬─────────────────────────────────────┘
                               │
               ┌───────────────┼───────────────┐
               │               │               │
               ▼               ▼               ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│  Spark Engine   │ │  Flink Engine   │ │Standalone Engine│
├─────────────────┤ ├─────────────────┤ ├─────────────────┤
│                 │ │                 │ │                 │
│ Execution:      │ │ Execution:      │ │ Execution:      │
│ • Distributed   │ │ • Async I/O     │ │ • Sync read     │
│ • RDD parallel  │ │ • Streaming     │ │ • Single thread │
│ • Task queue    │ │ • Backpressure  │ │ • Simple loop   │
│                 │ │                 │ │                 │
│ Data Format:    │ │ Data Format:    │ │ Data Format:    │
│ • InternalRow   │ │ • Flink Row     │ │ • ColumnarBatch │
│ • ColumnarBatch │ │ • RowData       │ │                 │
│                 │ │                 │ │                 │
│ Caching:        │ │ Caching:        │ │ Caching:        │
│ • RDD cache     │ │ • State backend │ │ • In-memory Vec │
│ • Catalyst opts │ │ • Managed state │ │ • Simple array  │
│                 │ │                 │ │                 │
└─────────────────┘ └─────────────────┘ └─────────────────┘
       │                    │                    │
       └────────────────────┴────────────────────┘
                           │
                           ▼
              All produce Iterator<Batch>
              (in their own format)
```

**Key**: Same logical plan, completely different execution strategies.

---

## Architectural Decision: Why Logical Plans?

```
┌────────────────────────────────────────────────────────────────────┐
│  Alternative 1: Wide Interface (Current Approach)                   │
├────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Engine must implement:                                             │
│  • 20+ Engine trait methods (read_parquet, read_json, etc.)        │
│  • Complete EngineData trait (ColumnarBatch, ColumnVector, etc.)   │
│  • Rust-specific iterator types                                     │
│                                                                      │
│  Problems:                                                          │
│  ❌ Tight coupling                                                  │
│  ❌ Cannot use engine's native data format                          │
│  ❌ Cannot control execution strategy                               │
│  ❌ Difficult to version and maintain                               │
│                                                                      │
└────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────┐
│  Alternative 2: Serialize Plans to JSON                             │
├────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Rust → JSON string → Engine parses                                │
│                                                                      │
│  Problems:                                                          │
│  ❌ Serialization overhead (O(plan size))                          │
│  ❌ Parsing overhead                                                │
│  ❌ Type safety lost                                                │
│  ✅ Language agnostic (good for Python/JS)                         │
│                                                                      │
└────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────┐
│  ✅ Our Approach: Logical Plans via Visitor Pattern                │
├────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Rust → Visitor callbacks → Engine reconstructs plan tree          │
│                                                                      │
│  Benefits:                                                          │
│  ✅ Narrow interface (O(plan nodes), not O(plan size))             │
│  ✅ Type-safe across boundary                                       │
│  ✅ No serialization overhead                                       │
│  ✅ Engine controls execution                                       │
│  ✅ Engine uses native data formats                                │
│  ✅ Easier to version (just plan schema)                           │
│  ✅ Natural abstraction boundary (declarative vs imperative)       │
│                                                                      │
└────────────────────────────────────────────────────────────────────┘
```

---

## FFI Interface Details

### FFI Surface

The boundary between Rust and engines uses:

**State Machine Control**:
```rust
// Check phase type
pub extern "C" fn phase_type(
    phase: Handle<SharedPhase>
) -> PhaseType;

// Get logical plan from phase
pub extern "C" fn phase_get_plan(
    phase: Handle<SharedPhase>
) -> Handle<SharedLogicalPlan>;

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

**Plan Transfer**:
```rust
// Visit logical plan using callbacks
pub extern "C" fn visit_logical_plan(
    plan: Handle<SharedLogicalPlan>,
    visitor: &mut EnginePlanVisitor
) -> usize;

// EnginePlanVisitor has ~10 callback function pointers
#[repr(C)]
pub struct EnginePlanVisitor {
    pub data: *mut c_void,
    pub make_plan_list: extern "C" fn(...) -> usize,
    pub visit_scan: Option<extern "C" fn(...)>,
    pub visit_filter: Option<extern "C" fn(...)>,
    pub visit_select: Option<extern "C" fn(...)>,
    pub visit_union: Option<extern "C" fn(...)>,
    // ... etc (~10 callbacks total)
}
```

**Total: ~15 functions** for complete interface.

### Compare to Wide Interface

```
Old Approach (Wide Interface):
  ├─ Engine trait: 20+ methods
  ├─ EngineData trait: Complete implementation
  ├─ Custom iterator types
  └─ Total: 30+ functions/types to implement

New Approach (Logical Plans):
  ├─ State machine: 4 functions
  ├─ Plan visitor: 1 function + ~10 callbacks
  └─ Total: ~15 functions
```

### FFI Data Transfer

For CONSUME phases that return results to kernel:
- Uses **Arrow C Data Interface** (`FFI_ArrowArrayStream`)
- Zero-copy when possible
- Only for phases where kernel consumes results
- PARTIAL_RESULT phases don't transfer data back

---

## Summary Diagram

```
╔═══════════════════════════════════════════════════════════════════════╗
║                    ARCHITECTURAL BENEFITS                              ║
╠═══════════════════════════════════════════════════════════════════════╣
║                                                                         ║
║  RUST KERNEL (delta-kernel-rs)                                        ║
║  ┌───────────────────────────────────────────────────────────────┐   ║
║  │ Responsibilities:                                              │   ║
║  │ • Read Delta transaction log                                  │   ║
║  │ • Parse protocol/metadata/actions                             │   ║
║  │ • Apply deduplication logic                                   │   ║
║  │ • Generate logical plans (WHAT to compute)                    │   ║
║  │ • Manage state machine phases                                 │   ║
║  └───────────────────────────────────────────────────────────────┘   ║
║                              ↓                                          ║
║                     NARROW INTERFACE                                   ║
║                   (state machine + plans)                              ║
║                              ↓                                          ║
║  ENGINE / CONNECTOR (Java/Scala/C++/Python)                           ║
║  ┌───────────────────────────────────────────────────────────────┐   ║
║  │ Responsibilities:                                              │   ║
║  │ • Drive state machine                                         │   ║
║  │ • Execute logical plans (HOW to compute)                      │   ║
║  │ • Control execution strategy (sync/async, parallel)          │   ║
║  │ • Use native data formats (InternalRow, Arrow, etc.)         │   ║
║  │ • Substitute custom operators                                 │   ║
║  │ • Control caching and materialization                         │   ║
║  │ • Decide parallelization granularity                          │   ║
║  └───────────────────────────────────────────────────────────────┘   ║
║                                                                         ║
║  RESULT: Decoupled, flexible, universal interface                     ║
║                                                                         ║
╚═══════════════════════════════════════════════════════════════════════╝
```

**Enablement**: Provides opportunities for parallelization and caching at every level the engine chooses.
