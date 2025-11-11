<!-- a9105cc5-db15-4d70-a960-0639691f408f 3b1eb14a-7ced-453d-bfba-82e55f87c722 -->
# Rewrite Presentation Documents with Correct Architectural Framing

## Core Changes

Replace performance-focused messaging with architectural decoupling benefits across all three documents.

## Key Additions Required

### 1. User's State Machine Diagram

Incorporate the provided diagram showing the flow and boundary between Kernel-rs (orange) and Engine/Driver (blue/cyan).

### 2. Pseudocode Examples

**State Machine Driver (Terminus-based)** - for Snapshot:

```
phase = snapshot_builder_into_state_machine(table_path)

// TODO: make this a generic terminus state machine driver
while true:
  type = phase_type(phase)
  if type == TERMINUS:
    snapshot = snapshot_get_from_terminus(phase)
    return snapshot
  else if type == CONSUME:
    plan = phase_get_plan(phase)
    results = engine.execute(plan)  // Engine controls execution
    phase = consume_phase_next(phase, results)  // Kernel consumes results
  else if type == PARTIAL_RESULT:
    return Err("unexpected for terminus based")  
```

**State Machine Driver (Partial Result-based)** - for Scan:

```
// SCAN should be turned into state machine, not scan_builder
phase = scan__into_state_machine(snapshot)

// TODO: make this a generic partial result scan builder.
while true:
  type = phase_type(phase)
  if type == TERMINUS:
    scan = scan_get_from_terminus(phase)
    // Finished executing
    return scan
  else if type == PARTIAL_RESULT:
    plan = phase_get_plan(phase)  // PARTIAL_RESULT produces plans too!
    scan_files = engine.execute(plan)  // Engine gets scan files
    // Engine can cache, parallelize, materialize - full control
    phase = partial_result_phase_next(phase)  // No results to kernel
  else if type == CONSUME:
    plan = phase_get_plan(phase)
    results = engine.execute(plan)
    phase = consume_phase_next(phase, results)
```

**Key Clarification**: Two patterns, shared FFI - no code duplication

- Both use same FFI: phase_type(), phase_get_plan(), phase_next()
- Terminus-based: Final result is extracted (Snapshot)
- Partial-result-based: Intermediate results not consumed by kernel (scan files)
- Engine chooses pattern based on what it's building

**Scan Execution Example**:

```
// Engine implements plan execution using its own APIs
function execute(plan):
  switch plan:
    case ScanPlan(files, schema):
      // Engine uses its own Parquet reader
      return engine.parquetHandler.read(files, schema)
    case UnionPlan(left, right):
      leftIter = execute(left)   // Engine controls recursion
      rightIter = execute(right)
      return chain(leftIter, rightIter)  // Engine's iterator model
    case SelectPlan(child, expressions, schema):
      batches = execute(child)
      // Engine uses its own expression evaluator
      return engine.expressionHandler.eval(batches, expressions, schema)
```

### 3. Show Engine API Compatibility

Add section: "Any engine implementing Engine APIs can implement logical plans"

```
// If engine has these APIs:
interface Engine {
  ParquetHandler getParquetHandler();
  JsonHandler getJsonHandler();
  ExpressionHandler getExpressionHandler();
}

// Then engine can execute logical plans:
class MyPlanExecutor {
  execute(ScanPlan) → engine.getParquetHandler().read()
  execute(SelectPlan) → engine.getExpressionHandler().eval()
  execute(FilterByExpressionPlan) → engine.getExpressionHandler().evalPredicate()
  // No need to implement Rust Engine trait
}
```

### 4. Fix FFI Interface Description

Update: "Narrow FFI surface: State machine control (phase_type, phase_get_plan, phase_next) + plan transfer (visit_logical_plan)"

## Documents to Update

### ONE_PAGE_SUMMARY.md

- Replace problem (lines 4-7): Architectural coupling, not performance
- Add state machine diagram (user's diagram)
- Add both driver patterns with clarification (no duplication)
- Add scan execution pseudocode (showing ExpressionHandler for SelectPlan)
- Add "Engine API compatibility" section
- Fix FFI interface description
- Reframe performance as "enables opportunities"

### PRESENTATION_SUMMARY.md

- Rewrite problem statement: Decoupling focus
- Add both driver pseudocode (terminus + partial result)
- Clarify: Two patterns, shared FFI, no duplication
- Add scan execution pseudocode (emphasize SelectPlan uses ExpressionHandler)
- Add "Universal: Any Engine implementation works" section
- Add state machine flow with user's diagram
- Add Java implementation proof (3200 LOC, 12 tests)
- Fix FFI interface description

### ARCHITECTURE_DIAGRAM.md

- Start with user's state machine diagram
- Add driver patterns section with both examples
- Clarify: Shared FFI, different patterns
- Add scan execution flow (show SelectPlan → ExpressionHandler)
- Show Engine API → Plan execution mapping
- Emphasize boundary and control flow
- Fix FFI interface description

## Tone Throughout

- Remove: "10x faster", "bottleneck", "slow", "performance problem"
- Add: "decoupled", "flexible", "engine control", "opportunities", "universal"
- Frame performance as: "Enables parallelization and caching opportunities"