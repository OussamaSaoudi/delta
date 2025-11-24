# Distributed Log Replay: Architecture Comparison

## Overview

This document compares two approaches to solving the distributed log replay problem:
1. **PR #1160**: Non-plan-based approach (WIP)
2. **Our Implementation**: Plan-based approach using `ValuesNode`

Both aim to enable distributed log replay where the driver can serialize kernel state (RemoveSet, StateInfo) and transfer it to executors.

---

## The Problem

**Context**: Log replay processes Delta transaction logs to build table state. For large tables:
- **Driver** processes metadata (log files) to determine which data files to scan
- **Executors** need deduplication state (RemoveSet) and schema/transform info (StateInfo)
- **Challenge**: How to efficiently serialize and transfer kernel-side state from driver to executors?

---

## PR #1160: Non-Plan-Based Approach

### Key Changes

#### 1. Trait Design Modification (`log_replay.rs`)
```rust
// Before
pub(crate) trait LogReplayProcessor: Sized { ... }

// After
pub(crate) trait LogReplayProcessor {
    fn process_actions_iter(...)
    where
        Self: Sized,
    { ... }
}
```

**Purpose**: Allows trait objects (`dyn LogReplayProcessor`) while keeping iterator support for concrete types.

#### 2. Container Value Nullability (`derive-macros`)
- Removes restriction that `allow_null_container_values` only applies to HashMap
- Adds attribute to `Add` action's `tags` field

```rust
#[allow_null_container_values]
pub tags: Option<HashMap<String, String>>,
```

#### 3. Checkpoint Processor Enhancement
- Adds `#[derive(Debug)]` to `CheckpointLogReplayProcessor`
- Likely prepares for more sophisticated state management

### Architecture Implications

```
┌─────────────────────────────────────────────────────────────┐
│  Driver                                                     │
│  ┌──────────────────────────────────────┐                  │
│  │ LogReplayProcessor (trait object)    │                  │
│  │  - seen_file_keys: HashSet           │                  │
│  │  - partition_filter: Option<Pred>    │                  │
│  └──────────────────────────────────────┘                  │
│                    │                                         │
│                    │ Custom Serialization?                  │
│                    ▼                                         │
│         ┌────────────────────┐                              │
│         │ Serialized State   │                              │
│         │ (Format TBD)       │                              │
│         └────────────────────┘                              │
└─────────────────────────────────────────────────────────────┘
                      │
                      │ Transfer via engine
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  Executor                                                   │
│  ┌──────────────────────────────────────────┐              │
│  │ Reconstruct LogReplayProcessor           │              │
│  │ Custom Deserialization?                  │              │
│  └──────────────────────────────────────────┘              │
└─────────────────────────────────────────────────────────────┘
```

### Characteristics

**Pros:**
- Minimal trait changes
- Potentially simpler initial implementation
- Direct state transfer

**Cons:**
- ❌ **Tight Coupling**: Engine must understand kernel's internal state representation
- ❌ **Custom Serialization**: Requires bespoke serialization for each state structure
- ❌ **No Engine Control**: Engine cannot decide how to distribute/partition the data
- ❌ **Not Declarative**: Doesn't fit the "logical plan" philosophy of Oxidized Kernel
- ❌ **Limited Extensibility**: Each new state type needs new serialization logic

---

## Our Approach: Plan-Based with `ValuesNode`

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Driver - Phase 1 (Rust Kernel)                             │
│  ┌──────────────────────────────────────────┐              │
│  │ ScanLogReplayProcessor                   │              │
│  │  - seen_file_keys: HashSet<FileActionKey>│              │
│  │  - state_info: Arc<StateInfo>            │              │
│  └──────────────────────────────────────────┘              │
│                    │                                         │
│                    │ export_remove_set_as_values_node()     │
│                    ▼                                         │
│         ┌────────────────────────┐                          │
│         │  ValuesNode            │                          │
│         │  - schema: SchemaRef   │                          │
│         │  - data: KernelData    │                          │
│         └────────────────────────┘                          │
│                    │                                         │
│                    │ Via LogicalPlan enum                   │
│                    ▼                                         │
│         ┌────────────────────────┐                          │
│         │ LogicalPlanNode::Values│                          │
│         └────────────────────────┘                          │
└─────────────────────────────────────────────────────────────┘
                      │
                      │ FFI Transfer (Arrow RecordBatch)
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  Engine - Distribution Layer                                │
│  ┌──────────────────────────────────────────┐              │
│  │ DefaultPlanExecutor::executeValues()     │              │
│  │  - Receives: ValuesNode with Arrow data  │              │
│  │  - Decides: Broadcast? Partition?        │              │
│  │  - Executes: Using engine's APIs         │              │
│  └──────────────────────────────────────────┘              │
└─────────────────────────────────────────────────────────────┘
                      │
                      │ Distributed to executors
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  Executor - Phase 2 (Rust Kernel)                           │
│  ┌──────────────────────────────────────────┐              │
│  │ deserialize_remove_set(kernel_data)      │              │
│  │  → HashSet<FileActionKey>                │              │
│  │                                           │              │
│  │ Continue log replay with restored state  │              │
│  └──────────────────────────────────────────┘              │
└─────────────────────────────────────────────────────────────┘
```

### Key Components

#### 1. **Vectorized Data Structure** (`kernel_data.rs`)
```rust
pub enum VectorizedKernelData {
    Bool(Vec<bool>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    String(Vec<String>),
    // ... more types
}

pub struct KernelData {
    pub data: Vec<VectorizedKernelData>,
    pub len: usize,
    pub schema: SchemaRef,
}
```

#### 2. **Logical Plan Node** (`kernel_df.rs`)
```rust
pub enum LogicalPlanNode {
    Scan(ScanNode),
    Filter(FilterNode),
    Values(ValuesNode),  // ← New for constant relations
    // ...
}

pub struct ValuesNode {
    pub schema: SchemaRef,
    pub data: Arc<KernelData>,
}
```

#### 3. **RemoveSet Serialization** (`kernel_data.rs`)
```rust
pub fn serialize_remove_set(
    remove_set: &HashSet<FileActionKey>
) -> DeltaResult<KernelData>

pub fn deserialize_remove_set(
    data: &KernelData
) -> DeltaResult<HashSet<FileActionKey>>
```

#### 4. **Integration with Processor** (`scan/log_replay.rs`)
```rust
impl ScanLogReplayProcessor {
    pub fn export_remove_set_as_values_node(&self) 
        -> DeltaResult<ValuesNode> {
        let kernel_data = serialize_remove_set(&self.seen_file_keys)?;
        Ok(ValuesNode {
            schema: kernel_data.schema.clone(),
            data: Arc::new(kernel_data),
        })
    }
}
```

### Characteristics

**Pros:**
- ✅ **Declarative**: Kernel says "here's data with this schema"
- ✅ **Engine Agnostic**: Engine decides distribution strategy (broadcast, partition, compress)
- ✅ **Unified Abstraction**: Reuses existing plan execution infrastructure
- ✅ **Schema-Driven**: Self-describing data with type safety
- ✅ **Extensible**: New state types just need schema + serialize/deserialize
- ✅ **Zero-Copy FFI**: `KernelData` → Arrow via `to_arrow()` for efficient transfer
- ✅ **Testable**: Clear boundaries between kernel, FFI, and engine layers

**Cons:**
- More upfront design complexity
- Requires FFI visitor implementation (follow-on work)

---

## Detailed Comparison

| Aspect | PR #1160 | Our Plan-Based Approach |
|--------|----------|-------------------------|
| **Philosophy** | Imperative state transfer | Declarative logical plan |
| **Coupling** | Tight (engine knows kernel internals) | Loose (engine sees schema + data) |
| **Extensibility** | Custom per-state-type | Generic via schema |
| **Engine Control** | Limited | Full (broadcast, partition, compress) |
| **FFI Complexity** | Custom serialization | Standard Arrow interface |
| **Testing** | Integration-heavy | Unit + Integration |
| **Alignment with Oxidized Kernel** | Partial | Full |

---

## Example: RemoveSet Transfer

### PR #1160 Approach (Hypothetical)
```rust
// Driver
let processor = ScanLogReplayProcessor::new(...);
// ... process actions ...
let serialized = processor.serialize_state()?; // Custom format

// Engine (must understand kernel internals)
engine.transfer_opaque_blob(serialized);

// Executor
let processor = ScanLogReplayProcessor::deserialize(blob)?;
```

### Our Plan-Based Approach
```rust
// Driver - Kernel generates plan
let processor = ScanLogReplayProcessor::new(...);
// ... process actions ...
let values_node = processor.export_remove_set_as_values_node()?;
let plan = LogicalPlanNode::Values(values_node);

// Engine - Decides strategy
match plan {
    LogicalPlanNode::Values(node) => {
        let arrow_batch = node.data.to_arrow()?;
        // Engine decides: broadcast? partition?
        spark.broadcast(arrow_batch) // or
        spark.distribute(arrow_batch, partitions=10)
    }
}

// Executor - Kernel reconstructs
let arrow_batch = /* received from engine */;
let kernel_data = KernelData::from_arrow(arrow_batch)?;
let remove_set = deserialize_remove_set(&kernel_data)?;
```

---

## StateInfo Serialization

### Next Steps for Our Approach

**StateInfo contains:**
```rust
pub struct StateInfo {
    pub logical_schema: SchemaRef,
    pub physical_schema: SchemaRef,
    pub physical_predicate: PhysicalPredicate,
    pub transform_spec: Option<Arc<TransformSpec>>,
}
```

**Serialization Strategy:**
1. **Schemas**: Already serializable (can convert to JSON or nested struct)
2. **PhysicalPredicate**: Expression tree (can serialize as nested struct)
3. **TransformSpec**: Transform expressions (can serialize as nested struct)

**Implementation:**
```rust
pub fn serialize_state_info(
    state_info: &StateInfo
) -> DeltaResult<KernelData> {
    // Convert to columnar representation
    // Schema can be JSON strings or nested structs
    // Predicates as expression trees
}
```

---

## Recommendations

### Why Plan-Based is Superior

1. **Architectural Consistency**
   - Oxidized Kernel's core value proposition is *decoupling via logical plans*
   - PR #1160 creates a backdoor for tight coupling

2. **Engine Empowerment**
   - Engines know their distributed systems best
   - Let Spark decide broadcast vs. partition
   - Let engines apply compression

3. **Future-Proofing**
   - New kernel state? Just add schema + ser/deser
   - New engine? Implement `executeValues()`
   - No custom per-state-type FFI

4. **Testing & Debugging**
   - Plan-based: inspect the plan, verify schema, test ser/deser independently
   - Custom serialization: black box blob

### Migration Path

If we adopt plan-based now:
- ✅ RemoveSet serialization: **DONE**
- 🔄 StateInfo serialization: **Next**
- 🔄 FFI visitor callback: **Follow-on**
- 🔄 End-to-end integration test: **Follow-on**

---

## Conclusion

**PR #1160** appears to be exploring a more direct, imperative approach to distributed log replay. While potentially simpler initially, it sacrifices the core architectural principle of the Oxidized Kernel: declarative logical plans that decouple kernel from engine.

**Our Plan-Based Approach** fully embraces the logical plan philosophy:
- Kernel declares "WHAT" (constant relation with schema)
- Engine decides "HOW" (broadcast, partition, compress)
- Clean separation of concerns
- Extensible and testable

**Recommendation**: Continue with plan-based approach. It's more work upfront but aligns with the Oxidized Kernel's vision and provides a more maintainable, extensible foundation for distributed log replay.

---

## References

- PR #1160: https://github.com/delta-io/delta-kernel-rs/pull/1160/files
- Our Implementation: `REMOVESET_IMPLEMENTATION_SUMMARY.md`
- Architecture Docs: `ONE_PAGE_SUMMARY.md`, `ARCHITECTURE_DIAGRAM.md`


