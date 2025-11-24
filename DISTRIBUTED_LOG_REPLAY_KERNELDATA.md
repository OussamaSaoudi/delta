# Distributed Log Replay with KernelData Serialization

## Overview

This document describes the implementation of distributed log replay using `KernelData` serialization without going through logical plans. This approach bridges PR #1160's simpler direct approach with our plan-based architecture by reusing the vectorized serialization layer.

## Architecture

### High-Level Flow

```
┌─────────────────────────────────────────────────────────────┐
│  Driver - Phase 1                                           │
│  ┌──────────────────────────────────────────┐              │
│  │ ScanLogReplayProcessor                   │              │
│  │  - Process commit logs                   │              │
│  │  - Build seen_file_keys (RemoveSet)      │              │
│  └──────────────────────────────────────────┘              │
│                    │                                         │
│                    │ export_state()                          │
│                    ▼                                         │
│         ┌────────────────────────┐                          │
│         │  SerializedScanState   │                          │
│         │  - remove_set          │                          │
│         │  - state_info          │                          │
│         └────────────────────────┘                          │
│                    │                                         │
│                    │ to_arrow()                              │
│                    ▼                                         │
│         ┌────────────────────────┐                          │
│         │ Arrow RecordBatch      │                          │
│         └────────────────────────┘                          │
└─────────────────────────────────────────────────────────────┘
                      │
                      │ Engine transfers via broadcast/partition
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  Executor - Phase 2                                         │
│         ┌────────────────────────┐                          │
│         │ Arrow RecordBatch      │                          │
│         └────────────────────────┘                          │
│                    │                                         │
│                    │ from_arrow()                            │
│                    ▼                                         │
│         ┌────────────────────────┐                          │
│         │  SerializedScanState   │                          │
│         └────────────────────────┘                          │
│                    │                                         │
│                    │ from_serialized_state()                 │
│                    ▼                                         │
│  ┌──────────────────────────────────────────┐              │
│  │ ScanLogReplayProcessor                   │              │
│  │  - Pre-populated seen_file_keys          │              │
│  │  - Continue processing data files        │              │
│  └──────────────────────────────────────────┘              │
└─────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. KernelData - Vectorized Data Structure

```rust
pub enum VectorizedKernelData {
    Bool(Vec<bool>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),
    String(Vec<String>),
    Binary(Vec<Vec<u8>>),
}

pub struct KernelData {
    pub data: Vec<VectorizedKernelData>,
    pub len: usize,
    pub schema: SchemaRef,
}
```

**Key Features:**
- Simple enum-based columnar storage
- Zero-copy FFI via Arrow C Data Interface
- Type-safe with schema validation
- Efficient for small to medium datasets (metadata, not data files)

### 2. Serialization Functions

#### RemoveSet (HashSet<FileActionKey>)

```rust
// Schema: (path: String, dv_unique_id: String)
pub fn serialize_remove_set(remove_set: &HashSet<FileActionKey>) -> DeltaResult<KernelData>
pub fn deserialize_remove_set(data: &KernelData) -> DeltaResult<HashSet<FileActionKey>>
```

**Use Case:** Transfer file deduplication state from driver to executors.

#### Transaction Set (HashSet<String>)

```rust
// Schema: (app_id: String)
pub fn serialize_txn_set(txns: &HashSet<String>) -> DeltaResult<KernelData>
pub fn deserialize_txn_set(data: &KernelData) -> DeltaResult<HashSet<String>>
```

**Use Case:** Transfer transaction deduplication state for checkpoint writing.

#### StateInfo Components

```rust
pub struct SerializedStateInfo {
    pub logical_schema: KernelData,      // JSON-serialized schema
    pub physical_schema: KernelData,     // JSON-serialized schema
    pub physical_predicate: KernelData,  // Variant + optional JSON
    pub transform_spec: KernelData,      // Debug format (TODO: proper serde)
}

pub fn serialize_state_info(state_info: &StateInfo) -> DeltaResult<SerializedStateInfo>
pub fn deserialize_state_info(serialized: &SerializedStateInfo) -> DeltaResult<StateInfo>
```

**Note:** Expression and transform serialization currently uses Debug format as a placeholder. Full serde support for expressions is needed for production use.

### 3. Processor State Export/Import

#### ScanLogReplayProcessor

```rust
pub struct SerializedScanState {
    pub remove_set: KernelData,
    pub state_info: SerializedStateInfo,
}

impl ScanLogReplayProcessor {
    /// Export current state for distribution to executors
    pub fn export_state(&self) -> DeltaResult<SerializedScanState>
    
    /// Reconstruct processor with pre-populated state on executor
    pub fn from_serialized_state(
        engine: &dyn Engine,
        serialized_state: SerializedScanState,
    ) -> DeltaResult<Self>
}
```

#### ActionReconciliationProcessor

```rust
pub struct SerializedReconciliationState {
    pub remove_set: KernelData,
    pub seen_txns: KernelData,
    pub seen_protocol: bool,
    pub seen_metadata: bool,
    pub minimum_file_retention_timestamp: i64,
    pub txn_expiration_timestamp: Option<i64>,
}

impl ActionReconciliationProcessor {
    /// Export current state for distribution to executors
    pub fn export_state(&self) -> DeltaResult<SerializedReconciliationState>
    
    /// Reconstruct processor with pre-populated state on executor
    pub fn from_serialized_state(
        serialized_state: SerializedReconciliationState,
    ) -> DeltaResult<Self>
}
```

## Usage Examples

### Example 1: Distributed Scan with RemoveSet Transfer

#### Driver Side (Phase 1)

```rust
use delta_kernel::scan::log_replay::ScanLogReplayProcessor;

// Phase 1: Driver processes commit logs
let state_info = Arc::new(StateInfo::try_new(
    logical_schema,
    table_configuration,
    predicate,
    (),
)?);

let mut processor = ScanLogReplayProcessor::new(engine, state_info);

// Process commit log actions on driver
for batch in commit_log_batches {
    let metadata = processor.process_actions_batch(batch)?;
    // ... handle metadata ...
}

// Export state for distribution
let serialized_state = processor.export_state()?;

// Convert to Arrow for transfer
let remove_set_arrow = serialized_state.remove_set.to_arrow()?;
let logical_schema_arrow = serialized_state.state_info.logical_schema.to_arrow()?;
// ... convert other components ...

// Engine broadcasts or partitions the state
engine.broadcast(remove_set_arrow);
```

#### Executor Side (Phase 2)

```rust
// Phase 2: Executor receives serialized state
let remove_set_kernel = KernelData::from_arrow(remove_set_arrow)?;
let logical_schema_kernel = KernelData::from_arrow(logical_schema_arrow)?;
// ... reconstruct other components ...

let serialized_state = SerializedScanState {
    remove_set: remove_set_kernel,
    state_info: SerializedStateInfo {
        logical_schema: logical_schema_kernel,
        // ... other components ...
    },
};

// Reconstruct processor with pre-populated state
let mut processor = ScanLogReplayProcessor::from_serialized_state(
    engine,
    serialized_state,
)?;

// Continue processing data file actions
for batch in data_file_batches {
    let metadata = processor.process_actions_batch(batch)?;
    // ... scan files ...
}
```

### Example 2: Distributed Checkpoint Writing

#### Driver Side

```rust
// Driver processes commit logs
let mut processor = ActionReconciliationProcessor::new(
    retention_timestamp,
    txn_expiration,
);

// Process all commit log files
for batch in commit_log_batches {
    let result = processor.process_actions_batch(batch)?;
    // ... checkpoint data ...
}

// Export state
let serialized_state = processor.export_state()?;

// Distribute to executors
let remove_set_arrow = serialized_state.remove_set.to_arrow()?;
let seen_txns_arrow = serialized_state.seen_txns.to_arrow()?;
engine.distribute_state(remove_set_arrow, seen_txns_arrow);
```

#### Executor Side

```rust
// Reconstruct processor from distributed state
let processor = ActionReconciliationProcessor::from_serialized_state(
    serialized_state,
)?;

// Process checkpoint files
for batch in checkpoint_file_batches {
    let result = processor.process_actions_batch(batch)?;
    // ... write checkpoint ...
}
```

## Comparison with PR #1160

### PR #1160: Non-Plan-Based Approach

**Characteristics:**
- Direct state transfer via trait objects
- Custom serialization per data structure
- Tighter coupling between kernel and engine
- Simpler initial implementation

**Architecture:**
```
Kernel State → Custom Serde → Opaque Blob → Engine → Executor
```

### Our Approach: KernelData-Based (Incremental to Plan-Based)

**Characteristics:**
- Reuses vectorized `KernelData` serialization
- Schema-driven with type safety
- Easier migration to plan-based approach
- Engine sees structured data (Arrow)

**Architecture:**
```
Kernel State → KernelData → Arrow RecordBatch → Engine → Executor
```

**Migration Path to Plan-Based:**
```rust
// Current approach (direct transfer):
let serialized_state = processor.export_state()?;
engine.transfer_opaque(serialized_state.to_arrow()?);

// Future plan-based approach (one line change):
let values_node = processor.export_state_as_values_node()?;
let plan = LogicalPlanNode::Values(values_node);
engine.execute(plan); // Engine interprets schema and optimizes
```

## Advantages Over PR #1160

### 1. **Reusable Infrastructure**
- `KernelData` already implemented and tested
- Works for any HashSet/Vec-based state
- Easy to add new state types

### 2. **Schema-Driven**
- Self-describing data with schemas
- Type-safe serialization/deserialization
- Easy to inspect and debug

### 3. **Arrow Integration**
- Efficient zero-copy FFI via Arrow C Data Interface
- Compatible with existing engine APIs
- Standard data format

### 4. **Incremental Path**
- Start with direct transfer (this approach)
- Later wrap in `ValuesNode` (one-line change)
- Eventually full plan-based optimization

### 5. **Engine Flexibility**
- Engine can choose broadcast vs. partition
- Engine can apply compression
- Engine can cache state

## Limitations and TODOs

### Current Limitations

1. **Expression Serialization**: PhysicalPredicate and TransformSpec use Debug format
   - **Impact**: Can't fully deserialize predicates with expressions
   - **Workaround**: Only None/StaticSkipAll predicates supported for now
   - **TODO**: Implement proper expression tree serialization

2. **StateInfo Deserialization**: Incomplete due to expression limitation
   - **Impact**: Can export but not import StateInfo with predicates
   - **Workaround**: For scan use cases without predicates or with None predicate
   - **TODO**: Add expression parsing/deserialization

3. **No FFI Integration**: Currently Rust-only
   - **Impact**: Can't call from Java/Scala yet
   - **TODO**: Add FFI visitor callbacks for `SerializedScanState`

### Future Enhancements

1. **Checkpoint Writer Integration**
   - Add `export_processor_state()` to `CheckpointWriter`
   - Add `checkpoint_data_distributed()` for executor-side processing
   - Enable true distributed checkpoint writing

2. **Expression Serialization**
   - Implement serde for `Expression` enum
   - Support full predicate serialization
   - Enable complex transform spec serialization

3. **Plan-Based Migration**
   - Wrap `SerializedProcessorState` in `ValuesNode`
   - Integrate with `LogicalPlanNode` enum
   - Let engine interpret and optimize distribution

4. **Performance Optimization**
   - Benchmark serialization overhead
   - Consider binary formats for large RemoveSets
   - Add compression hints for engine

## Testing

### Unit Tests

```rust
#[test]
fn test_remove_set_serialization() {
    let mut remove_set = HashSet::new();
    remove_set.insert(FileActionKey::new("file1.parquet", Some("dv1".to_string())));
    
    let kernel_data = serialize_remove_set(&remove_set).unwrap();
    let deserialized = deserialize_remove_set(&kernel_data).unwrap();
    
    assert_eq!(remove_set, deserialized);
}

#[test]
fn test_txn_set_serialization() {
    let mut txn_set = HashSet::new();
    txn_set.insert("app1".to_string());
    
    let kernel_data = serialize_txn_set(&txn_set).unwrap();
    let deserialized = deserialize_txn_set(&kernel_data).unwrap();
    
    assert_eq!(txn_set, deserialized);
}

#[test]
fn test_schema_serialization() {
    let schema = Arc::new(StructType::new_unchecked(vec![
        StructField::new("id", DataType::LONG, false),
    ]));
    
    let kernel_data = serialize_schema(&schema).unwrap();
    let deserialized = deserialize_schema(&kernel_data).unwrap();
    
    assert_eq!(schema, deserialized);
}
```

### Integration Tests (TODO)

```rust
#[test]
fn test_scan_state_export_import() {
    // Driver side
    let processor1 = ScanLogReplayProcessor::new(engine, state_info);
    // ... process actions ...
    let serialized = processor1.export_state().unwrap();
    
    // Executor side
    let processor2 = ScanLogReplayProcessor::from_serialized_state(
        engine,
        serialized,
    ).unwrap();
    
    // Verify same deduplication behavior
    // ... test ...
}
```

## Conclusion

This implementation provides a solid foundation for distributed log replay using `KernelData` serialization. It:

- ✅ Reuses existing vectorized data infrastructure
- ✅ Provides type-safe, schema-driven serialization
- ✅ Enables efficient Arrow-based FFI transfer
- ✅ Supports scan and checkpoint writing use cases
- ✅ Offers clear migration path to plan-based approach

**Next Steps:**
1. Implement expression serialization for full StateInfo support
2. Add FFI visitor callbacks for Java/Scala integration
3. Add CheckpointWriter distributed methods
4. Write comprehensive integration tests
5. Migrate to plan-based approach (wrap in `ValuesNode`)

This incremental approach demonstrates the value of vectorized serialization while maintaining compatibility with the eventual plan-based architecture.


