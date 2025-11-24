# Distributed Log Replay with ValuesNode

## Overview

This document demonstrates how to use ValuesNode for distributed log replay, where the RemoveSet (deduplication state) is serialized from the driver and transferred to executors.

## Architecture

```
┌──────────────────────────────────────┐
│ DRIVER (Phase 1)                     │
│                                      │
│ 1. Create ScanLogReplayProcessor     │
│ 2. Process log actions               │
│ 3. Build RemoveSet (deduplication)   │
│ 4. Serialize to ValuesNode           │
│                                      │
│    remove_set_node = processor       │
│      .export_remove_set_as_values_node()│
│                                      │
└────────────┬─────────────────────────┘
             │
             │ Engine Distributes
             │ (broadcast/partition)
             │
       ┌─────┴──────┬──────────┐
       ▼            ▼          ▼
┌──────────────────────────────────────┐
│ EXECUTOR 1, 2, 3... (Phase 2)        │
│                                      │
│ 1. Receive ValuesNode                │
│ 2. Execute ValuesNode plan           │
│ 3. Deserialize to RemoveSet          │
│ 4. Continue log replay with state    │
│                                      │
│    let data = execute(values_node)?; │
│    let remove_set =                  │
│      deserialize_remove_set(&data)?; │
│                                      │
└──────────────────────────────────────┘
```

## Code Example

### Driver Side (Rust)

```rust
use std::collections::HashSet;
use std::sync::Arc;
use delta_kernel::scan::log_replay::ScanLogReplayProcessor;
use delta_kernel::kernel_df::{ValuesNode, LogicalPlanNode};
use delta_kernel::log_replay::FileActionKey;

// Phase 1: Driver performs initial log replay
fn phase_1_driver(engine: &dyn Engine, state_info: Arc<StateInfo>) 
    -> DeltaResult<LogicalPlanNode> 
{
    // Create processor
    let mut processor = ScanLogReplayProcessor::new(engine, state_info);
    
    // Process log actions (checkpoint + commit files)
    // This builds up the seen_file_keys (RemoveSet) for deduplication
    for actions_batch in log_actions_iter {
        processor.process_actions_batch(actions_batch)?;
    }
    
    // Export RemoveSet as ValuesNode
    let remove_set_node = processor.export_remove_set_as_values_node()?;
    
    println!("RemoveSet contains {} deduplicated file actions", 
             processor.seen_file_keys().len());
    
    // Return as logical plan for engine to distribute
    Ok(LogicalPlanNode::Values(remove_set_node))
}
```

### Executor Side (Rust)

```rust
use delta_kernel::kernel_data::deserialize_remove_set;
use delta_kernel::kernel_df::DefaultPlanExecutor;

// Phase 2: Executor receives and uses RemoveSet
fn phase_2_executor(
    engine: &dyn Engine,
    remove_set_plan: LogicalPlanNode,
    state_info: Arc<StateInfo>,
) -> DeltaResult<()> 
{
    // Execute the ValuesNode to get the data
    let executor = DefaultPlanExecutor { engine: Arc::new(engine) };
    let mut data_iter = executor.execute(remove_set_plan)?;
    
    // Get the single batch containing RemoveSet data
    let batch = data_iter.next()
        .ok_or_else(|| Error::generic("No data in ValuesNode"))??;
    
    // Deserialize back to HashSet<FileActionKey>
    let remove_set = deserialize_remove_set(&batch)?;
    
    println!("Executor received RemoveSet with {} entries", remove_set.len());
    
    // Create a new processor with the deserialized RemoveSet
    // (Implementation would need to support this - currently internal)
    let mut processor = ScanLogReplayProcessor::with_remove_set(
        engine,
        state_info,
        remove_set,
    )?;
    
    // Continue processing remaining log actions on executor
    for actions_batch in sidecar_files_iter {
        processor.process_actions_batch(actions_batch)?;
    }
    
    Ok(())
}
```

### Engine Integration (Conceptual - Spark/Flink)

```scala
// Spark Example
def distributedLogReplay(
    tablePath: String,
    engine: Engine,
    stateInfo: StateInfo
): DataFrame = {
  
  // Driver: Phase 1 - Build RemoveSet
  val removeSetPlan: LogicalPlanNode = phase1Driver(engine, stateInfo)
  
  // Broadcast RemoveSet to all executors
  val removeSetBroadcast = sparkContext.broadcast(removeSetPlan)
  
  // Partition sidecar files across executors
  val sidecarFiles: RDD[FilePath] = getSidecarFiles(tablePath)
  
  // Executors: Phase 2 - Process with RemoveSet
  val scanResults = sidecarFiles.mapPartitions { partition =>
    val removeSetPlan = removeSetBroadcast.value
    phase2Executor(engine, removeSetPlan, stateInfo, partition)
  }
  
  scanResults.toDF()
}
```

## Data Format

### RemoveSet Schema

```
struct RemoveSet {
    path: String (non-null),
    dv_unique_id: String (nullable)
}
```

### Example Data

```
| path                  | dv_unique_id |
|-----------------------|--------------|
| "file1.parquet"       | "dv_abc123"  |
| "file2.parquet"       | NULL         |
| "file3.parquet"       | "dv_xyz789"  |
```

## Performance Characteristics

### Serialization Cost
- **Time**: O(n) where n = number of file actions
- **Space**: O(n) for column vectors
- **Copy**: One copy during KernelData → Arrow conversion

### Transfer Cost
- **Network**: Proportional to RemoveSet size
- **Format**: Arrow C Data Interface (efficient columnar)
- **Compression**: Engine decides (can compress before broadcast)

### Deserialization Cost
- **Time**: O(n) to rebuild HashSet
- **Space**: O(n) for HashSet storage

## Typical Sizes

For reference, RemoveSet sizes in practice:

| Table Size | # Files | # Versions | RemoveSet Size | Serialized Size |
|------------|---------|------------|----------------|-----------------|
| Small      | 100     | 10         | ~50 entries    | ~10 KB         |
| Medium     | 10K     | 100        | ~5K entries    | ~1 MB          |
| Large      | 100K    | 1000       | ~50K entries   | ~10 MB         |
| Very Large | 1M      | 10K        | ~500K entries  | ~100 MB        |

## Benefits

✅ **Distributed Processing**: Executors can process sidecar files in parallel  
✅ **Correct Deduplication**: All executors share same RemoveSet state  
✅ **Efficient Transfer**: Columnar format minimizes network overhead  
✅ **Engine Control**: Engine decides broadcast vs partition strategy  
✅ **Type-Safe**: Rust ensures correct serialization/deserialization  

## Future Enhancements

### Chunking for Large RemoveSets
```rust
// For very large RemoveSets (> 1M entries), chunk into multiple batches
fn export_remove_set_chunked(&self, chunk_size: usize) 
    -> DeltaResult<Vec<ValuesNode>> 
{
    // Split RemoveSet into chunks
    // Return multiple ValuesNodes
}
```

### Incremental Updates
```rust
// Only transfer delta (new removes) instead of full RemoveSet
fn export_remove_set_delta(&self, baseline: &HashSet<FileActionKey>) 
    -> DeltaResult<ValuesNode> 
{
    // Only serialize entries not in baseline
}
```

### Compression
```rust
// Engine-side compression before broadcast
fn compress_remove_set_node(node: ValuesNode) -> CompressedNode {
    // Apply zstd/lz4 compression
}
```

## See Also

- `VALUES_NODE_IMPLEMENTATION.md` - ValuesNode implementation details
- `kernel_data.rs` - Serialization functions
- `scan/log_replay.rs` - ScanLogReplayProcessor integration


