# Two-Phase Log Replay API - Implementation Status

## Goal
Implement a two-phase iterator API for distributed log replay that matches this pattern:

```rust
// 1) initialize snapshot
let snapshot = Snapshot::builder_for(url).build(&engine)?;

// Start phase 1 replay. This reads json files and top-level checkpoint in parallel.
let mut phase1 = snapshot.phase_1_log_replay(engine)?;

// Iterate over each batch
for batch in phase1 {
    // Process batches. Each batch contains selected Add actions
}

// Complete phase 1. All batches in the json and top-level manifest file are processed
let (processor, sidecars) = phase1.finish()?;

// If there are still manifest lists to process, distribute them
if !sidecars.empty() {
    let serialized_processor = processor.serialize();
    // Distribute to executors...
}

// EXECUTOR-SIDE:
let processor = Processor.deserialize(serialized_processor);
let phase2 = processor.phase_2_log_replay(sidecars);
for batch in phase2 {
    // Process batches from checkpoint files
}
```

## Current Status

### Completed ✅
- `KernelData` serialization framework
- `serialize_remove_set()` / `deserialize_remove_set()`
- `serialize_state_info()` / `deserialize_state_info()`  
- `ScanLogReplayProcessor::export_state()` / `from_serialized_state()`
- `ActionReconciliationProcessor::export_state()` / `from_serialized_state()`

### In Progress ⏳
- `Snapshot::phase_1_log_replay()` - API defined, implementation has type issues
- `Phase1ScanLogReplay` - struct defined, iterator implementation needs work
- `Phase2ScanLogReplay` - struct defined, needs type fixes

### Blocked Issues 🚧
1. **Type Mismatch**: `read_actions_with_projected_checkpoint_actions()` returns `Iterator<Item = ActionsBatch>` but we need compatibility with `ScanLogReplayProcessor::process_actions_batch()` which expects `Box<dyn EngineData>`

2. **Iterator Semantics**: Unclear what the Phase1/Phase2 iterators should yield:
   - Option A: `ScanResult` (file metadata)
   - Option B: Raw `ActionsBatch` 
   - Option C: Nothing (just consume internally, return state via `finish()`)

3. **Missing Infrastructure**: No existing two-phase checkpoint API to model from

## Recommendations

1. **Simplify Initially**: Make Phase1/Phase2 non-yielding - they process internally and only expose `finish()`
2. **Add Checkpoint API First**: Implement `CheckpointWriter::phase1_checkpoint_data()` as it's simpler
3. **Iterate**: Once basic structure works, add batch yielding if needed

## Files Modified
- `/kernel/src/snapshot.rs` - Added `phase_1_log_replay()` method
- `/kernel/src/snapshot/phases.rs` - Added `Phase1ScanLogReplay` and `Phase2ScanLogReplay` structs
- `/kernel/src/scan/mod.rs` - Made schemas `pub(crate)`, added `state_info()` accessor
- `/kernel/src/scan/log_replay.rs` - Already has serialization support
- `/kernel/src/kernel_data.rs` - Serialization complete

## Next Steps
1. Fix type issues in Phase1/Phase2 iterators
2. Implement checkpoint two-phase API
3. Create end-to-end test
4. Add processor serialization methods (`serialize()` / `deserialize()`)

