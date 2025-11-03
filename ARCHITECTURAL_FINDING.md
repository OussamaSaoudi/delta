# CRITICAL ARCHITECTURAL FINDING

## The Real State Machine Architecture for Scans

After deep analysis of `scan/phases.rs`, I discovered the correct architecture:

### ❌ WRONG Understanding (What I Implemented)
```
1. ScanBuilder → build_state_machine → PartialResult phases → Terminus(Scan)
2. Scan → scan_into_state_machine() → NEW execution phases → scan files
```

### ✅ CORRECT Understanding (How Rust Works)
```
1. ScanBuilder → build_state_machine → PartialResult phases
   - CommitReplayPhase.get_plan() returns plan that yields Add actions (scan files!)
   - CheckpointReplayPhase.get_plan() returns plan that yields more Add actions
   - Finally → Terminus(Scan)
2. The SCAN FILES come from the PARTIAL_RESULT phases during scan BUILDING
3. There is NO separate execution phase - building IS execution!
```

### Key Evidence from `scan/phases.rs` Test (lines 196-390)

The test `proof_state_machine_matches_current_implementation()` shows:

```rust
let phase = ScanBuilder::new(snapshot).build_state_machine()?;

match phase {
    StateMachinePhase::PartialResult(commit_phase) => {
        // Execute the plan to get scan files!
        let plan = commit_phase.get_plan()?;
        let batches = executor.execute(plan)?;
        
        // Extract add.path from batches - THESE ARE THE SCAN FILES!
        for batch in batches {
            extract_paths_from_add_actions(batch);
        }
        
        // Transition to checkpoint phase
        let next = commit_phase.next()?;
        match next {
            StateMachinePhase::PartialResult(checkpoint_phase) => {
                // Execute checkpoint plan for MORE scan files
                let plan = checkpoint_phase.get_plan()?;
                let batches = executor.execute(plan)?;
                extract_paths_from_add_actions(batches);
                
                // Finally get the Scan
                match checkpoint_phase.next()? {
                    StateMachinePhase::Terminus(scan) => scan,
                    _ => panic!(),
                }
            },
            StateMachinePhase::Terminus(scan) => scan,
            _ => panic!(),
        }
    },
    _ => panic!(),
}
```

### What This Means for Java Implementation

The Java `Scan.execute()` method is **FUNDAMENTALLY WRONG**. Here's what needs to happen:

**Option 1: Don't build Scan separately**
```java
// Don't do this:
Scan scan = snapshot.scanBuilder().build(); // ← Discards scan files!
CloseableIterator<FilteredColumnarBatch> files = scan.execute(); // ← Can't get them back!

// Instead do THIS:
CloseableIterator<FilteredColumnarBatch> files = snapshot.scanBuilder()
    .buildAndExecute(); // ← Drives state machine, collects files during building
```

**Option 2: Cache scan files during build**
```java
class Scan {
    private List<FilteredColumnarBatch> cachedScanFiles; // Collected during build
    
    CloseableIterator<FilteredColumnarBatch> execute() {
        return cachedScanFiles.iterator(); // Return cached files
    }
}
```

### FFI Implications

**NO `scan_into_state_machine` function is needed!**

The scan files are extracted from PARTIAL_RESULT phases during `scan_builder_into_state_machine`.

### Action Items

1. ❌ Remove `StateMachineFFI.scanIntoStateMachine()` from Java
2. ❌ Remove bogus `scan_into_state_machine` FFI function from Rust (already done)
3. ✅ Fix `ScanBuilder.build()` in Java to:
   - Drive state machine
   - Collect scan files from PARTIAL_RESULT phases
   - Cache them in the Scan object
   - Return Scan with cached files
4. ✅ Fix `Scan.execute()` to return cached files

### Why This Wasn't Obvious

The state machine API is **reusing the same phases** for both:
- Building the Scan object (metadata about schemas, predicates, etc.)
- Producing scan files (the Add actions from commit/checkpoint replay)

The PARTIAL_RESULT phases serve a dual purpose!



