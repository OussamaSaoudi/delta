# Multi-Plan State Machine Architecture - Unified Execution Model

## Executive Summary

Two independent state machines run in parallel with **identical execution patterns**:
1. **Metadata Machine**: Generates scan metadata (Add actions) - streams continuously
2. **Data Machine**: Consumes metadata stream, generates data read plans - runs concurrently

**Key Innovation**: All sinks (Consume, Relation, Stream) are handled transparently by the PlanExecutor. All phases use the same execution pattern - no special iteration logic.

---

## Core Principles

1. **Unified Execution** - All phases execute identically: `executor.execute(plan, context)`
2. **Executor-Managed Relations** - Relation registry lives in executor, transparent to user
3. **Callback-Based Consume** - Consume sink triggers phase callbacks during execution
4. **Streaming by Default** - Metadata streams continuously (no forced materialization)
5. **Configuration-Driven** - Single codebase, multiple strategies
6. **Kernel Never Does IO** - Phases generate plans but don't execute them (GenerativePhase pattern)

---

## Sink Model

```rust
pub enum SinkDestination {
    Consume(PhaseId),                         // Callback to phase's consume()
    Relation { name: String, scope: RelationScope },  // Materialized storage
    Stream { name: String },                  // Append-only stream
    File { path: Url, format: FileFormat },   // Persistent storage
}

pub enum RelationScope {
    Driver,                    // Driver-local only
    Broadcast,                 // Immutable copy on all executors
    Partition(Vec<String>),    // Partitioned across executors by keys
}
```

---

## Unified Execution Pattern

### All Phases Execute Identically

```rust
/// Drive ANY state machine - metadata or data
fn execute_state_machine(
    mut machine: StateMachine,
    executor: &PlanExecutor,
    context: &mut ExecutionContext,
) -> DeltaResult<()> {
    
    loop {
        match machine {
            StateMachine::Terminus => break,
            _ => {
                // 1. GET PLANS (with sinks)
                let plans = machine.get_plans()?;
                
                // 2. EXECUTE (executor handles all sinks internally)
                for plan in plans {
                    executor.execute(plan, context)?;
                }
                
                // 3. EXECUTE HINTS (async, opportunistic)
                let hints = machine.get_hint_plans()?;
                for hint in hints {
                    executor.execute_async(hint, context);
                }
                
                // 4. TRANSITION
                machine = machine.next()?;
            }
        }
    }
    
    Ok(())
}
```

**Key Property**: No iteration, no manual sink handling - executor does everything.

---

## PlanExecutor: Sink & Relation Management

### Executor Interface

```rust
pub struct PlanExecutor {
    engine: Arc<dyn Engine>,
    relations: RelationRegistry,
}

impl PlanExecutor {
    /// Execute plan - handles ALL sinks internally
    pub fn execute(
        &self,
        plan: LogicalPlanNode,
        context: &mut ExecutionContext,
    ) -> DeltaResult<()> {
        
        // 1. Extract terminal sink
        let (core_plan, sink) = self.extract_sink(plan)?;
        
        // 2. Execute core plan (may have RelationRef nodes)
        let data_iterator = self.execute_core(core_plan, context)?;
        
        // 3. Handle sink destination
        match sink.destination {
            SinkDestination::Consume(phase_id) => {
                // Get phase from context and call consume()
                let phase = context.get_phase_mut(phase_id)?;
                for batch in data_iterator {
                    phase.consume(batch?)?;
                }
            }
            
            SinkDestination::Relation { name, scope } => {
                // Materialize and store in registry
                let data = self.materialize(data_iterator)?;
                self.relations.register(name, scope, data)?;
            }
            
            SinkDestination::Stream { name } => {
                // Append to streaming relation
                for batch in data_iterator {
                    self.relations.append(name, batch?)?;
                }
            }
            
            SinkDestination::File { path, format } => {
                // Write to persistent storage
                self.write_file(data_iterator, path, format)?;
            }
        }
        
        Ok(())
    }
    
    /// Execute core plan (resolves RelationRef nodes)
    fn execute_core(
        &self,
        plan: LogicalPlanNode,
        context: &ExecutionContext,
    ) -> DeltaResult<DataIterator> {
        
        match plan {
            LogicalPlanNode::RelationRef(name) => {
                // Resolve from registry
                let data = self.relations.get(&name)?;
                Ok(Box::new(std::iter::once(Ok(data))))
            }
            
            LogicalPlanNode::Filter(filter_node) => {
                let child_data = self.execute_core(*filter_node.child, context)?;
                Ok(self.apply_filter(child_data, filter_node.filter))
            }
            
            LogicalPlanNode::Union(union_node) => {
                let mut iterators = vec![];
                for child in union_node.children {
                    iterators.push(self.execute_core(child, context)?);
                }
                Ok(Box::new(iterators.into_iter().flatten()))
            }
            
            // ... other nodes
        }
    }
}
```

---

## Relation Registry Management

### RelationRegistry Structure

```rust
pub struct RelationRegistry {
    // Materialized relations (immutable)
    materialized: HashMap<String, MaterializedRelation>,
    
    // Streaming relations (append-only)
    streaming: HashMap<String, StreamingRelation>,
}

pub struct MaterializedRelation {
    data: Arc<dyn EngineData>,
    scope: RelationScope,
    created_at: Instant,
}

pub struct StreamingRelation {
    sender: mpsc::Sender<EngineData>,
    receiver: Arc<Mutex<mpsc::Receiver<EngineData>>>,
    scope: RelationScope,
    closed: AtomicBool,
}
```

### Registry Operations

```rust
impl RelationRegistry {
    /// Register materialized relation
    pub fn register(
        &mut self,
        name: String,
        scope: RelationScope,
        data: Arc<dyn EngineData>,
    ) -> DeltaResult<()> {
        if self.materialized.contains_key(&name) {
            return Err(Error::generic(format!("Relation {} already exists", name)));
        }
        
        self.materialized.insert(name.clone(), MaterializedRelation {
            data,
            scope,
            created_at: Instant::now(),
        });
        
        // Broadcast/partition based on scope
        self.distribute_if_needed(name, scope)?;
        
        Ok(())
    }
    
    /// Get materialized relation
    pub fn get(&self, name: &str) -> DeltaResult<Arc<dyn EngineData>> {
        self.materialized
            .get(name)
            .map(|rel| rel.data.clone())
            .ok_or_else(|| Error::generic(format!("Relation {} not found", name)))
    }
    
    /// Append to streaming relation
    pub fn append(&mut self, name: String, batch: Arc<dyn EngineData>) -> DeltaResult<()> {
        // Create stream if doesn't exist
        if !self.streaming.contains_key(&name) {
            self.create_stream(name.clone(), RelationScope::Driver)?;
        }
        
        let stream = self.streaming.get(&name).unwrap();
        if stream.closed.load(Ordering::Relaxed) {
            return Err(Error::generic(format!("Stream {} is closed", name)));
        }
        
        stream.sender.send(batch)
            .map_err(|_| Error::generic("Stream receiver dropped"))?;
        
        Ok(())
    }
    
    /// Get streaming relation (returns iterator)
    pub fn subscribe(&self, name: &str) -> DeltaResult<StreamSubscription> {
        let stream = self.streaming.get(name)
            .ok_or_else(|| Error::generic(format!("Stream {} not found", name)))?;
        
        Ok(StreamSubscription {
            receiver: stream.receiver.clone(),
            closed: stream.closed.clone(),
        })
    }
    
    /// Close streaming relation (signal completion)
    pub fn close_stream(&mut self, name: &str) -> DeltaResult<()> {
        if let Some(stream) = self.streaming.get(name) {
            stream.closed.store(true, Ordering::Relaxed);
            // Sender will be dropped, signaling EOF
        }
        Ok(())
    }
    
    /// Distribute relation based on scope
    fn distribute_if_needed(&self, name: String, scope: RelationScope) -> DeltaResult<()> {
        match scope {
            RelationScope::Driver => {
                // No distribution needed
            }
            RelationScope::Broadcast => {
                // Serialize and send to all executors
                self.engine.broadcast_relation(name)?;
            }
            RelationScope::Partition(keys) => {
                // Partition and distribute
                self.engine.partition_relation(name, keys)?;
            }
        }
        Ok(())
    }
}
```

### Relation Lifecycle

```
1. CREATION
   - Materialized: Created when plan with Relation sink completes
   - Streaming: Created on first append (lazy)

2. ACCESS
   - RelationRef node triggers registry.get() or registry.subscribe()
   - Executor transparently resolves during plan execution

3. DISTRIBUTION
   - Driver scope: Stays local
   - Broadcast: Serialized + sent to all executors on registration
   - Partition: Split + distributed on registration

4. CLEANUP
   - Materialized: Dropped after query completes (or explicit cleanup)
   - Streaming: Closed when producer calls close_stream()
   - Automatic cleanup on ExecutionContext drop
```

---

## Parallel State Machines

### High-Level Flow

```
User: scan_builder.build_state_machines()
  ↓
┌─────────────────────────────────────┐
│ Spawns Two Parallel Machines        │
├─────────────────────────────────────┤
│                                     │
│ 1. METADATA MACHINE (Driver)       │
│    Phase 1: Commit                  │
│    Phase 2: RemoveSetExport         │
│    Phase 3: Manifest                │
│    Phase 4: Checkpoint              │
│    Phase 5: Terminus                │
│                                     │
│    Produces → "scan_metadata" stream│
│                                     │
├─────────────────────────────────────┤
│                                     │
│ 2. DATA MACHINE (Executors)        │
│    Phase 1: MapperInit              │
│    Phase 2: DataGeneration          │
│    Phase 3: Terminus                │
│                                     │
│    Consumes ← "scan_metadata" stream│
│    Produces → "user_data" stream    │
│                                     │
└─────────────────────────────────────┘
```

### Spawning API: Paired State Machines

```rust
pub struct DistributedStateMachine {
    pub driver_machine: Box<dyn StateMachine>,
    pub executor_machine: Box<dyn StateMachine>,
}

impl ScanBuilder {
    pub fn build_distributed(self) -> DeltaResult<DistributedStateMachine> {
        // Build BOTH machines as a pair
        let driver_machine = MetadataStateMachine::new(
            snapshot: self.snapshot,
            predicate: self.predicate,  // None = no data skipping
        );
        
        let executor_machine = DataStateMachine::new(
            // Configuration encoded as relation names
            config_relation: "dataMapperConfig",
            input_relation: "scan_metadata",
            output_relation: "user_data",
        );
        
        Ok(DistributedStateMachine {
            driver_machine: Box::new(driver_machine),
            executor_machine: Box::new(executor_machine),
        })
    }
}
```

### StateMachine Trait: Declarative Communication

```rust
pub trait StateMachine {
    // What relations does this machine produce?
    fn produces_relations(&self) -> Vec<RelationSpec> {
        vec![]
    }
    
    // What relations does this machine consume?
    fn consumes_relations(&self) -> Vec<String> {
        vec![]
    }
    
    // Standard state machine methods
    fn get_plans(&self) -> Vec<LogicalPlanNode>;
    fn next(self: Box<Self>) -> DeltaResult<StateMachinePhase>;
}

pub struct RelationSpec {
    pub name: String,
    pub scope: RelationScope,
}
```

**Example Implementation**:

```rust
impl StateMachine for MetadataStateMachine {
    fn produces_relations(&self) -> Vec<RelationSpec> {
        vec![
            RelationSpec { name: "scan_metadata", scope: Driver },
            RelationSpec { name: "removeSet", scope: Broadcast },
            RelationSpec { name: "dataMapperConfig", scope: Broadcast },
            RelationSpec { name: "sidecarFiles", scope: Partition(vec!["path"]) },
        ]
    }
}

impl StateMachine for DataStateMachine {
    fn consumes_relations(&self) -> Vec<String> {
        vec!["scan_metadata", "dataMapperConfig"]
    }
    
    fn produces_relations(&self) -> Vec<RelationSpec> {
        vec![
            RelationSpec { name: "user_data", scope: Driver },
        ]
    }
}
```

### Engine Distribution Handler

```rust
impl PlanExecutor {
    pub fn execute_distributed(
        &self,
        machines: DistributedStateMachine,
    ) -> DeltaResult<DataStream> {
        
        // 1. Spawn driver machine (on current process)
        let driver_handle = thread::spawn(|| {
            self.execute_state_machine(machines.driver_machine)
        });
        
        // 2. Spawn executor machines (distributed across cluster)
        //    Engine serializes executor_machine and sends to all executors
        let executor_handles = self.engine.spawn_on_all_executors(
            machines.executor_machine,
            context: self.context.clone(),
        )?;
        
        // 3. Return stream of results
        Ok(self.relations.subscribe("user_data")?)
    }
}
```

---

## METADATA STATE MACHINE

### Phase 1: Commit Phase (Driver)

**Purpose**: Process commits, stream results immediately.

#### Implementation

```rust
impl Phase for CommitPhase {
    fn get_plans(&self) -> Vec<LogicalPlanNode> {
        let plan = ScanJson(commit_files, schema)
            .parse_json("add.stats", stats_schema, "parsed_stats")
            .filter_by_expression(predicate)          // Data skipping
            .filter_ordered(CommitDedupFilter)
            .sink(Stream { name: "scan_metadata" });  // STREAMING
        
        vec![plan]
    }
    
    fn get_hint_plans(&self) -> Vec<LogicalPlanNode> {
        vec![
            ScanParquet([_manifest.parquet], schema)
                .sink(Relation { 
                    name: "checkpoint_manifest", 
                    scope: Driver 
                })
        ]
    }
    
    fn next(self) -> DeltaResult<StateMachinePhase> {
        let remove_set = self.dedup_filter.export_seen_keys();
        Ok(StateMachinePhase::Phase(Box::new(
            RemoveSetExportPhase { remove_set }
        )))
    }
}
```

**Execution** (user code):
```rust
// Get plans
let plans = commit_phase.get_plans()?;

// Execute (executor streams to "scan_metadata" internally)
for plan in plans {
    executor.execute(plan, context)?;
}

// Execute hints (async)
for hint in commit_phase.get_hint_plans()? {
    executor.execute_async(hint, context);
}

// Transition
commit_phase = commit_phase.next()?;
```

---

### Phase 2: RemoveSetExport Phase (Driver)

```rust
impl Phase for RemoveSetExportPhase {
    fn get_plans(&self) -> Vec<LogicalPlanNode> {
        let plan = Values(
            schema: {path: String, dv_unique_id: String?},
            data: KernelData::from_hashset(self.remove_set)
        )
        .sink(Relation { 
            name: "removeSet", 
            scope: Broadcast      // Distributes to all executors
        });
        
        vec![plan]
    }
}
```

**Execution**:
```rust
executor.execute(plan, context)?;
// Executor automatically broadcasts to executors
```

---

### Phase 3: Manifest Phase (Driver)

```rust
impl Phase for ManifestPhase {
    fn get_plans(&self) -> Vec<LogicalPlanNode> {
        vec![
            // Plan 1: Extract sidecars
            RelationRef("checkpoint_manifest")
                .or_else(ScanParquet([_manifest.parquet], schema))
                .select(["sidecar.path", "sidecar.size"])
                .sink(Relation { 
                    name: "sidecarFiles", 
                    scope: Partition(["path"])  // Distributes for parallel processing
                }),
            
            // Plan 2: Data mapper config
            Values(mapper_config_data)
                .sink(Relation { 
                    name: "dataMapperConfig", 
                    scope: Broadcast 
                }),
        ]
    }
}
```

**Execution**:
```rust
for plan in plans {
    executor.execute(plan, context)?;
}
// Both relations registered and distributed automatically
```

---

### Phase 4: Checkpoint Phase (Executors - Distributed)

```rust
impl Phase for CheckpointPhase {
    fn get_plans(&self) -> Vec<LogicalPlanNode> {
        let plan = RelationRef("sidecarFiles")[partition]
            .scan_parquet_from_paths("path", add_schema)
            .filter_by_expression(
                NOT IN (["path", "dv_unique_id"], RelationRef("removeSet"))
            )
            .sink(Stream { name: "scan_metadata" });  // APPENDS to stream
        
        vec![plan]
    }
}
```

**Execution** (on executors):
```rust
executor.execute(plan, context)?;
// Executor appends results to "scan_metadata" stream
// Data machine receives batches immediately
```

---

## DATA STATE MACHINE (Parallel)

### Phase 1: MapperInit Phase

```rust
impl Phase for MapperInitPhase {
    fn get_plans(&self) -> Vec<LogicalPlanNode> {
        let plan = RelationRef("dataMapperConfig")
            .sink(Consume(self.phase_id()));  // Callback to this phase
        
        vec![plan]
    }
    
    fn consume(&mut self, batch: Arc<dyn EngineData>) -> DeltaResult<()> {
        // Deserialize mapper config (pure - no IO)
        let config = DataMapperConfig::from_engine_data(batch)?;
        self.mapper = Some(DataPhaseMapper::from_config(config)?);
        Ok(())
    }
    
    fn next(self) -> DeltaResult<StateMachinePhase> {
        Ok(StateMachinePhase::Phase(Box::new(
            DataGenerationPhase { 
                mapper: self.mapper.unwrap(),
            }
        )))
    }
}
```

**Execution**:
```rust
executor.execute(plan, context)?;
// Executor calls mapper_init_phase.consume(batch) automatically
```

---

### Phase 2: DataGeneration Phase (GenerativePhase)

**Special Phase Type**: Consumes batches and generates plans (NO IO in kernel).

```rust
pub trait GenerativePhase {
    fn get_source_plan(&self) -> DeltaResult<LogicalPlanNode>;
    fn consume_and_generate(&mut self, batch: Arc<dyn EngineData>) -> DeltaResult<Option<LogicalPlanNode>>;
    fn next(self: Box<Self>) -> DeltaResult<StateMachinePhase>;
}

impl GenerativePhase for DataGenerationPhase {
    fn get_source_plan(&self) -> DeltaResult<LogicalPlanNode> {
        // Plan to fetch metadata batches
        Ok(RelationRef("scan_metadata"))
    }
    
    fn consume_and_generate(
        &mut self, 
        metadata_batch: Arc<dyn EngineData>
    ) -> DeltaResult<Option<LogicalPlanNode>> {
        
        // 1. Extract Add actions (pure - no IO)
        let add_actions = parse_add_actions(metadata_batch)?;
        
        if add_actions.is_empty() {
            return Ok(None);
        }
        
        // 2. Map each Add to data plan (pure - no IO)
        let per_file_plans: Vec<_> = add_actions
            .iter()
            .map(|add| self.mapper.map_add_action(add))
            .collect::<DeltaResult<_>>()?;
        
        // 3. Return plan (kernel never executes it)
        let data_plan = Union(per_file_plans)
            .sink(Stream { name: "user_data" });
        
        Ok(Some(data_plan))
    }
    
    fn next(self: Box<Self>) -> DeltaResult<StateMachinePhase> {
        Ok(StateMachinePhase::Terminus(DataComplete))
    }
}
```

**Execution** (driver code - outside kernel):
```rust
// Special handling for generative phases
let source_plan = data_gen_phase.get_source_plan()?;
let metadata_batches = executor.execute_to_iterator(source_plan, context)?;

// Driver loop: fetch batch → generate plan → execute plan
for metadata_batch in metadata_batches {
    let metadata_batch = metadata_batch?;
    
    // Generate data plan (pure kernel code - no IO)
    if let Some(data_plan) = data_gen_phase.consume_and_generate(metadata_batch)? {
        // Execute generated plan (IO happens here, outside kernel)
        executor.execute(data_plan, context)?;
    }
}

// Transition when source stream exhausted
data_gen_phase = data_gen_phase.next()?;
```

---

## DataPhaseMapper (Serialized State)

### Structure

```rust
pub struct DataPhaseMapper {
    logical_schema: SchemaRef,
    physical_schema: SchemaRef,
    transform_specs: Vec<TransformSpec>,
    table_root: Url,
}
```

### Mapping Logic

```rust
impl DataPhaseMapper {
    fn map_add_action(&self, add: &AddAction) -> DeltaResult<LogicalPlanNode> {
        // 1. Data file read
        let data_read = ScanParquet(
            [self.table_root.join(add.path)],
            self.physical_schema
        );
        
        // 2. DV read (if present)
        let dv_read = add.deletion_vector.map(|dv| 
            ReadDV(dv, self.table_root)
        );
        
        // 3. Apply DV
        let with_dv = if let Some(dv) = dv_read {
            ApplyDV(data_read, dv)
        } else {
            data_read
        };
        
        // 4. Build transform from Add metadata
        let transform_expr = get_transform_expr(
            &self.transform_specs,
            add.partition_values,
            &self.physical_schema,
            add.base_row_id
        )?;
        
        // 5. Apply transform
        Ok(with_dv.transform(transform_expr))
    }
}
```

**Transform Components**:
- Column mapping (physical → logical names)
- Partition value injection
- Row ID generation (baseRowId + row_index)

---

## Complete Execution Timeline

```
T0: User calls build_state_machines()
    ├─ Metadata machine created
    └─ Data machine created

T1: Spawn metadata thread
    └─ execute_state_machine(metadata_machine, executor, context)

T2: Spawn data thread
    └─ Special generative phase handling for DataGenerationPhase

T3: Metadata Phase 1 (Commit) executes
    └─ Results streamed to "scan_metadata"

T4: Data machine processes first batch
    ├─ consume_and_generate() returns data plan (pure)
    ├─ Driver executes data plan (IO)
    └─ Streams to "user_data"
    
    [USER CAN NOW PROCESS DATA - LIMIT 10 works!]

T5: Metadata phases 2-4 execute
    └─ Each checkpoint batch → "scan_metadata" → data machine

T6: Metadata machine reaches Terminus
    └─ Closes "scan_metadata" stream

T7: Data machine processes final batches
    └─ Reaches Terminus when stream closed
```

---

## Relation Flow Diagram: Paired State Machines

```
DRIVER MACHINE                    EXECUTOR MACHINES
(MetadataStateMachine)           (DataStateMachine)

┌─────────────────┐              ┌──────────────────┐
│ CommitPhase     │              │                  │
│   ↓             │              │ (waiting for     │
│ sink(Stream     │─────────────→│  scan_metadata)  │
│  "scan_metadata"│  streaming   │                  │
│  )              │              │                  │
└─────────────────┘              └──────────────────┘
        ↓                                 ↓
┌─────────────────┐              ┌──────────────────┐
│RemoveSetExport  │              │ MapperInitPhase  │
│   ↓             │              │   ↓              │
│ sink(Relation   │─────────────→│ RelationRef(     │
│  "removeSet",   │  broadcast   │  "dataMapper     │
│  Broadcast)     │              │  Config")        │
└─────────────────┘              │   ↓              │
        ↓                        │ consume()        │
┌─────────────────┐              │ (reconstruct     │
│ ManifestPhase   │              │  mapper)         │
│   ↓             │              └──────────────────┘
│ sink(Relation   │─────────────→         ↓
│  "dataMapper    │  broadcast   ┌──────────────────┐
│  Config",       │              │ DataGeneration   │
│  Broadcast)     │              │  Phase           │
│   ↓             │              │   ↓              │
│ sink(Relation   │              │ RelationRef(     │
│  "sidecarFiles",│─────────────→│  "scan_metadata")│
│  Partition)     │  partition   │   ↓              │
└─────────────────┘              │ consume_and_     │
        ↓                        │  generate()      │
┌─────────────────┐              │   ↓              │
│ CheckpointPhase │              │ generates data   │
│   ↓             │              │  plans (pure)    │
│ RelationRef(    │←─────────────│   ↓              │
│  "sidecarFiles")│  executor    │ sink(Stream      │
│   ↓             │  reads       │  "user_data")    │
│ RelationRef(    │←─────────────│                  │
│  "removeSet")   │  broadcast   │                  │
│   ↓             │  reads       │                  │
│ sink(Stream     │─────────────→│                  │
│  "scan_metadata"│  append      │                  │
│  )              │              │                  │
└─────────────────┘              └──────────────────┘
        ↓                                 ↓
┌─────────────────┐              ┌──────────────────┐
│ Terminus        │   results    │ Results stream   │
│                 │←─────────────│  back to driver  │
│                 │              │                  │
└─────────────────┘              └──────────────────┘
        ↓
      USER
  (reads "user_data")
```

**Key Communication Patterns**:
1. **Streaming**: `scan_metadata` continuously flows from driver → executors
2. **Broadcast**: `removeSet`, `dataMapperConfig` copied to all executors
3. **Partition**: `sidecarFiles` split across executors by key
4. **Return**: `user_data` streams from executors → driver → user

---

## User Code Example

```rust
/// User query with LIMIT (Simple API!)
fn user_query_limit_10() -> DeltaResult<()> {
    let engine = Arc::new(DistributedEngine::new());
    let table = Table::new("s3://bucket/table");
    
    // 1. Build paired machines (driver + executor)
    let machines = table.scan()
        .with_predicate(col("id") < 1000)
        .build_distributed()?;
    
    // 2. Engine handles everything (spawning, distribution, communication)
    let data_stream = engine.execute_distributed(machines)?;
    
    // 3. LIMIT 10 - completes as soon as first batch available
    let mut count = 0;
    for batch in data_stream {
        let batch = batch?;
        for row in batch.rows() {
            println!("{:?}", row);
            count += 1;
            if count >= 10 {
                return Ok(()); // Early exit - no wait for checkpoint!
            }
        }
    }
    
    Ok(())
}
```

**Key Property**: Single API call `execute_distributed(machines)` handles:
- Spawning driver machine on driver process
- Serializing and spawning executor machine on all executors
- Setting up relation registry for communication
- Routing data between machines based on scopes

---

## Key Design Points

### 1. Paired State Machines

- **Symmetric Design**: Driver and executor both run state machines
- **Single API Call**: `execute_distributed(machines)` handles everything
- **Declarative Communication**: Machines declare relations they produce/consume
- **Engine Handles Logistics**: Distribution, serialization, spawning
- **Type-Safe**: Mismatch between produced/consumed relations caught at build time

### 2. Unified Execution

All phases use identical pattern:
```rust
let plans = phase.get_plans()?;
for plan in plans {
    executor.execute(plan, context)?;
}
phase = phase.next()?;
```

### 3. Executor-Managed Relations

- RelationRegistry lives in PlanExecutor
- Transparent resolution of RelationRef nodes
- Automatic distribution based on scope
- Lifecycle management (creation, access, cleanup)

### 4. GenerativePhase Pattern

- **Kernel Never Does IO**: Phases generate plans but don't execute them
- `consume_and_generate(batch) -> Option<Plan>`: Pure function returns plan
- Driver loop: fetch batch → generate plan → execute plan → repeat

### 5. Streaming by Default

- Metadata streams to data machine as produced
- No forced union/materialization
- Enables efficient LIMIT queries

### 6. Scope-Driven Distribution

- **Data Location**: Sink scopes declare where data lives (Driver/Broadcast/Partition)
- **Compute Location**: Engine decides where plans execute based on input relation scopes
- **Separation of Concerns**: Kernel declares intent, engine implements strategy

---

## Data Location vs Compute Location

### Separation of Concerns

**Data Location** (declared by kernel via sink scopes):
- Where data physically resides
- How data is distributed across nodes
- `Partition(["path"])` = data split across executors by path
- `Broadcast` = full copy on all executors
- `Driver` = only on driver node

**Compute Location** (decided by engine):
- Where plans execute
- Inferred from input relation scopes
- Plans consuming partitioned data → execute on executors
- Plans producing partitioned data → execute on driver, then distribute

### Engine Decision Rules

```rust
fn determine_execution_location(plan: &LogicalPlanNode) -> ExecutionLocation {
    let input_relations = extract_relation_refs(plan);
    
    for rel_name in input_relations {
        let scope = registry.get_scope(rel_name);
        if scope.is_partitioned() {
            // Any partitioned input → distribute execution
            return ExecutionLocation::Executors;
        }
    }
    
    // No partitioned inputs → run on driver
    ExecutionLocation::Driver
}
```

### Examples

**Example 1: Producing Partitioned Data**
```rust
// Manifest phase (driver)
RelationRef("checkpoint_manifest")
    .select(["sidecar.path"])
    .sink(Relation { 
        name: "sidecarFiles", 
        scope: Partition(["path"])  // Data location
    })

// Execution: Driver (no partitioned inputs)
// Result: Produces data, then engine partitions and distributes
```

**Example 2: Consuming Partitioned Data**
```rust
// Checkpoint phase
RelationRef("sidecarFiles")  // Partitioned input!
    .scan_parquet_from_paths("path", schema)
    .sink(Stream { name: "scan_metadata" })

// Execution: Executors (partitioned input detected)
// Result: Each executor processes its partition
```

**Example 3: Mixed Scopes**
```rust
// Checkpoint phase
RelationRef("sidecarFiles")  // Partitioned
    .scan_parquet_from_paths("path", schema)
    .filter_by_expression(
        NOT IN (["path"], RelationRef("removeSet"))  // Broadcast
    )
    .sink(Stream { name: "scan_metadata" })

// Execution: Executors (partitioned input takes precedence)
// removeSet is broadcast variable, available on all executors
```

### Key Insight

**Kernel declares intent (data location)** → **Engine implements strategy (compute location)**

This separation enables:
- Kernel stays pure (no IO knowledge)
- Engine can optimize execution (e.g., co-locate compute with data)
- Different engines can implement different distribution strategies
- Same kernel code works in single-node and distributed modes

---

## Node Types Required

1. **SinkNode** - Terminal output (handled by executor)
2. **RelationRefNode** - Reference to named relation
3. **ParseJsonNode** - Parse JSON string column
4. **FilterByExpressionNode** - Expression-based filter
5. **NotInNode** - Anti-join with relation
6. **ScanParquetFromPathsNode** - Expand file paths to scans
7. **TransformNode** - Apply expression transform
8. **ApplyDVNode** - Apply deletion vector
9. **ReadDVNode** - Read DV file
10. **ValuesNode** - Kernel-materialized constant data

---

## Open Design Questions

1. **Thread Model**: OS threads, async tasks, or engine-managed executors for parallel machines?
   - *Answer leaning towards*: Engine-managed (engine.spawn_on_all_executors)

2. **Backpressure**: How to handle slow data generation vs fast metadata production?
   - *Approach*: Streaming relations use bounded channels with configurable buffer size

3. **Error Propagation**: Should data machine failure stop metadata machine?
   - *Likely*: Yes, fail-fast - either machine failure cancels entire distributed execution

4. **Stream Buffer Size**: How many batches to buffer in streaming relations?
   - *Engine decision*: Configurable per-relation or global default

5. **Relation Cleanup**: Explicit cleanup API vs automatic on context drop?
   - *Approach*: Automatic on ExecutionContext drop, with optional explicit cleanup for long-running sessions

6. **GenerativePhase Driver**: Should driver loop be in kernel (manual_state_machines.rs style) or external?
   - *Approach*: External (engine-side) - kernel returns plans, engine executes them

7. **Serialization**: How are state machines serialized for executor distribution?
   - *Needs exploration*: Trait-based serialization, enum-based state machines, or custom protocol?

8. **Executor Discovery**: How does engine know which executors are available?
   - *Engine responsibility*: Engine maintains executor pool/registry

9. **Partial Failure**: What happens if some executors fail but others succeed?
   - *Needs policy*: Retry, partial results, or full failure?

