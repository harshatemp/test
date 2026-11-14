# MP Runtime Skill File

## Metadata

| Property | Value |
|----------|---------|
| Knowledge Area | Ab Initio MP Runtime |
| Purpose | Wrapper Script Interpretation |
| Target Consumer | LLM |
| Conversion Target | PySpark |
| Scope | MP Commands, Runtime Semantics, Execution Behavior |
| Primary Use Case | Wrapper Script → PySpark Conversion |
| Source | mp_help.txt 【1-d05c52】 |

---

# SECTION 1: MP OVERVIEW

## What is MP

MP (Multi-Processing) is the Ab Initio runtime execution framework responsible for executing graphs, components, partitions, datasets, and runtime services. MP acts as the execution layer between graph design and actual runtime processing. It is responsible for process orchestration, data movement, partition management, resource allocation, and runtime coordination. 【1-d05c52】

MP is primarily a runtime execution mechanism rather than a business transformation framework. Business transformation logic is generally defined in graph components, transformations, DML layouts, metadata definitions, joins, filters, and aggregation specifications. MP is responsible for executing those definitions. 【1-d05c52】

---

## Why MP Exists

MP solves the runtime execution problems associated with large-scale ETL processing:

- Parallel execution
- Distributed processing
- Dataset management
- Graph orchestration
- Process coordination
- Resource allocation
- Partition management
- Fault tolerance
- Runtime monitoring

Without MP, graph definitions would remain logical specifications without a mechanism for execution.

---

## Where MP Appears in Wrapper Scripts

MP commonly appears within:

- Generated run scripts
- Wrapper scripts
- Batch execution frameworks
- Scheduler-controlled execution scripts
- Deployment automation scripts

Typical wrapper scripts invoke multiple MP commands to execute graphs, phases, datasets, and supporting runtime operations.

---

## Execution Chain

```text
Wrapper Script
    ↓
MP Commands
    ↓
Runtime Topology Creation
    ↓
Graph Execution
    ↓
Component Execution
    ↓
Flow Execution
    ↓
Partition Processing
    ↓
Data Processing
```

---

## Relationship Model

```text
Wrapper Script
    Contains
        ↓
MP Commands
    Create/Control
        ↓
Graph Runtime
        ↓
Components
        ↓
Ports
        ↓
Flows
        ↓
Partitions
        ↓
Datasets
        ↓
Business Results
```

---

## Business Logic vs Runtime Logic

### Business Logic

Business logic typically resides in:

- DML layouts
- Metadata definitions
- Transform functions (xfr)
- Reformat transforms
- Join logic
- Lookup logic
- Filter conditions
- Aggregation logic
- Normalize/Denormalize logic
- SQL definitions

These concepts should typically be converted into PySpark.

### Runtime Logic

Runtime logic typically resides in:

- MP command execution
- Process spawning
- Host assignment
- Resource allocation
- Logging
- Reporting
- Monitoring
- Runtime recovery
- Checkpointing

These concepts should typically NOT generate PySpark transformation code.

---

# SECTION 2: MP EXECUTION MODEL

## Graph Execution Lifecycle

```text
Wrapper Starts
    ↓
MP Initializes Runtime
    ↓
Load Metadata/Layout Definitions
    ↓
Build Runtime Topology
    ↓
Allocate Partitions
    ↓
Spawn Runtime Processes
    ↓
Connect Flows
    ↓
Execute Components
    ↓
Process Records
    ↓
Collect Results
    ↓
Cleanup Resources
```

---

## Component Execution Lifecycle

```text
Initialize
    ↓
Load Metadata
    ↓
Open Input Ports
    ↓
Read Records
    ↓
Apply Business Logic
    ↓
Generate Output Records
    ↓
Flush Output
    ↓
Close Resources
    ↓
Exit
```

---

## Runtime Orchestration

MP coordinates:

- Component startup
- Component shutdown
- Data routing
- Dataset access
- Resource usage
- Process synchronization
- Buffer management
- Runtime monitoring
- Error propagation

---

## Phase Execution

MP supports execution phases.

```text
Phase 1
    ↓
Synchronization Barrier
    ↓
Phase 2
    ↓
Synchronization Barrier
    ↓
Phase N
```

Phases are execution boundaries and generally represent orchestration semantics rather than business logic.

---

## Flow Execution

Common flow patterns:

| Flow Type | Meaning |
|------------|----------|
| Straight Flow | One producer → One consumer |
| Fan-In Flow | Multiple producers → One consumer |
| Fan-Out Flow | One producer → Multiple consumers |
| All-To-All Flow | Many-to-many routing |
| Adaptive Flow | Runtime-selected routing |
| Broadcast Flow | Data duplicated to all partitions |

---

## Partition Execution

Partitioning controls data distribution.

Common strategies:

### Hash Partition

```text
partition_id = hash(key)
```

Used for joins and aggregations.

### Range Partition

```text
Partition determined by key range.
```

Used for ordering and range-based operations.

### Round Robin Partition

```text
Records distributed equally.
```

Used for balanced distribution.

### Function Partition

```text
Routing determined by custom logic.
```

### Load-Level Partition

```text
Routing based on workload balancing.
```

---

## Process Management

MP typically executes:

```text
Partition 1 → Process A

Partition 2 → Process B

Partition 3 → Process C

Partition 4 → Process D
```

Partitions execute independently and concurrently.

---

## Resource Management

Runtime tuning controls include:

- max-core
- auto-max-core
- ramp
- stage count
- buffer allocation
- throttling

These generally influence performance rather than business output.

---

# SECTION 3: MASTER PARAMETER DICTIONARY

| Parameter Pattern | Canonical Concept | Purpose | Category | Conversion Importance | Runtime Impact | Business Impact | PySpark Mapping |
|------------------|-------------------|----------|----------|----------------------|----------------|----------------|----------------|
| layout | Layout | Define record structure | Schema | Critical | Medium | High | StructType |
| metadata | Metadata | Describe record structure | Metadata | Critical | Medium | High | Metadata |
| key | Business Key | Identify records | Join/Aggregate | Critical | Medium | High | join/groupBy |
| collate | Sorting Definition | Define ordering | Sorting | Critical | Medium | High | orderBy |
| sort | Sorting Definition | Define ordering | Sorting | Critical | Medium | High | orderBy |
| partition | Distribution | Distribute records | Partitioning | Important | High | Medium | repartition |
| partition-key | Distribution Key | Route records | Partitioning | Critical | High | Medium | repartition(column) |
| xfr | Business Logic | Transform records | Transformation | Critical | Low | High | withColumn/UDF |
| select | Filter Logic | Select records | Filter | Critical | Low | High | filter |
| package | Transform Package | Reusable logic | Transformation | Critical | Low | High | UDF/library |
| format | Data Format | Define storage format | Dataset | Critical | Medium | High | format() |
| dataset | Persisted Storage | Store intermediate data | Dataset | Important | Medium | Medium | persist/save |
| file | File Definition | Read/write storage | Dataset | Critical | Medium | High | read/write |
| overwrite | Output Mode | Replace output | Dataset | Important | Low | Medium | overwrite |
| append | Output Mode | Append output | Dataset | Important | Low | Medium | append |
| limit | Row Limiting | Limit processing | Validation | Important | Low | Medium | limit() |
| reject-limit | Error Threshold | Control failures | Validation | Optional | Low | Medium | validation |
| compressed | Compression | Compress storage | Storage | Optional | Low | Low | compression option |
| encrypted | Encryption | Secure storage | Security | Optional | Low | Low | encryption |
| checkpoint | Recovery Point | Enable restart | Recovery | Ignore | High | None | Spark checkpoint |
| recover | Recovery Logic | Resume execution | Recovery | Ignore | High | None | retry |
| ramp | Runtime Tuning | Gradual scaling | Performance | Ignore | Medium | None | N/A |
| max-core | Memory Control | Resource tuning | Performance | Ignore | High | None | N/A |
| auto-max-core | Auto Memory Control | Resource tuning | Performance | Ignore | High | None | N/A |
| log | Logging | Diagnostics | Monitoring | Ignore | Medium | None | logging |
| report | Reporting | Monitoring | Monitoring | Ignore | Medium | None | N/A |
| stages | Pipeline Tuning | Runtime optimization | Runtime | Ignore | Medium | None | N/A |
| buffer | Buffering | Runtime optimization | Runtime | Ignore | Medium | None | N/A |
| hostname | Infrastructure | Connectivity | Infrastructure | Ignore | Medium | None | N/A |
| port | Infrastructure | Connectivity | Infrastructure | Ignore | Medium | None | N/A |

---

# SECTION 4: COMMON CONCEPT CONSOLIDATION

## LAYOUT_CONCEPT

### Meaning

Defines the structure of a record.

### Execution Impact

Controls parsing and serialization.

### Business Meaning

Schema definition.

### PySpark Equivalent

```python
StructType()
```

### Conversion Priority

Critical

### Synonyms

- layout
- input-layout
- output-layout
- record-layout
- layout-file

---

## METADATA_CONCEPT

### Meaning

Descriptive information attached to records and fields.

### Business Meaning

Schema metadata.

### PySpark Equivalent

```python
StructField(metadata={})
```

### Conversion Priority

Critical

---

## KEY_CONCEPT

### Meaning

Business identifier used for joins, grouping, sorting, or deduplication.

### Execution Impact

Determines record matching and aggregation.

### PySpark Equivalent

```python
join()
groupBy()
dropDuplicates()
Window.partitionBy()
```

### Conversion Priority

Critical

### Synonyms

- key
- keys
- natural-key
- surrogate-key
- partition-key
- major-key
- minor-key

---

## SORT_CONCEPT

### Meaning

Defines record ordering.

### Execution Impact

Required for merge operations and ordered aggregations.

### PySpark Equivalent

```python
orderBy()
sortWithinPartitions()
```

### Conversion Priority

Critical

### Synonyms

- sort
- collate
- sorted-input
- sort-key

---

## PARTITION_CONCEPT

### Meaning

Controls data distribution.

### Execution Impact

Affects parallel processing.

### PySpark Equivalent

```python
repartition()
coalesce()
repartitionByRange()
```

### Conversion Priority

Important

---

## DATASET_CONCEPT

### Meaning

Intermediate or persistent storage object.

### Execution Impact

Defines storage boundaries.

### PySpark Equivalent

```python
persist()
cache()
write()
```

### Conversion Priority

Important

---

## CHECKPOINT_CONCEPT

### Meaning

Recovery marker.

### Business Meaning

None.

### Conversion Priority

Ignore

---

## LOGGING_CONCEPT

### Meaning

Diagnostic information.

### Business Meaning

None.

### Conversion Priority

Ignore

---

# SECTION 5: PARAMETER NORMALIZATION RULES

## LAYOUT_CONCEPT

### Related Parameters

```text
layout
input-layout
output-layout
record-layout
layout-file
```

### Meaning

Unified schema definition.

### Conversion Guidance

Generate Spark schema.

---

## KEY_CONCEPT

### Related Parameters

```text
key
keys
natural-key
surrogate-key
major-key
minor-key
partition-key
```

### Meaning

Unified business identifier concept.

### Conversion Guidance

Generate:

- join conditions
- groupBy clauses
- dedup rules
- window specifications

---

## PARTITION_CONCEPT

### Related Parameters

```text
partition
hash-partition
range-partition
roundrobin-partition
function-partition
load-level-partition
```

### Meaning

Unified data distribution model.

### Conversion Guidance

Generate repartition logic where relevant.

---

## SORT_CONCEPT

### Related Parameters

```text
sort
collate
sorted-input
merge
check-sort
sort-key
```

### Meaning

Unified ordering concept.

### Conversion Guidance

Generate orderBy logic.

---

## TRANSFORM_CONCEPT

### Related Parameters

```text
xfr
transform
expression
package
select
reformat
```

### Meaning

Unified business transformation concept.

### Conversion Guidance

Generate DataFrame transformations.

---

## STORAGE_CONCEPT

### Related Parameters

```text
dataset
file
publish
subscribe
readfiles
writefiles
copy-to-dataset
```

### Meaning

Unified storage abstraction.

### Conversion Guidance

Generate DataFrame read/write code.

---

# SECTION 6: PYSPARK CONVERSION KNOWLEDGE

## MUST CONVERT

The following concepts directly impact business output and must be preserved.

### Schema Concepts

- Layout
- Metadata
- DML

### Transformation Concepts

- xfr logic
- Reformat logic
- Business expressions
- Package functions

### Relational Concepts

- Keys
- Join conditions
- Lookup conditions

### Ordering Concepts

- Sort definitions
- Collate definitions

### Aggregation Concepts

- Rollups
- Aggregations
- Scan operations

### Filtering Concepts

- Business filters
- Record selection logic

### Dataset Semantics

- Input formats
- Output formats

---

## MAY AFFECT CONVERSION

### Parallelism

Examples:

- Hash partition
- Range partition
- Fan-out
- Fan-in

Possible mappings:

```python
repartition()
repartitionByRange()
broadcast()
```

### Dataset Persistence

Possible mappings:

```python
cache()
persist()
```

### Compression

Possible mappings:

```python
.option("compression","gzip")
```

---

## RUNTIME ONLY

Examples:

- Logging
- Tracing
- Monitoring
- Recovery
- Checkpointing
- Scheduling
- Runtime reporting
- Process startup controls

Generally do not generate transformation code.

---

## IGNORE DURING CONVERSION

Examples:

```text
max-core
auto-max-core
stages
buffers
hostnames
ports
SSL
permissions
monitoring
diagnostics
runtime statistics
```

Reason:

No effect on resulting business data.

---

# SECTION 7: WRAPPER SCRIPT INTERPRETATION RULES

## Step 1

Identify MP command.

---

## Step 2

Determine command category.

Possible categories:

- Read
- Write
- Transform
- Join
- Aggregate
- Sort
- Partition
- Runtime

---

## Step 3

Extract command parameters.

---

## Step 4

Normalize parameters using canonical concepts.

---

## Step 5

Classify semantics.

### Business Semantics

Retain.

### Runtime Semantics

Discard.

---

## Step 6

Reconstruct logical data flow.

Example:

```text
Read
→ Filter
→ Join
→ Aggregate
→ Write
```

---

## Step 7

Preserve conversion-relevant concepts.

Retain:

- layouts
- metadata
- filters
- joins
- aggregations
- transformations

---

## Step 8

Discard operational controls.

Discard:

- checkpointing
- logging
- runtime reporting

---

## Step 9

Generate equivalent PySpark pipeline.

---

# SECTION 8: CONVERSION HEURISTICS

| Concept | Likely Meaning | Conversion Impact | Confidence | PySpark Mapping |
|----------|---------------|------------------|------------|----------------|
| layout | Schema | High | Very High | StructType |
| metadata | Field Metadata | High | Very High | StructField Metadata |
| key | Business Identifier | High | Very High | join/groupBy |
| join-key | Join Definition | High | Very High | join |
| partition | Data Distribution | Medium | High | repartition |
| hash-partition | Hash Distribution | Medium | High | repartition |
| range-partition | Ordered Distribution | Medium | High | repartitionByRange |
| collate | Sort Definition | High | Very High | orderBy |
| sort-key | Sort Column | High | Very High | orderBy |
| sorted-input | Pre-sorted Data | High | High | orderBy |
| normalize | Expand Records | High | Very High | explode |
| denormalize | Collapse Records | High | Very High | collect_list |
| dedup | Remove Duplicates | High | Very High | dropDuplicates |
| hash-join | Join | High | Very High | join |
| merge-join | Sorted Join | High | Very High | join |
| rollup | Aggregate | High | Very High | groupBy().agg() |
| hash-rollup | Aggregate | High | Very High | groupBy().agg() |
| scan | Running Aggregate | High | High | Window |
| lookup | Reference Lookup | High | Very High | join |
| broadcast | Replicate Dataset | Medium | High | broadcast |
| reject-limit | Data Quality Threshold | Medium | Medium | Validation |
| checkpoint | Recovery | Low | High | checkpoint |

---

# SECTION 9: LLM REFERENCE TABLES

| Concept | Meaning | PySpark Equivalent |
|----------|----------|----------|
| Layout | Record Schema | StructType |
| Metadata | Field Metadata | StructField Metadata |
| Key | Business Key | join/groupBy |
| Join | Record Matching | join |
| Partition | Data Distribution | repartition |
| Broadcast | Data Replication | broadcast |
| Sort | Ordering | orderBy |
| Collate | Sort Definition | orderBy |
| Aggregate | Summarization | agg |
| Rollup | Group Aggregation | groupBy().agg() |
| Scan | Running Aggregation | Window |
| Normalize | Row Expansion | explode |
| Denormalize | Row Consolidation | collect_list |
| Dedup | Duplicate Removal | dropDuplicates |
| Lookup | Reference Enrichment | join |
| Dataset | Persistent Storage | persist |
| Filter | Record Selection | filter |
| Transform | Business Logic | withColumn |
| Publish | Write Output | DataFrame.write |
| Subscribe | Read Input | Spark Reader |
| Compression | Storage Optimization | compression option |
| Encryption | Data Protection | encryption |

---

# SECTION 10: COMMAND REFERENCE SUMMARIZATION

## Runtime Commands

| Command | Purpose | Execution Stage | Business Relevance | Runtime Relevance | PySpark Relevance |
|----------|----------|----------|----------|----------|----------|
| run | Execute graph | Runtime | None | High | Ignore |
| phase | Execute phase | Runtime | None | High | Ignore |
| checkpoint | Recovery point | Runtime | None | High | Ignore |
| recover | Restart execution | Runtime | None | High | Ignore |
| show | Runtime information | Runtime | None | High | Ignore |
| check | Validation | Runtime | None | High | Ignore |
| reset | Runtime reset | Runtime | None | High | Ignore |

---

## Flow Commands

| Command | Purpose | PySpark Relevance |
|----------|----------|----------|
| straight-flow | Direct routing | Pipeline topology |
| fan-in-flow | Merge streams | Union/Merge semantics |
| fan-out-flow | Split streams | Branching |
| all-to-all-flow | Many-to-many routing | Repartition |
| adaptive-flow | Dynamic routing | Custom routing |

---

## Partition Commands

| Command | Purpose | PySpark Mapping |
|----------|----------|----------|
| hash-partition | Hash distribution | repartition |
| range-partition | Range distribution | repartitionByRange |
| roundrobin-partition | Balanced distribution | repartition |
| function-partition | Custom routing | Custom partition logic |
| load-level-partition | Load balancing | repartition |
| broadcast | Replication | broadcast |

---

## Transformation Commands

| Command | Purpose | PySpark Mapping |
|----------|----------|----------|
| reformat-transform | Transform records | select/withColumn |
| select-transform | Filter rows | filter |
| normalize | Expand rows | explode |
| denormalize | Collapse rows | collect_list |
| dedup | Remove duplicates | dropDuplicates |
| aggregate-transform | Aggregate data | agg |
| split | Branch processing | Multiple DataFrames |
| combine | Merge streams | union |

---

## Join Commands

| Command | Purpose | PySpark Mapping |
|----------|----------|----------|
| hash-join | Join datasets | join |
| merge-join | Sorted join | join |
| lookup | Reference join | join |
| broadcast lookup | Broadcast join | broadcast + join |

---

## Sort Commands

| Command | Purpose | PySpark Mapping |
|----------|----------|----------|
| local-sort | Sort records | orderBy |
| partial-sort | Partial ordering | sortWithinPartitions |
| sort-groups | Sort groups | orderBy |
| merge-runs | Merge sorted inputs | orderBy/join support |

---

## Dataset Commands

| Command | Purpose | PySpark Mapping |
|----------|----------|----------|
| readfiles | Read input | spark.read |
| writefiles | Write output | DataFrame.write |
| publish | Persist output | write |
| subscribe | Read source | read |
| copy-to-dataset | Create dataset | save |

---

# SECTION 11: BUSINESS VS RUNTIME CLASSIFICATION

## BUSINESS LOGIC

Always Preserve:

- layouts
- metadata
- schemas
- transformations
- packages
- joins
- lookup rules
- filters
- sort definitions
- aggregation definitions
- normalization rules
- denormalization rules
- SQL definitions
- business validations

---

## DATA MOVEMENT SEMANTICS

Preserve when conversion-relevant:

- partitioning
- broadcast
- fan-out
- fan-in
- all-to-all routing
- dataset persistence

---

## RUNTIME EXECUTION LOGIC

Usually Ignore:

- phase execution
- recovery logic
- process creation
- worker allocation
- runtime coordination
- checkpoint control

---

## INFRASTRUCTURE SEMANTICS

Always Ignore:

- logging
- reporting
- monitoring
- diagnostics
- host assignment
- ports
- SSL
- permissions
- memory settings
- buffering
- throttling

---

## Classification Rule

```text
If parameter changes records
    → BUSINESS

If parameter changes data distribution
    → DATA MOVEMENT

If parameter changes execution behavior
    → RUNTIME

If parameter changes infrastructure
    → IGNORE
```

---

# SECTION 12: FINAL KNOWLEDGE BASE OUTPUT

## Canonical MP Interpretation Model

```text
MP
=
Runtime Execution Framework

Business Transformation
=
Layouts
+
Metadata
+
Transforms
+
Filters
+
Joins
+
Aggregations
+
Lookups

Data Movement
=
Flows
+
Partitions
+
Broadcasts
+
Datasets

Runtime Control
=
Run
+
Phase
+
Recovery
+
Checkpoint

Infrastructure
=
Hosts
+
Memory
+
Buffers
+
Monitoring
+
Logging
```

---

## Wrapper Script Conversion Strategy

```text
1. Parse MP commands.

2. Identify executed graph/component.

3. Extract parameters.

4. Normalize concepts.

5. Separate business semantics from runtime semantics.

6. Preserve schemas, filters, joins, transforms,
   aggregations, and datasets.

7. Preserve partitioning only when it impacts
   correctness or performance.

8. Ignore infrastructure controls.

9. Reconstruct logical business pipeline.

10. Generate equivalent PySpark DataFrame logic.
```

---

## Golden Rule

MP primarily describes how Ab Initio executes workloads, not what business transformation is being performed.

For wrapper-script-to-PySpark conversion:

PRESERVE:
- Schema definitions
- Metadata
- Transform logic
- Filters
- Joins
- Aggregations
- Lookups
- Dataset semantics
- Ordering requirements
- Distribution semantics when relevant

IGNORE:
- Logging
- Monitoring
- Recovery
- Diagnostics
- Resource allocation
- Host assignment
- Memory tuning
- Buffer tuning
- Process orchestration

Generate PySpark from business intent rather than runtime mechanics.
