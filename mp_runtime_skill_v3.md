---
name: abinitio-mp-runtime-to-pyspark
description: Interpret Ab Initio wrapper scripts built from `mp` commands (mp job/layout/metadata/file/flow/component/phase/run) and convert the graph they execute into an equivalent PySpark pipeline. Use whenever input contains `mp <command>` lines, a deployed graph .ksh, or a request to explain or convert Ab Initio runtime commands.
version: v3.2
source: mp help (Ab Initio Co>Operating System `mp` usage text, 438 commands, 308 options)
consumer: LLM (conversion agent)
target: PySpark (Spark 3.3+ DataFrame API)
---

# MP RUNTIME SKILL — Ab Initio `mp` wrapper scripts → PySpark

## 0. GOLDEN RULES (read first, never violate)

1. **`mp` builds a job, `mp run` executes it.** Every `mp` line before `mp run` only *declares* objects (layouts, metadata, files, components, flows, phases). Parse the whole job first, then build the DAG, then generate code.
2. **`layout` ≠ schema.** `mp layout` = where/how many partitions run (parallelism). The schema is `mp metadata` (DML) carried on flows via `-metadata`. Metadata → `StructType`.
3. **`-limit`/`-ramp` are reject thresholds, not row limits.** The component aborts when `rejects > limit + ramp × records_processed`. Never emit `DataFrame.limit()` for them.
4. **Business semantics are preserved exactly**: schemas, transforms (xfr), filters, keys, join types, NULL key semantics, dedup survivor rules, aggregations, ordering *when observable*, output paths/formats/write modes, side effects (truncate, delete, rename, SQL), reject handling.
5. **Runtime mechanics are dropped**: partitioning, departitioning, layouts, memory (`-max-core`), buffers, stages, folding, logging/reporting, checkpoints, tiled/internal components. Record what was dropped in the conversion report.
6. **Phases run strictly in order** (barrier). All writes of phase N complete before phase N+1 starts.
7. **Sorting is only kept when order is observable** (ordered target file, first/last selection, running totals, sequence numbers, `concat` order). Sorts that exist only to feed rollup/merge-join/dedup are dropped.
8. **Ports carry routing semantics**: `out`, `deselect`, `reject`, `error`, `log`, indexed `in0..inN`/`out0..outN`. A connected `deselect`/`reject` port means a second DataFrame must be produced and written.
9. **Join NULL semantics**: with `-sql-null-comparisons` NULL keys never match (`==`); without it NULL matches NULL (`eqNullSafe`). This default is read from the flag's meaning — confirm on a sample containing NULL keys during reconciliation.
10. **Side effects are kept in phase order** (db-truncate before load, delfile, rename, ftp, db-runsql, send-mail).
11. **Never invent semantics.** Unknown command, unresolved parameter or missing transform → mark `MANUAL_REVIEW`, list it under `unresolved`, emit a `NotImplementedError` stub. Never hard-code passwords, secrets or credentials.
12. **Output follows the Output Contract** (section 13): parsed job YAML → PySpark module → conversion report.

---

## 1. PURPOSE & SCOPE

This file is the knowledge base an LLM uses to:

- read an Ab Initio wrapper script (generated/deployed `.ksh` or hand-written shell) that contains `mp` commands;
- reconstruct the graph it runs (datasets, components, flows, phases, parameters);
- separate business semantics from runtime mechanics;
- produce a faithful PySpark implementation plus an auditable conversion report.

Out of scope here (handled by the transform/DML skill when present): full translation of DML transform language (`.xfr`) bodies. This file gives the mapping for component *shapes* and the most common DML expressions.

Reading order for the LLM: §0 rules → §2–§6 model → §7 classification → §12 algorithm → §13 contract → look up each command in §9 and each option in §8 as encountered.

---

## 2. EXECUTION MODEL — how a graph runs from a wrapper script

```text
Wrapper script (ksh/bash)
  ├─ environment & parameters  (export AI_SERIAL=..., RUN_DT=..., sourced env files)
  ├─ optional pre-steps        (m_rm, m_mkdir, mkdir, cleanup, file arrival checks)
  ├─ mp job <graph_name>        ← start job definition
  ├─ mp layout / metadata / include / file / ifile / ofile / error
  ├─ mp <component> ...         ← component definitions (name + positional args + options)
  ├─ mp add-port / port         ← extra ports / port options
  ├─ mp *-flow ...              ← connect component ports (with -metadata)
  ├─ mp phase / checkpoint      ← execution boundaries
  ├─ mp run [-report ...]       ← EXECUTE: build topology, spawn processes, move data
  ├─ status=$?                  ← non-zero = failure
  └─ optional post-steps        (rename/move outputs, send mail, exit $status)
```

Runtime lifecycle inside `mp run`:

```text
Resolve definitions → build topology (layouts × components) → allocate partitions
→ spawn one process per component partition → connect flows (pipes/files)
→ phase 0 components run concurrently, records stream through flows
→ phase barrier (+ optional checkpoint) → phase 1 ... → phase N → cleanup → exit status
```

Data is created only by components that write (ofile targets, writers, db loads, publishers, lookup writers). Everything between is in-flight records. In PySpark the equivalent is a lazy DataFrame DAG per phase, materialized by write actions in phase order.

Mapping of the model:

| Ab Initio runtime | PySpark |
|---|---|
| job | Python module / `main()` with `SparkSession` |
| parameters / env vars | `config` dict / CLI args (`argparse`) |
| phase | function `run_phase_<n>(spark, cfg)` called sequentially |
| component | DataFrame transformation (or side-effect step) |
| flow | Python variable holding a DataFrame |
| metadata (DML) | `StructType` |
| layout (N partitions) | nothing (Spark manages parallelism) |
| ifile / ofile | `spark.read` / `df.write` |
| reject/deselect port | extra DataFrame (+ write if connected) |
| checkpoint | idempotent writes so the job can be rerun |
| `mp run` exit status | process exit code / raised exception |

---

## 3. OBJECT MODEL

| Object | Declared by | Meaning | Converts to |
|---|---|---|---|
| Job | `mp job <name>` | Container of all following definitions until `mp run` | App/module name |
| Layout | `mp layout <name> [-exact\|-workdir] <partition>...` or `-hosts <host>...` | Partitions/hosts a component runs on (degree of parallelism, working dirs). **Not a schema.** | Nothing (note serial vs parallel) |
| Metadata | `mp metadata <name> [-file] <dml>` / `-string <dml>` [`-family`] | Record format (DML) | `StructType` + reader options |
| File | `mp file/ifile/ofile/iofile <name> <url>...` | Dataset; one URL per partition; `mfile:` = multifile | read/write path(s) |
| Component | `mp <command> <name> <args> [options] -layout <layout>` | Processing step | DataFrame op / side effect |
| Port | `<component>.<port>[.<subport>]` | Connection point | Input/output DataFrame |
| Flow | `mp <type>-flow <name> <from-port> <to-port> [-metadata md]` | Data edge | Variable passing |
| Phase | `mp phase <name>` | Execution barrier | Sequential function |
| Checkpoint | `mp checkpoint <name>` | Recovery point | Idempotency |
| Macro | `mp begin-macro` … `mp end-macro`, `mp port <macro>.<port> <port>` | Subgraph | Python function |
| Error | `mp error <name> <error-string>...` | Named error definitions | Exceptions / reject reasons |

### Ports

Standard port names (observed in graph definitions): `in`, `out`, `reject`, `error`, `log`, `deselect`; file datasets expose `read` (source side) and `write` (target side). Multi-input/output components use indexed ports `in0, in1, …` / `out0, out1, …`.

- `in0` is the first (left) input of joins/merges; order is significant.
- `reject` = records the transform could not process (counted against `-limit`/`-ramp`).
- `error` = diagnostic text for each reject; `log` = log events — runtime, ignore unless written to a business target.
- `deselect` = records failing a filter expression.
- A flow attached to a port is what makes that output real. Unconnected optional ports produce nothing.

---

## 4. PARSING RULES (shell → mp commands)

1. Apply shell semantics first: join `\` line continuations; strip comments (`#`); honour quoting ('single' = literal, "double" = expands `$VAR`); here-docs fed to `mp -` are mp commands.
2. Resolve parameters: `$VAR`, `${VAR}`, `$(command)`. If a value is known (exported earlier in the script or supplied), substitute it. If not, keep it symbolic as a config parameter `cfg["VAR"]` and list it under `unresolved` — never guess values.
3. Tokenize each `mp` line: `mp <command> <tokens...>`. The command is the first token after `mp` (case-sensitive: `Read_JSON` ≠ `read-json`).
4. Look up the command in §9. Match tokens against its signature:
   - Positional `<arg>`s are taken in order (e.g. `hash-join <name> <key> <xfr>`).
   - Tokens starting with `-` (or `+` flags) are options; an option consumes a value when its signature shows `<value>`, `{a|b}` or `(a | b)`; otherwise it is a boolean flag.
   - Options may repeat (`-key` in write-lookup-indexes, `-report` in run); keep a list.
   - `N`-suffixed options (`-primarySqlN`, `-filterN`, `-transformN`, `-secondarySqlN`) are numbered groups: `-primarySql1`, `-filter1`, …
   - Signatures ending in `...` or `<options...>` have undocumented options: capture raw tokens verbatim.
5. `-layout <layout>` appears on almost every component: record it, do not convert it.
6. Commands that appear more than once in mp help with different forms (`port`, `layout`, `metadata`) are disambiguated by their tokens (e.g. `-hosts`, `-string`, or a second argument that is a port).
7. Non-mp lines in the wrapper (shell, `m_*` utilities, `air` commands, `if`/`for` logic) are interpreted per §15.

### Positional argument types

| Placeholder in help | Meaning | Conversion |
|---|---|---|
| `<name>` | Component/flow/object name | Variable / function name (sanitize to snake_case, keep original in comments) |
| `<key>`, `<collate>`, `<collator>`, `<natural_key>`, `<surrogate_key>` | Key specifier, e.g. `{cust_id; txn_dt descending}` (§11) | join/groupBy/orderBy columns |
| `<xfr>`, `<xfr-file>`, `<transform>`, `<package>`, `<package_file>` | DML transform/package (file path or inline text) | Business logic (translate body) |
| `<select>`, `<function>`, `<dml_expression>` | DML expression | `F.expr` / Column expression |
| `<limit>`, `<ramp>` | Reject threshold | §6 |
| `<logevents>`, `<log-events>` | Log event mask | Ignore |
| `<layout>` | Layout name | Ignore (parallelism) |
| `<url>` | Dataset URL (`file://host/path`, `mfile:`, `hdfs://`, `s3://`...) | Path (strip host for local FS; multifile → directory) |
| `<db-config-file>` | `.dbc` connection file | JDBC/connector config from secrets |
| `<sql-file>`, `<sql-select-clause>`, `<update-statement-file>`, `<insert-statement-file>` | SQL | Query / MERGE statement |
| `<ablocal-expression>` | Per-partition WHERE for parallel unload | Spark JDBC `predicates`/partitioning |
| `<blocksize>`, `<npartitions>`, `<ndesired>`, `<max-records>`, `<num_records>` | Sizes/counts | As documented per command |

---

## 5. PHASE, CHECKPOINT & ORDERING SEMANTICS

- **Phase barrier**: all components in a phase run concurrently; the next phase starts only when every component of the previous one has finished. Convert phases to sequential functions executed in order; a dataset written in phase N and read in phase N+1 must be fully written (action executed) before it is read.
- **Phase membership**: components belong to the phase most recently declared with `mp phase` before their definition (convention of generated scripts); if the script assigns phases otherwise, follow the script. With no `mp phase`, everything is one phase.
- **Checkpoint**: recovery point after a phase. No code; make every phase idempotent (overwrite or partition-overwrite targets, MERGE instead of blind INSERT) so a rerun of a failed job is safe.
- **Ordering**: Spark DataFrames have no inherent order. Preserve order only where it is observable (§0 rule 7). When needed, capture an explicit sequence (`F.monotonically_increasing_id()` right after the read, or a business timestamp) and use it in `orderBy`/Window. A sort that only prepares sorted input for rollup, merge-join, dedup or merge is not needed in Spark: drop it.
- **Serial outputs**: a target on a single-partition (serial) layout that downstream systems expect as ONE file → `coalesce(1)` and rename the part file in a post-step; a multifile target → directory of part files.

---

## 6. REJECT, ERROR & DESELECT HANDLING

Most transform components carry `-limit <limit> -ramp <ramp>` (or positional `<limit> <ramp>` in deprecated forms).

```text
abort if   rejected_records > limit + ramp × records_processed
limit 0, ramp 0.0  → abort on the first reject ("never reject")
limit 1, ramp 0.01 → tolerate 1 + 1% rejects
large limit (e.g. 999999999) → effectively never abort
```

Ab Initio evaluates the threshold while records flow (it can abort early); a batch conversion evaluates it on totals — note this in the report.

PySpark pattern:

```python
def enforce_reject_threshold(component: str, n_in: int, n_rej: int, limit: int, ramp: float) -> None:
    """Ab Initio reject-threshold semantics: abort if rejects > limit + ramp * records."""
    if n_rej > limit + ramp * n_in:
        raise RuntimeError(f"{component}: {n_rej} rejects exceed limit={limit} ramp={ramp} for {n_in} records")
```

- Rejects come from: transform evaluation errors (bad casts/parsing, NULL into non-nullable field, explicit `force_error`), invalid input records against the DML, and filter evaluation errors.
- Build the reject DataFrame explicitly (e.g. `try_cast`/validation predicates producing `_reject_reason`), write it only if the `reject` port is connected, then call `enforce_reject_threshold`.
- `deselect` (select-transform, reformat `-select` variants, dedup duplicates port) = records not selected; produce it only when the port is connected.
- Loader thresholds (`-num_errors`, `-num-errors-allowed`, `-max_errors`, `-maximum_errors`, ...) follow the same idea for database writes.

---

## 7. CLASSIFICATION FRAMEWORK

Every command (§9) and option (§8) carries one class:

| Class | Meaning | Conversion action |
|---|---|---|
| `DEFINE` | Declares job structure (job, metadata, files, ports, macros) | Consume while building the DAG |
| `BUSINESS` | Changes record content, cardinality or selection | **Convert exactly** |
| `IO_READ` | Data source | `spark.read` / connector read |
| `IO_WRITE` | Data target | `df.write` / connector write (mode!) |
| `DATA_MOVEMENT` | Partitioning, flows, departitioning, copies, codecs | Usually no code; keep only order/broadcast semantics that matter |
| `CONTROL` | Phases, checkpoints, run, streaming mode | Program structure |
| `SIDE_EFFECT` | Acts outside the data stream (truncate, delete, rename, transfer, SQL, mail) | Keep as ordered pre/post step |
| `VALIDATION` | Checks, thresholds, reconciliation | Assertions / reject thresholds |
| `RUNTIME_IGNORE` | Tuning, logging, debug, internal/tiled/EME tooling | Drop, list in report |
| `MANUAL_REVIEW` | Specialised connectors/protocols, undocumented usage | Stub + `unresolved` entry |

Decision rule for anything not listed:

```text
changes record values/fields/count/selection   → BUSINESS
reads or writes a persistent dataset           → IO_READ / IO_WRITE
changes external state without data flow       → SIDE_EFFECT
changes only where/how records are processed   → DATA_MOVEMENT
changes only execution order/recovery          → CONTROL
changes only performance/diagnostics           → RUNTIME_IGNORE
cannot be determined from the text             → MANUAL_REVIEW
```

---

## 8. PARAMETER DICTIONARY (every option in mp help)

Each option is listed once with its meaning, class (§7) and PySpark action. Options not listed for a command in §9 do not apply to it.


### 8.1 Schema / record format

| Option(s) | Meaning | Class | PySpark action |
|---|---|---|---|
| `-metadata` | Record format (DML) carried by a flow | BUSINESS | DataFrame schema (StructType) |
| `-string` | `mp metadata -string`: inline DML | BUSINESS | Parse DML -> StructType |
| `-file` | `mp metadata -file`: DML file | BUSINESS | Parse DML file -> StructType |
| `-family` | Metadata family (type system) | DEFINE | Informational |
| `-dynamic` | Dynamic type description for generated records | BUSINESS | Schema for generated DataFrame |
| `-nullable`, `-case`, `-prefer`, `-endianness`, `-utf8_aware` | SAS metadata conversion controls | DEFINE | Influence derived schema (nullability, name case, format preference, byte order, UTF-8) |

### 8.2 Keys, ordering & selection (business)

| Option(s) | Meaning | Class | PySpark action |
|---|---|---|---|
| `-key` | Key specifier (lookup/index/join/upsert key); may repeat | BUSINESS | join keys / groupBy / MERGE ON |
| `-major-key`, `-minor-key` | sort-groups: group key / within-group sort key | BUSINESS | Window.partitionBy(major).orderBy(minor) |
| `-composite_key` | Name of composite key (SAS) | BUSINESS | Key columns |
| `-select` | DML filter expression (reformat/dedup/db: SQL select) | BUSINESS | `filter(expr)`; for db components: source query |
| `-select-is-encoded` | format-for-dump select encoding | RUNTIME_IGNORE | None |
| `-keep` | dedup survivor: first \| last \| unique-only | BUSINESS | row_number()==1 asc/desc, or count==1 |
| `-allow-unsorted` | Tolerate unsorted major key | RUNTIME_IGNORE | None |
| `-check-sort` | Fail if input not sorted | RUNTIME_IGNORE | None (Spark needs no sorted input) |
| `-maintain-order`, `-maintain_order` | Preserve driving-input order | DATA_MOVEMENT | Only if order is observable: carry a sequence column and orderBy it |
| `-driving` | Driving (streamed) input index of hash-join | RUNTIME_IGNORE | None (optional broadcast of the other side) |
| `-sql-null-comparisons` | SQL NULL semantics on keys (NULL never equal) | BUSINESS | Present: `==`; absent: `eqNullSafe` |
| `-override_surrogate_key` | Explicit surrogate key spec | BUSINESS | Surrogate key column |
| `-generate_unique_key` | IMS lookup: generate unique key | BUSINESS | Add generated key |

### 8.3 Transforms & expressions (business)

| Option(s) | Meaning | Class | PySpark action |
|---|---|---|---|
| `-package` | DML package (helper functions/transform) | BUSINESS | Python helper functions / UDF-free expressions |
| `-transform` | Transform for generated records | BUSINESS | withColumn logic |
| `-transformN`, `-filterN`, `-primarySqlN`, `-secondarySqlN` | db-multi-update statement N: transform, filter, primary SQL, fallback SQL | BUSINESS | Per-statement filter + MERGE/UPDATE |
| `-output-index`, `-output-indexes` | Routing transform: output port index (or vector) | BUSINESS | One filtered DataFrame per out port |
| `-expression`, `-sequential`, `-minimum`, `-maximum`, `-default`, `-seed` | generate: field expression / sequence start / min / max / default / RNG seed | BUSINESS | spark.range + withColumn; F.rand(seed) |
| `-while` | batch-subscribe: continue while expression true | BUSINESS | Filter bound on input |
| `-delete`, `-rename`, `-touch`, `-chmod` | Transform producing paths for delete/rename/touch/chmod | SIDE_EFFECT | File-system side effect per path |
| `-insert` | db-load custom INSERT SQL | BUSINESS | Custom insert (foreachPartition) |
| `-update_count`, `-do_insert` | SAS upsert: update count / allow inserts | BUSINESS | MERGE when-not-matched insert |

### 8.4 Reject thresholds & error policy (business-relevant)

| Option(s) | Meaning | Class | PySpark action |
|---|---|---|---|
| `-limit` | Reject threshold constant: abort when rejects > limit + ramp * records | VALIDATION | Count reject rows; raise if threshold exceeded. NEVER `DataFrame.limit()` |
| `-ramp` | Reject threshold rate per record processed | VALIDATION | Same formula as -limit |
| `-reject-threshold` | Service-call reject policy | VALIDATION | Fail/continue policy on failed calls |
| `-num_errors`, `-num-errors-allowed`, `-num-discards-allowed`, `-max_errors`, `-max_rejects`, `-maximum_errors` | Loader / SQL error tolerances | VALIDATION | Reject threshold for writes/SQL |
| `-file-empty`, `-file-missing`, `-file-unavailable`, `-filename-error`, `-input-error` | File error policies (Fail\|Notify\|Ignore\|Accept) | VALIDATION | Pre-checks: raise / warn / skip |
| `-extra-data` | BC data outside index: abort\|ignore\|process | VALIDATION | Raise / drop / keep |
| `-missing-fields` | xml-split: ignore \| error | VALIDATION | Fail on missing elements |
| `-on-address-error` | send-mail: Reject \| Log \| Ignore | VALIDATION | Notification error policy |
| `-ignore_drop_table_errors` | Ignore DROP TABLE errors | VALIDATION | try/except around DROP |
| `-ignore_missing` | Ignore missing files on delete | VALIDATION | No error when absent |
| `-ignoreDuplicates` | Ignore duplicate-key errors (compat\|False\|True) | VALIDATION | MERGE without failing on duplicates |
| `-no_actions_ok` | Allow statements affecting zero rows | VALIDATION | Do not fail on 0 rows |
| `-match_required`, `-match_optional` | Lookup: match required (inner) / optional (left) | BUSINESS | inner vs left join |
| `-maximum_matches` | Max matches per lookup key | BUSINESS | row_number <= n per key |
| `-error_group`, `-log_group` | WS-security error/log group | RUNTIME_IGNORE | None |

### 8.5 Datasets, files & formats

| Option(s) | Meaning | Class | PySpark action |
|---|---|---|---|
| `-format` | File format: uninterpreted\|orc\|parquet\|seqfile\|avro\|texttable | BUSINESS | `spark.read/write.format(...)` (uninterpreted = DML-described binary/text via schema) |
| `-hadoop-codec` | Codec: none\|gzip\|bzip2\|zlib\|snappy\|lzo\|lz4\|deflate | DATA_MOVEMENT | `.option("compression", codec)` |
| `-compressed` | Compressed data/flow | DATA_MOVEMENT | Compression option (flows: ignore) |
| `-compression`, `-decompress` | broadcast compression level / decompress | RUNTIME_IGNORE | None |
| `-flags` | File open flags | IO_WRITE | Append vs overwrite hint (e.g. contains O_APPEND -> append) |
| `-mode` | File open mode / flow file mode / loader mode (insert\|append\|replace) / List_Files mode / permission mode | IO_WRITE | Write mode or listing mode depending on command |
| `-create-directories` | Create parent directories | RUNTIME_IGNORE | None (Spark creates paths) |
| `-mvs_alloc_options` | MVS dataset allocation | RUNTIME_IGNORE | None |
| `-xmf`, `-rxmf`, `-xmf_type` | Extended multifile control / relative\|absolute | DATA_MOVEMENT | Treat as directory of partition files |
| `-overwrite` | Overwrite target | IO_WRITE | `mode("overwrite")` |
| `-max-files` | Max open output files | RUNTIME_IGNORE | None |
| `-grouped` | Write grouped files (one per group) | IO_WRITE | `partitionBy(group_col)` |
| `-permissions`, `-file-mode` | Output permissions | SIDE_EFFECT | Post-write chmod if required |
| `-skip-count`, `-read-count` | Skip N / read N records per file | BUSINESS | Header skip / bounded read |
| `-read_as_single_record`, `-read-blocks`, `-maximum-block-bytes` | Read file as one record / in blocks | BUSINESS | `wholetext` / `binaryFile` |
| `-file-pattern`, `-directory-prune`, `-singledir` | List_Files glob include / exclude / single-dir expansion | BUSINESS | Path filtering |
| `-publish-style`, `-publish_style`, `-create_empty`, `-create-empty`, `-delete_file_if_exists` | Publish style / create empty output / delete before write | IO_WRITE | Write mode & empty-file handling |
| `-varstring` | Variable-length string framing | BUSINESS | Record framing on read/write |
| `-more`, `-id`, `-wait`, `-nodelete`, `-infile`, `-wait_for_commit`, `-poll_delay` | Subscriber controls (continuation, id, wait, keep files, local file, commit wait, polling) | DATA_MOVEMENT | Batch vs streaming source config |
| `-checkpoint-trigger`, `-checkpoint_interval` | Subscriber checkpoint trigger / interval | CONTROL | Streaming checkpointLocation/trigger |
| `-continuous` | Continuous (streaming) mode | CONTROL | Structured Streaming |
| `-sandbox`, `-lock`, `-break-locks`, `-validate` | EME sandbox dataset operations | RUNTIME_IGNORE | None |
| `-path` | Simulator path | RUNTIME_IGNORE | None |
| `-full_url` | SAS run URL | MANUAL_REVIEW | Review |
| `-memport` | SAS in-memory port (port, dml_file, dml_name) | MANUAL_REVIEW | Review |

### 8.6 XML / document formatting

| Option(s) | Meaning | Class | PySpark action |
|---|---|---|---|
| `-parser` | XML parser expat\|xerces | RUNTIME_IGNORE | None |
| `-ignore-blank-cdata`, `-custom-format`, `-root-element`, `-base-element`, `-leaf-element`, `-character-data-field` | XML mapping controls | BUSINESS | spark-xml rowTag/rootTag/valueTag |
| `-add-bom`, `-pretty-formatting`, `-indent-size`, `-document-header`, `-document-per-record`, `-xml-doc-field-name` | XML output formatting / doc granularity / field | BUSINESS | to_xml formatting; doc-per-record |
| `-i`, `-o`, `-port` | Named input/output port (xml-combine/split) or network port | DEFINE | DAG wiring / connection config |
| `-as-xml`, `-print-metadata`, `-print-functions`, `-print-internals`, `-progress`, `-start`, `-records`, `-attribute`, `-protocol`, `-mark-invalid`, `-easy-sort` | format-for-dump display controls | RUNTIME_IGNORE | None |

### 8.7 Database

| Option(s) | Meaning | Class | PySpark action |
|---|---|---|---|
| `-table`, `-table_name`, `-itable`, `-otable` | Target/source table name | BUSINESS | JDBC dbtable |
| `-database`, `-collection` | Mongo database / collection | BUSINESS | Connector options |
| `-commitnum`, `-commit_number`, `-rows_per_commit`, `-batchSize`, `-bindsize` | Commit / batch size | RUNTIME_IGNORE | JDBC `batchsize` (performance) |
| `-commitTable`, `-jobid` | Restart bookkeeping table / job id | CONTROL | None (make MERGE idempotent) |
| `-max-rows-per-partition` | Unload partition size | RUNTIME_IGNORE | JDBC partitioning/fetchsize |
| `-controller-node`, `-haostname` | Loader controller host | RUNTIME_IGNORE | None |
| `-direct`, `-directfile`, `-directparallel` | Oracle direct-path load | RUNTIME_IGNORE | None |
| `-interface` | DB interface (api) / IMS local\|remote | RUNTIME_IGNORE | None |
| `-batch_mode`, `-ims_parameters`, `-alternate_psbname`, `-parent_segment_data` | IMS controls | MANUAL_REVIEW | Review |
| `-unique_identifier` | DB2 USS load unique id | RUNTIME_IGNORE | None |
| `-rebuild_loader`, `-ifs_data_path`, `-ifs_dml_path` | AS/400 loader internals | RUNTIME_IGNORE | None |
| `-brief` | Less verbose | RUNTIME_IGNORE | None |
| `-dialect`, `-dialect2`, `-control-dependencies` | SQL_Analysis dialect / dependency control | RUNTIME_IGNORE | None |
| `-options` | `mvs_alloc_options` value | RUNTIME_IGNORE | None |

### 8.8 Lookup indexing

| Option(s) | Meaning | Class | PySpark action |
|---|---|---|---|
| `-block_size`, `-bitmap_size`, `-miss_rate`, `-skip_empty`, `-keep_bitmap` | Lookup index tuning | RUNTIME_IGNORE | None |
| `-reverse`, `-read_only` | vsamlookup: reverse key traversal / open read-only | MANUAL_REVIEW | Review (ordering of matches; no writes) |

### 8.9 Runtime, performance & topology (ignore for logic)

| Option(s) | Meaning | Class | PySpark action |
|---|---|---|---|
| `-layout` | Execution layout (partitions/hosts) of the component | DATA_MOVEMENT | None (parallelism only) |
| `-hosts`, `-exact`, `-workdir` | Layout forms: host list / exact partitions / workdirs | DATA_MOVEMENT | None |
| `-max-core`, `-max_core`, `-auto-max-core`, `-max_memory` | Memory limits | RUNTIME_IGNORE | None |
| `-stages`, `-buffer`, `-buffer-bytes`, `-buffer-records`, `-buffer-kbytes`, `-n-buffers`, `-flush`, `-breakable`, `-throttle` | Pipeline/buffer/flow tuning | RUNTIME_IGNORE | None |
| `-encrypted` | Encrypted flow transport | RUNTIME_IGNORE | None (use TLS/at-rest encryption config) |
| `-log`, `-logging`, `-log_input`, `-log_output`, `-log_reject`, `-debug-file-name`, `-report_file`, `-report_period` | Logging / diagnostics | RUNTIME_IGNORE | Python logging only |
| `-report` | `mp run` reporting: verbose-errors, expanded-graph, flows, table-flows, file-percentages, times, spillage, totals, skew[=n], scroll, file=, interval=, show-folding | RUNTIME_IGNORE | Optional metrics logging |
| `-startup-seconds`, `-startup-attempts`, `-startup-retry-seconds`, `-max-clients`, `-progress-timeout-seconds`, `-io-method`, `-group` | read-shared service runtime | RUNTIME_IGNORE | None |
| `-save-output-frequency`, `-concurrency-mode`, `-concurrent_requests-allowed`, `-concurrent-requests-max-number`, `-concurrent-requests-maintain-record-order`, `-asynchronous`, `-timeout`, `-always-reconnect` | Service-call concurrency / timeouts | RUNTIME_IGNORE | Client config (keep order if maintain-record-order) |
| `-multicast_IP_address`, `-udp_block_size`, `-max_send_speed`, `-max_send_speed_check_period`, `-resend_period`, `-acknowledgement_period`, `-acknowledgement_group_size` | Multicast/UDP network tuning | RUNTIME_IGNORE | None |
| `-grab-input` | send-to-ui input grab | RUNTIME_IGNORE | None |

### 8.10 Connectivity & credentials (secrets)

| Option(s) | Meaning | Class | PySpark action |
|---|---|---|---|
| `-hostname`, `-host`, `-rhost`, `-smtp-server-host`, `-smtp-server-port` | Remote host/port | SIDE_EFFECT | Connection config from environment |
| `-user`, `-user-name`, `-pw`, `-password`, `-password-encrypted`, `-acct`, `-passphrase`, `-encrypted-passphrase`, `-private-key-password`, `-private-key-password-encrypted`, `-ssl_key_password`, `-ssl_key_password_encrypted` | Credentials | SIDE_EFFECT | Secret manager reference; never hard-code |
| `-no-passphrase`, `-need-passphrase`, `-sftp`, `-scp`, `-program`, `-extra-args`, `-tolerate-output`, `-dont-tolerate-output`, `-sftp-script`, `-source-file`, `-target-file`, `-temp-target` | sftp/scp controls & paths | SIDE_EFFECT | paramiko/scp step |
| `-rfile`, `-ascii`, `-binary`, `-site`, `-appremote`, `-write_raw_data`, `-session-per-blip`, `-session-per-transfer` | FTP controls & paths | SIDE_EFFECT | ftplib step |
| `-ssl`, `-ssl_cert_file`, `-ssl_key_file`, `-ssl_key_type`, `-ssl_cacert_file`, `-ssl_cacert_path`, `-ssl_verify_cert`, `-ssl_ciphers`, `-ssl_random_file`, `-ssl_random_length`, `-ssl_configuration_string` | TLS settings | RUNTIME_IGNORE | Client TLS config |
| `-verify-username-password`, `-verify-timestamp`, `-verify-client-certificate`, `-verify-signatures`, `-public-key`, `-public-key-type`, `-cacert-file`, `-cacert-path` | WS-Security verification | MANUAL_REVIEW | Security library config |
| `-password-auth-type`, `-password-digest-type`, `-client-certificate`, `-client-certificate-type`, `-sign`, `-sign-body`, `-sign-usernametoken`, `-sign-timestamp`, `-sign-xpointer`, `-xpointer-expression`, `-private-key`, `-private-key-type`, `-signature-algorithm` | WS-Security signing | MANUAL_REVIEW | Security library config |
| `-validate-addresses` | Validate e-mail addresses | SIDE_EFFECT | Notification config |

---

## 9. COMMAND CATALOG (all mp commands)

Entry format: `command` · exact mp help usage · Class · Meaning · PySpark action. In code, `F` = `pyspark.sql.functions`. Options are explained in §8.


### 9.1 Job administration & execution

Build and launch the job definition. Every command before `mp run` only *defines* the job; nothing executes until `mp run`. Default class: `CONTROL`. Default PySpark: No DataFrame code. Drives program structure.

- `job` `mp job [ <jobname> ]` Class: DEFINE. Meaning: Starts a new job definition; `<jobname>` = graph name. Everything until `mp run` belongs to this job. PySpark: Name of the generated PySpark application/module (`app_name`).
- `reset` `mp reset` Class: RUNTIME_IGNORE. Meaning: Clears stored definitions (wrapper hygiene before `mp job`). PySpark: None.
- `show` `mp show` Class: RUNTIME_IGNORE. Meaning: Prints stored definitions (debug). PySpark: None.
- `check` `mp check` Class: VALIDATION. Meaning: Validates stored definitions without running. PySpark: None (optional: static validation of the converted job).
- `comment` `mp comment <comment text>` Class: RUNTIME_IGNORE. Meaning: Free-text documentation inside the job. PySpark: Emit as a Python comment only.
- `-` `mp -` Class: DEFINE. Meaning: `mp -` reads mp commands from standard input (often a here-doc). Treat its content exactly like inline mp commands. PySpark: None. Parse the commands from the stdin/here-doc block as if inline.
- `run` `mp run [-report verbose-errors] [-report expanded-graph] [-report flows] [-report table-flows] [-report file-percentages] [-report times] [-report spillage] [-report totals] [-report skew[=<n>]] [-report scroll={true|false}] [-report file=<filename>] [-report interval=<n>] [-report show-folding]` Class: CONTROL. Meaning: Executes the defined job. Exit status non-zero = failure; wrappers usually test `$?`. All `-report` sub-options are monitoring only. PySpark: Entry point: `main()` executes phases in order. `-report` values -> optional Spark/structured logging, never business logic.

### 9.2 Definitions: files, layouts, metadata, includes, errors

Declarative objects referenced by components and flows. Default class: `DEFINE`.

- `file` `mp file <name> <url> ... [-flags <flags>] [-mode <mode>] [-create-directories] [-mvs_alloc_options <options>] [-format {uninterpreted|orc|parquet|seqfile|avro|texttable}] [-xmf|-rxmf|-xmf_type {relative|absolute}] [-hadoop-codec {none|gzip|bzip2|zlib|snappy|lzo|lz4|deflate}]` Class: DEFINE. Meaning: Generic file definition. `<url>` may repeat (one URL per partition). URL schemes: `file://host/path`, `mfile:` (multifile = N partition files -> treat as a directory / list of paths), `hdfs://`, `s3://` etc. PySpark: Dataset path; direction inferred from the flow port (`.read` = source, `.write` = target). `-format` -> `spark.read.format(...)`; `-hadoop-codec` -> `.option("compression", ...)`.
- `ifile` `mp ifile <name> <url> ... [-flags <flags>] [-mode <mode>] [-create-directories] [-mvs_alloc_options <options>] [-format {uninterpreted|orc|parquet|seqfile|avro|texttable}]` Class: IO_READ. Meaning: Input file. Data source. PySpark: `spark.read` with schema from the flow metadata (DML -> StructType).
- `ofile` `mp ofile <name> <url> ... [-flags <flags>] [-mode <mode>] [-create-directories] [-mvs_alloc_options <options>] [-format {uninterpreted|orc|parquet|seqfile|avro|texttable}] [-xmf|-rxmf|-xmf_type {relative|absolute}] [-hadoop-codec {none|gzip|bzip2|zlib|snappy|lzo|lz4|deflate}]` Class: IO_WRITE. Meaning: Output file. Data target. PySpark: `df.write` (mode overwrite unless `-flags`/`-mode` indicate append).
- `iofile` `mp iofile <name> <url> ... [-flags <flags>] [-mode <mode>] [-create-directories] [-mvs_alloc_options <options>] [-format {uninterpreted|orc|parquet|seqfile|avro|texttable}] [-xmf|-rxmf|-xmf_type {relative|absolute}] [-hadoop-codec {none|gzip|bzip2|zlib|snappy|lzo|lz4|deflate}]` Class: IO_READ. Meaning: Input/output file (read and written in the same job, typically different phases). PySpark: Read before write; if the same path is read then rewritten, materialize (`checkpoint()`/write to temp) first.
- `layout` `mp layout <name> [-exact | -workdir] <partition> [<partition> ...]` | `mp layout <name> -hosts <hostname> [<hostname> ...]` Class: DATA_MOVEMENT. Meaning: Execution topology: list of partition directories or hosts. NOT a record schema. `-exact` = use partitions exactly as listed; `-workdir` = partitions are working directories; `-hosts` = host list. PySpark: None for logic. Partition count = parallelism hint only; serial (1 partition) output may imply `coalesce(1)` for single-file targets.
- `metadata` `mp metadata <name> [-file] <filename> [-family <mdfamily>]` | `mp metadata <name> -string <string> [-family <mdfamily>]` Class: DEFINE. Meaning: Record format (DML). `-file` reads DML from a file; `-string` inline DML; `-family` = metadata family/type system. This IS the schema. PySpark: DML record -> `StructType` (see DML mapping section). Delimiters/fixed widths -> reader options.
- `include` `mp include <filename>` Class: DEFINE. Meaning: Includes a file (DML/XFR/package) into the job definition. PySpark: Load referenced DML/XFR so transforms and types can be resolved.
- `error` `mp error <name> <error-string> [<error-string> ...]` Class: DEFINE. Meaning: Defines named error strings/conditions used by the job. PySpark: Map to reject/abort handling: raise custom exception with these messages.

### 9.3 Ports, macros, folds

Wiring of components. Port reference = `<component>.<port>[.<subport>]`. Default class: `DEFINE`. Default PySpark: Used only to build the DAG; no direct code.

- `add-port` `mp add-port <component>.<portname>[.<subportname>] [<option> ...]` Class: DEFINE. Meaning: Adds a port to a component (e.g. extra input `in1`, extra output `out2`) with options. Port count drives the number of input/output DataFrames. PySpark: Used only to build the DAG; no direct code.
- `port` `mp port <macroname>.<portname>[.<subportname>] <port>` | `mp port <component>.<portname>[.<subportname>] [<option> ...]` | `mp port <macroname>.<portname>[.<subportname>] <port>` Class: DEFINE. Meaning: Two forms: (1) configure a component port with options, (2) map a macro port to an inner port. Resolve through macros to the real component. PySpark: Used only to build the DAG; no direct code.
- `begin-macro` `mp begin-macro <name>` Class: DEFINE. Meaning: Start of a macro (subgraph) definition. PySpark: Inline the macro body as a Python function (subgraph).
- `end-macro` `mp end-macro <name>` Class: DEFINE. Meaning: End of a macro definition. PySpark: Used only to build the DAG; no direct code.
- `begin-fold` `mp begin-fold <name>` Class: RUNTIME_IGNORE. Meaning: Component folding (runtime process optimization; see `-report show-folding`). PySpark: None.

### 9.4 Execution control

Ordering and recovery boundaries. Default class: `CONTROL`.

- `phase` `mp phase <name>` Class: CONTROL. Meaning: Phase barrier: every component of phase N finishes before phase N+1 starts. Data written in an earlier phase may be read later. PySpark: Execute phases strictly in order; each phase = function; all writes of phase N complete before phase N+1 reads.
- `checkpoint` `mp checkpoint <name>` Class: CONTROL. Meaning: Recovery point after a phase (restart from here). Runtime only. PySpark: No code. Make each phase idempotent (overwrite/partition-overwrite) so a rerun is safe.
- `begin-transaction` `mp begin-transaction <name> <command-line>` Class: MANUAL_REVIEW. Meaning: Starts a transactional section with a command line. PySpark: Wrap in a transactional boundary (e.g. Delta transaction / staging + swap).
- `end-transaction` `mp end-transaction <name> <command-line>` Class: MANUAL_REVIEW. Meaning: Ends a transactional section. PySpark: Close the transactional boundary opened by begin-transaction (commit/swap).

### 9.5 Flows

Connect `<from-port>` to `<to-port>`. Common options: `-metadata <mdname>` (record format on the flow = schema of the DataFrame), `-mode`, `-breakable`, `-encrypted`, `-compressed`, `-flush`, `-stages <n>`, `-buffer` (all except -metadata are runtime). Default class: `DATA_MOVEMENT`. Default PySpark: DataFrame variable passed from producer to consumer. Schema = flow `-metadata`.

- `straight-flow` `mp straight-flow <name> <from-port> <to-port> [-metadata <mdname>] [-mode <file-modes>] [-breakable] [-encrypted] [-compressed] [-flush] [-stages <n>] [-buffer]` Class: DATA_MOVEMENT. Meaning: Same partitioning on both ends (1:1). PySpark: Direct hand-off of the DataFrame.
- `fan-in-flow` `mp fan-in-flow <name> <from-port> <to-port> [-metadata <mdname>] [-mode <file-modes>] [-breakable] [-encrypted] [-compressed] [-flush] [-stages <n>] [-buffer]` Class: DATA_MOVEMENT. Meaning: Many partitions -> fewer (departition side). PySpark: Departition: no code (Spark DataFrame is already logical).
- `fan-out-flow` `mp fan-out-flow <name> <from-port> <to-port> [-metadata <mdname>] [-mode <file-modes>] [-breakable] [-encrypted] [-compressed] [-flush] [-stages <n>] [-buffer]` Class: DATA_MOVEMENT. Meaning: Few partitions -> many (partition side). PySpark: Partition: no code or `repartition` hint.
- `all-to-all-flow` `mp all-to-all-flow <name> <from-port> <to-port> [-metadata <mdname>] [-mode <file-modes>] [-breakable] [-encrypted] [-compressed] [-flush] [-stages <n>] [-buffer]` Class: DATA_MOVEMENT. Meaning: Repartitioning flow (each source partition to every target partition). PySpark: `repartition(keys)` only if it implements a key-based shuffle needed downstream; otherwise none.
- `fan-into-ply0-flow` `mp fan-into-ply0-flow <name> <from-port> <to-port> [-metadata <mdname>] [-mode <file-modes>] [-breakable] [-encrypted] [-compressed] [-flush] [-stages <n>] [-buffer]` Class: DATA_MOVEMENT. Meaning: Fan-in into partition 0 (ply 0), usually serial collection. PySpark: None.
- `fan-outof-ply0-flow` `mp fan-outof-ply0-flow <name> <from-port> <to-port> [-metadata <mdname>] [-mode <file-modes>] [-breakable] [-encrypted] [-compressed] [-flush] [-stages <n>] [-buffer]` Class: DATA_MOVEMENT. Meaning: Fan-out from partition 0. PySpark: None.
- `adaptive-flow` `mp adaptive-flow <name> <from-port> <to-port> [-metadata <mdname>] [-mode <file-modes>] [-breakable] [-encrypted] [-compressed] [-flush] [-stages <n>] [-buffer]` Class: DATA_MOVEMENT. Meaning: Runtime-chosen routing. PySpark: None.

### 9.6 Partition components

Distribute records across partitions. In Spark these are almost always unnecessary for correctness. Default class: `DATA_MOVEMENT`. Default PySpark: No code by default; optional `repartition`/`repartitionByRange` as performance hint.

- `hash-partition` `mp hash-partition <name> <collate> [-stages <n>] -layout <layout>` Class: DATA_MOVEMENT. Meaning: `<collate>` = partition key. PySpark: `df.repartition(*keys)` (optional).
- `range-partition` `mp range-partition <name> <collate> [-stages <n>] -layout <layout>` Class: DATA_MOVEMENT. Meaning: `<collate>` = range key. PySpark: `df.repartitionByRange(*keys)`; with a following per-partition sort this produces a global order -> `orderBy(keys)` if the output order matters.
- `roundrobin-partition` `mp roundrobin-partition <name> <blocksize> [-stages <n>] -layout <layout>` Class: DATA_MOVEMENT. Meaning: `<blocksize>` = records per block. PySpark: None (or `repartition(n)`).
- `function-partition` `mp function-partition <name> <function> [-stages <n>] [-package <package>] [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: `<function>` = DML expression returning partition index; `-package` = function package. PySpark: Only matters if partition number is later used as data (e.g. split to different outputs): compute `expr` as a column and filter per partition. Otherwise none.
- `load-level-partition` `mp load-level-partition <name> -layout <layout>` Class: DATA_MOVEMENT. Meaning: Load-balanced distribution. PySpark: None.
- `broadcast` `mp broadcast [-compression <compression level>] [-decompress] -layout <layout>` Class: DATA_MOVEMENT. Meaning: Replicates every record to all partitions. `-compression`/`-decompress` runtime. PySpark: If feeding a join/lookup: `F.broadcast(df)`; else none.
- `percentage-partition` `mp percentage-partition` Class: BUSINESS. Meaning: Partition by percentage. PySpark: `df.randomSplit([...])` if outputs are consumed separately.
- `partition-transactions-by-expression` `mp partition-transactions-by-expression <name> <function> -layout <layout>` Class: MANUAL_REVIEW. Meaning: Continuous-flow transaction partitioning by expression. PySpark: Streaming/transactional partitioning; review.
- `partition-transactions-to-pool` `mp partition-transactions-to-pool <name> -layout <layout>` Class: MANUAL_REVIEW. Meaning: Transactions to worker pool. PySpark: No code by default; optional `repartition`/`repartitionByRange` as performance hint.
- `serial-file-partition` `mp serial-file-partition <file list ...>` Class: IO_READ. Meaning: Partitions a serial list of files across partitions. PySpark: `spark.read...load([paths])` (a list of files read as one DataFrame).
- `serial-hsf-partition` `mp serial-hsf-partition <file list ...>` Class: IO_READ. Meaning: Same as serial-file-partition for HSF files. PySpark: No code by default; optional `repartition`/`repartitionByRange` as performance hint.
- `pbe-via-tiled` `mp pbe-via-tiled <cmdline>` Class: RUNTIME_IGNORE. Meaning: Partition-by-expression via tiled execution (internal runtime). PySpark: None.
- `pbk-via-tiled` `mp pbk-via-tiled <cmdline>` Class: RUNTIME_IGNORE. Meaning: Partition-by-key via tiled execution (internal runtime). PySpark: None.
- `pbrr-via-tiled` `mp pbrr-via-tiled <cmdline>` Class: RUNTIME_IGNORE. Meaning: Partition-round-robin via tiled execution (internal runtime). PySpark: None.

### 9.7 Departition & copy

Combine partitions/streams or pass through. Default class: `DATA_MOVEMENT`.

- `gather` `mp gather <name> -layout <layout>` Class: DATA_MOVEMENT. Meaning: Collects partitions/flows in arbitrary order. PySpark: None (or `unionByName` when gathering several flows). No order guarantee.
- `concat` `mp concat <name> -layout <layout>` Class: BUSINESS. Meaning: Concatenates inputs in order: all of in0 then in1 ... (order preserved). PySpark: `unionByName` in input order; if downstream depends on the order, add `_src_idx` (input index) + original sequence column and `orderBy` them.
- `interleave` `mp interleave <name> <blocksize> -layout <layout>` Class: DATA_MOVEMENT. Meaning: Round-robin merge by `<blocksize>`. PySpark: `unionByName`; exact round-robin order is not reproducible in Spark (flag if order matters).
- `copy` `mp copy [-throttle <flow rate description>] -layout <layout>` Class: DATA_MOVEMENT. Meaning: Copy records unchanged. `-throttle` = rate limit (runtime). PySpark: Pass-through (reuse DataFrame).
- `buffered-copy` `mp buffered-copy [-throttle <flow rate description>] [-buffer-bytes <nbytes>] [-buffer-records <nrecords>] -layout <layout>` Class: DATA_MOVEMENT. Meaning: Copy with buffering (`-buffer-bytes`, `-buffer-records`, `-throttle` runtime). PySpark: Pass-through.
- `Replicate_Internal` `mp Replicate_Internal <name> -layout <layout>` Class: DATA_MOVEMENT. Meaning: Replicate: same records to every output port. PySpark: Reuse the same DataFrame for every consumer; `cache()` if consumed 2+ times.
- `merge-runs` `mp merge-runs <name> <collator> -layout <layout>` Class: BUSINESS. Meaning: Merges sorted runs into one sorted stream. PySpark: `unionByName` + `orderBy(collator)` only if ordered output is required.
- `local-merge` `mp local-merge <name> <collate> -layout <layout>` Class: BUSINESS. Meaning: Merges sorted inputs keeping order by `<collate>`. PySpark: `unionByName` + `orderBy(collate)` only if ordered output is required.

### 9.8 Sorting

Ordering. In Spark, sorting is only needed when (a) the target must be ordered, (b) a following component depends on order (first/last, scan, sequence numbers). Sorting purely to feed rollup/merge-join/dedup is unnecessary. Default class: `BUSINESS`. Default PySpark: `orderBy(<collate>)` only when order is observable; otherwise drop.

- `local-sort` `mp local-sort <name> <collate> [-auto-max-core] [-max-core <nbytes>] -layout <layout>` Class: BUSINESS. Meaning: `<collate>` key specifier; `-max-core`/`-auto-max-core` memory (ignore). PySpark: `sortWithinPartitions` or `orderBy` (global if after range-partition or before a serial write).
- `partial-sort` `mp partial-sort <name> <collator> [-max-core <nbytes] -layout <layout>` Class: BUSINESS. Meaning: Partial sort. PySpark: `sortWithinPartitions(<collator>)`.
- `sort-groups` `mp sort-groups <name> -major-key <key> -minor-key <key> [-auto-max-core] [-max-core <nbytes>] [-allow-unsorted] -layout <layout>` Class: BUSINESS. Meaning: Input already grouped by `-major-key`; sorts each group by `-minor-key`. `-allow-unsorted` tolerates unsorted major keys. PySpark: `orderBy(major + minor)`; often feeds scan/rollup -> use Window(partitionBy(major).orderBy(minor)).
- `check-order` `mp check-order <name> <collate> <limit> -layout <layout>` Class: VALIDATION. Meaning: Fails if records are not ordered by `<collate>` (more than `<limit>` violations). PySpark: Optional assertion: verify order; no transformation.
- `find-splitters` `mp find-splitters <name> <collate> <npartitions> -layout <layout>` Class: RUNTIME_IGNORE. Meaning: Computes range-partition boundaries for `<npartitions>`. PySpark: None (`repartitionByRange` computes its own bounds).
- `adaptive-sampleL` `mp adaptive-sampleL <name> <ndesired> -layout <layout>` Class: RUNTIME_IGNORE. Meaning: Local adaptive sample for splitters. PySpark: None.
- `adaptive-sampleG` `mp adaptive-sampleG <name> <ndesired> -layout <layout>` Class: RUNTIME_IGNORE. Meaning: Global adaptive sample for splitters. PySpark: None.
- `DF_Sort` `mp DF_Sort <name> [options...] -layout <layout>` Class: BUSINESS. Meaning: Dataflow-style sort component; options carry key. PySpark: `orderBy`.
- `Sort_within_Window` `mp Sort_within_Window <name> <package> [options...]` Class: BUSINESS. Meaning: Sorts records within a window (package defines window). PySpark: Window function ordering (`Window.partitionBy(...).orderBy(...)`).

### 9.9 Record transformation

Business logic. `<xfr>` = transform (file or inline); `<select>` = DML filter expression. Standard ports: `in`, `out`, `reject`, `error`, `log`, `deselect` (filters). Default class: `BUSINESS`.

- `reformat-transform` `mp reformat-transform <name> [-limit <limit>] [-ramp <ramp>] [-select <select>] [-output-index <transform>] [-output-indexes <transform>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Reformat. `-output-index <transform>` returns one output port index per record; `-output-indexes` returns a vector of indexes (record can go to several outputs). PySpark: `select`/`withColumn` per xfr; `-select` -> `filter` before transform; multiple outputs (`out0..outN`, `-output-index(es)`) -> one DataFrame per output filtered by the index expression.
- `multi-reformat-transform` `mp multi-reformat-transform <name> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Reformat with multiple outputs, one transform each. PySpark: One `select` per output port (one transform per out port).
- `select-transform` `mp select-transform <name> <select> [-package <package>] [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Filter by Expression. `<select>` true -> `out`, else -> `deselect`; evaluation errors -> `reject`. PySpark: `out = df.filter(F.coalesce(cond, F.lit(False)))`; `deselect = df.filter(~F.coalesce(cond, F.lit(False)))` only if deselect port is connected.
- `Dynamic_Filter_by_Expression` `mp Dynamic_Filter_by_Expression <name> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Filter whose expression is supplied at runtime. PySpark: `df.filter(F.expr(runtime_expr))` with expression from config.
- `normalize` `mp normalize <name> <xfr> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: One input record -> N output records. PySpark: `explode`/`posexplode` of the vector field the xfr iterates (length() function = array size).
- `normalize-transform` `mp normalize-transform <name> <xfr-file> <prefix> <limit> <ramp> <log-events> -layout <layout> [DEPRECATED: Use normalize instead.]` Class: BUSINESS. Meaning: DEPRECATED form: positional `<xfr-file> <prefix> <limit> <ramp> <log-events>`. PySpark: Same as normalize.
- `denormalize` `mp denormalize <name> <collator> <xfr> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Many records per key -> one record containing a vector. PySpark: `groupBy(collator).agg(F.collect_list(...))` (use `sort_array`/window to preserve element order).
- `denormalize-transform` `mp denormalize-transform <name> <collator> <xfr-file> <prefix> <limit> <ramp> <log-events> -layout <layout> [DEPRECATED: Use denormalize instead.]` Class: BUSINESS. Meaning: DEPRECATED positional form. PySpark: Same as denormalize.
- `hash-denormalize` `mp hash-denormalize <name> <collator> <xfr> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] [-layout <layout>]` Class: BUSINESS. Meaning: In-memory denormalize. PySpark: Same as denormalize (input need not be sorted).
- `fuse-transform` `mp fuse-transform <name> <xfr> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Combines record i of every input into one output record. PySpark: Positional zip of inputs: add `row_number` over the original order to each input, join on it.
- `merge-transform` `mp merge-transform <name> <key> <xfr> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Merges inputs on `<key>` applying `<xfr>`. PySpark: Key-based combine of sorted inputs: implement as join on `<key>` (outer semantics unless xfr shows otherwise).
- `combine` `mp combine <name> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Combine (assemble sub-records / vectors into records). PySpark: Build nested records: `groupBy` + `collect_list(struct(...))` per the DML.
- `split` `mp split <name> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Split (inverse of combine: hierarchical record -> flat records). PySpark: `explode`/`inline` of nested vectors into flat records, one DataFrame per output.
- `meta_pivot` `mp meta_pivot <name> <name_field> <value_field> [<pivot_field>[-><pivot_out_field>]]+ -layout <layout>` Class: BUSINESS. Meaning: Each input record -> one record per pivot field with `<name_field>`=field name, `<value_field>`=value. `<pivot_field>-><pivot_out_field>` renames. PySpark: Unpivot: `df.select(..., F.expr("stack(n, 'f1', f1, ...)") as (name, value))`.
- `DF_Transpose` `mp DF_Transpose <name> [options...] -layout <layout>` Class: BUSINESS. Meaning: Dataflow transpose. PySpark: `pivot`/`unpivot` as configured.
- `Transform_Ruleset` `mp Transform_Ruleset <name> <xfr> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Business rules ruleset (BRE) transform. PySpark: Chain of `F.when(...).otherwise(...)` generated from the ruleset.
- `simple-parse` `mp simple-parse <name> <xfr> -layout <layout>` Class: BUSINESS. Meaning: Parses a string field into fields. PySpark: `split`/`regexp_extract`/`from_csv` as per xfr.
- `read-sv` `mp read-sv <name> <xfr> -layout <layout>` Class: IO_READ. Meaning: Reads separated values with an xfr. PySpark: `spark.read.csv` (separator from DML).
- `Write_Separated_Values` `mp Write_Separated_Values <name> -layout <layout>` Class: IO_WRITE. Meaning: Writes separated values. PySpark: `df.write.csv` (sep/header from DML).
- `read-tagged-values` `mp read-tagged-values <name> [options...] -layout <layout>` Class: BUSINESS. Meaning: Parses tag=value text. PySpark: `F.str_to_map(col, pair_delim, kv_delim)` then select keys.
- `read-tagged-values-package` `mp read-tagged-values-package <name> <package> [options...] -layout <layout>` Class: BUSINESS. Meaning: Same, with a package. PySpark: `F.str_to_map` + package helper logic.
- `canonicalize` `mp canonicalize <name> <canon-xfr-fname> <vc-format-fname> -layout <layout>` Class: BUSINESS. Meaning: Canonicalizes values to a standard format. PySpark: Column normalization expressions per `<canon-xfr-fname>`.
- `repair-input` `mp repair-input <name> <package> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Repairs malformed input records using `<package>`. PySpark: Cleansing expressions; invalid records -> reject DataFrame.
- `validate` `mp validate <name> [-package <package>] [-limit <limit>] [-ramp <ramp>] [-log <logevents>]` Class: VALIDATION. Meaning: Validates records with `<package>`. PySpark: Data-quality rules -> filter valid/invalid; invalid to reject.
- `assign-new-keys` `mp assign-new-keys <name> <natural_key> <surrogate_key> [-override_surrogate_key <key spec>] [-max_core <nbytes>] [-maintain_order] -layout <layout>` Class: BUSINESS. Meaning: `<natural_key>` -> `<surrogate_key>`; `-override_surrogate_key`, `-maintain_order`, `-max_core`. PySpark: Surrogate keys: left join on natural key to existing key map; new keys = max(existing) + `row_number()`; append new map rows.
- `Rank_N_Records` `mp Rank_N_Records <name> <package> <num_records> [options...] -layout <layout>` Class: BUSINESS. Meaning: Top-N per group (`<num_records>`). PySpark: Window `rank`/`row_number` then `filter(rank <= N)`.
- `proctrans` `mp proctrans <xfr> ... -layout <layout>` Class: MANUAL_REVIEW. Meaning: Procedural transform chain. PySpark: Review; usually a sequence of transforms -> chained `select`.
- `dp-exec` `mp dp-exec <name> <xfr-file> -layout <layout>` Class: MANUAL_REVIEW. Meaning: Executes a data-parallel xfr. PySpark: Review.
- `reformat-3270` `mp reformat-3270 <name> <xfr> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: MANUAL_REVIEW. Meaning: 3270 terminal reformat. PySpark: Mainframe screen scraping; out of Spark scope.
- `reformat-s3270` `mp reformat-s3270 <name> <xfr> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: MANUAL_REVIEW. Meaning: Secure 3270 reformat. PySpark: Out of Spark scope (mainframe screen interaction) -> stub.
- `T3270` `mp T3270 <name> <transform> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout_name>` Class: MANUAL_REVIEW. Meaning: 3270 terminal interaction. PySpark: Out of Spark scope (mainframe screen interaction) -> stub.

### 9.10 Joins

`<key>` = join key specifier (same field names on all inputs unless the xfr renames). Join type is NOT in the mp signature: derive from the component parameters (`join_type`, `record_requiredN`) or xfr; default inner. Default class: `BUSINESS`. Default PySpark: `df0.join(df1, on=..., how=...)`

- `hash-join` `mp hash-join <name> <key> <xfr> [-auto-max-core] [-max-core <nbytes>] [-driving <input_index>] [-maintain-order] [-sql-null-comparisons] [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: `-driving <input_index>` = streamed input (perf only); `-maintain-order` keeps driving-input order (only matters if order is observable). PySpark: `join`; NULL keys: with `-sql-null-comparisons` use `==` (NULLs never match); without it use `eqNullSafe` (NULL matches NULL).
- `merge-join` `mp merge-join <name> <key> <xfr> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout> [-max-core <memory_limit>] [-check-sort] [-sql-null-comparisons]` Class: BUSINESS. Meaning: Join of key-sorted inputs; `-check-sort` validates order (ignore). PySpark: Same as hash-join. Sorted input not needed in Spark.
- `DF_Join` `mp DF_Join <name> [options...] -layout <layout>` Class: BUSINESS. Meaning: Dataflow join. PySpark: Same as hash-join with options.
- `join-with-bc-lookup` `mp join-with-bc-lookup join-with-bc-lookup <options...>` Class: BUSINESS. Meaning: Join against block-compressed lookup. PySpark: Join with lookup DataFrame (broadcast if small).
- `ldap-join` `mp ldap-join ldap-join <host> <port> <version> <base-dn> <anonymous> <user-dn> <password> <user-dn-is-relative> <object-class> <attr-filter> <multi-valued-attributes> <referral> <deref-aliases> <timeout> <size-limit>` Class: MANUAL_REVIEW. Meaning: Join against an LDAP directory. PySpark: Enrichment from LDAP: pre-extract to a table, then join.
- `continuous-ldap-join` `mp continuous-ldap-join ldap-join <host> <port> <version> <base-dn> <anonymous> <user-dn> <password> <user-dn-is-relative> <object-class> <attr-filter> <multi-valued-attributes> <referral> <deref-aliases> <timeout> <size-limit>` Class: MANUAL_REVIEW. Meaning: Continuous LDAP join. PySpark: `df0.join(df1, on=..., how=...)`.

### 9.11 Aggregation & running calculations

`<collator>`/`<key>` = group key; `<xfr>` defines aggregates (rollup/scan templates: `temporary_type`, `initialize`, `scan`/`rollup`, `finalize`). Default class: `BUSINESS`.

- `rollup` `mp rollup <name> <collator> <xfr> [-limit <limit>] [-ramp <ramp>] [-check-sort] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Sorted rollup. `-check-sort` validates order (ignore). PySpark: `groupBy(keys).agg(...)`. An empty key `{}` = one global aggregate. Sorted input not needed.
- `rollup-transform` `mp rollup-transform <name> <collator> <xfr-file> <prefix> <limit> <ramp> <log-events> -layout <layout> [DEPRECATED: Use rollup instead.]` Class: BUSINESS. Meaning: DEPRECATED positional form. PySpark: Same as rollup.
- `hash-rollup` `mp hash-rollup <name> <collator> <xfr> [-auto-max-core] [-max-core <bytes>] [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: In-memory rollup (unsorted input). PySpark: `groupBy(keys).agg(...)`.
- `DF_Rollup` `mp DF_Rollup <name> [options...] -layout <layout>` Class: BUSINESS. Meaning: Dataflow rollup. PySpark: `groupBy().agg()`.
- `aggregate-transform` `mp aggregate-transform <name> <key> <xfr> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Legacy aggregate. PySpark: `groupBy(<key>).agg(...)`.
- `hash-aggregate` `mp hash-aggregate <name> <collator> <xfr> [-auto-max-core] [-max-core <bytes>] [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: In-memory aggregate. PySpark: `groupBy(<collator>).agg(...)`.
- `scan` `mp scan <name> <collator> <xfr> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Running (cumulative) aggregate. PySpark: Window: `Window.partitionBy(keys).orderBy(order_cols).rowsBetween(Window.unboundedPreceding, 0)`; one output per input record. Requires an explicit order column.
- `scan-transform` `mp scan-transform <name> <collator> <xfr-file> <prefix> <limit> <ramp> <log-events> -layout <layout> [DEPRECATED: Use scan instead.]` Class: BUSINESS. Meaning: DEPRECATED positional form. PySpark: Same as scan.
- `hash-scan` `mp hash-scan <name> <collator> <xfr> [-auto-max-core] [-max-core <bytes>] [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: In-memory scan. PySpark: Same as scan.
- `continuous-rollup` `mp continuous-rollup <name> <collator> <xfr> [-max-core <bytes>] [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Rollup in a continuous flow. PySpark: Structured Streaming `groupBy` (+ watermark) or batch `groupBy`.
- `continuous-scan` `mp continuous-scan <name> <collator> <xfr> [-max-core <bytes>] [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Scan in a continuous flow. PySpark: Streaming stateful aggregation / batch window.
- `compute-closure` `mp compute-closure <name> <number-of-partitions> <temporary-file-prefix> <key-length>` Class: BUSINESS. Meaning: `<number-of-partitions> <temporary-file-prefix> <key-length>`. PySpark: Transitive closure / connected components over key pairs: GraphFrames `connectedComponents` or iterative self-join until no change.
- `recirculate` `mp recirculate <name> <number-of-partitions> <temporary-file-prefix> <key-field-1> <key-field-2>` Class: BUSINESS. Meaning: Recirculates records for iterative processing on two key fields. PySpark: Iterative loop (while changes: join/union); cap iterations.
- `file-recirculate` `mp file-recirculate` Class: BUSINESS. Meaning: File-based recirculation. PySpark: Iterative loop persisting each iteration to storage; cap iterations.

### 9.12 Deduplication

`<key>` = duplicate key; `-keep (first | last | unique-only)`; `-select` = pre-filter expression; `-package` = helper functions. Default class: `BUSINESS`.

- `dedup` `mp dedup <name> <key> [-package <package>] [-select <select-expr>] [-keep (first | last | unique-only)] [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Dedup Sorted. Default keep = first. PySpark: first/last: `row_number().over(Window.partitionBy(keys).orderBy(order_col asc|desc)) == 1` (needs a deterministic order column, e.g. ingest sequence). unique-only: keep keys with `count == 1`. Duplicates port -> the complement.
- `hash-dedup` `mp hash-dedup <name> <key> [-select <select-expr>] [-package <package>] [-max-core <bytes>] [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: In-memory dedup. PySpark: Same as dedup (`dropDuplicates(keys)` only if which record survives is irrelevant).

### 9.13 Sampling, generation, limiting

Test-data generation and sampling. Default class: `BUSINESS`.

- `leading-records` `mp leading-records <name> <max-records> -layout <layout>` Class: BUSINESS. Meaning: First N records. PySpark: `orderBy(order_col).limit(<max-records>)` (needs deterministic order).
- `percentage-sample` `mp percentage-sample <percentage>` Class: BUSINESS. Meaning: Random sample. PySpark: `df.sample(fraction=<percentage>/100)`.
- `exact-sample` `mp exact-sample` Class: BUSINESS. Meaning: Exact-size sample. PySpark: `orderBy(F.rand(seed)).limit(n)`.
- `over-sample` `mp over-sample` Class: BUSINESS. Meaning: Oversampling. PySpark: `sample(withReplacement=True, ...)`.
- `sample-control` `mp sample-control` Class: RUNTIME_IGNORE. Meaning: Sampling control (runtime). PySpark: None.
- `generate` `mp generate <name> <num_records> { [-expression <field> <expr>] [-sequential <field> <minimum>] [-minimum <field> <minimum>] [-maximum <field> <maximum>] [-default <field>] }* [-seed <rng seed string>] -layout <layout>` Class: BUSINESS. Meaning: Generates `<num_records>` synthetic records. PySpark: `spark.range(n)` + `withColumn` per `-expression`/`-sequential`/`-minimum`/`-maximum`/`-default`; `-seed` -> `F.rand(seed)`.
- `generate-with-transform` `mp generate-with-transform <name> <num_records> { [-expression <field> <expr>] [-sequential <field> <minimum>] [-minimum <field> <minimum>] [-maximum <field> <maximum>] [-default <field>] }* [-dynamic <type description>] [-seed <rng seed string>] [-transform <transform name>] -layout <layout>` Class: BUSINESS. PySpark: As generate; `-transform` = xfr per record; `-dynamic` = type description.
- `random` `mp random <name> <nrecords> <recordlength> -layout <layout>` Class: BUSINESS. Meaning: Random records. PySpark: `spark.range(n)` + random bytes/strings of `<recordlength>`.
- `Create_Data` `mp Create_Data <name> <number-of-records> <xfr-file> [options] -layout <layout>` Class: BUSINESS. Meaning: Create Data component (`<number-of-records> <xfr-file>`). PySpark: `spark.range(n)` + xfr columns.

### 9.14 Readers

Data sources. Business critical: path, format, schema, options. Default class: `IO_READ`.

- `readfiles` `mp readfiles <transform> [-compressed] [-skip-count <count>] [-read-count <count>] -layout <layout> [-file-empty <Fail|Notify|Ignore|Accept>] [-file-missing <Fail|Notify|Ignore>] [-file-unavailable <Fail|Notify|Ignore>] [-filename-error <Fail|Notify|Ignore>] [-input-error <Fail|Notify|Ignore>] [-read_as_single_record] [-read-blocks] [-maximum-block-bytes <max>]` Class: IO_READ. Meaning: Read Multiple Files. Error policies `-file-empty|-file-missing|-file-unavailable|-filename-error|-input-error <Fail|Notify|Ignore|Accept>` -> pre-checks on path existence/size: Fail=raise, Notify=log warning, Ignore/Accept=skip/allow. PySpark: `spark.read.format(...).schema(...).load(paths)`; paths come from the input records (transform yields file names). `-skip-count`/`-read-count` -> skip/limit rows per file; `-read_as_single_record` -> `wholetext`/`binaryFile`; `-compressed` -> codec auto.
- `Read_JSON` `mp Read_JSON <name> [options...] -layout <layout>` Class: IO_READ. PySpark: `spark.read.json` / `from_json`.
- `Extract_JSON_Message` `mp Extract_JSON_Message <name> <package> [options...] -layout <layout>` Class: IO_READ. Meaning: Parses JSON messages from a field. PySpark: `from_json(col, schema)`.
- `XML_To_JSON` `mp XML_To_JSON <name> [options...] -layout <layout>` Class: IO_READ. PySpark: XML parse (spark-xml / `from_xml` in Spark 4) then `to_json`.
- `Read_Avro` `mp Read_Avro <name> [options...] -layout <layout>` Class: IO_READ. PySpark: `spark.read.format("avro")`.
- `Read_Parquet` `mp Read_Parquet <name> [options...] -layout <layout>` Class: IO_READ. PySpark: `spark.read.parquet`.
- `DF_Read_Parquet` `mp DF_Read_Parquet <name> [options...] -layout <layout>` Class: IO_READ. Meaning: Dataflow Parquet read. PySpark: `spark.read.parquet`.
- `Read_Excel` `mp Read_Excel Read_Excel <options...>` Class: IO_READ. PySpark: `spark.read.format("com.crealytics.spark.excel")` or pandas `read_excel` -> `createDataFrame`.
- `Read_Excel_Flow` `mp Read_Excel_Flow Read_Excel_Flow <options...>` Class: IO_READ. Meaning: Excel read from a flow. PySpark: spark-excel reader or pandas `read_excel` -> `createDataFrame`.
- `Read_Nested` `mp Read_Nested <name> [options...] -layout <layout>` Class: IO_READ. Meaning: Nested/hierarchical data. PySpark: Read with nested `StructType`/`ArrayType` schema.
- `Read_Nested_Sequence` `mp Read_Nested_Sequence <name> [options...] -layout <layout>` Class: IO_READ. Meaning: Nested sequence data. PySpark: Read with nested `StructType`/`ArrayType` schema.
- `read-xml` `mp read-xml <name> [ -parser { expat | xerces } ] [ -ignore-blank-cdata ] [ -custom-format ] [ -root-element <name> ] [ -base-element <name> ] [ -leaf-element <name> ] [ -character-data-field <name> ] -layout <layout>` Class: IO_READ. Meaning: `-parser expat|xerces` (ignore), `-custom-format`. PySpark: spark-xml `rowTag`=`-base-element`/`-leaf-element`; `-root-element` -> `rootTag`; `-character-data-field` -> `valueTag`; `-ignore-blank-cdata` -> trim.
- `read-xml-xfr` `mp read-xml-xfr read-xml-xfr <options...>` Class: IO_READ. PySpark: XML read + xfr mapping.
- `xml-split` `mp xml-split <name> [ -parser { expat | xerces } ] [ -o <port-name> ] [ -xml-doc-field-name <name> ] [ -missing-fields { ignore | error } ] [ -limit <limit> ] [ -ramp <ramp> ] [ -log <log_event_mask> ] [ -package <package> ] -layout <layout>` Class: BUSINESS. Meaning: Splits XML documents into records. `-xml-doc-field-name` = field holding XML. PySpark: Parse XML field (`from_xml`/spark-xml) and `explode` repeating elements; `-missing-fields error` -> fail on missing.
- `validate-xml-xfr` `mp validate-xml-xfr validate-xml-xfr <options...>` Class: VALIDATION. PySpark: XSD validation (UDF / external).
- `Read_ASN.1_Transform` `mp Read_ASN.1_Transform <name> <package> [options...] -layout <layout>` Class: MANUAL_REVIEW. PySpark: ASN.1 decoding needs a dedicated library (asn1tools UDF).
- `ASN.1_Decoder` `mp ASN.1_Decoder <name> [options...] -layout <layout>` Class: MANUAL_REVIEW. PySpark: ASN.1 decoding UDF (asn1tools) -> stub until library chosen.
- `ASN.1_Splitter` `mp ASN.1_Splitter <name> [options...] -layout <layout>` Class: MANUAL_REVIEW. PySpark: ASN.1 message split UDF -> stub.
- `Extract_ASN.1_Message` `mp Extract_ASN.1_Message <name> <package> [options...] -layout <layout>` Class: MANUAL_REVIEW. PySpark: ASN.1 message extraction UDF -> stub.
- `mvsdasd` `mp mvsdasd mvsdasd <options>` Class: MANUAL_REVIEW. Meaning: MVS DASD dataset. PySpark: Mainframe dataset: EBCDIC/copybook reader (e.g. Cobrix `spark.read.format("cobol")`).
- `read-shared` `mp read-shared [-startup-seconds <startup seconds>] [-startup-attempts <connection tries>] [-startup-retry-seconds <retry delay>] [-max-clients <clients per server>] [-n-buffers <n-buffers>] [-buffer-kbytes <buffer-kbytes>] [-file-mode <filemode>] [-progress-timeout-seconds <timeout-seconds>] [-io-method direct | conventional] [-group <alphanumeric group name>]` Class: RUNTIME_IGNORE. Meaning: Shared-memory reader runtime service (all options runtime). PySpark: None.
- `List_Files` `mp List_Files <transform> -mode <files|files-and-directories|directories|hdfs-blocks> [-singledir <Summary|Expansion>] [-file-pattern <glob pattern>] [-directory-prune <glob pattern>] [-skip-count <count>] [-read-count <count>] -layout <layout> [-file-empty <Fail|Notify|Ignore|Accept>] [-file-unavailable <Fail|Notify|Ignore>] [-filename-error <Fail|Notify|Ignore>] [-input-error <Fail|Notify|Ignore>]` Class: IO_READ. Meaning: Lists files as records. PySpark: List paths (`dbutils.fs.ls` / Hadoop FileSystem / `glob`), produce DataFrame of file names. `-file-pattern` glob, `-directory-prune` exclusion, `-mode files|directories|...`.
- `BusinessObjects_41_UnxUnload` `mp BusinessObjects_41_UnxUnload BusinessObjects_41_UnxUnload <options...>` Class: MANUAL_REVIEW. PySpark: Export via BO API, then read file.
- `sas-input-program` `mp sas-input-program sas-input-program <libname> <dataset> <outputFile> [-log <logfile>]` Class: MANUAL_REVIEW. PySpark: Read SAS dataset (`spark.read.format("com.github.saurfang.sas.spark")` or pandas `read_sas`).
- `sas-idataset-v2` `mp sas-idataset-v2 sas-idataset-v2 <libname> <dataset> <output_file> [-log <logfile>]` Class: MANUAL_REVIEW. PySpark: Read SAS dataset (spark-sas7bdat or pandas `read_sas`).
- `sas-metadata` `mp sas-metadata sas-metadata <libname> <dataset> [-nullable <true|false>] [-case <as_is|all_upper|all_lower>] [-prefer <first_defined|informat_first|format_first|informat_only|format_only>] [-endianness <true|false>] [-utf8_aware <true|false>]` Class: MANUAL_REVIEW. PySpark: Derive schema from SAS dataset metadata.

### 9.15 Writers

Data targets. Business critical: path, format, mode, partitioning of output files. Default class: `IO_WRITE`.

- `writefiles` `mp writefiles <transform> [-compressed] [-overwrite] [-max-files <num>] [-max-core <num>] [-permissions <mode>] [-grouped] -layout <layout>` Class: IO_WRITE. Meaning: Write Multiple Files. PySpark: `df.write.format(...)`; target path per record from transform -> `partitionBy(path_col)` or per-group writes. `-overwrite` -> mode overwrite; `-max-files` cap on open files (ignore); `-compressed` -> compression; `-grouped` -> one file per group; `-permissions` -> post-write chmod (side effect).
- `continuous-writefiles` `mp continuous-writefiles <transform> [-publish-style <overwrite | files_after>] [-max-files <num>] [-permissions <mode>] [-grouped] -layout <layout>` Class: IO_WRITE. PySpark: Structured Streaming `writeStream` file sink; `-publish-style overwrite|files_after`.
- `Write_JSON` `mp Write_JSON <name> [options...] -layout <layout>` Class: IO_WRITE. PySpark: `df.write.json` / `to_json`.
- `Write_Avro` `mp Write_Avro <name> [options...] -layout <layout>` Class: IO_WRITE. PySpark: `df.write.format("avro")`.
- `Write_Excel` `mp Write_Excel Write_Excel <options...>` Class: IO_WRITE. PySpark: pandas `to_excel` (small) or spark-excel.
- `Write_Excel_Flow` `mp Write_Excel_Flow Write_Excel_Flow <options...>` Class: IO_WRITE. PySpark: pandas `to_excel` (small) or spark-excel writer.
- `Write_Nested_Sequence` `mp Write_Nested_Sequence <name> [options...] -layout <layout>` Class: IO_WRITE. PySpark: Write nested schema (parquet/json).
- `write-xml` `mp write-xml <name> [ -custom-format ] [ -root-element <name> ] [ -base-element <name> ] [ -leaf-element <name> ] [ -character-data-field <name> ] [ -add-bom ] -layout <layout>` Class: IO_WRITE. PySpark: spark-xml writer / `to_xml`; `-add-bom` -> prepend BOM (post-process).
- `write-xml-xfr` `mp write-xml-xfr write-xml-xfr <options...>` Class: IO_WRITE. PySpark: XML writer + xfr mapping (`to_xml` / spark-xml).
- `xml-combine` `mp xml-combine <name> [ -pretty-formatting on|off ] [ -indent-size <num> ] [ -document-header on|off ] [ -add-bom ] [ -document-per-record ] [ -xml-doc-field-name <field-name> ] [ -limit <limit> ] [ -ramp <ramp> ] [ -log <log_event_mask> ] [ -package <package> ] [ -i <port-name> ] [ -port <port-name> ] -layout <layout>` Class: BUSINESS. Meaning: Combines records into XML documents. PySpark: `to_xml`/string build per record or collect into documents; `-document-per-record` -> one doc per row; `-pretty-formatting`, `-indent-size`, `-document-header`, `-add-bom` formatting.
- `xml-reformat` `mp xml-reformat <name> <xfr> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. PySpark: Transform XML field with xfr (`from_xml` -> select -> `to_xml`).
- `Write_ASN.1_Transform` `mp Write_ASN.1_Transform <name> <package> [options...] -layout <layout>` Class: MANUAL_REVIEW. PySpark: ASN.1 encoding UDF -> stub.
- `ASN.1_Encoder` `mp ASN.1_Encoder <name> [options...] -layout <layout>` Class: MANUAL_REVIEW. PySpark: ASN.1 encoding UDF -> stub.
- `write-iceberg-files` `mp write-iceberg-files <transform> [-compressed] [-overwrite] [-max-files <num>] [-max-core <num>] [-permissions <mode>] [-grouped] -layout <layout>` Class: IO_WRITE. Meaning: Writes Iceberg data files. PySpark: `df.writeTo("catalog.db.table").append()/overwritePartitions()`.
- `Manage_Iceberg_Table` `mp Manage_Iceberg_Table <name> [options...] -layout <layout>` Class: SIDE_EFFECT. PySpark: Iceberg DDL/maintenance via `spark.sql` (CREATE/ALTER/expire_snapshots).
- `copy-to-dataset` `mp copy-to-dataset <dataset name> { <fopen option> }+` Class: IO_WRITE. Meaning: Copies stream to a named dataset. PySpark: `df.write.mode(...).save(path)`; fopen options -> write mode (append vs truncate).
- `sas-output-program` `mp sas-output-program sas-output-program <libname> <dataset> <inputFile> [-log <logfile>] [-key <keyspec>] [-composite_key <name>]` Class: MANUAL_REVIEW. PySpark: Write SAS dataset (external).
- `sas-odataset-v2` `mp sas-odataset-v2 sas-odataset-v2 <libname> <dataset> <inputfile> [-log <logfile>]` Class: MANUAL_REVIEW. PySpark: Write SAS dataset externally (no native Spark writer) -> stub.
- `sas-upsert-program` `mp sas-upsert-program sas-upsert-program <libname> <dataset> <inputFile> -key <key> [-do_insert] [-update_count <update-count>] [-composite_key <key_name>] [-log <logfile>]` Class: MANUAL_REVIEW. PySpark: Upsert into SAS dataset by `-key`.
- `sas-upsert` `mp sas-upsert <name> <dataset> [-update_count <count>] [-layout <layout>]` Class: MANUAL_REVIEW. PySpark: Upsert into SAS dataset by key -> external step / stub.
- `To_PGWire` `mp To_PGWire` Class: MANUAL_REVIEW. PySpark: Serve/write via PostgreSQL wire protocol -> JDBC write to Postgres-compatible target.
- `Write_MSLI` `mp Write_MSLI <name> ...` Class: MANUAL_REVIEW. PySpark: Unknown/specialized writer -> review.

### 9.16 Lookup files & block-compressed (BC) lookups

Lookup datasets are reference tables used by `lookup(...)` calls inside xfr. In Spark: write the lookup as a table/parquet; consume it with a (broadcast) join. Default class: `BUSINESS`.

- `write-lookup` `mp write-lookup -key <key> [-block_size <desired uncompressed data bytes per indexed block>] [-bitmap_size <log(2) of bitmap size>] [-miss_rate <0 to 4>] [-skip_empty] [-package <transform>] [+<flag> ...] -layout <layout>` Class: IO_WRITE. Meaning: Writes an indexed lookup file. `-block_size`, `-bitmap_size`, `-miss_rate`, `-skip_empty`, `+<flag>` = index tuning (ignore). PySpark: Write reference DataFrame (parquet/Delta) keyed by `-key`.
- `writelookups` `mp writelookups <transform> [-continuous] -key <collator> [-overwrite] [-max-files <num>] [-max-core <num>] [-permissions <mode>] [-grouped] -layout <layout>` Class: IO_WRITE. Meaning: Write Multiple Lookup Files. PySpark: Write lookup table(s); `-key` = lookup key; `-continuous` = incremental.
- `write-bc-lookups` `mp write-bc-lookups <transform> [-continuous] -key <collator> [-block_size <desired uncompressed data bytes per indexed block>] [-bitmap_size <log(2) of bitmap size>] [-overwrite] [-max-files <num>] [-max-core <num>] [-permissions <mode>] [-grouped] -layout <layout>` Class: IO_WRITE. Meaning: Block-compressed lookup writer. PySpark: Same as writelookups.
- `write-lookup-indexes` `mp write-lookup-indexes <name> "-key <key> [-key <key> ...] -layout <layout>` Class: RUNTIME_IGNORE. Meaning: Builds lookup indexes for repeated `-key`. PySpark: None (Spark needs no index files).
- `compute-bc-lookups` `mp compute-bc-lookups <transform> [-continuous] -key <collator> [-block_size <desired uncompressed data bytes per indexed block>] [-bitmap_size <log(2) of bitmap size>] [-overwrite] [-max-files <num>] [-max-core <num>] [-permissions <mode>] [-grouped] -layout <layout>` Class: IO_WRITE. Meaning: Computes BC lookup data + index; `-grouped`, `-overwrite`. PySpark: Write lookup table.
- `consolidate-bc-index` `mp consolidate-bc-index data-url index-url key [-keep_bitmap]` Class: RUNTIME_IGNORE. Meaning: Index maintenance. PySpark: None.
- `read-bc-data` `mp read-bc-data data-url [data-url ...] index-url [index-url ...] key [-extra-data <abort|ignore|process>]` Class: IO_READ. PySpark: `spark.read` of the lookup table; `-extra-data abort|ignore|process` -> fail/drop/keep rows outside index.
- `read-multi-bc-data` `mp read-multi-bc-data <transform package> key [-input-error <abort|ignore|notify>] [-extra-data <abort|ignore|process>] [-limit <limit>] [-ramp <ramp>] [-log <logmask>]` Class: IO_READ. Meaning: `-input-error abort|ignore|notify`. PySpark: `spark.read` of several lookup tables (union).
- `merge-bc-data` `mp merge-bc-data data-url [data-url ...] index-url [index-url ...] key [-extra-data <abort|ignore|process>]` Class: IO_READ. PySpark: Union of lookup tables, dedup by key (latest wins).
- `merge-multi-bc-data` `mp merge-multi-bc-data <transform package> key [-input-error <abort|ignore|notify>] [-extra-data <abort|ignore|process>] [-limit <limit>] [-ramp <ramp>] [-log <logmask>]` Class: IO_READ. PySpark: Union lookup tables, dedup by key (latest wins).
- `vsamlookup` `mp vsamlookup <vsam url> <transform-file> [-match_optional] [-maximum_matches <n>] [-limit <n>] [-ramp <n>] [-log <log-string>] [-reverse] [-read_only]` Class: MANUAL_REVIEW. PySpark: Mainframe VSAM lookup: extract VSAM to table, then join. `-match_optional` -> left join; `-maximum_matches` -> cap matches per key (row_number filter).

### 9.17 Database components

`<db-config-file>` (.dbc) = connection definition -> JDBC/connector config from a secret store. `<sql-select-clause>`/`-select` = source query; `<ablocal-expression>` = per-partition WHERE clause for parallel unload (Spark: `partitionColumn`/`predicates`). Never hard-code credentials.

- `db-unload` `mp db-unload <name> <db-config-file> <sql-select-clause> <ablocal-expression> [-itable <itable-name>]  [-max-rows-per-partition <num>] -layout <layout>` Class: IO_READ. Meaning: Generic unload (Input Table). PySpark: `spark.read.jdbc(url, table=f"({sql}) q", ...)` or `.option("query", sql)`; `-max-rows-per-partition` -> `fetchsize`/partitioning.
- `itable` `mp itable <name> <db-config-file> -select <sql-select-statement> [-layout <layout>]` Class: IO_READ. Meaning: Input Table. PySpark: JDBC read with `-select` query.
- `itable-ims` `mp itable-ims <name> <db-config-file> <table> [-layout <layout>]` Class: MANUAL_REVIEW. PySpark: IMS source: extract externally.
- `itable-mongo` `mp itable-mongo <name> <db-config-file> <table> [-layout <layout>]` Class: IO_READ. PySpark: Mongo Spark connector read.
- `otable` `mp otable <name> <db-config-file> -table <tablename> [-layout <layout>]` Class: IO_WRITE. Meaning: Output Table. PySpark: JDBC write to `-table`.
- `db-load` `mp db-load <name> <db-config-file> [-table <tablename> | -insert <sql>] ... -layout <layout>` Class: IO_WRITE. Meaning: Generic load. PySpark: `df.write.jdbc(table, mode="append")`; `-insert <sql>` -> custom insert (foreachPartition). Truncate-then-load patterns come from a preceding db-truncate/db-runsql.
- `db-load-continuous` `mp db-load-continuous <name> <db-config-file> <update-statement-file> <insert-statement-file> [-commitnum <commit-block-size>] -layout <layout>` Class: IO_WRITE. PySpark: Streaming `foreachBatch` upsert.
- `db-update` `mp db-update <name> <db-config-file> <update-statement-file> <insert-statement-file> [-commitnum <commit-block-size>] -layout <layout>` Class: IO_WRITE. Meaning: Update Table. PySpark: Upsert: update-statement-file + insert-statement-file -> `MERGE INTO` (Delta/warehouse) or JDBC `foreachPartition` executing update-then-insert-if-0-rows. `-commitnum` = batch size.
- `db-update-continuous` `mp db-update-continuous <name> <db-config-file> <update-statement-file> <insert-statement-file> [-commitnum <commit-block-size>] -layout <layout>` Class: IO_WRITE. PySpark: Streaming `foreachBatch` MERGE.
- `db-multi-update` `mp db-multi-update <name> db_config_file -commitTable <commit-table> [ -jobid <jobid> ] -batchSize <batch-size> ( -primarySqlN <sql1> [ -secondarySqlN <sql2> ] [ -filterN <expr>] [ -transformN <package> ]) ... [-continuous] [-no_actions_ok] [-ignoreDuplicates (compat | False | True)] -layout <layout>` Class: IO_WRITE. Meaning: Multi Update Table. PySpark: Per N: `-primarySqlN` (+ `-secondarySqlN` if primary affects 0 rows), filtered by `-filterN`, mapped by `-transformN` -> `foreachPartition` executing statements; `-batchSize` rows per commit; `-commitTable`/`-jobid` = restartability (ignore -> idempotent MERGE).
- `db-multi-update-continuous` `mp db-multi-update-continuous <name> db_config_file -commitTable <commit-table> [ -jobid <jobid> ] -batchSize <batch-size> ( -primarySqlN <sql1> [ -secondarySqlN <sql2> ] [ -filterN <expr>] [ -transformN <package> ]) ... [-continuous] [-no_actions_ok] [-ignoreDuplicates (compat | False | True)] -layout <layout>` Class: IO_WRITE. PySpark: Structured Streaming `foreachBatch` executing the numbered statements.
- `db-continuous-select-multi` `mp db-continuous-select-multi <name> db_config_file interval delay as_of -layout <layout>` Class: IO_READ. PySpark: Polling read every `interval` -> streaming/micro-batch JDBC read.
- `db-continuous-update-multi` `mp db-continuous-update-multi <name> db_config_file commit_table -layout <layout>` Class: IO_WRITE. PySpark: Structured Streaming `foreachBatch` MERGE with commit table bookkeeping.
- `db-lookup` `mp db-lookup <db-config-file> <esql-lookup-file> <transform-file> <execute-when-not-found-file> [-match_optional] -maximum_matches <n> -commit_number <n> [-limit <n>] [-ramp <n>] [-log <log-string>]` Class: BUSINESS. Meaning: DB lookup per record. PySpark: JDBC read of lookup query -> join; `-match_optional` -> left join; `-maximum_matches` -> cap per key; `<execute-when-not-found-file>` -> logic for non-matching rows.
- `db-runsql` `mp db-runsql <name> <db-config-file> <sql-file> [-maximum_errors <num>] [-interface api] [-ignore_drop_table_errors] -layout <layout>` Class: SIDE_EFFECT. Meaning: Run SQL. PySpark: Execute SQL file via JDBC connection (pre/post step, ordered by phase). `-ignore_drop_table_errors` -> try/except on DROP.
- `db-runsql-continuous` `mp db-runsql-continuous <name> <db-config-file> <sql-file> [-maximum_errors <num>] [-interface api] [-ignore_drop_table_errors] -layout <layout>` Class: SIDE_EFFECT. PySpark: Execute SQL per micro-batch (`foreachBatch`).
- `db-truncate` `mp db-truncate <name> <db-config-file> <sql-file> -layout <layout>` Class: SIDE_EFFECT. Meaning: Truncate Table. PySpark: Run `TRUNCATE` (or write mode overwrite) BEFORE the load in the same phase; must not be dropped.
- `db-call-stored-procedure` `mp db-call-stored-procedure <name> db_config_file select_sql [<switches>]` Class: SIDE_EFFECT. PySpark: JDBC `CallableStatement` (driver-side or foreachPartition).
- `db-controller` `mp db-controller <name> <db-config-file> [-select <sql>] [-itable <name>] [-layout <layout>]` Class: RUNTIME_IGNORE. PySpark: None (parallel unload coordination).
- `db-ims-lookup` `mp db-ims-lookup <name> <db-config-file> <segment_name> <package> -parent_segment_data <key_only | full_segment> -batch_mode <DLI | BMP> -ims_parameters <parameters> -match_required  -maximum_matches <num> -limit <num> -ramp <num> -layout <layout>` Class: MANUAL_REVIEW. PySpark: IMS lookup: extract and join.
- `db-ims-lookup-segments` `mp db-ims-lookup-segments <name> db_config_file target_segment [-batch_mode <DLI|BMP>] [-interface <local|remote>] [-match_required] -maximum_matches <max> [-generate_unique_key] [-ims_parameters <ims_parameters>] [-alternate_psbname <alternate_psbname>]` Class: MANUAL_REVIEW. PySpark: IMS segment lookup: extract and join -> stub.
- `db-ims-unload` `mp db-ims-unload <name> <db-config-file> <tablename> <control-file> [-otable <name> -controller-node <node>] [-mode <insert|append|replace>] [-direct=true] [-directfile=false] [-directparallel=true] [-num-discards-allowed <num>] [-num-errors-allowed <num>] [-commitnum <num>] [-bindsize <num>] -layout <layout>` Class: MANUAL_REVIEW. PySpark: IMS unload: external extract -> stub.
- `db-ora-sqlldr` `mp db-ora-sqlldr <name> <db-config-file> <tablename> <control-file> [-otable <name> -controller-node <node>] [-mode <insert|append|replace>] [-direct=true] [-directfile=false] [-directparallel=true] [-num-discards-allowed <num>] [-num-errors-allowed <num>] [-commitnum <num>] [-bindsize <num>] -layout <layout>` Class: IO_WRITE. Meaning: Oracle SQL*Loader. PySpark: JDBC/Oracle bulk write; `-mode insert|append|replace` -> append/append/overwrite(truncate=True); `-direct*`, `-bindsize` tuning (ignore); `-num-errors-allowed`/`-num-discards-allowed` -> reject threshold.
- `db-mssql-ldr` `mp db-mssql-ldr <name> <db-config-file> <tablename> <control-file> [-otable <name> -controller-node <node>]  [-num_errors <num>] [-rows_per_commit <num>]  -layout <layout>` Class: IO_WRITE. PySpark: JDBC write (SQL Server bulk copy). `-num_errors` threshold; `-rows_per_commit` -> batchsize.
- `db-db2-ldr` `mp db-db2-ldr <name> <db-config-file> <tablename>` Class: IO_WRITE. PySpark: JDBC write to DB2.
- `db-db2uss-load` `mp db-db2uss-load <name> <db-config-file> <jcl-tranform-file> <control-file> [-table_name <tablename>] [-unique_identifier <uid>] [-max_errors <num>] [-max_rejects <max>] -layout <layout>` Class: IO_WRITE. Meaning: DB2 z/OS load. `-max_errors`, `-max_rejects` -> thresholds. PySpark: JDBC write to DB2 z/OS; `-max_errors`/`-max_rejects` -> threshold.
- `db-db2uss-loader` `mp db-db2uss-loader <name> <db-config-file> <tablename> <control-file> [-otable <name> -controller-node <node>] -layout <layout>` Class: IO_WRITE. PySpark: JDBC write to DB2 z/OS.
- `db-db2uss-unload` `mp db-db2uss-unload <name> <db-config-file> <sql-select-clause> <ablocal-expression> [-itable <itable-name>] -layout <layout>` Class: IO_READ. PySpark: JDBC read DB2.
- `db-db2-genheader` `mp db-db2-genheader <name> <db-config-file> <tablename>` Class: RUNTIME_IGNORE. PySpark: None (loader helper).
- `db-db2-pad` `mp db-db2-pad <name> <db-config-file> <tablename>` Class: RUNTIME_IGNORE. PySpark: None (loader helper).
- `db-db2-split` `mp db-db2-split <name> <db-config-file> <tablename> [[abcol1name=dbcol1name]..]` Class: RUNTIME_IGNORE. Meaning: Column mapping `abcol=dbcol` -> apply as renames before write. PySpark: None (loader helper); apply `abcol=dbcol` renames before write.
- `db-db2-xfrpad` `mp db-db2-xfrpad <name> <db-config-file> <tablename>` Class: RUNTIME_IGNORE. PySpark: None (loader helper).
- `db-db2-xfrsplit` `mp db-db2-xfrsplit <name> <db-config-file> <tablename> [[abcol1name=dbcol1name]..]` Class: RUNTIME_IGNORE. PySpark: None (loader helper).
- `db-as400-load` `mp db-as400-load <name> <db-config-file> <tablename> <control-file> [-otable <name> -controller-node <node>]  [-num_errors <num>] [-rows_per_commit <num>] [-haostname <hostname>] [-rebuild_loader] [-ifs_data_path <path>] [-ifs_dml_path <path>] -layout <layout>` Class: IO_WRITE. PySpark: JDBC write (AS/400). `-ifs_*` paths, `-rebuild_loader` ignore.
- `db-netezza-load` `mp db-netezza-load <name> <db-config-file> <tablename> <control-file> [-otable <name> -controller-node <node>]  [-num_errors <num>] [-rows_per_commit <num>]  -layout <layout>` Class: IO_WRITE. PySpark: JDBC write Netezza.
- `db-netezza-unload` `mp db-netezza-unload <name> <db-config-file> <sql-select-clause> <ablocal-expression> [-itable <itable-name>]  [-max-rows-per-partition <num>] -layout <layout>` Class: IO_READ. PySpark: JDBC read Netezza.
- `db-nza-extload` `mp db-nza-extload <name> <db-config-file> <tablename> <control-file> [-otable <name> -controller-node <node>]  [-num_errors <num>] [-rows_per_commit <num>]  -layout <layout>` Class: IO_WRITE. PySpark: JDBC write to Netezza (external table load).
- `db-redbrick-unload` `mp db-redbrick-unload <name> <db-config-file> <sql-select-clause> <ablocal-expression> [-itable <itable-name>]  [-max-rows-per-partition <num>] -layout <layout>` Class: IO_READ. PySpark: JDBC read (Red Brick) with the select clause.
- `db-sybase-bcp-ldr` `mp db-sybase-bcp-ldr <name> <db-config-file> <tablename> [-otable <name> -controller-node <node>]  [-num_errors <num>] [-rows_per_commit <num>]  -layout <layout>` Class: IO_WRITE. PySpark: JDBC write Sybase.
- `db-sybaseiq-dbisql-load` `mp db-sybaseiq-dbisql-load <name> <db-config-file> <tablename> [-otable <name> -controller-node <node>]  [-num_errors <num>] [-rows_per_commit <num>]  -layout <layout>` Class: IO_WRITE. PySpark: JDBC write to Sybase IQ.
- `db-sybaseiq-load` `mp db-sybaseiq-load <name> <db-config-file> <tablename> [-otable <name> -controller-node <node>]  [-num_errors <num>] [-rows_per_commit <num>]  -layout <layout>` Class: IO_WRITE. PySpark: JDBC write to Sybase IQ.
- `db-sybaseiq-unload` `mp db-sybaseiq-unload <name> <db-config-file> <sql-select-clause> <ablocal-expression> [-itable <name> -controller-node <node>] -layout <layout>` Class: IO_READ. PySpark: JDBC read Sybase IQ.
- `db-ter-unload-data` `mp db-ter-unload-data <name> <db-config-file> -select <sql-select-statement> [-layout <layout>]` Class: IO_READ. PySpark: Teradata JDBC read (`-select`).
- `db-ter-fast-export` `mp db-ter-fast-export <name> <db-config-file> -select <sql-select-statement> [-layout <layout>]` Class: IO_READ. PySpark: Teradata JDBC read (`-select`).
- `db-ter-direct-unload` `mp db-ter-direct-unload <XXX usage file to be written later>` Class: IO_READ. Meaning: Usage text not documented in mp help. PySpark: Teradata JDBC read.
- `db-ter-fexp` `mp db-ter-fexp <XXX usage file to be written later>` Class: IO_READ. Meaning: Usage text not documented in mp help. PySpark: Teradata JDBC read.
- `db-ter-load-data` `mp db-ter-load-data <name> <db-config-file> -select <sql-select-statement> [-layout <layout>]` Class: IO_WRITE. PySpark: Teradata JDBC write.
- `db-ter-update-data` `mp db-ter-update-data <name> <db-config-file>` Class: IO_WRITE. PySpark: Teradata MERGE.
- `db-ter-delete-data` `mp db-ter-delete-data <name> <db-config-file> -delete <delete stmt> [-brief] [-layout <layout>]` Class: SIDE_EFFECT. Meaning: `-brief` = less output. PySpark: Execute `-delete` statement (pre/post step).
- `db-ter-direct-load` `mp db-ter-direct-load This component is internal and is not intended for direct use by end users.` Class: RUNTIME_IGNORE. PySpark: None (internal component, not for direct use).
- `db-ter-fmload` `mp db-ter-fmload This component is internal and is not intended for direct use by end users.` Class: RUNTIME_IGNORE. PySpark: None (internal component).
- `continuous-db-ter-fmload` `mp continuous-db-ter-fmload This component is internal and is not intended for direct use by end users.` Class: RUNTIME_IGNORE. PySpark: None (internal component).
- `db-inf-unload-data` `mp db-inf-unload-data <name> <db-config-file> -select <sql-select-statement> [-layout <layout>]` Class: IO_READ. PySpark: Informix JDBC read.
- `db-inf-load-data` `mp db-inf-load-data This component is internal and is not intended for direct use by end users.` Class: RUNTIME_IGNORE. PySpark: None (internal component).
- `db-inf-insert` `mp db-inf-insert This component is internal and is not intended for direct use by end users.` Class: RUNTIME_IGNORE. PySpark: None (internal component).
- `db-inf-phlp` `mp db-inf-phlp This component is internal and is not intended for direct use by end users.` Class: RUNTIME_IGNORE. PySpark: None (internal component).
- `db-inf-var2delim` `mp db-inf-var2delim This component is internal and is not intended for direct use by end users.` Class: RUNTIME_IGNORE. PySpark: None (internal component).
- `db-inf-xtract` `mp db-inf-xtract This component is internal and is not intended for direct use by end users.` Class: RUNTIME_IGNORE. PySpark: None (internal component).
- `db-genutil-extract` `mp db-genutil-extract <XXX usage file to be written later>` Class: IO_READ. Meaning: Usage not documented in mp help. PySpark: JDBC read.
- `db-genutil-extract-np` `mp db-genutil-extract-np <XXX usage file to be written later>` Class: IO_READ. Meaning: Usage not documented in mp help. PySpark: JDBC read.
- `db-genutil-insert` `mp db-genutil-insert This component is internal and is not intended for direct use by end users.` Class: RUNTIME_IGNORE. PySpark: None (internal component).
- `dbi-udl-01-controller` `mp dbi-udl-01-controller This component is internal and is not intended for direct use by end users.` Class: RUNTIME_IGNORE. PySpark: None (internal component).
- `dbi-udl-01-datareader-via-file` `mp dbi-udl-01-datareader-via-file This component is internal and is not intended for direct use by end users.` Class: RUNTIME_IGNORE. PySpark: None (internal component).
- `dbi-udl-01-datareader-via-npipe` `mp dbi-udl-01-datareader-via-npipe This component is internal and is not intended for direct use by end users.` Class: RUNTIME_IGNORE. PySpark: None (internal component).
- `db-bigquery-load-gcs-util` `mp db-bigquery-load-gcs-util <name> <db-config-file> [options]` Class: IO_WRITE. PySpark: `df.write.format("bigquery")` (indirect via GCS).
- `db-bigquery-load-storage-api-stream-writer` `mp db-bigquery-load-storage-api-stream-writer <name> <db-config-file> [options]` Class: IO_WRITE. PySpark: `df.write.format("bigquery").option("writeMethod","direct")`.
- `db-bigquery-unload-gcs-util` `mp db-bigquery-unload-gcs-util <name> <db-config-file> [options]` Class: IO_READ. PySpark: `spark.read.format("bigquery")`.
- `db-bigquery-unload-storage-api-controller` `mp db-bigquery-unload-storage-api-controller <name> <db-config-file> [options]` Class: IO_READ. PySpark: `spark.read.format("bigquery")` (Storage API).
- `db-bigquery-unload-storage-api-worker` `mp db-bigquery-unload-storage-api-worker <name> <db-config-file> [options]` Class: IO_READ. PySpark: `spark.read.format("bigquery")` (Storage API).
- `db-cassandra-load` `mp db-cassandra-load <name> <db-config-file> [options]` Class: IO_WRITE. PySpark: Spark Cassandra connector write.
- `db-dynamodb-read` `mp db-dynamodb-read <name> <db-config-file> [options]` Class: IO_READ. PySpark: DynamoDB connector / boto3 export.
- `db-hbase-load` `mp db-hbase-load <name> <db-config-file> -table <tablename> ... -layout <layout>` Class: IO_WRITE. PySpark: HBase connector write to `-table`.
- `db-hbase-bulkload` `mp db-hbase-bulkload <name> <db-config-file> [options]` Class: IO_WRITE. PySpark: HBase connector bulk write.
- `db-hbase-unload` `mp db-hbase-unload <name> <db-config-file> [options]` Class: IO_READ. PySpark: HBase connector read.
- `db-hbase-lookup` `mp db-hbase-lookup <name> <db-config-file> [options]` Class: BUSINESS. PySpark: Read HBase table -> join.
- `db-hbase-update` `mp db-hbase-update <name> <db-config-file> [options]` Class: IO_WRITE. PySpark: HBase connector upsert.
- `db-hbase-update-continuous` `mp db-hbase-update-continuous <name> <db-config-file> [options]` Class: IO_WRITE. PySpark: Streaming `foreachBatch` HBase upsert.
- `db-kudu-update` `mp db-kudu-update <name> <db-config-file> [options]` Class: IO_WRITE. PySpark: Kudu connector upsert.
- `db-kudu-update-continuous` `mp db-kudu-update-continuous <name> <db-config-file> [options]` Class: IO_WRITE. PySpark: Streaming `foreachBatch` Kudu upsert.
- `db-mongo-unload` `mp db-mongo-unload <name> <db-config-file> [options]` Class: IO_READ. PySpark: Mongo connector read.
- `db-mongo-lookup` `mp db-mongo-lookup <name> <db-config-file> [options]` Class: BUSINESS. PySpark: Mongo read -> join.
- `db-mongo-update` `mp db-mongo-update <name> <db-config-file> [options]` Class: IO_WRITE. PySpark: Mongo connector write (upsert).
- `db-mongo-update-continuous` `mp db-mongo-update-continuous <name> <db-config-file> [-options]` Class: IO_WRITE. PySpark: Streaming Mongo connector write.
- `db-mongo-trunc` `mp db-mongo-trunc <name> <db-config-file> -database <db> -collection <collection>` Class: SIDE_EFFECT. PySpark: Delete all docs in `-database`/`-collection` before load.
- `db-doc-store-unload` `mp db-doc-store-unload <name> <db-config-file> [options]` Class: IO_READ. PySpark: Document-store connector read.
- `db-doc-store-lookup` `mp db-doc-store-lookup <name> <db-config-file> [options]` Class: BUSINESS. PySpark: Read -> join.
- `db-doc-store-update` `mp db-doc-store-update <name> <db-config-file> [options]` Class: IO_WRITE. PySpark: Document-store connector upsert.
- `db-doc-store-update-continuous` `mp db-doc-store-update-continuous <name> <db-config-file> [options]` Class: IO_WRITE. PySpark: Streaming `foreachBatch` document-store upsert.
- `db-key-value-store-lookup` `mp db-key-value-store-lookup <name> <db-config-file> [options]` Class: BUSINESS. PySpark: Read -> join.
- `db-key-value-store-update` `mp db-key-value-store-update <name> <db-config-file> [options]` Class: IO_WRITE. PySpark: Key-value store client upsert in `foreachPartition`.
- `db-key-value-store-update-continuous` `mp db-key-value-store-update-continuous <name> <db-config-file> [options]` Class: IO_WRITE. PySpark: Streaming `foreachBatch` key-value upsert.
- `CDC_Oracle_XStream` `mp CDC_Oracle_XStream <name> <db-config-file> <outbound-server-name>` Class: MANUAL_REVIEW. PySpark: CDC stream (Debezium/Kafka -> readStream).
- `dblibunisub` `mp dblibunisub <args>` Class: MANUAL_REVIEW. Meaning: Database universal subscriber. PySpark: CDC subscription -> stub (Debezium/Kafka readStream).
- `dblibunisub2` `mp dblibunisub2 <args>` Class: MANUAL_REVIEW. PySpark: CDC subscription -> stub.

### 9.18 Queues, publish/subscribe & messaging

Ab Initio queues/continuous flows and external brokers. Batch_* variants = bounded read (batch); others = continuous (streaming).

- `publish` `mp publish <name> <Queue URL> [-publish_style (appended | file_after | queue)] [-delete_file_if_exists] [-create_empty (True | False)] [-package <package>] -layout <layout> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] [-permissions <mode>]` Class: IO_WRITE. PySpark: Write to queue directory/URL: file sink (append) or Kafka; `-publish_style appended|file_after|queue`.
- `multipublish` `mp multipublish <name> <queue directory> -layout <layout> [-create-empty] [-log <logevents>] [-compressed] [-varstring] [-permissions <mode>] [-package <package>]` Class: IO_WRITE. Meaning: `-varstring`, `-compressed`, `-create-empty`. PySpark: Append write to queue directory (file sink) or Kafka.
- `publish_recycle` `mp publish_recycle <name> <Recycle_Queue_URL> -layout <layout>` Class: IO_WRITE. Meaning: Recycle queue. PySpark: Write to recycle queue path (append).
- `unipublish` `mp unipublish <name> <command line> -layout <layout>` Class: IO_WRITE. PySpark: Universal publisher -> broker client write (review).
- `subscribe` `mp subscribe <name> <URL> <package_file> [-id <subscriber-id>] [-more (appended | files_after | rotating | none | files_in_directory | files_after_time)] [-checkpoint-trigger (time_interval record_count infile_boundary dml_driven service)] [-checkpoint_interval <n_records>] [-wait] [-nodelete] -layout <layout> [-log <logmask>] [-limit <limit>] [-ramp <ramp>]` Class: IO_READ. PySpark: Batch read of queue files or `readStream`; `-more` = what follows (none=bounded); `-checkpoint-trigger`/`-checkpoint_interval` -> streaming checkpointing; `-wait`/`-nodelete` runtime.
- `batch-subscribe` `mp batch-subscribe <name> [-infile <local_URL>] [-id <subscriber-id>] [-more (files_after | none)] [-while <dml_expression>] [-wait] [-nodelete] [-package <package>] -layout <layout> [-log <logmask>] [-limit <limit>] [-ramp <ramp>]` Class: IO_READ. PySpark: Bounded read; `-while <dml_expression>` -> read until predicate false (filter by offset/time).
- `unisubscribe` `mp unisubscribe <name> <package_file> [-wait_for_commit] [-poll_delay <delay in seconds>] [-checkpoint-trigger (time_interval record_count dml_driven service)] [-checkpoint_interval <n_records>] -layout <layout> [-log <logmask>] [-limit <limit>] [-ramp <ramp>]` Class: IO_READ. PySpark: `readStream` from the broker/queue; `-wait_for_commit`, `-poll_delay` runtime.
- `subscribe_recycle` `mp subscribe_recycle <name> <Recycle_Queue_URL> -layout <layout>` Class: IO_READ. PySpark: Read recycle queue path.
- `uniadapter` `mp uniadapter <name> <command-line>` Class: MANUAL_REVIEW. Meaning: Universal adapter command line. PySpark: Review adapter command line -> stub.
- `Kafka_Publish` `mp Kafka_Publish <name> ...` Class: IO_WRITE. PySpark: `df.write.format("kafka")` / `writeStream`.
- `Kafka_Publish2` `mp Kafka_Publish2 <name> ...` Class: IO_WRITE. PySpark: `df.write.format("kafka")` / `writeStream`.
- `Kafka_Subscribe` `mp Kafka_Subscribe <name> ...` Class: IO_READ. PySpark: `spark.readStream.format("kafka")`.
- `Kafka_Subscribe2` `mp Kafka_Subscribe2 <name> ...` Class: IO_READ. PySpark: `spark.readStream.format("kafka")`.
- `Batch_Kafka_Subscribe` `mp Batch_Kafka_Subscribe <name> ...` Class: IO_READ. PySpark: `spark.read.format("kafka")` with start/end offsets.
- `Batch_Kafka_Subscribe2` `mp Batch_Kafka_Subscribe2 <name> ...` Class: IO_READ. PySpark: `spark.read.format("kafka")` with offsets.
- `Kinesis_Publish` `mp Kinesis_Publish <name> ...` Class: IO_WRITE. PySpark: Kinesis connector / boto3 `foreachPartition`.
- `Kinesis_Subscribe` `mp Kinesis_Subscribe <name> ...` Class: IO_READ. PySpark: Kinesis streaming source.
- `Batch_Kinesis_Subscribe` `mp Batch_Kinesis_Subscribe <name> ...` Class: IO_READ. PySpark: Bounded Kinesis read (connector / export).
- `Pulsar_Publish` `mp Pulsar_Publish <name> ...` Class: IO_WRITE. PySpark: Pulsar connector.
- `Pulsar_Subscribe` `mp Pulsar_Subscribe <name> ...` Class: IO_READ. PySpark: Pulsar connector `readStream`.
- `Batch_Pulsar_Subscribe` `mp Batch_Pulsar_Subscribe <name> ...` Class: IO_READ. PySpark: Pulsar connector bounded read.
- `RabbitMQ_Publish` `mp RabbitMQ_Publish <name> ...` Class: IO_WRITE. PySpark: Client lib in `foreachPartition`.
- `RabbitMQ_Subscribe` `mp RabbitMQ_Subscribe <name> ...` Class: IO_READ. PySpark: External consumer -> landing files -> read.
- `Batch_RabbitMQ_Subscribe` `mp Batch_RabbitMQ_Subscribe <name> ...` Class: IO_READ. PySpark: External consumer -> landing files -> `spark.read`.
- `MQ_Publish` `mp MQ_Publish <args>` Class: IO_WRITE. PySpark: IBM MQ client in `foreachPartition`.
- `MQ_Subscribe` `mp MQ_Subscribe <args>` Class: IO_READ. PySpark: IBM MQ consumer -> landing files -> read.
- `Batch_MQ_Subscribe` `mp Batch_MQ_Subscribe <args>` Class: IO_READ. PySpark: IBM MQ bounded consume -> landing files -> read.
- `JMS_Publish` `mp JMS_Publish <args>` Class: IO_WRITE. PySpark: JMS client in `foreachPartition`.
- `Batch_JMS_Publish` `mp Batch_JMS_Publish <args>` Class: IO_WRITE. PySpark: JMS client in `foreachPartition`.
- `Batch_JMS_Subscribe` `mp Batch_JMS_Subscribe <args>` Class: IO_READ. PySpark: JMS bounded consume -> landing files -> read.
- `MQTT_Publish` `mp MQTT_Publish <args>` Class: IO_WRITE. PySpark: MQTT client in `foreachPartition`.
- `MQTT_Subscribe` `mp MQTT_Subscribe <args>` Class: IO_READ. PySpark: MQTT consumer -> landing files -> read.
- `Google_PubSub_Publish` `mp Google_PubSub_Publish <name> ...` Class: IO_WRITE. PySpark: Pub/Sub client / connector.
- `Google_PubSub_Subscribe` `mp Google_PubSub_Subscribe <name> ...` Class: IO_READ. PySpark: Pub/Sub connector `readStream`.
- `Batch_Google_PubSub_Subscribe` `mp Batch_Google_PubSub_Subscribe <name> ...` Class: IO_READ. PySpark: Pub/Sub bounded pull -> read.
- `SQS_Publish` `mp SQS_Publish <name> ...` Class: IO_WRITE. PySpark: boto3 in `foreachPartition`.
- `SQS_Subscribe` `mp SQS_Subscribe <name> ...` Class: IO_READ. PySpark: boto3 consumer -> landing files -> read.
- `Batch_SQS_Subscribe` `mp Batch_SQS_Subscribe <name> ...` Class: IO_READ. PySpark: boto3 bounded receive -> read.
- `S3_Publish` `mp S3_Publish <name> ...` Class: IO_WRITE. PySpark: `df.write` to `s3a://` path.
- `S3_Subscribe` `mp S3_Subscribe <name> ...` Class: IO_READ. PySpark: `spark.read`/`readStream` from `s3a://`.
- `Batch_S3_Subscribe` `mp Batch_S3_Subscribe <name> ...` Class: IO_READ. PySpark: `spark.read` from `s3a://` prefix.
- `HTTP_Publish` `mp HTTP_Publish <name> ...` Class: IO_WRITE. PySpark: HTTP client in `foreachPartition`.
- `HTTP_Subscribe` `mp HTTP_Subscribe <name> ...` Class: IO_READ. PySpark: HTTP pull/ingest -> landing files -> read.
- `UDP_Publish` `mp UDP_Publish <name> [-hostname <host>] [-port <port>] [-varstring] [-package <file>]` Class: MANUAL_REVIEW. PySpark: Network sink -> stub.
- `UDP_Subscribe` `mp UDP_Subscribe <name> ...` Class: MANUAL_REVIEW. PySpark: Network source -> stub.
- `Multicast` `mp Multicast Multicast [-multicast_IP_address <ip_addr>] [-port <port>] [-max_memory <max_mem_size_in_MB>] [-udp_block_size <udp_block_size_in_bytes>] [-max_send_speed <blocks_per_sec>] [-max_send_speed_check_period <block_count>] [-resend_period <period_in_microseconds>] [-acknowledgement_period <period_in_microseconds>] [-acknowledgement_group_size <block_count>] [-report_file <URL>] [-report_period <period_in_seconds>]` Class: MANUAL_REVIEW. Meaning: All options are network tuning. PySpark: Network multicast -> stub.
- `SMTP_Subscribe` `mp SMTP_Subscribe <args>` Class: MANUAL_REVIEW. PySpark: Mail ingestion -> stub.
- `Simulator_Publish` `mp Simulator_Publish <name> [-path <path>]` Class: RUNTIME_IGNORE. PySpark: None (test simulator).
- `Batch_Cluster_Subscribe` `mp Batch_Cluster_Subscribe Batch_Cluster_Subscribe <options...>` Class: MANUAL_REVIEW. PySpark: Cluster subscription -> stub.
- `tiled-worker-publish` `mp tiled-worker-publish <args>` Class: RUNTIME_IGNORE. PySpark: None (internal tiled runtime).
- `tiled-worker-subscribe` `mp tiled-worker-subscribe <args>` Class: RUNTIME_IGNORE. PySpark: None (internal tiled runtime).

### 9.19 Service calls & external programs

Calls to external services per record. Spark: UDF / `mapInPandas` / `foreachPartition` with a client; keep calls idempotent; credentials from secrets. Default class: `MANUAL_REVIEW`.

- `call-web-service` `mp call-web-service internet-connector <options...>` Class: MANUAL_REVIEW. PySpark: HTTP client inside `mapInPandas` (batching, retries).
- `continuous-call-web-service` `mp continuous-call-web-service internet-connector <options...>` Class: MANUAL_REVIEW. PySpark: HTTP client in streaming `foreachBatch`/`mapInPandas`.
- `Call_TCP` `mp Call_TCP call-tcp <package-name> [-reject-threshold <choice>] [-ramp <ramp>] [-limit <limit>] [-concurrency-mode <choice>] [-concurrent_requests-allowed] [-concurrent-requests-max-number <nn>] [-concurrent-requests-maintain-record-order] [-save-output-frequency <nn>] [-logging] [-log_input <choice>] [-log_output <choice>] [-log_reject<choice>] [-hostname <string>] [-port <nn>] [-timeout <nn>] [-always-reconnect] [-ssl] [-ssl_cert_file <file>] [-ssl_key_file <file>] [-ssl_key_type <PEM|ASN1>] [-ssl_key_password <xxx>] [-ssl_key_password_encrypted <xxx>] [-ssl_cacert_file <file>] [-ssl_cacert_path <dir>] [-ssl_verify_cert] [-ssl_ciphers <list>] [-ssl_random_file <file>] [-ssl_random_length <nn>]` Class: MANUAL_REVIEW. PySpark: Socket client in `mapInPandas`. `-reject-threshold`, concurrency, SSL options = client config.
- `SMPP_Transmit` `mp SMPP_Transmit call-tcp <package-name> [-reject-threshold <choice>] [-ramp <ramp>] [-limit <limit>] [-concurrency-mode <choice>] [-concurrent_requests-allowed] [-concurrent-requests-max-number <nn>] [-concurrent-requests-maintain-record-order] [-save-output-frequency <nn>] [-asynchronous] [-logging] [-log_input <choice>] [-log_output <choice>] [-log_reject<choice>] [-hostname <string>] [-port <nn>] [-timeout <nn>] [-always-reconnect] [-ssl] [-ssl_cert_file <file>] [-ssl_key_file <file>] [-ssl_key_type <PEM|ASN1>] [-ssl_key_password <xxx>] [-ssl_key_password_encrypted <xxx>] [-ssl_cacert_file <file>] [-ssl_cacert_path <dir>] [-ssl_verify_cert] [-ssl_ciphers <list>] [-ssl_random_file <file>] [-ssl_random_length <nn>] [-ssl_configuration_string <openssl_config_string>]` Class: MANUAL_REVIEW. PySpark: SMS gateway client.
- `Call_Ab_RPC` `mp Call_Ab_RPC call-ab-rpc <options...>` Class: MANUAL_REVIEW. PySpark: RPC client in `mapInPandas` -> stub until endpoint known.
- `Call_Ab_RPC_Transaction` `mp Call_Ab_RPC_Transaction Call_Ab_RPC_Transactions <options...>` Class: MANUAL_REVIEW. PySpark: RPC client (transactional) -> stub.
- `Call_Active_Data` `mp Call_Active_Data Call_Active_Data <options...>` Class: MANUAL_REVIEW. PySpark: Service client -> stub.
- `Call_Cluster` `mp Call_Cluster Call_Cluster <options...>` Class: MANUAL_REVIEW. PySpark: Service client -> stub.
- `Call_MQSeries_Service` `mp Call_MQSeries_Service Call_MQSeries_Service <options...>` Class: MANUAL_REVIEW. PySpark: MQ request/reply client in `mapInPandas` -> stub.
- `Call_MQSeries_Service_Transaction` `mp Call_MQSeries_Service_Transaction Call_MQSeries_Service_Transaction <options...>` Class: MANUAL_REVIEW. PySpark: MQ request/reply (transactional) -> stub.
- `ldap-reader` `mp ldap-reader ldap-reader <host> <port> <version> <base-dn> <anonymous> <user-dn> <password> <user-dn-is-relative> <object-class> <attr-filter> <multi-valued-attributes> <referral> <deref-aliases> <timeout> <size-limit>` Class: IO_READ. PySpark: LDAP extract (ldap3) -> DataFrame.
- `ldap-writer` `mp ldap-writer ldap-writer <host> <port> <version> <base-dn> <anonymous> <user-dn> <password> <user-dn-is-relative> <object-class> <multi-valued-attributes> <referral> <deref-aliases> <timeout> <size-limit>` Class: IO_WRITE. PySpark: LDAP client writes.
- `ldap-publisher` `mp ldap-publisher ldap-publisher <host> <port> <version> <base-dn> <anonymous> <user-dn> <password> <user-dn-is-relative> <object-class> <multi-valued-attributes> <referral> <deref-aliases> <timeout> <size-limit>` Class: IO_WRITE. PySpark: LDAP client writes (ldap3) in `foreachPartition`.
- `filter` `mp filter <name> <command> [<option> ...] -layout <layout>` Class: MANUAL_REVIEW. Meaning: Runs a shell command as a record filter. PySpark: Pipes records through a Unix `<command>`: `df.rdd.pipe(cmd)` or reimplement the command logic.
- `custom` `mp custom <name> <template> [<option> ...] -layout <layout>` Class: MANUAL_REVIEW. PySpark: Custom component from `<template>`: review implementation.
- `multi` `mp multi <arg> ... -layout <layout>` Class: MANUAL_REVIEW. Meaning: Generic multi-argument component. PySpark: Review arguments -> stub.
- `sas-run-program` `mp sas-run-program <name> <sas_file> -layout <layout>` Class: MANUAL_REVIEW. PySpark: Run SAS program externally (orchestrator) or rewrite in PySpark.
- `sas-run-v2` `mp sas-run-v2 sas-run-v2 <inputs> <outputs> <sasfilename> [-full_url <url>] [-limit <limit>] [-ramp <ramp>] [-memport <port> <dml_file> <dml_name>]` Class: MANUAL_REVIEW. Meaning: `-memport` passes in-memory port with DML. PySpark: Run SAS program externally or rewrite in PySpark.
- `run-micrograph` `mp run-micrograph <name> ...` Class: MANUAL_REVIEW. PySpark: Micrograph = reusable subgraph run per record: convert the micrograph separately and call it as a function.
- `Run_DMLScript` `mp Run_DMLScript <name> ...` Class: MANUAL_REVIEW. PySpark: Rewrite the DML script logic in Python.
- `send-mail` `mp send-mail <name> <transform> -smtp-server-host <host> [-smtp-server-port <port>] [-validate-addresses] [-on-address-error (Reject | Log | Ignore)] [-debug-file-name <debug-file-name>] [-continuous] [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout_name>` Class: SIDE_EFFECT. Meaning: `-on-address-error Reject|Log|Ignore`, `-validate-addresses`. PySpark: Notification via orchestrator/SMTP after the job (not per record unless required).
- `continuous-send-mail` `mp continuous-send-mail <name> <transform> -smtp-server-host <host> [-smtp-server-port <port>] [-validate-addresses] [-on-address-error (Reject | Log | Ignore)] [-debug-file-name <debug-file-name>] [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout_name>` Class: SIDE_EFFECT. PySpark: Notification per micro-batch via SMTP client.
- `send-to-ui` `mp send-to-ui <name> [ -grab-input <bool> ] -layout <layout>` Class: RUNTIME_IGNORE. Meaning: Sends data to GDE UI (debug). `-grab-input`. PySpark: None.
- `ws-security-encode` `mp ws-security-encode <name> <package-file> [-user-name <username>] [-password <password>] [-password-encrypted <password>] [-password-auth-type {none, plaintext, digest, plaintext-with-expiration, digest-with-expiration}] [-pretty-formatting {off, on}] [-password-digest-type {SHA-1, SHA-256}] [-client-certificate <cert-path>] [-client-certificate-type {PEM, ASN1}] [-sign-body] [-sign-usernametoken] [-sign-timestamp] [-sign certificate] [-sign-xpointer] [-xpointer-expression <xpointer-expression>] [-private-key <key-path>] [-private-key-password <password>] [-private-key-password-encrypted <password>] [-private-key-type {PEM, ASN1, Binary}] [-signature-algorithm {RSA_SHA1, HMAC-SHA1}] [-error_group <group>] [-log_group <group>] [-limit <limit>] [-ramp <ramp>] [-log <logevents>]` Class: MANUAL_REVIEW. Meaning: All key/password options are secrets. PySpark: WS-Security header signing: python library (zeep/xmlsec).
- `ws-security-decode` `mp ws-security-decode <name> <package-file> [-verify-username-password] [-verify-timestamp] [-verify-client-certificate] [-verify-signatures] [-public-key <path-to-key>] [-public-key-type {PEM, ASN1, Binary}] [-cacert-file <cacert-file>] [-cacert-path <cacert-path>] [-error_group <group>] [-log_group <group>] [-limit <limit>] [-ramp <ramp>] [-log <logevents>]` Class: MANUAL_REVIEW. PySpark: WS-Security verification library (xmlsec/zeep) -> stub.

### 9.20 File-system & transfer operations

Side effects outside DataFrame logic. Preserve them, in phase order, as pre/post steps (Hadoop FileSystem API, `dbutils.fs`, boto3, or the orchestrator). Default class: `SIDE_EFFECT`.

- `delfile` `mp delfile <name> <file> -layout <layout>` Class: SIDE_EFFECT. PySpark: Delete `<file>` (`fs.delete(path, True)`).
- `Delete_Multiple_Files` `mp Delete_Multiple_Files -delete <transform> [-ignore_missing] -layout <layout>` Class: SIDE_EFFECT. PySpark: Delete paths produced by `-delete <transform>`; `-ignore_missing` -> no error if absent.
- `Rename_Multiple_Files` `mp Rename_Multiple_Files -rename <transform> -layout <layout>` Class: SIDE_EFFECT. PySpark: `fs.rename(src, dst)` per record.
- `Touch_Files` `mp Touch_Files -touch <transform> -layout <layout>` Class: SIDE_EFFECT. PySpark: Create empty marker files (e.g. `_SUCCESS`, trigger files).
- `Set_File_Permissions` `mp Set_File_Permissions -chmod <transform> -mode <file_permission> -layout <layout>` Class: SIDE_EFFECT. PySpark: `fs.setPermission` / chmod with `-mode`.
- `ftp-from` `mp ftp-from <name> -rfile <remote-file> -rhost <remote-host> [-ascii | -binary] [-user <remote username>] [-pw <remote password>] [-acct <remote account password>] [-site <string>] -layout <layout>` Class: SIDE_EFFECT. PySpark: Download via ftplib/orchestrator before reading. `-ascii|-binary` transfer mode.
- `ftp-to` `mp ftp-to <name> -rfile <remote-file> -rhost <remote-host> [-ascii | -binary] [-user <remote username>] [-pw <remote password>] [-acct <remote account password>] [-appremote] [-site <string>] -layout <layout>` Class: SIDE_EFFECT. PySpark: Upload after writing; `-appremote` = append remote.
- `ftp-multiple-from` `mp ftp-multiple-from <name> <transform> -skip-count <count> -read-count <count> -layout <layout> -file-empty <Fail|Notify|Ignore|Accept> -file-unavailable <Fail|Notify|Ignore> -filename-error <Fail|Notify|Ignore> -read_as_single_record -rhost <remote-host> [-ascii | -binary] [-user <remote username>] [-pw <remote password>] [-acct <remote account password>] [-site <string>] -layout <layout>` Class: SIDE_EFFECT. PySpark: ftplib download per file listed by transform, before read.
- `ftp-multiple-to` `mp ftp-multiple-to <transform> -layout <layout> [-write_raw_data] [-session-per-blip] [-session-per-transfer] [-grouped] [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -rhost <remote-host> [-ascii | -binary] [-user <remote username>] [-pw <remote password>] [-acct <remote account password>] [-site <string>]` Class: SIDE_EFFECT. PySpark: ftplib upload per file after write.
- `sftp-from` `mp sftp-from <name> -sftp|-scp -host <remote hostname> -source-file <filename to copy> [-user <remote username>] [-passphrase <password or passphrase>] [-encrypted-passphrase <encrypted password or passphrase>] [-program <path to sftp program>] [-no-passphrase|-need-passphrase] [-extra-args <extra arguments to sftp>] [-tolerate-output|-dont-tolerate-output] [-sftp-script <path of template script>]` Class: SIDE_EFFECT. PySpark: paramiko/scp download before read.
- `sftp-to` `mp sftp-to <name> -sftp|-scp -host <remote hostname> -target-file <destination of copy> [-temp-target <temporary destination>] [-user <remote username>] [-passphrase <password or passphrase>] [-encrypted-passphrase <encrypted password or passphrase>] [-program <path to sftp program>] [-no-passphrase|-need-passphrase] [-extra-args <extra arguments to sftp>] [-tolerate-output|-dont-tolerate-output] [-sftp-script <path of template script>]` Class: SIDE_EFFECT. PySpark: paramiko/scp upload after write; `-temp-target` = upload then rename.
- `S3_Abort_Multipart_Upload` `mp S3_Abort_Multipart_Upload <name> ...` Class: SIDE_EFFECT. PySpark: boto3 operation.
- `S3_Copy_Object` `mp S3_Copy_Object <name> ...` Class: SIDE_EFFECT. PySpark: boto3 `copy_object`.
- `S3_Create_Bucket` `mp S3_Create_Bucket <name> ...` Class: SIDE_EFFECT. PySpark: boto3.
- `S3_Delete_Bucket` `mp S3_Delete_Bucket <name> ...` Class: SIDE_EFFECT. PySpark: boto3.
- `S3_Delete_Bucket_Tags` `mp S3_Delete_Bucket_Tags <name> ...` Class: SIDE_EFFECT. PySpark: boto3.
- `S3_Delete_Object_Tags` `mp S3_Delete_Object_Tags <name> ...` Class: SIDE_EFFECT. PySpark: boto3.
- `S3_Delete_Objects` `mp S3_Delete_Objects <name> ...` Class: SIDE_EFFECT. PySpark: boto3 `delete_objects`.
- `S3_Delete_Version` `mp S3_Delete_Version <name> ...` Class: SIDE_EFFECT. PySpark: boto3.
- `S3_Get_Bucket_ACL` `mp S3_Get_Bucket_ACL <name> ...` Class: SIDE_EFFECT. PySpark: boto3.
- `S3_Get_Bucket_Versioning` `mp S3_Get_Bucket_Versioning <name> ...` Class: SIDE_EFFECT. PySpark: boto3.
- `S3_Get_Object_ACL` `mp S3_Get_Object_ACL <name> ...` Class: SIDE_EFFECT. PySpark: boto3.
- `S3_Get_Object_Metadata` `mp S3_Get_Object_Metadata <name> ...` Class: SIDE_EFFECT. PySpark: boto3 `head_object`.
- `S3_List_Bucket_Tags` `mp S3_List_Bucket_Tags <name> ...` Class: SIDE_EFFECT. PySpark: boto3.
- `S3_List_Buckets` `mp S3_List_Buckets <name> ...` Class: SIDE_EFFECT. PySpark: boto3.
- `S3_List_Multipart_Uploads` `mp S3_List_Multipart_Uploads <name> ...` Class: SIDE_EFFECT. PySpark: boto3.
- `S3_List_Objects` `mp S3_List_Objects <name> ...` Class: SIDE_EFFECT. PySpark: boto3 `list_objects_v2` -> DataFrame of keys.
- `S3_List_Versions` `mp S3_List_Versions <name> ...` Class: SIDE_EFFECT. PySpark: boto3.
- `S3_Set_Bucket_Canned_ACL` `mp S3_Set_Bucket_Canned_ACL <name> ...` Class: SIDE_EFFECT. PySpark: boto3.
- `S3_Set_Bucket_Tags` `mp S3_Set_Bucket_Tags <name> ...` Class: SIDE_EFFECT. PySpark: boto3.
- `S3_Set_Bucket_Versioning` `mp S3_Set_Bucket_Versioning <name> ...` Class: SIDE_EFFECT. PySpark: boto3.
- `S3_Set_Object_Canned_ACL` `mp S3_Set_Object_Canned_ACL <name> ...` Class: SIDE_EFFECT. PySpark: boto3.
- `S3_Set_Object_Tags` `mp S3_Set_Object_Tags <name> ...` Class: SIDE_EFFECT. PySpark: boto3.
- `Google_Cloud_Storage_Create_Bucket` `mp Google_Cloud_Storage_Create_Bucket <name> ...` Class: SIDE_EFFECT. PySpark: google-cloud-storage client.
- `Google_Cloud_Storage_Delete_Bucket` `mp Google_Cloud_Storage_Delete_Bucket <name> ...` Class: SIDE_EFFECT. PySpark: google-cloud-storage client.
- `Google_Cloud_Storage_Delete_Objects` `mp Google_Cloud_Storage_Delete_Objects <name> ...` Class: SIDE_EFFECT. PySpark: google-cloud-storage client.
- `Google_Cloud_Storage_List_Buckets` `mp Google_Cloud_Storage_List_Buckets <name> ...` Class: SIDE_EFFECT. PySpark: google-cloud-storage client.
- `Google_Cloud_Storage_List_Objects` `mp Google_Cloud_Storage_List_Objects <name> ...` Class: SIDE_EFFECT. PySpark: google-cloud-storage client.
- `Azure_Blob_Storage_Create_Container` `mp Azure_Blob_Storage_Create_Container <name> ...` Class: SIDE_EFFECT. PySpark: azure-storage-blob client.
- `Azure_Blob_Storage_Delete_Blob` `mp Azure_Blob_Storage_Delete_Blob <name> ...` Class: SIDE_EFFECT. PySpark: azure-storage-blob client.
- `Azure_Blob_Storage_Delete_Container` `mp Azure_Blob_Storage_Delete_Container <name> ...` Class: SIDE_EFFECT. PySpark: azure-storage-blob client.
- `Azure_Blob_Storage_List_Blobs` `mp Azure_Blob_Storage_List_Blobs <name> ...` Class: SIDE_EFFECT. PySpark: azure-storage-blob client.
- `Azure_Blob_Storage_List_Containers` `mp Azure_Blob_Storage_List_Containers <name> ...` Class: SIDE_EFFECT. PySpark: azure-storage-blob client.
- `Write_File_Datasets` `mp Write_File_Datasets -sandbox <sandbox> [-lock] [-break-locks] [-validate] [-create-directories]` Class: RUNTIME_IGNORE. PySpark: None (EME/sandbox dataset registration; `-lock`, `-break-locks`, `-validate`).
- `Write_Sandbox_Files` `mp Write_Sandbox_Files -sandbox <sandbox> [-lock] [-break-locks] [-create-directories]` Class: RUNTIME_IGNORE. PySpark: None (sandbox management).
- `Write_Table_Datasets` `mp Write_Table_Datasets -sandbox <sandbox> [-lock] [-break-locks] [-validate] [-create-directories]` Class: RUNTIME_IGNORE. PySpark: None (EME table dataset registration).

### 9.21 Compression & encryption

Byte-stream codecs and crypto. Default class: `BUSINESS`.

- `compress` `mp compress <name> -layout <layout>` Class: DATA_MOVEMENT. PySpark: Writer `.option("compression", "gzip")`; no transformation.
- `uncompress` `mp uncompress <name> -layout <layout>` Class: DATA_MOVEMENT. PySpark: Reader handles codecs automatically by extension.
- `gzip` `mp gzip <name> -layout <layout>` Class: DATA_MOVEMENT. PySpark: `.option("compression", "gzip")`.
- `gunzip` `mp gunzip <name> -layout <layout>` Class: DATA_MOVEMENT. PySpark: Automatic on read for `.gz`.
- `inflate` `mp inflate <name> -layout <layout>` Class: DATA_MOVEMENT. PySpark: zlib/deflate codec.
- `encrypt` `mp encrypt <name> <key>` Class: BUSINESS. Meaning: `<key>` is a secret. PySpark: `F.aes_encrypt(col, key)` (Spark 3.3+) with key from secret store.
- `decrypt` `mp decrypt <name> <key>` Class: BUSINESS. PySpark: `F.aes_decrypt(col, key)`.
- `aes_encrypt` `mp aes_encrypt ...` Class: BUSINESS. PySpark: `F.aes_encrypt`.
- `aes_decrypt` `mp aes_decrypt ...` Class: BUSINESS. PySpark: `F.aes_decrypt`.

### 9.22 Validation, testing & diagnostics

Quality checks and debugging. Never part of business output. Default class: `VALIDATION`.

- `compute-checksum` `mp compute-checksum <name> -layout <layout>` Class: VALIDATION. Meaning: Computes checksum of the stream. PySpark: Optional reconciliation: `F.sha2(F.concat_ws(...))` aggregated.
- `compare-checksums` `mp compare-checksums <name> -layout <layout>` Class: VALIDATION. PySpark: Optional reconciliation assertion.
- `compare-flows` `mp compare-flows <name> <limit> -layout <layout>` Class: VALIDATION. PySpark: Optional: `df_a.exceptAll(df_b)` count <= `<limit>`.
- `dump` `mp dump <name> -layout <layout>` Class: RUNTIME_IGNORE. PySpark: None (or `df.show()` in debug mode).
- `format-for-dump` `mp format-for-dump <name> normal| hex-fields | hex-record <domversion> [ -print-metadata ] [ -print-functions ] [ -print-internals ] [ -progress <secs> ] [ -select <expression> ] [ -start <recordnum> ] [ -records <recordnums> ] [ -attribute <name>=<value> ] [ -protocol <version> ] [ -as-xml ] [ -select-is-encoded ]  [ -mark-invalid ] [ -easy-sort ] -layout <layout>` Class: RUNTIME_IGNORE. Meaning: Formats records for dump (`-print-metadata`, `-select`, `-records` ...). PySpark: None.
- `logger` `mp logger <name> <file> <starttext> <endtext> -layout <layout>` Class: RUNTIME_IGNORE. Meaning: Writes `<starttext>`/`<endtext>` to `<file>`. PySpark: Python `logging`.
- `Handle_Errors` `mp Handle_Errors <name> [-package package] [options] -layout <layout>` Class: VALIDATION. PySpark: Error routing: union reject/error DataFrames, write to error table; raise per policy.
- `Handle_Logs` `mp Handle_Logs <name> [options] -layout <layout>` Class: RUNTIME_IGNORE. PySpark: Python `logging`.
- `Analyze_Graph` `mp Analyze_Graph analyze_graph <options...>` Class: RUNTIME_IGNORE. PySpark: None (metadata tooling).
- `DML_Cross_Reference` `mp DML_Cross_Reference DML_Cross_Reference <options...>` Class: RUNTIME_IGNORE. PySpark: None (lineage tooling).
- `Summarize_Lineage` `mp Summarize_Lineage Summarize_Lineage <options...>` Class: RUNTIME_IGNORE. PySpark: None (lineage tooling).
- `SQL_Analysis` `mp SQL_Analysis sqlanalysis [-case {Upper | Lower | Mixed}]  [-control-dependencies {True|False}] [-dialect <dialect>] [-dialect2 <dialect>]` Class: RUNTIME_IGNORE. PySpark: None (SQL lineage analysis).
- `Unload_Analysis` `mp Unload_Analysis unload_analysis <options...>` Class: RUNTIME_IGNORE. PySpark: None.
- `Load_Collection` `mp Load_Collection Load_Collection <options...>` Class: RUNTIME_IGNORE. PySpark: None (EME collection).
- `Unload_Collection` `mp Unload_Collection Unload_Collection <options...>` Class: RUNTIME_IGNORE. PySpark: None (EME collection).
- `Do_All_in_Collection` `mp Do_All_in_Collection Do_All_in_Collection <options...>` Class: RUNTIME_IGNORE. PySpark: None (EME collection).

### 9.23 Internal runtime, continuous-flow & tiled infrastructure

Infrastructure with no business meaning. Default class: `RUNTIME_IGNORE`. Default PySpark: None.

- `tiled-connector-source` `mp tiled-connector-source <cmdline>` Class: RUNTIME_IGNORE. PySpark: None.
- `tiled-connector-target` `mp tiled-connector-target <cmdline>` Class: RUNTIME_IGNORE. PySpark: None.
- `tiled-hook` `mp tiled-hook <cmdline>` Class: RUNTIME_IGNORE. PySpark: None.
- `tiled-run` `mp tiled-run <cmdline>` Class: RUNTIME_IGNORE. PySpark: None.
- `tiled-worker` `mp tiled-worker <cmdline>` Class: RUNTIME_IGNORE. PySpark: None.
- `to-tiled` `mp to-tiled <cmdline>` Class: RUNTIME_IGNORE. PySpark: None.
- `from-tiled` `mp from-tiled <cmdline>` Class: RUNTIME_IGNORE. PySpark: None.
- `copybomb` `mp copybomb <name> copybomb` Class: RUNTIME_IGNORE. Meaning: Internal. PySpark: None.
- `revind` `mp revind This component is internal and is not intended for direct use by end users.` Class: RUNTIME_IGNORE. Meaning: Internal component. PySpark: None.
- `continuous_sequencer` `mp continuous_sequencer continuous_sequencer <name> -layout <layout>` Class: MANUAL_REVIEW. PySpark: Streaming sequencing: review.
- `gather-transactions` `mp gather-transactions <name> -layout <layout>` Class: MANUAL_REVIEW. PySpark: Transactional continuous flow: review.
- `gather-transactions-from-pool` `mp gather-transactions-from-pool <name> -layout <layout>` Class: MANUAL_REVIEW. PySpark: None.
- `Transactional_Filter` `mp Transactional_Filter <name> -layout <layout>` Class: MANUAL_REVIEW. PySpark: None.
- `Free_Shared_Variables` `mp Free_Shared_Variables <name> <package> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: RUNTIME_IGNORE. Meaning: Frees shared variables used by `<package>`. PySpark: None.


---

## 10. DML → SPARK SCHEMA MAPPING

`mp metadata` holds DML (`record ... end;`). Convert each field to a `StructField`; the record delimiter and field delimiters become reader options.

| DML type | Example | Spark type | Reader handling |
|---|---|---|---|
| delimited string | `string("\|") name;` | `StringType` | CSV `sep="\|"`; last field delimiter `"\n"` = record separator (`lineSep`) |
| fixed string | `string(10) code;` | `StringType` | fixed-width: read lines with `spark.read.text`, then `substring(value, pos, len)` |
| length-prefixed string | `string(integer(4)) s;` | `StringType` | binary parse → MANUAL_REVIEW unless a library is available |
| decimal (text) | `decimal(12.2) amt;` / `decimal(",") id;` | `DecimalType(p, s)` (p ≥ digits, s = digits after point; delimited → `DecimalType(38, s)`) | parse as string then `cast`; invalid → reject |
| packed decimal | `packed decimal(9.2)` | `DecimalType` | EBCDIC/copybook reader (e.g. Cobrix) |
| binary integer | `integer(1/2/4/8)` | `ByteType/ShortType/IntegerType/LongType` | binary format; `unsigned integer(4)` → `LongType`, `unsigned integer(8)` → `DecimalType(20,0)` |
| real | `real(4)` / `real(8)` | `FloatType` / `DoubleType` | |
| date | `date("YYYYMMDD")` | `DateType` | `to_date(col, "yyyyMMdd")` |
| datetime | `datetime("YYYY-MM-DD HH24:MI:SS")` | `TimestampType` | `to_timestamp(col, "yyyy-MM-dd HH:mm:ss")` |
| void | `void(4)` | skip or `BinaryType` | filler bytes |
| nested record | `record ... end sub;` | `StructType` | |
| vector | `string(",")[integer(4)] tags;` / `type[n]` | `ArrayType(elem)` | |
| union | `union ... end` | MANUAL_REVIEW | choose representation explicitly |
| nullable | `string(",") x = NULL;` / `NULL("")` | `nullable=True` | `nullValue=""` |
| conditional field | `if (flag == 'Y') string(5) extra;` | nullable field | two-pass parse |
| charset | `ebcdic string(10)` / `utf8 string(",")` | `StringType` | `encoding`/`charset` option |

Date/time format tokens: `YYYY→yyyy`, `YY→yy`, `MM→MM`, `DD→dd`, `HH24→HH`, `HH→hh`, `MI→mm`, `SS→ss`, `MON→MMM`.

---

## 11. KEY SPECIFIER & COMMON DML EXPRESSIONS

### Key specifier (`<key>`, `<collate>`, `<collator>`, `-key`)

| DML key | PySpark |
|---|---|
| `{cust_id}` | `["cust_id"]` |
| `{cust_id; txn_dt}` | `["cust_id", "txn_dt"]` |
| `{cust_id; txn_dt descending}` | `[F.col("cust_id").asc(), F.col("txn_dt").desc()]` (ordering only) |
| `{}` (empty key) | whole input is one group (`df.agg(...)`) |
| other modifiers (case-insensitive collation etc.) | apply equivalent (`F.lower`) and flag for review |

NULL ordering is not stated by mp help: make it explicit (`asc_nulls_first()`/`desc_nulls_last()`) and confirm with the business owner when ordering is observable.

### DML expression quick map (for `-select`, `<select>`, `<function>`)

| DML | PySpark |
|---|---|
| `a == b`, `a != b`, `<`, `>` | `==`, `!=`, `<`, `>` |
| `and` / `&&`, `or` / `\|\|`, `not` / `!` | `&`, `\|`, `~` |
| `is_null(x)` / `is_defined(x)` | `x.isNull()` / `x.isNotNull()` |
| `is_blank(s)` | `F.trim(s) == ""` |
| `if (c) a else b` | `F.when(c, a).otherwise(b)` |
| `first_defined(a, b)` | `F.coalesce(a, b)` |
| `string_substring(s, start, len)` (1-based) | `F.substring(s, start, len)` |
| `string_lrtrim(s)` / `string_upcase(s)` / `string_downcase(s)` | `F.trim` / `F.upper` / `F.lower` |
| `string_concat(a, b)` | `F.concat(a, b)` |
| `member [vector 'A', 'B']` | `col.isin("A", "B")` |
| `lookup("LKP", key).field` | join with the lookup DataFrame (broadcast if small) |
| `now()` / `today()` | `F.current_timestamp()` / `F.current_date()` |

Full `.xfr` bodies (`out :: reformat(in) = begin ... end;`, rollup/scan templates with `initialize`/`rollup`/`finalize`) are translated by the transform skill; this file supplies the component shape around them.

---

## 12. INTERPRETATION ALGORITHM (deterministic)

1. **Pre-parse** the wrapper with shell rules (§4.1–4.2). Collect parameters and environment assignments in order.
2. **Split jobs**: each `mp job` … `mp run` block is one job (a script may contain several, run sequentially).
3. **Collect pre/post shell steps** outside the job (§15) with their position relative to `mp run`.
4. **Register definitions**: layouts, metadata (parse DML → schema), files (direction, URL list, format, codec, write mode), includes, errors.
5. **Register components**: command, name, positional args by signature, options; attach catalog entry (§9) and option classes (§8).
6. **Register ports and flows**: resolve `component.port[.subport]`, macro ports, flow type and `-metadata`.
7. **Assign phases** (§5) and checkpoints.
8. **Build the DAG**; topologically sort each phase; check every component input is connected and every flow has a schema (inherit from producer when absent).
9. **Classify** every component and option (§7). Drop `RUNTIME_IGNORE`; collapse pure `DATA_MOVEMENT` nodes into pass-through edges, keeping broadcast-to-join and order requirements.
10. **Derive business intent** per component: join type and NULL semantics, dedup survivor rule, filter routing (out/deselect), reject thresholds, ordering requirements, write modes.
11. **Order side effects** (truncate before load, delete before write, rename/transfer/mail after write).
12. **Emit the parsed job YAML** (§13.1).
13. **Generate PySpark** (§13.2), one function per phase, executed in order; side effects in place.
14. **Emit the conversion report** (§13.3) and run the self-check (§17).

---

## 13. OUTPUT CONTRACT

### 13.1 Parsed job (YAML, fixed key order)

```yaml
job:
  name: <mp job name>
  source_script: <file name>
  parameters:            # every $VAR used
    - {name: RUN_DT, value: null, resolved: false, used_in: [Txn_File]}
layouts:
  - {name: layout_4way, form: partitions, partitions: 4, serial: false}
metadata:
  - name: md_txn
    source: string        # string | file
    record_format: {type: delimited, field_sep: ",", record_sep: "\n"}
    fields:
      - {name: cust_id, dml_type: 'decimal(",")', spark_type: "DecimalType(38,0)", nullable: false}
datasets:
  - {name: Txn_File, kind: ifile, urls: ["file://etlhost/data/serial/txn_${RUN_DT}.dat"], format: delimited, codec: none, write_mode: null, multifile: false}
components:
  - name: Join_Cust
    command: hash-join
    family: join
    class: BUSINESS
    phase: "0"
    args: {key: "{cust_id}", xfr: "$AI_XFR/join_cust.xfr"}
    business_options: {sql_null_comparisons: false}
    ignored_options: [-max-core, -layout, -log]
    reject_threshold: {limit: 0, ramp: 0.0}
    ports: {in0: F2, in1: F3, out: F4, reject: null}
    pyspark_intent: "inner join on cust_id with eqNullSafe; select per xfr"
    review_required: false
    review_reason: null
flows:
  - {name: F4, type: straight-flow, from: Join_Cust.out, to: Agg_Daily.in, metadata: md_joined}
phases:
  - {name: "0", order: 0, components: [Txn_File, Cust_File, Filter_Valid, Join_Cust, Agg_Daily, Out_File], checkpoint: false}
side_effects:
  - {component: null, action: "m_rm old output", phase: pre, timing: pre}
unresolved:
  - {item: RUN_DT, reason: "parameter value supplied at runtime"}
```

### 13.2 PySpark module requirements

- Single module: imports, `CONFIG` built from parameters (CLI/env), explicit `StructType` per metadata, helper `enforce_reject_threshold`, one `run_phase_<n>()` per phase, `main()` calling phases in order and exiting non-zero on failure.
- Keep original Ab Initio names in comments next to each step (`# [hash-join] Join_Cust`).
- Explicit schemas on every read; no schema inference for DML-described data.
- Write modes explicit (`overwrite`/`append`); idempotent reruns.
- No `collect()`/`toPandas()` on large data; `cache()` only for DataFrames consumed more than once.
- Secrets come from environment/secret manager; never hard-code passwords or credentials.
- Every `MANUAL_REVIEW` item becomes `raise NotImplementedError("<component>: <reason>")` in the right place.

### 13.3 Conversion report (markdown)

Sections: Summary (components converted / ignored / manual), Dropped runtime elements (component/option → reason), Assumptions (e.g. join type default, NULL ordering, reject evaluated on totals), Unresolved items, Side effects and their order, Validation steps to run (row counts per dataset, reject counts, checksums of outputs vs Ab Initio run).

---

## 14. WORKED EXAMPLE

Illustrative wrapper (commands and signatures from mp help; names, paths and DML are examples):

```ksh
#!/bin/ksh
export AI_SERIAL=/data/serial
RUN_DT=$1
m_rm -f file://etlhost$AI_SERIAL/cust_daily_$RUN_DT.dat

mp job cust_daily_agg
mp layout layout_serial file://etlhost/data/serial
mp layout layout_4way file://etlhost/data/p0 file://etlhost/data/p1 file://etlhost/data/p2 file://etlhost/data/p3
mp metadata md_txn -string 'record decimal(",") cust_id; date("YYYYMMDD")(",") txn_dt; decimal(12.2)("\n") amount; end;'
mp metadata md_cust -string 'record decimal("|") cust_id; string("\n") segment; end;'
mp metadata md_joined -string 'record decimal(",") cust_id; date("YYYYMMDD")(",") txn_dt; decimal(12.2)(",") amount; string("\n") segment; end;'
mp metadata md_out -string 'record decimal(",") cust_id; date("YYYYMMDD")(",") txn_dt; string(",") segment; decimal(14.2)(",") total_amt; decimal("\n") txn_cnt; end;'
mp ifile Txn_File file://etlhost$AI_SERIAL/txn_$RUN_DT.dat
mp ifile Cust_File file://etlhost$AI_SERIAL/customer.dat
mp ofile Out_File file://etlhost$AI_SERIAL/cust_daily_$RUN_DT.dat
mp ofile Rej_File file://etlhost$AI_SERIAL/txn_rejects_$RUN_DT.dat
mp phase 0
mp hash-partition Part_Txn '{cust_id}' -layout layout_4way
mp select-transform Filter_Valid 'amount > 0' -limit 10 -ramp 0.01 -log 0 -layout layout_4way
mp hash-join Join_Cust '{cust_id}' /xfr/join_cust.xfr -max-core 104857600 -limit 0 -ramp 0.0 -layout layout_4way
mp hash-rollup Agg_Daily '{cust_id; txn_dt}' /xfr/agg_daily.xfr -auto-max-core -limit 0 -ramp 0.0 -layout layout_4way
mp gather Gather_Out -layout layout_serial
mp straight-flow F1 Txn_File.read Part_Txn.in -metadata md_txn
mp all-to-all-flow F2 Part_Txn.out Filter_Valid.in -metadata md_txn
mp straight-flow F3 Filter_Valid.out Join_Cust.in0 -metadata md_txn
mp straight-flow F3r Filter_Valid.reject Rej_File.write -metadata md_txn
mp fan-out-flow F4 Cust_File.read Join_Cust.in1 -metadata md_cust
mp straight-flow F5 Join_Cust.out Agg_Daily.in -metadata md_joined
mp fan-in-flow F6 Agg_Daily.out Gather_Out.in -metadata md_out
mp straight-flow F7 Gather_Out.out Out_File.write -metadata md_out
mp run -report flows -report times
status=$?
exit $status
```

`/xfr/join_cust.xfr`: inner join, output = all `in0` fields + `in1.segment`.
`/xfr/agg_daily.xfr`: `segment :: first(in.segment); total_amt :: sum(in.amount); txn_cnt :: count(1)`.

Interpretation: `Part_Txn`, `F2` all-to-all, `F4` fan-out, `Gather_Out`, layouts, `-max-core`, `-auto-max-core`, `-log`, `-report` → dropped (DATA_MOVEMENT/RUNTIME). `m_rm` → pre-step delete. `Filter_Valid.reject` is connected → reject DataFrame written + threshold `10 + 0.01 × n`. `deselect` not connected → non-matching rows dropped. Join has no `-sql-null-comparisons` → `eqNullSafe`.

```python
import sys
from pyspark.sql import SparkSession, functions as F, types as T

CONFIG = {
    "AI_SERIAL": "/data/serial",
    "RUN_DT": sys.argv[1] if len(sys.argv) > 1 else None,   # $1 in wrapper
}

md_txn = T.StructType([
    T.StructField("cust_id", T.StringType()),   # decimal(",") parsed below
    T.StructField("txn_dt", T.StringType()),    # date("YYYYMMDD")
    T.StructField("amount", T.StringType()),    # decimal(12.2)
])
md_cust = T.StructType([T.StructField("cust_id", T.StringType()), T.StructField("segment", T.StringType())])


def enforce_reject_threshold(component, n_in, n_rej, limit, ramp):
    if n_rej > limit + ramp * n_in:
        raise RuntimeError(f"{component}: {n_rej} rejects > {limit} + {ramp}*{n_in}")


def delete_path(spark, path):  # m_rm pre-step (side effect)
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    p.getFileSystem(spark._jsc.hadoopConfiguration()).delete(p, True)


def run_phase_0(spark, cfg):
    base = cfg["AI_SERIAL"]
    out_path = f"{base}/cust_daily_{cfg['RUN_DT']}.dat"
    delete_path(spark, out_path)                                    # m_rm (pre-step)

    # [ifile] Txn_File  (md_txn)
    txn_raw = spark.read.csv(f"{base}/txn_{cfg['RUN_DT']}.dat", schema=md_txn, sep=",")
    txn = (txn_raw
           .withColumn("cust_id_d", F.expr("try_cast(cust_id AS DECIMAL(38,0))"))
           .withColumn("txn_dt_d", F.to_date("txn_dt", "yyyyMMdd"))
           .withColumn("amount_d", F.expr("try_cast(amount AS DECIMAL(12,2))")))
    # [hash-partition] Part_Txn, [all-to-all-flow] F2 -> dropped (data movement)

    # [select-transform] Filter_Valid  'amount > 0'  -limit 10 -ramp 0.01
    bad = F.col("cust_id_d").isNull() | F.col("txn_dt_d").isNull() | F.col("amount_d").isNull()
    rejects = txn.filter(bad).select(*md_txn.fieldNames())           # reject port -> Rej_File
    valid = (txn.filter(~bad)
             .select(F.col("cust_id_d").alias("cust_id"), F.col("txn_dt_d").alias("txn_dt"),
                     F.col("amount_d").alias("amount")))
    selected = valid.filter(F.coalesce(F.col("amount") > 0, F.lit(False)))   # out; deselect not connected
    n_in, n_rej = txn.count(), rejects.count()
    enforce_reject_threshold("Filter_Valid", n_in, n_rej, 10, 0.01)
    rejects.write.mode("overwrite").csv(f"{base}/txn_rejects_{cfg['RUN_DT']}.dat", sep=",")

    # [ifile] Cust_File (md_cust) ; [fan-out-flow] F4 -> dropped
    cust = (spark.read.csv(f"{base}/customer.dat", schema=md_cust, sep="|")
            .withColumn("cust_id", F.col("cust_id").cast("decimal(38,0)")))

    # [hash-join] Join_Cust '{cust_id}' (no -sql-null-comparisons -> NULL matches NULL)
    s, c = selected.alias("in0"), cust.alias("in1")
    joined = (s.join(c, F.col("in0.cust_id").eqNullSafe(F.col("in1.cust_id")), "inner")
               .select("in0.*", F.col("in1.segment")))

    # [hash-rollup] Agg_Daily '{cust_id; txn_dt}'
    agg = (joined.groupBy("cust_id", "txn_dt")
           .agg(F.first("segment").alias("segment"),
                F.sum("amount").cast("decimal(14,2)").alias("total_amt"),
                F.count(F.lit(1)).alias("txn_cnt")))

    # [gather] Gather_Out (serial layout) -> single output file expected
    (agg.select("cust_id", F.date_format("txn_dt", "yyyyMMdd").alias("txn_dt"),
                "segment", "total_amt", "txn_cnt")
        .coalesce(1).write.mode("overwrite").csv(out_path, sep=","))   # [ofile] Out_File


def main():
    spark = SparkSession.builder.appName("cust_daily_agg").getOrCreate()
    try:
        run_phase_0(spark, CONFIG)        # phases run sequentially (only phase 0 here)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
```

Report notes for this example: `first(segment)` survivor depends on order (single segment per customer assumed — confirm); reject threshold evaluated on totals; serial target written as one part file in a directory (rename post-step if a plain file is required).

---

## 15. EDGE CASES & UNKNOWN HANDLING

| Situation | Rule |
|---|---|
| **Unknown command** (not in §9) | Classify `MANUAL_REVIEW`; record raw line in `unresolved`; emit `NotImplementedError` stub; never guess |
| Signature with `...`/`<options...>` | Keep raw options verbatim; interpret only well-known flags (`-limit`, `-ramp`, `-layout`, `-log`) |
| Internal components ("not intended for direct use") | `RUNTIME_IGNORE`; still list them; if they sit on a data path, keep the edge as pass-through |
| Deprecated `*-transform` forms (`rollup-transform`, `scan-transform`, `normalize-transform`, `denormalize-transform`) | Positional `<xfr-file> <prefix> <limit> <ramp> <log-events>`; convert exactly like the modern command |
| Unresolved `$VAR` / `$(...)` | Keep as `CONFIG["VAR"]`; list in `unresolved`; never invent values |
| Several `mp job` blocks | Separate jobs run in script order; shared files are dependencies |
| Shell `if`/`case`/`for` around mp lines | Translate to Python control flow around the generated phases |
| Exit status checks (`$?`, `\|\| exit 1`) | Raise exception / `sys.exit(1)` |
| `m_*` utilities in wrapper | `m_rm`→delete, `m_mkdir`→mkdirs, `m_touch`→create marker, `m_cp`→copy, `m_mv`→rename, `m_ls`→list, `m_dump`→debug (ignore), `m_rollback`/`m_cleanup`→recovery (ignore), `m_mkfs`→multifile system creation (ignore), `m_eval`→evaluate expression into a parameter |
| `air` commands (EME/sandbox) | Ignore (repository management) |
| Flow without `-metadata` | Inherit producer port metadata; if unknown → `unresolved` |
| One out port feeding several flows | Reuse the DataFrame; `cache()` when consumed 2+ times |
| Same dataset written then read (iofile, cross-phase files) | Materialize the write before the read (phase order); avoid reading and overwriting the same path in one action |
| Multifile URL (`mfile:`) or several URLs | Directory or list of paths; output part files |
| Layout with one partition writing a target | Single output file expected → `coalesce(1)` + rename post-step if required |
| `-select` that references fields not in metadata | `unresolved` (likely parameter-substituted expression) |
| Join type not determinable | Default inner, `review_required: true`, record assumption |
| Order-dependent logic without an order column | Add `_seq` at read time (`monotonically_increasing_id`) and state the assumption |
| Credentials in script text | Replace with secret references; flag in report; never copy them into code |
| Streaming/continuous components (`continuous-*`, subscribe with `-more` not `none`) | Structured Streaming with checkpoint location, or bounded batch if the job is batch — state choice |

---

## 16. COMMON MISINTERPRETATIONS (corrections to earlier versions)

| Wrong | Right |
|---|---|
| `layout` = record schema → `StructType` | `layout` = execution topology (partitions/hosts). Schema = `metadata` (DML) |
| `limit` → `DataFrame.limit()` | `-limit`/`-ramp` = reject threshold `limit + ramp × records` |
| `ramp` = gradual scaling/tuning | `ramp` = reject rate per record |
| `checkpoint` → `df.checkpoint()` | Recovery point; no code; ensure idempotency |
| All partitioning → `repartition()` | Usually drop; only `broadcast` into joins and range-partition+sort ordering can matter |
| Sort before rollup/merge-join/dedup must be kept | Drop unless order is observable |
| Join default compares NULLs like SQL | Only with `-sql-null-comparisons`; otherwise NULL keys match (`eqNullSafe`) |
| `dedup` = `dropDuplicates()` | Must honour `-keep` first / last / unique-only with an order column |
| `concat` = `gather` | `concat` preserves input order; `gather` does not |
| `reformat -select` is a separate component | It is a filter applied inside the reformat |
| `db-truncate`, `delfile`, `db-runsql` are runtime noise | They change external state; keep them in order |
| `-log` / `-report` / `-max-core` affect data | Never; drop |
| `meta_pivot` = pivot | It is an unpivot (field → name/value rows) |
| `normalize`/`denormalize` direction confusion | normalize = 1→N (`explode`); denormalize = N→1 (`collect_list`) |
| Read errors silently dropped | `-file-*` / `-input-error` policies decide Fail / Notify / Ignore |

---

## 17. SELF-CHECK BEFORE EMITTING OUTPUT

- [ ] Every `mp` line accounted for (converted, dropped with reason, or unresolved).
- [ ] Every component has a class from §7 and a catalog entry from §9 (or MANUAL_REVIEW).
- [ ] Schemas come from `metadata`, never from `layout`.
- [ ] Every `-limit`/`-ramp` pair became a reject-threshold check (not `limit()`).
- [ ] Connected `reject`/`deselect`/extra `outN` ports produce DataFrames that are written.
- [ ] Join type and NULL-key semantics stated; `in0` = left input.
- [ ] Dedup/first/last/scan/leading-records use an explicit deterministic order.
- [ ] Phases executed sequentially; cross-phase reads happen after writes.
- [ ] Side effects (truncate/delete/rename/transfer/SQL/mail) kept in order.
- [ ] Write modes explicit; reruns idempotent.
- [ ] No hard-coded credentials; parameters in CONFIG.
- [ ] Dropped runtime elements listed in the report.
- [ ] Unresolved items listed; no invented values.
- [ ] Reconciliation plan included: row counts, reject counts and output checksums vs. an Ab Initio run (include NULL-key and duplicate-key samples).

---

## 18. CHANGELOG

| Version | Change |
|---|---|
| v1 (README.md) | Conceptual overview, ~30-parameter dictionary, heuristics. Errors: layout→StructType, limit→`limit()`, ramp as tuning |
| v2 (skill_file_v1) | Command inventory (~120 commands), corrected layout as topology; no semantics, no PySpark detail per command |
| v3.2 | Complete inventory generated from mp help (all commands and options) with class + PySpark mapping per command; reject-threshold, NULL-join, dedup, ordering, phase and side-effect semantics; DML mapping; parsing rules; output contract; worked example; edge cases; self-check |
