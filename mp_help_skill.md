---
name: abinitio-mp-help
description: Reference for interpreting Ab Initio `mp` commands (mp job, layout, metadata, file, flows, components, phase, checkpoint, run) as they appear in wrapper scripts and deployed graph scripts. Use whenever input contains `mp <command>` lines or asks what an mp command, argument or option means.
version: v1.2
source: mp help (Ab Initio Co>Operating System `mp` usage text — 438 commands, 308 options)
consumer: LLM
scope: Interpretation of mp commands only. No target-language code.
---
## Index
1. Overview
2. Purpose
3. Architecture
4. Core Concepts
5. Command Inventory
   5.1 Graph Commands
   5.2 Component Commands
   5.3 Parameter Commands
   5.4 Relation Commands
   5.5 Utility Commands
6. Entity Definitions
   6.1 Repository (XXGrepository)
   6.2 Graph (XXGgraph)
   6.3 Component (XXGpvertex)
   6.4 Parameters (XXparameter)
   6.5 Parameter Sets (XXparameter_set)
   6.6 Relations
7. Parsing Framework
   7.1 Tokenization
   7.2 Group Extraction
   7.3 Block Detection
   7.4 Nested Structure Handling
   7.5 Escaped Character Processing
8. Component Extraction
   8.1 Component Metadata
   8.2 Ports
   8.3 Attributes
   8.4 Parameter Sets
   8.5 DML Handling
9. Parameter Processing
   9.1 Parameter Extraction Logic
   9.2 Parameter Data Model
   9.3 Nested Parameters
   9.4 Parameter Resolution
   9.5 Environment Variable Resolution
10. Flow Extraction
    10.1 Input Links
    10.2 Output Links
    10.3 Relation Mapping
    10.4 Component Connections
11. Repository Parsing
    11.1 Repository Metadata
    11.2 Graph Metadata
    11.3 Entity Relationships
12. Data Structures
    12.1 Component Dictionary
    12.2 Parameter Dictionary
    12.3 Flow Dictionary
    12.4 JSON Schema
13. PySpark Generation
    13.1 Component Mapping
    13.2 Transformation Generation
    13.3 Flow Translation
    13.4 Runtime Parameters
14. APIs and Functions
    14.1 extract_blocks()
    14.2 tokenize()
    14.3 parse_component()
    14.4 parse_parameter_set()
    14.5 parse_parameter()
    14.6 build_tree()
    14.7 extract_flow()
15. Examples
    15.1 Simple Graph
    15.2 Complex Graph
    15.3 Nested Parameters
    15.4 End-to-End Conversion
16. Error Handling
17. Best Practices
18. Known Limitations
19. Troubleshooting
20. Version History

# MP HELP SKILL — interpreting Ab Initio `mp` commands

## 0. GOLDEN RULES (read first)

1. **`mp` builds a job; `mp run` executes it.** Every `mp` line before `mp run` only *declares* objects (layouts, metadata, files, components, flows, phases). Read the whole job before explaining what it does.
2. **`layout` is not a schema.** `mp layout` says where and on how many partitions a component runs (parallelism). The record format is `mp metadata` (DML), attached to flows with `-metadata`.
3. **`-limit` / `-ramp` are reject thresholds, not row limits.** A component aborts when `rejects > limit + ramp × records_processed`.
4. **Separate what changes data from what changes execution.** Transforms, filters, keys, joins, aggregations, readers/writers and side effects change data; layouts, partitioning, memory, buffers, logging, reporting and checkpoints do not.
5. **Phases run in order.** All components of phase N finish before phase N+1 starts.
6. **Ports carry meaning.** `in`/`out`, indexed `in0..inN`/`out0..outN`, `reject`, `error`, `log`, `deselect`; datasets have `read`/`write`. Only ports with a flow attached produce data.
7. **Join NULL keys**: without `-sql-null-comparisons` NULL keys match each other; with it they never match (read from the flag's meaning; confirm on real data).
8. **Order matters only where it is used** (first/last selection, running values, `concat`, ordered output files).
9. **Side effects count**: truncate, delete, rename, file transfer, SQL execution, mail — they change things outside the data flow and their order matters.
10. **Never invent.** If a command, parameter value or transform is not known, say so and mark it `MANUAL_REVIEW` / `unresolved`. Never reveal or copy credentials found in scripts.

---

## 1. PURPOSE & SCOPE

This file lets an LLM act as "help" for `mp`: explain any `mp` command, its positional arguments and options; read a wrapper script and describe the graph it runs (datasets, components, flows, phases, parameters); and say which parts affect the data and which are runtime-only.

It does not cover translation into another language or full DML transform (`.xfr`) semantics beyond the basics in §10.

How to use: rules in §0 → model in §2–§6 → classes in §7 → look up each command in §9 and each option in §8 → follow §11 → answer in the §12 format.

---

## 2. EXECUTION MODEL — how a graph runs from a wrapper script

```text
Wrapper script (ksh/bash)
  ├─ environment & parameters  (export AI_SERIAL=..., RUN_DT=$1, sourced env files)
  ├─ optional pre-steps        (m_rm, mkdir, cleanup, file arrival checks)
  ├─ mp job <graph_name>        ← start job definition
  ├─ mp layout / metadata / include / file / ifile / ofile / error
  ├─ mp <component> ...         ← component definitions (name + positional args + options)
  ├─ mp add-port / port         ← extra ports / port options
  ├─ mp *-flow ...              ← connect component ports (with -metadata)
  ├─ mp phase / checkpoint      ← execution boundaries
  ├─ mp run [-report ...]       ← EXECUTE
  ├─ status=$?                  ← non-zero = failure
  └─ optional post-steps        (rename/move outputs, send mail, exit $status)
```

What `mp run` does:

```text
resolve definitions → build topology (layouts × components) → allocate partitions
→ start one process per component partition → connect flows (pipes or files)
→ phase 0 components run concurrently; records stream through flows
→ phase barrier (+ checkpoint if declared) → phase 1 → … → phase N → cleanup → exit status
```

Data is persisted only by components that write: output files, writers, database loads, publishers, lookup writers. Everything else is records in flight between processes.

---

## 3. OBJECT MODEL

| Object | Declared by | Meaning |
|---|---|---|
| Job | `mp job <name>` | Container for all definitions until `mp run`; name = graph name |
| Layout | `mp layout <name> [-exact\|-workdir] <partition>...` or `-hosts <host>...` | Partitions/hosts a component runs on (degree of parallelism, working directories) |
| Metadata | `mp metadata <name> [-file] <dml>` / `-string <dml>` [`-family`] | Record format (DML) = the schema of records on a flow |
| File | `mp file/ifile/ofile/iofile <name> <url>...` | Dataset; one URL per partition; `mfile:` = multifile (one file per partition) |
| Component | `mp <command> <name> <args> [options] -layout <layout>` | Processing step |
| Port | `<component>.<port>[.<subport>]` | Connection point on a component or dataset |
| Flow | `mp <type>-flow <name> <from-port> <to-port> [-metadata md]` | Data edge between ports |
| Phase | `mp phase <name>` | Execution barrier |
| Checkpoint | `mp checkpoint <name>` | Recovery point (restart from here) |
| Macro | `mp begin-macro` … `mp end-macro`, `mp port <macro>.<port> <port>` | Subgraph with external ports |
| Error | `mp error <name> <error-string>...` | Named error definitions |

### Ports

| Port | Meaning |
|---|---|
| `in`, `in0..inN` | Inputs; `in0` is the first input (left side of joins, first in `concat`) |
| `out`, `out0..outN` | Outputs |
| `reject` | Records the component could not process; counted against `-limit`/`-ramp` |
| `error` | Error text describing each reject |
| `log` | Log events (diagnostic) |
| `deselect` | Records that failed a filter expression |
| `read` / `write` | Dataset ports: `read` = data leaves the file into the graph; `write` = data goes into the file |

These names are confirmed by the port lists in Ab Initio graph definitions (e.g. Filter by Expression: `in`, `out`, `deselect`, `reject`, `error`, `log`; Output File: `write`).

---

## 4. PARSING RULES

1. Apply shell semantics first: join `\` continuations, drop `#` comments, honour quoting ('single' literal, "double" expands `$VAR`). A here-doc fed to `mp -` contains mp commands.
2. Parameters: substitute `$VAR`, `${VAR}`, `$(...)` when their value is defined earlier in the script or supplied; otherwise keep them symbolic and list them as unresolved. Never guess values.
3. Tokenize `mp <command> <tokens...>`; the command is the first token after `mp` and is case-sensitive (`Read_JSON` ≠ `read-json`).
4. Match tokens against the command's signature in §9:
   - Positional `<arg>`s are taken in order (e.g. `hash-join <name> <key> <xfr>`).
   - Tokens beginning with `-` (or `+` flags) are options. An option takes a value when its signature shows `<value>`, `{a|b}` or `(a | b)`; otherwise it is a flag.
   - Options may repeat (`-key`, `-report`).
   - `N`-suffixed options (`-primarySqlN`, `-secondarySqlN`, `-filterN`, `-transformN`) are numbered groups: `-primarySql1`, `-filter1`, …
   - Signatures ending in `...` or `<options...>` have undocumented options — record them verbatim.
5. Most components end with `-layout <layout>`: it names where the component runs.
6. Commands with several forms (`port`, `layout`, `metadata`) are told apart by their tokens (`-hosts`, `-string`, a macro port vs a component port).
7. Non-mp lines (shell, `m_*` utilities, `air` commands, `if`/`for`) are covered in §14.

### Positional argument types

| Placeholder | Meaning |
|---|---|
| `<name>` | Name of the component/flow/object being defined |
| `<key>`, `<collate>`, `<collator>`, `<natural_key>`, `<surrogate_key>`, `-major-key`, `-minor-key` | Key specifier, e.g. `{cust_id; txn_dt descending}` (§10) |
| `<xfr>`, `<xfr-file>`, `<transform>`, `<package>`, `<package_file>`, `<package-name>` | DML transform or package (file path or inline text) — holds the business logic |
| `<select>`, `<select-expr>`, `<function>`, `<dml_expression>`, `<expr>` | DML expression (filter or routing condition) |
| `<limit>`, `<ramp>` | Reject threshold (§6) |
| `<logevents>`, `<log-events>`, `<logmask>` | Which log events to emit |
| `<layout>` | Layout name (where the component runs) |
| `<url>`, `<file>`, `<filename>` | Dataset location (`file://host/path`, `mfile:`, `hdfs://`, `s3://` …) |
| `<db-config-file>` | `.dbc` database connection definition |
| `<sql-file>`, `<sql-select-clause>`, `<update-statement-file>`, `<insert-statement-file>`, `<esql-lookup-file>` | SQL text or file |
| `<ablocal-expression>` | Per-partition WHERE clause for parallel unload |
| `<blocksize>`, `<npartitions>`, `<ndesired>`, `<max-records>`, `<num_records>`, `<nrecords>`, `<recordlength>` | Sizes and counts |
| `<prefix>` | Prefix argument of deprecated `*-transform` forms |

---

## 5. PHASE, CHECKPOINT & ORDERING

- **Phase barrier**: components in a phase run concurrently; the next phase starts only when all of them have finished. A dataset written in phase N can be read in phase N+1.
- **Phase membership**: components belong to the phase most recently declared with `mp phase` before their definition (convention of generated scripts; confirm against the script). No `mp phase` → one phase.
- **Checkpoint**: recovery point after a phase; on rerun after failure the job restarts from the last completed checkpoint. It does not change data.
- **Ordering**: records keep their order through straight flows within a partition. Partitioning, `gather` and `interleave` change cross-partition order; `concat` and sorted merges define it. Order is relevant only when something uses it (first/last, running values, sequence numbers, ordered output files).
- **Serial vs parallel**: a layout with one partition is serial (one file); several partitions, or an `mfile:` URL, mean one file per partition.

---

## 6. REJECT, ERROR & DESELECT HANDLING

Many components take `-limit <limit> -ramp <ramp>` (positional `<limit> <ramp>` in deprecated forms).

```text
abort if   rejected_records > limit + ramp × records_processed
limit 0, ramp 0.0  → abort on the first reject
limit 1, ramp 0.01 → tolerate 1 reject plus 1% of records processed
very large limit   → effectively never abort
```

The check runs while records flow, so a job can abort early in the run.

- Rejects arise from transform evaluation errors (bad conversions, NULL into a non-nullable field, explicit `force_error`), records that do not match the DML, and filter evaluation errors.
- Rejected records go to the `reject` port (with reasons on `error`) only if those ports have flows; otherwise they are counted and discarded.
- `deselect` receives records for which a filter expression is false.
- Database loaders use their own error tolerances (`-num_errors`, `-num-errors-allowed`, `-max_errors`, `-maximum_errors`, …) with the same idea.

---

## 7. CLASSIFICATION

Every command (§9) and option (§8) carries one class:

| Class | Meaning | Affects output data? |
|---|---|---|
| `DEFINE` | Declares job structure (job, metadata, files, ports, macros) | Structure only |
| `BUSINESS` | Changes record content, count or selection | Yes |
| `IO_READ` | Reads a persistent dataset into the graph | Yes (source) |
| `IO_WRITE` | Writes a persistent dataset | Yes (target) |
| `DATA_MOVEMENT` | Partitioning, flows, departitioning, copies, codecs | No content change; may change order/placement |
| `CONTROL` | Phases, checkpoints, run, continuous mode | Execution order/recovery |
| `SIDE_EFFECT` | Acts outside the data stream (truncate, delete, rename, transfer, SQL, mail, cloud admin) | External state |
| `VALIDATION` | Checks, thresholds, reconciliation | Can abort the job |
| `RUNTIME_IGNORE` | Tuning, logging, debug, internal/tiled/EME tooling | No |
| `MANUAL_REVIEW` | Specialised connector/protocol or usage undocumented in mp help | Unclear — needs review |

For anything not listed:

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

Each option once, with meaning, class (§7) and whether it affects output data. Which options a command accepts is shown in its §9 signature.


### 8.1 Schema / record format

| Option(s) | Meaning | Class | Affects data? |
|---|---|---|---|
| `-metadata` | Record format (DML) carried by a flow | BUSINESS | Yes |
| `-string` | `mp metadata -string`: inline DML | BUSINESS | Yes |
| `-file` | `mp metadata -file`: DML file | BUSINESS | Yes |
| `-family` | Metadata family (type system) | DEFINE | Structure |
| `-dynamic` | Dynamic type description for generated records | BUSINESS | Yes |
| `-nullable`, `-case`, `-prefer`, `-endianness`, `-utf8_aware` | SAS metadata conversion controls | DEFINE | Structure |

### 8.2 Keys, ordering & selection (business)

| Option(s) | Meaning | Class | Affects data? |
|---|---|---|---|
| `-key` | Key specifier (lookup/index/join/upsert key); may repeat | BUSINESS | Yes |
| `-major-key`, `-minor-key` | sort-groups: group key / within-group sort key | BUSINESS | Yes |
| `-composite_key` | Name of composite key (SAS) | BUSINESS | Yes |
| `-select` | DML filter expression (reformat/dedup/db: SQL select) | BUSINESS | Yes |
| `-select-is-encoded` | format-for-dump select encoding | RUNTIME_IGNORE | No |
| `-keep` | dedup survivor: first \| last \| unique-only | BUSINESS | Yes |
| `-allow-unsorted` | Tolerate unsorted major key | RUNTIME_IGNORE | No |
| `-check-sort` | Fail if input not sorted | RUNTIME_IGNORE | No |
| `-maintain-order`, `-maintain_order` | Preserve driving-input order | DATA_MOVEMENT | Order/placement only |
| `-driving` | Driving (streamed) input index of hash-join | RUNTIME_IGNORE | No |
| `-sql-null-comparisons` | SQL NULL semantics on keys (NULL never equal) | BUSINESS | Yes |
| `-override_surrogate_key` | Explicit surrogate key spec | BUSINESS | Yes |
| `-generate_unique_key` | IMS lookup: generate unique key | BUSINESS | Yes |

### 8.3 Transforms & expressions (business)

| Option(s) | Meaning | Class | Affects data? |
|---|---|---|---|
| `-package` | DML package (helper functions/transform) | BUSINESS | Yes |
| `-transform` | Transform for generated records | BUSINESS | Yes |
| `-transformN`, `-filterN`, `-primarySqlN`, `-secondarySqlN` | db-multi-update statement N: transform, filter, primary SQL, fallback SQL | BUSINESS | Yes |
| `-output-index`, `-output-indexes` | Routing transform: output port index (or vector) | BUSINESS | Yes |
| `-expression`, `-sequential`, `-minimum`, `-maximum`, `-default`, `-seed` | generate: field expression / sequence start / min / max / default / RNG seed | BUSINESS | Yes |
| `-while` | batch-subscribe: continue while expression true | BUSINESS | Yes |
| `-delete`, `-rename`, `-touch`, `-chmod` | Transform producing paths for delete/rename/touch/chmod | SIDE_EFFECT | External state |
| `-insert` | db-load custom INSERT SQL | BUSINESS | Yes |
| `-update_count`, `-do_insert` | SAS upsert: update count / allow inserts | BUSINESS | Yes |

### 8.4 Reject thresholds & error policy (business-relevant)

| Option(s) | Meaning | Class | Affects data? |
|---|---|---|---|
| `-limit` | Reject threshold constant: abort when rejects > limit + ramp * records | VALIDATION | Can abort |
| `-ramp` | Reject threshold rate per record processed | VALIDATION | Can abort |
| `-reject-threshold` | Service-call reject policy | VALIDATION | Can abort |
| `-num_errors`, `-num-errors-allowed`, `-num-discards-allowed`, `-max_errors`, `-max_rejects`, `-maximum_errors` | Loader / SQL error tolerances | VALIDATION | Can abort |
| `-file-empty`, `-file-missing`, `-file-unavailable`, `-filename-error`, `-input-error` | File error policies (Fail\|Notify\|Ignore\|Accept) | VALIDATION | Can abort |
| `-extra-data` | BC data outside index: abort\|ignore\|process | VALIDATION | Can abort |
| `-missing-fields` | xml-split: ignore \| error | VALIDATION | Can abort |
| `-on-address-error` | send-mail: Reject \| Log \| Ignore | VALIDATION | Can abort |
| `-ignore_drop_table_errors` | Ignore DROP TABLE errors | VALIDATION | Can abort |
| `-ignore_missing` | Ignore missing files on delete | VALIDATION | Can abort |
| `-ignoreDuplicates` | Ignore duplicate-key errors (compat\|False\|True) | VALIDATION | Can abort |
| `-no_actions_ok` | Allow statements affecting zero rows | VALIDATION | Can abort |
| `-match_required`, `-match_optional` | Lookup: match required (inner) / optional (left) | BUSINESS | Yes |
| `-maximum_matches` | Max matches per lookup key | BUSINESS | Yes |
| `-error_group`, `-log_group` | WS-security error/log group | RUNTIME_IGNORE | No |

### 8.5 Datasets, files & formats

| Option(s) | Meaning | Class | Affects data? |
|---|---|---|---|
| `-format` | File format: uninterpreted\|orc\|parquet\|seqfile\|avro\|texttable | BUSINESS | Yes |
| `-hadoop-codec` | Codec: none\|gzip\|bzip2\|zlib\|snappy\|lzo\|lz4\|deflate | DATA_MOVEMENT | Order/placement only |
| `-compressed` | Compressed data/flow | DATA_MOVEMENT | Order/placement only |
| `-compression`, `-decompress` | broadcast compression level / decompress | RUNTIME_IGNORE | No |
| `-flags` | File open flags | IO_WRITE | Yes (target) |
| `-mode` | File open mode / flow file mode / loader mode (insert\|append\|replace) / List_Files mode / permission mode | IO_WRITE | Yes (target) |
| `-create-directories` | Create parent directories | RUNTIME_IGNORE | No |
| `-mvs_alloc_options` | MVS dataset allocation | RUNTIME_IGNORE | No |
| `-xmf`, `-rxmf`, `-xmf_type` | Extended multifile control / relative\|absolute | DATA_MOVEMENT | Order/placement only |
| `-overwrite` | Overwrite target | IO_WRITE | Yes (target) |
| `-max-files` | Max open output files | RUNTIME_IGNORE | No |
| `-grouped` | Write grouped files (one per group) | IO_WRITE | Yes (target) |
| `-permissions`, `-file-mode` | Output permissions | SIDE_EFFECT | External state |
| `-skip-count`, `-read-count` | Skip N / read N records per file | BUSINESS | Yes |
| `-read_as_single_record`, `-read-blocks`, `-maximum-block-bytes` | Read file as one record / in blocks | BUSINESS | Yes |
| `-file-pattern`, `-directory-prune`, `-singledir` | List_Files glob include / exclude / single-dir expansion | BUSINESS | Yes |
| `-publish-style`, `-publish_style`, `-create_empty`, `-create-empty`, `-delete_file_if_exists` | Publish style / create empty output / delete before write | IO_WRITE | Yes (target) |
| `-varstring` | Variable-length string framing | BUSINESS | Yes |
| `-more`, `-id`, `-wait`, `-nodelete`, `-infile`, `-wait_for_commit`, `-poll_delay` | Subscriber controls (continuation, id, wait, keep files, local file, commit wait, polling) | DATA_MOVEMENT | Order/placement only |
| `-checkpoint-trigger`, `-checkpoint_interval` | Subscriber checkpoint trigger / interval | CONTROL | Execution |
| `-continuous` | Continuous (streaming) mode | CONTROL | Execution |
| `-sandbox`, `-lock`, `-break-locks`, `-validate` | EME sandbox dataset operations | RUNTIME_IGNORE | No |
| `-path` | Simulator path | RUNTIME_IGNORE | No |
| `-full_url` | SAS run URL | MANUAL_REVIEW | Review |
| `-memport` | SAS in-memory port (port, dml_file, dml_name) | MANUAL_REVIEW | Review |

### 8.6 XML / document formatting

| Option(s) | Meaning | Class | Affects data? |
|---|---|---|---|
| `-parser` | XML parser expat\|xerces | RUNTIME_IGNORE | No |
| `-ignore-blank-cdata`, `-custom-format`, `-root-element`, `-base-element`, `-leaf-element`, `-character-data-field` | XML mapping controls | BUSINESS | Yes |
| `-add-bom`, `-pretty-formatting`, `-indent-size`, `-document-header`, `-document-per-record`, `-xml-doc-field-name` | XML output formatting / doc granularity / field | BUSINESS | Yes |
| `-i`, `-o`, `-port` | Named input/output port (xml-combine/split) or network port | DEFINE | Structure |
| `-as-xml`, `-print-metadata`, `-print-functions`, `-print-internals`, `-progress`, `-start`, `-records`, `-attribute`, `-protocol`, `-mark-invalid`, `-easy-sort` | format-for-dump display controls | RUNTIME_IGNORE | No |

### 8.7 Database

| Option(s) | Meaning | Class | Affects data? |
|---|---|---|---|
| `-table`, `-table_name`, `-itable`, `-otable` | Target/source table name | BUSINESS | Yes |
| `-database`, `-collection` | Mongo database / collection | BUSINESS | Yes |
| `-commitnum`, `-commit_number`, `-rows_per_commit`, `-batchSize`, `-bindsize` | Commit / batch size | RUNTIME_IGNORE | No |
| `-commitTable`, `-jobid` | Restart bookkeeping table / job id | CONTROL | Execution |
| `-max-rows-per-partition` | Unload partition size | RUNTIME_IGNORE | No |
| `-controller-node`, `-haostname` | Loader controller host | RUNTIME_IGNORE | No |
| `-direct`, `-directfile`, `-directparallel` | Oracle direct-path load | RUNTIME_IGNORE | No |
| `-interface` | DB interface (api) / IMS local\|remote | RUNTIME_IGNORE | No |
| `-batch_mode`, `-ims_parameters`, `-alternate_psbname`, `-parent_segment_data` | IMS controls | MANUAL_REVIEW | Review |
| `-unique_identifier` | DB2 USS load unique id | RUNTIME_IGNORE | No |
| `-rebuild_loader`, `-ifs_data_path`, `-ifs_dml_path` | AS/400 loader internals | RUNTIME_IGNORE | No |
| `-brief` | Less verbose | RUNTIME_IGNORE | No |
| `-dialect`, `-dialect2`, `-control-dependencies` | SQL_Analysis dialect / dependency control | RUNTIME_IGNORE | No |
| `-options` | `mvs_alloc_options` value | RUNTIME_IGNORE | No |

### 8.8 Lookup indexing

| Option(s) | Meaning | Class | Affects data? |
|---|---|---|---|
| `-block_size`, `-bitmap_size`, `-miss_rate`, `-skip_empty`, `-keep_bitmap` | Lookup index tuning | RUNTIME_IGNORE | No |
| `-reverse`, `-read_only` | vsamlookup: reverse key traversal / open read-only | MANUAL_REVIEW | Review |

### 8.9 Runtime, performance & topology (ignore for logic)

| Option(s) | Meaning | Class | Affects data? |
|---|---|---|---|
| `-layout` | Execution layout (partitions/hosts) of the component | DATA_MOVEMENT | Order/placement only |
| `-hosts`, `-exact`, `-workdir` | Layout forms: host list / exact partitions / workdirs | DATA_MOVEMENT | Order/placement only |
| `-max-core`, `-max_core`, `-auto-max-core`, `-max_memory` | Memory limits | RUNTIME_IGNORE | No |
| `-stages`, `-buffer`, `-buffer-bytes`, `-buffer-records`, `-buffer-kbytes`, `-n-buffers`, `-flush`, `-breakable`, `-throttle` | Pipeline/buffer/flow tuning | RUNTIME_IGNORE | No |
| `-encrypted` | Encrypted flow transport | RUNTIME_IGNORE | No |
| `-log`, `-logging`, `-log_input`, `-log_output`, `-log_reject`, `-debug-file-name`, `-report_file`, `-report_period` | Logging / diagnostics | RUNTIME_IGNORE | No |
| `-report` | `mp run` reporting: verbose-errors, expanded-graph, flows, table-flows, file-percentages, times, spillage, totals, skew[=n], scroll, file=, interval=, show-folding | RUNTIME_IGNORE | No |
| `-startup-seconds`, `-startup-attempts`, `-startup-retry-seconds`, `-max-clients`, `-progress-timeout-seconds`, `-io-method`, `-group` | read-shared service runtime | RUNTIME_IGNORE | No |
| `-save-output-frequency`, `-concurrency-mode`, `-concurrent_requests-allowed`, `-concurrent-requests-max-number`, `-concurrent-requests-maintain-record-order`, `-asynchronous`, `-timeout`, `-always-reconnect` | Service-call concurrency / timeouts | RUNTIME_IGNORE | No |
| `-multicast_IP_address`, `-udp_block_size`, `-max_send_speed`, `-max_send_speed_check_period`, `-resend_period`, `-acknowledgement_period`, `-acknowledgement_group_size` | Multicast/UDP network tuning | RUNTIME_IGNORE | No |
| `-grab-input` | send-to-ui input grab | RUNTIME_IGNORE | No |

### 8.10 Connectivity & credentials (secrets)

| Option(s) | Meaning | Class | Affects data? |
|---|---|---|---|
| `-hostname`, `-host`, `-rhost`, `-smtp-server-host`, `-smtp-server-port` | Remote host/port | SIDE_EFFECT | External state |
| `-user`, `-user-name`, `-pw`, `-password`, `-password-encrypted`, `-acct`, `-passphrase`, `-encrypted-passphrase`, `-private-key-password`, `-private-key-password-encrypted`, `-ssl_key_password`, `-ssl_key_password_encrypted` | Credentials | SIDE_EFFECT | External state |
| `-no-passphrase`, `-need-passphrase`, `-sftp`, `-scp`, `-program`, `-extra-args`, `-tolerate-output`, `-dont-tolerate-output`, `-sftp-script`, `-source-file`, `-target-file`, `-temp-target` | sftp/scp controls & paths | SIDE_EFFECT | External state |
| `-rfile`, `-ascii`, `-binary`, `-site`, `-appremote`, `-write_raw_data`, `-session-per-blip`, `-session-per-transfer` | FTP controls & paths | SIDE_EFFECT | External state |
| `-ssl`, `-ssl_cert_file`, `-ssl_key_file`, `-ssl_key_type`, `-ssl_cacert_file`, `-ssl_cacert_path`, `-ssl_verify_cert`, `-ssl_ciphers`, `-ssl_random_file`, `-ssl_random_length`, `-ssl_configuration_string` | TLS settings | RUNTIME_IGNORE | No |
| `-verify-username-password`, `-verify-timestamp`, `-verify-client-certificate`, `-verify-signatures`, `-public-key`, `-public-key-type`, `-cacert-file`, `-cacert-path` | WS-Security verification | MANUAL_REVIEW | Review |
| `-password-auth-type`, `-password-digest-type`, `-client-certificate`, `-client-certificate-type`, `-sign`, `-sign-body`, `-sign-usernametoken`, `-sign-timestamp`, `-sign-xpointer`, `-xpointer-expression`, `-private-key`, `-private-key-type`, `-signature-algorithm` | WS-Security signing | MANUAL_REVIEW | Review |
| `-validate-addresses` | Validate e-mail addresses | SIDE_EFFECT | External state |

---

## 9. COMMAND CATALOG (all mp commands)

Entry format: `command` · exact mp help usage · Class · Meaning. Options are explained in §8.


### 9.1 Job administration & execution

Build and launch the job definition. Every command before `mp run` only *defines* the job; nothing executes until `mp run`. Default class: `CONTROL`.

- `job` `mp job [ <jobname> ]` Class: DEFINE. Meaning: Starts a new job definition; `<jobname>` = graph name. Everything until `mp run` belongs to this job.
- `reset` `mp reset` Class: RUNTIME_IGNORE. Meaning: Clears stored definitions (wrapper hygiene before `mp job`).
- `show` `mp show` Class: RUNTIME_IGNORE. Meaning: Prints stored definitions (debug).
- `check` `mp check` Class: VALIDATION. Meaning: Validates stored definitions without running.
- `comment` `mp comment <comment text>` Class: RUNTIME_IGNORE. Meaning: Free-text documentation inside the job.
- `-` `mp -` Class: DEFINE. Meaning: `mp -` reads mp commands from standard input (often a here-doc). Treat its content exactly like inline mp commands.
- `run` `mp run [-report verbose-errors] [-report expanded-graph] [-report flows] [-report table-flows] [-report file-percentages] [-report times] [-report spillage] [-report totals] [-report skew[=<n>]] [-report scroll={true|false}] [-report file=<filename>] [-report interval=<n>] [-report show-folding]` Class: CONTROL. Meaning: Executes the defined job. Exit status non-zero = failure; wrappers usually test `$?`. All `-report` sub-options are monitoring only.

### 9.2 Definitions: files, layouts, metadata, includes, errors

Declarative objects referenced by components and flows. Default class: `DEFINE`.

- `file` `mp file <name> <url> ... [-flags <flags>] [-mode <mode>] [-create-directories] [-mvs_alloc_options <options>] [-format {uninterpreted|orc|parquet|seqfile|avro|texttable}] [-xmf|-rxmf|-xmf_type {relative|absolute}] [-hadoop-codec {none|gzip|bzip2|zlib|snappy|lzo|lz4|deflate}]` Class: DEFINE. Meaning: Generic file definition. `<url>` may repeat (one URL per partition). URL schemes: `file://host/path`, `mfile:` (multifile = N partition files -> treat as a directory / list of paths), `hdfs://`, `s3://` etc.
- `ifile` `mp ifile <name> <url> ... [-flags <flags>] [-mode <mode>] [-create-directories] [-mvs_alloc_options <options>] [-format {uninterpreted|orc|parquet|seqfile|avro|texttable}]` Class: IO_READ. Meaning: Input file. Data source. Data enters the graph through its `read` port.
- `ofile` `mp ofile <name> <url> ... [-flags <flags>] [-mode <mode>] [-create-directories] [-mvs_alloc_options <options>] [-format {uninterpreted|orc|parquet|seqfile|avro|texttable}] [-xmf|-rxmf|-xmf_type {relative|absolute}] [-hadoop-codec {none|gzip|bzip2|zlib|snappy|lzo|lz4|deflate}]` Class: IO_WRITE. Meaning: Output file. Data target. Data leaves the graph through its `write` port; existing content is replaced unless `-flags`/`-mode` say append.
- `iofile` `mp iofile <name> <url> ... [-flags <flags>] [-mode <mode>] [-create-directories] [-mvs_alloc_options <options>] [-format {uninterpreted|orc|parquet|seqfile|avro|texttable}] [-xmf|-rxmf|-xmf_type {relative|absolute}] [-hadoop-codec {none|gzip|bzip2|zlib|snappy|lzo|lz4|deflate}]` Class: IO_READ. Meaning: Input/output file (read and written in the same job, typically different phases). If read and rewritten in one job, the write must happen after the read finishes (normally a later phase).
- `layout` `mp layout <name> [-exact | -workdir] <partition> [<partition> ...]` | `mp layout <name> -hosts <hostname> [<hostname> ...]` Class: DATA_MOVEMENT. Meaning: Execution topology: list of partition directories or hosts. NOT a record schema. `-exact` = use partitions exactly as listed; `-workdir` = partitions are working directories; `-hosts` = host list.
- `metadata` `mp metadata <name> [-file] <filename> [-family <mdfamily>]` | `mp metadata <name> -string <string> [-family <mdfamily>]` Class: DEFINE. Meaning: Record format (DML). `-file` reads DML from a file; `-string` inline DML; `-family` = metadata family/type system. This IS the schema.
- `include` `mp include <filename>` Class: DEFINE. Meaning: Includes a file (DML/XFR/package) into the job definition.
- `error` `mp error <name> <error-string> [<error-string> ...]` Class: DEFINE. Meaning: Defines named error strings/conditions used by the job.

### 9.3 Ports, macros, folds

Wiring of components. Port reference = `<component>.<port>[.<subport>]`. Default class: `DEFINE`.

- `add-port` `mp add-port <component>.<portname>[.<subportname>] [<option> ...]` Class: DEFINE. Meaning: Adds a port to a component (e.g. extra input `in1`, extra output `out2`) with options. Port count drives the number of inputs/outputs.
- `port` `mp port <macroname>.<portname>[.<subportname>] <port>` | `mp port <component>.<portname>[.<subportname>] [<option> ...]` | `mp port <macroname>.<portname>[.<subportname>] <port>` Class: DEFINE. Meaning: Two forms: (1) configure a component port with options, (2) map a macro port to an inner port. Resolve through macros to the real component.
- `begin-macro` `mp begin-macro <name>` Class: DEFINE. Meaning: Start of a macro (subgraph) definition.
- `end-macro` `mp end-macro <name>` Class: DEFINE. Meaning: End of a macro definition.
- `begin-fold` `mp begin-fold <name>` Class: RUNTIME_IGNORE. Meaning: Component folding (runtime process optimization; see `-report show-folding`).

### 9.4 Execution control

Ordering and recovery boundaries. Default class: `CONTROL`.

- `phase` `mp phase <name>` Class: CONTROL. Meaning: Phase barrier: every component of phase N finishes before phase N+1 starts. Data written in an earlier phase may be read later.
- `checkpoint` `mp checkpoint <name>` Class: CONTROL. Meaning: Recovery point after a phase (restart from here). Runtime only.
- `begin-transaction` `mp begin-transaction <name> <command-line>` Class: MANUAL_REVIEW. Meaning: Starts a transactional section with a command line.
- `end-transaction` `mp end-transaction <name> <command-line>` Class: MANUAL_REVIEW. Meaning: Ends a transactional section.

### 9.5 Flows

Connect `<from-port>` to `<to-port>`. `-metadata <mdname>` = record format of the records on the flow; `-mode`, `-breakable`, `-encrypted`, `-compressed`, `-flush`, `-stages <n>`, `-buffer` are runtime settings. Flow type describes how partitions connect (straight = same partitioning, fan-out = partition, fan-in = departition, all-to-all = repartition). Default class: `DATA_MOVEMENT`.

- `straight-flow` `mp straight-flow <name> <from-port> <to-port> [-metadata <mdname>] [-mode <file-modes>] [-breakable] [-encrypted] [-compressed] [-flush] [-stages <n>] [-buffer]` Class: DATA_MOVEMENT. Meaning: Same partitioning on both ends (1:1).
- `fan-in-flow` `mp fan-in-flow <name> <from-port> <to-port> [-metadata <mdname>] [-mode <file-modes>] [-breakable] [-encrypted] [-compressed] [-flush] [-stages <n>] [-buffer]` Class: DATA_MOVEMENT. Meaning: Many partitions -> fewer (departition side).
- `fan-out-flow` `mp fan-out-flow <name> <from-port> <to-port> [-metadata <mdname>] [-mode <file-modes>] [-breakable] [-encrypted] [-compressed] [-flush] [-stages <n>] [-buffer]` Class: DATA_MOVEMENT. Meaning: Few partitions -> many (partition side).
- `all-to-all-flow` `mp all-to-all-flow <name> <from-port> <to-port> [-metadata <mdname>] [-mode <file-modes>] [-breakable] [-encrypted] [-compressed] [-flush] [-stages <n>] [-buffer]` Class: DATA_MOVEMENT. Meaning: Repartitioning flow (each source partition to every target partition).
- `fan-into-ply0-flow` `mp fan-into-ply0-flow <name> <from-port> <to-port> [-metadata <mdname>] [-mode <file-modes>] [-breakable] [-encrypted] [-compressed] [-flush] [-stages <n>] [-buffer]` Class: DATA_MOVEMENT. Meaning: Fan-in into partition 0 (ply 0), usually serial collection.
- `fan-outof-ply0-flow` `mp fan-outof-ply0-flow <name> <from-port> <to-port> [-metadata <mdname>] [-mode <file-modes>] [-breakable] [-encrypted] [-compressed] [-flush] [-stages <n>] [-buffer]` Class: DATA_MOVEMENT. Meaning: Fan-out from partition 0.
- `adaptive-flow` `mp adaptive-flow <name> <from-port> <to-port> [-metadata <mdname>] [-mode <file-modes>] [-breakable] [-encrypted] [-compressed] [-flush] [-stages <n>] [-buffer]` Class: DATA_MOVEMENT. Meaning: Runtime-chosen routing.

### 9.6 Partition components

Distribute records across partitions. Content is unchanged; only placement (and therefore cross-partition order) changes. Default class: `DATA_MOVEMENT`.

- `hash-partition` `mp hash-partition <name> <collate> [-stages <n>] -layout <layout>` Class: DATA_MOVEMENT. Meaning: `<collate>` = partition key. Changes only which partition a record is in; content unchanged.
- `range-partition` `mp range-partition <name> <collate> [-stages <n>] -layout <layout>` Class: DATA_MOVEMENT. Meaning: `<collate>` = range key. Changes only partition placement; with a following sort it yields globally ordered output.
- `roundrobin-partition` `mp roundrobin-partition <name> <blocksize> [-stages <n>] -layout <layout>` Class: DATA_MOVEMENT. Meaning: `<blocksize>` = records per block.
- `function-partition` `mp function-partition <name> <function> [-stages <n>] [-package <package>] [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: `<function>` = DML expression returning partition index; `-package` = function package.
- `load-level-partition` `mp load-level-partition <name> -layout <layout>` Class: DATA_MOVEMENT. Meaning: Load-balanced distribution.
- `broadcast` `mp broadcast [-compression <compression level>] [-decompress] -layout <layout>` Class: DATA_MOVEMENT. Meaning: Replicates every record to all partitions. `-compression`/`-decompress` runtime. Every partition receives every record (used for small reference data).
- `percentage-partition` `mp percentage-partition` Class: BUSINESS. Meaning: Partition by percentage.
- `partition-transactions-by-expression` `mp partition-transactions-by-expression <name> <function> -layout <layout>` Class: MANUAL_REVIEW. Meaning: Distributes transactions of a continuous flow to partitions chosen by `<function>`.
- `partition-transactions-to-pool` `mp partition-transactions-to-pool <name> -layout <layout>` Class: MANUAL_REVIEW. Meaning: Distributes transactions of a continuous flow to a pool of worker partitions.
- `serial-file-partition` `mp serial-file-partition <file list ...>` Class: IO_READ. Meaning: Partitions a serial list of files across partitions.
- `serial-hsf-partition` `mp serial-hsf-partition <file list ...>` Class: IO_READ. Meaning: Same as serial-file-partition for HSF files.
- `pbe-via-tiled` `mp pbe-via-tiled <cmdline>` Class: RUNTIME_IGNORE. Meaning: Partition-by-expression via tiled execution (internal runtime).
- `pbk-via-tiled` `mp pbk-via-tiled <cmdline>` Class: RUNTIME_IGNORE. Meaning: Partition-by-key via tiled execution (internal runtime).
- `pbrr-via-tiled` `mp pbrr-via-tiled <cmdline>` Class: RUNTIME_IGNORE. Meaning: Partition-round-robin via tiled execution (internal runtime).

### 9.7 Departition & copy

Combine partitions/streams or pass through. Default class: `DATA_MOVEMENT`.

- `gather` `mp gather <name> -layout <layout>` Class: DATA_MOVEMENT. Meaning: Collects partitions/flows in arbitrary order. Content unchanged; order across inputs not guaranteed.
- `concat` `mp concat <name> -layout <layout>` Class: BUSINESS. Meaning: Concatenates inputs in order: all of in0 then in1 ... (order preserved). Order-sensitive: downstream sees in0 records first.
- `interleave` `mp interleave <name> <blocksize> -layout <layout>` Class: DATA_MOVEMENT. Meaning: Round-robin merge by `<blocksize>`.
- `copy` `mp copy [-throttle <flow rate description>] -layout <layout>` Class: DATA_MOVEMENT. Meaning: Copy records unchanged. `-throttle` = rate limit (runtime).
- `buffered-copy` `mp buffered-copy [-throttle <flow rate description>] [-buffer-bytes <nbytes>] [-buffer-records <nrecords>] -layout <layout>` Class: DATA_MOVEMENT. Meaning: Copy with buffering (`-buffer-bytes`, `-buffer-records`, `-throttle` runtime).
- `Replicate_Internal` `mp Replicate_Internal <name> -layout <layout>` Class: DATA_MOVEMENT. Meaning: Replicate: same records to every output port.
- `merge-runs` `mp merge-runs <name> <collator> -layout <layout>` Class: BUSINESS. Meaning: Merges sorted runs into one sorted stream.
- `local-merge` `mp local-merge <name> <collate> -layout <layout>` Class: BUSINESS. Meaning: Merges sorted inputs keeping order by `<collate>`.

### 9.8 Sorting

Ordering components. Content is unchanged; order matters only where downstream logic or the target file depends on it. Sorts often exist only to satisfy sorted-input components (rollup, merge-join, dedup). Default class: `BUSINESS`.

- `local-sort` `mp local-sort <name> <collate> [-auto-max-core] [-max-core <nbytes>] -layout <layout>` Class: BUSINESS. Meaning: `<collate>` key specifier; `-max-core`/`-auto-max-core` memory (ignore). Changes only order; content unchanged.
- `partial-sort` `mp partial-sort <name> <collator> [-max-core <nbytes] -layout <layout>` Class: BUSINESS. Meaning: Partial sort.
- `sort-groups` `mp sort-groups <name> -major-key <key> -minor-key <key> [-auto-max-core] [-max-core <nbytes>] [-allow-unsorted] -layout <layout>` Class: BUSINESS. Meaning: Input already grouped by `-major-key`; sorts each group by `-minor-key`. `-allow-unsorted` tolerates unsorted major keys.
- `check-order` `mp check-order <name> <collate> <limit> -layout <layout>` Class: VALIDATION. Meaning: Fails if records are not ordered by `<collate>` (more than `<limit>` violations).
- `find-splitters` `mp find-splitters <name> <collate> <npartitions> -layout <layout>` Class: RUNTIME_IGNORE. Meaning: Computes range-partition boundaries for `<npartitions>`.
- `adaptive-sampleL` `mp adaptive-sampleL <name> <ndesired> -layout <layout>` Class: RUNTIME_IGNORE. Meaning: Local adaptive sample for splitters.
- `adaptive-sampleG` `mp adaptive-sampleG <name> <ndesired> -layout <layout>` Class: RUNTIME_IGNORE. Meaning: Global adaptive sample for splitters.
- `DF_Sort` `mp DF_Sort <name> [options...] -layout <layout>` Class: BUSINESS. Meaning: Dataflow-style sort component; options carry key.
- `Sort_within_Window` `mp Sort_within_Window <name> <package> [options...]` Class: BUSINESS. Meaning: Sorts records within a window (package defines window).

### 9.9 Record transformation

Business logic. `<xfr>` = transform (file or inline); `<select>` = DML filter expression. Standard ports: `in`, `out`, `reject`, `error`, `log`, `deselect` (filters). Default class: `BUSINESS`.

- `reformat-transform` `mp reformat-transform <name> [-limit <limit>] [-ramp <ramp>] [-select <select>] [-output-index <transform>] [-output-indexes <transform>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Reformat. `-output-index <transform>` returns one output port index per record; `-output-indexes` returns a vector of indexes (record can go to several outputs). `-select` filters records before the transform runs; with several out ports, each record goes to the port(s) chosen by `-output-index(es)`.
- `multi-reformat-transform` `mp multi-reformat-transform <name> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Reformat with multiple outputs, one transform each.
- `select-transform` `mp select-transform <name> <select> [-package <package>] [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Filter by Expression. `<select>` true -> `out`, else -> `deselect`; evaluation errors -> `reject`. Deselected records are lost unless the `deselect` port is connected.
- `Dynamic_Filter_by_Expression` `mp Dynamic_Filter_by_Expression <name> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Filter whose expression is supplied at runtime.
- `normalize` `mp normalize <name> <xfr> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: One input record -> N output records. One input record becomes N output records (N decided by the xfr, usually a vector length).
- `normalize-transform` `mp normalize-transform <name> <xfr-file> <prefix> <limit> <ramp> <log-events> -layout <layout> [DEPRECATED: Use normalize instead.]` Class: BUSINESS. Meaning: DEPRECATED form: positional `<xfr-file> <prefix> <limit> <ramp> <log-events>`.
- `denormalize` `mp denormalize <name> <collator> <xfr> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Many records per key -> one record containing a vector. All records of a key group become one record holding a vector.
- `denormalize-transform` `mp denormalize-transform <name> <collator> <xfr-file> <prefix> <limit> <ramp> <log-events> -layout <layout> [DEPRECATED: Use denormalize instead.]` Class: BUSINESS. Meaning: DEPRECATED positional form.
- `hash-denormalize` `mp hash-denormalize <name> <collator> <xfr> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] [-layout <layout>]` Class: BUSINESS. Meaning: In-memory denormalize.
- `fuse-transform` `mp fuse-transform <name> <xfr> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Combines record i of every input into one output record.
- `merge-transform` `mp merge-transform <name> <key> <xfr> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Merges inputs on `<key>` applying `<xfr>`.
- `combine` `mp combine <name> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Combine (assemble sub-records / vectors into records).
- `split` `mp split <name> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Split (inverse of combine: hierarchical record -> flat records).
- `meta_pivot` `mp meta_pivot <name> <name_field> <value_field> [<pivot_field>[-><pivot_out_field>]]+ -layout <layout>` Class: BUSINESS. Meaning: Each input record -> one record per pivot field with `<name_field>`=field name, `<value_field>`=value. `<pivot_field>-><pivot_out_field>` renames.
- `DF_Transpose` `mp DF_Transpose <name> [options...] -layout <layout>` Class: BUSINESS. Meaning: Dataflow transpose.
- `Transform_Ruleset` `mp Transform_Ruleset <name> <xfr> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Business rules ruleset (BRE) transform.
- `simple-parse` `mp simple-parse <name> <xfr> -layout <layout>` Class: BUSINESS. Meaning: Parses a string field into fields.
- `read-sv` `mp read-sv <name> <xfr> -layout <layout>` Class: IO_READ. Meaning: Reads separated values with an xfr.
- `Write_Separated_Values` `mp Write_Separated_Values <name> -layout <layout>` Class: IO_WRITE. Meaning: Writes separated values.
- `read-tagged-values` `mp read-tagged-values <name> [options...] -layout <layout>` Class: BUSINESS. Meaning: Parses tag=value text.
- `read-tagged-values-package` `mp read-tagged-values-package <name> <package> [options...] -layout <layout>` Class: BUSINESS. Meaning: Same, with a package.
- `canonicalize` `mp canonicalize <name> <canon-xfr-fname> <vc-format-fname> -layout <layout>` Class: BUSINESS. Meaning: Canonicalizes values to a standard format.
- `repair-input` `mp repair-input <name> <package> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Repairs malformed input records using `<package>`.
- `validate` `mp validate <name> [-package <package>] [-limit <limit>] [-ramp <ramp>] [-log <logevents>]` Class: VALIDATION. Meaning: Validates records with `<package>`.
- `assign-new-keys` `mp assign-new-keys <name> <natural_key> <surrogate_key> [-override_surrogate_key <key spec>] [-max_core <nbytes>] [-maintain_order] -layout <layout>` Class: BUSINESS. Meaning: `<natural_key>` -> `<surrogate_key>`; `-override_surrogate_key`, `-maintain_order`, `-max_core`. Records whose natural key already has a surrogate keep it; new natural keys receive new surrogate values.
- `Rank_N_Records` `mp Rank_N_Records <name> <package> <num_records> [options...] -layout <layout>` Class: BUSINESS. Meaning: Top-N per group (`<num_records>`). Keeps the top `<num_records>` records per group as ranked by `<package>`.
- `proctrans` `mp proctrans <xfr> ... -layout <layout>` Class: MANUAL_REVIEW. Meaning: Procedural transform chain.
- `dp-exec` `mp dp-exec <name> <xfr-file> -layout <layout>` Class: MANUAL_REVIEW. Meaning: Executes a data-parallel xfr.
- `reformat-3270` `mp reformat-3270 <name> <xfr> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: MANUAL_REVIEW. Meaning: 3270 terminal reformat.
- `reformat-s3270` `mp reformat-s3270 <name> <xfr> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: MANUAL_REVIEW. Meaning: Secure 3270 reformat.
- `T3270` `mp T3270 <name> <transform> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout_name>` Class: MANUAL_REVIEW. Meaning: 3270 terminal interaction.

### 9.10 Joins

`<key>` = join key specifier (same field names on all inputs unless the xfr renames). Join type is NOT in the mp signature: derive from the component parameters (`join_type`, `record_requiredN`) or xfr; default inner. Default class: `BUSINESS`.

- `hash-join` `mp hash-join <name> <key> <xfr> [-auto-max-core] [-max-core <nbytes>] [-driving <input_index>] [-maintain-order] [-sql-null-comparisons] [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Joins its inputs on `<key>`, building output records with `<xfr>`; inputs need not be sorted. `-driving <input_index>` = which input is streamed (performance only); `-maintain-order` keeps the driving input’s record order. Join type is not in the signature: it comes from the xfr/component settings (join type, record_requiredN); assume inner only if nothing says otherwise. `in0` is the first input. Without `-sql-null-comparisons` NULL keys match each other; with it NULL never matches.
- `merge-join` `mp merge-join <name> <key> <xfr> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout> [-max-core <memory_limit>] [-check-sort] [-sql-null-comparisons]` Class: BUSINESS. Meaning: Joins key-sorted inputs on `<key>` with `<xfr>`; `-check-sort` verifies the sort order; `-max-core` is memory only. Same join semantics as hash-join; inputs must already be sorted on `<key>`.
- `DF_Join` `mp DF_Join <name> [options...] -layout <layout>` Class: BUSINESS. Meaning: Dataflow join. Same join semantics as hash-join.
- `join-with-bc-lookup` `mp join-with-bc-lookup join-with-bc-lookup <options...>` Class: BUSINESS. Meaning: Join against block-compressed lookup.
- `ldap-join` `mp ldap-join ldap-join <host> <port> <version> <base-dn> <anonymous> <user-dn> <password> <user-dn-is-relative> <object-class> <attr-filter> <multi-valued-attributes> <referral> <deref-aliases> <timeout> <size-limit>` Class: MANUAL_REVIEW. Meaning: Join against an LDAP directory.
- `continuous-ldap-join` `mp continuous-ldap-join ldap-join <host> <port> <version> <base-dn> <anonymous> <user-dn> <password> <user-dn-is-relative> <object-class> <attr-filter> <multi-valued-attributes> <referral> <deref-aliases> <timeout> <size-limit>` Class: MANUAL_REVIEW. Meaning: Continuous LDAP join.

### 9.11 Aggregation & running calculations

`<collator>`/`<key>` = group key; `<xfr>` defines aggregates (rollup/scan templates: `temporary_type`, `initialize`, `scan`/`rollup`, `finalize`). Default class: `BUSINESS`.

- `rollup` `mp rollup <name> <collator> <xfr> [-limit <limit>] [-ramp <ramp>] [-check-sort] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Sorted rollup. `-check-sort` validates order (ignore). One output record per key group; an empty key `{}` produces one record for the whole input.
- `rollup-transform` `mp rollup-transform <name> <collator> <xfr-file> <prefix> <limit> <ramp> <log-events> -layout <layout> [DEPRECATED: Use rollup instead.]` Class: BUSINESS. Meaning: DEPRECATED positional form.
- `hash-rollup` `mp hash-rollup <name> <collator> <xfr> [-auto-max-core] [-max-core <bytes>] [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: In-memory rollup (unsorted input). One output record per key group (input need not be sorted).
- `DF_Rollup` `mp DF_Rollup <name> [options...] -layout <layout>` Class: BUSINESS. Meaning: Dataflow rollup.
- `aggregate-transform` `mp aggregate-transform <name> <key> <xfr> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Legacy aggregate. One output record per key group.
- `hash-aggregate` `mp hash-aggregate <name> <collator> <xfr> [-auto-max-core] [-max-core <bytes>] [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: In-memory aggregate. One output record per key group.
- `scan` `mp scan <name> <collator> <xfr> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Running (cumulative) aggregate. One output record per input record, carrying running (cumulative) values within each key group in input order.
- `scan-transform` `mp scan-transform <name> <collator> <xfr-file> <prefix> <limit> <ramp> <log-events> -layout <layout> [DEPRECATED: Use scan instead.]` Class: BUSINESS. Meaning: DEPRECATED positional form.
- `hash-scan` `mp hash-scan <name> <collator> <xfr> [-auto-max-core] [-max-core <bytes>] [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: In-memory scan. Running values per key group, one output per input record.
- `continuous-rollup` `mp continuous-rollup <name> <collator> <xfr> [-max-core <bytes>] [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Rollup in a continuous flow.
- `continuous-scan` `mp continuous-scan <name> <collator> <xfr> [-max-core <bytes>] [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Scan in a continuous flow.
- `compute-closure` `mp compute-closure <name> <number-of-partitions> <temporary-file-prefix> <key-length>` Class: BUSINESS. Meaning: `<number-of-partitions> <temporary-file-prefix> <key-length>`.
- `recirculate` `mp recirculate <name> <number-of-partitions> <temporary-file-prefix> <key-field-1> <key-field-2>` Class: BUSINESS. Meaning: Recirculates records for iterative processing on two key fields.
- `file-recirculate` `mp file-recirculate` Class: BUSINESS. Meaning: File-based recirculation.

### 9.12 Deduplication

`<key>` = duplicate key; `-keep (first | last | unique-only)`; `-select` = pre-filter expression; `-package` = helper functions. Default class: `BUSINESS`.

- `dedup` `mp dedup <name> <key> [-package <package>] [-select <select-expr>] [-keep (first | last | unique-only)] [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Dedup Sorted. Default keep = first. `-keep first`/`last` keeps the first/last record of each key group in input order; `unique-only` keeps only keys that occur once; removed records go to the duplicates port if connected.
- `hash-dedup` `mp hash-dedup <name> <key> [-select <select-expr>] [-package <package>] [-max-core <bytes>] [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: In-memory dedup. Keeps one record per `<key>` without needing sorted input.

### 9.13 Sampling, generation, limiting

Test-data generation and sampling. Default class: `BUSINESS`.

- `leading-records` `mp leading-records <name> <max-records> -layout <layout>` Class: BUSINESS. Meaning: First N records. Keeps the first `<max-records>` records in flow order.
- `percentage-sample` `mp percentage-sample <percentage>` Class: BUSINESS. Meaning: Random sample. Keeps a random `<percentage>` of records.
- `exact-sample` `mp exact-sample` Class: BUSINESS. Meaning: Exact-size sample.
- `over-sample` `mp over-sample` Class: BUSINESS. Meaning: Oversampling.
- `sample-control` `mp sample-control` Class: RUNTIME_IGNORE. Meaning: Sampling control (runtime).
- `generate` `mp generate <name> <num_records> { [-expression <field> <expr>] [-sequential <field> <minimum>] [-minimum <field> <minimum>] [-maximum <field> <maximum>] [-default <field>] }* [-seed <rng seed string>] -layout <layout>` Class: BUSINESS. Meaning: Generates `<num_records>` synthetic records. Fields are set by `-expression`, `-sequential` (counter from minimum), `-minimum`/`-maximum` (random range), `-default`; `-seed` makes it repeatable.
- `generate-with-transform` `mp generate-with-transform <name> <num_records> { [-expression <field> <expr>] [-sequential <field> <minimum>] [-minimum <field> <minimum>] [-maximum <field> <maximum>] [-default <field>] }* [-dynamic <type description>] [-seed <rng seed string>] [-transform <transform name>] -layout <layout>` Class: BUSINESS. Meaning: Generates `<num_records>` synthetic records like `generate`, plus a `-transform` applied per record and an optional `-dynamic` record type.
- `random` `mp random <name> <nrecords> <recordlength> -layout <layout>` Class: BUSINESS. Meaning: Random records.
- `Create_Data` `mp Create_Data <name> <number-of-records> <xfr-file> [options] -layout <layout>` Class: BUSINESS. Meaning: Create Data component (`<number-of-records> <xfr-file>`).

### 9.14 Readers

Data sources. Business critical: path, format, schema, options. Default class: `IO_READ`.

- `readfiles` `mp readfiles <transform> [-compressed] [-skip-count <count>] [-read-count <count>] -layout <layout> [-file-empty <Fail|Notify|Ignore|Accept>] [-file-missing <Fail|Notify|Ignore>] [-file-unavailable <Fail|Notify|Ignore>] [-filename-error <Fail|Notify|Ignore>] [-input-error <Fail|Notify|Ignore>] [-read_as_single_record] [-read-blocks] [-maximum-block-bytes <max>]` Class: IO_READ. Meaning: Read Multiple Files. Error policies `-file-empty|-file-missing|-file-unavailable|-filename-error|-input-error <Fail|Notify|Ignore|Accept>` -> pre-checks on path existence/size: Fail=raise, Notify=log warning, Ignore/Accept=skip/allow. Input records supply file names (via `<transform>`); `-skip-count`/`-read-count` skip/read N records per file; file error options choose Fail, Notify, Ignore or Accept.
- `Read_JSON` `mp Read_JSON <name> [options...] -layout <layout>` Class: IO_READ. Meaning: Parses JSON text into DML records.
- `Extract_JSON_Message` `mp Extract_JSON_Message <name> <package> [options...] -layout <layout>` Class: IO_READ. Meaning: Parses JSON messages from a field.
- `XML_To_JSON` `mp XML_To_JSON <name> [options...] -layout <layout>` Class: IO_READ. Meaning: Converts XML documents into JSON documents.
- `Read_Avro` `mp Read_Avro <name> [options...] -layout <layout>` Class: IO_READ. Meaning: Reads Avro data into DML records.
- `Read_Parquet` `mp Read_Parquet <name> [options...] -layout <layout>` Class: IO_READ. Meaning: Reads Parquet data into DML records.
- `DF_Read_Parquet` `mp DF_Read_Parquet <name> [options...] -layout <layout>` Class: IO_READ. Meaning: Dataflow Parquet read.
- `Read_Excel` `mp Read_Excel Read_Excel <options...>` Class: IO_READ. Meaning: Reads Excel workbook sheets into records.
- `Read_Excel_Flow` `mp Read_Excel_Flow Read_Excel_Flow <options...>` Class: IO_READ. Meaning: Excel read from a flow.
- `Read_Nested` `mp Read_Nested <name> [options...] -layout <layout>` Class: IO_READ. Meaning: Nested/hierarchical data.
- `Read_Nested_Sequence` `mp Read_Nested_Sequence <name> [options...] -layout <layout>` Class: IO_READ. Meaning: Nested sequence data.
- `read-xml` `mp read-xml <name> [ -parser { expat | xerces } ] [ -ignore-blank-cdata ] [ -custom-format ] [ -root-element <name> ] [ -base-element <name> ] [ -leaf-element <name> ] [ -character-data-field <name> ] -layout <layout>` Class: IO_READ. Meaning: `-parser expat|xerces` (ignore), `-custom-format`.
- `read-xml-xfr` `mp read-xml-xfr read-xml-xfr <options...>` Class: IO_READ. Meaning: Reads XML using a transform that maps elements to fields.
- `xml-split` `mp xml-split <name> [ -parser { expat | xerces } ] [ -o <port-name> ] [ -xml-doc-field-name <name> ] [ -missing-fields { ignore | error } ] [ -limit <limit> ] [ -ramp <ramp> ] [ -log <log_event_mask> ] [ -package <package> ] -layout <layout>` Class: BUSINESS. Meaning: Splits XML documents into records. `-xml-doc-field-name` = field holding XML.
- `validate-xml-xfr` `mp validate-xml-xfr validate-xml-xfr <options...>` Class: VALIDATION. Meaning: Validates XML documents (schema/structure) via a transform.
- `Read_ASN.1_Transform` `mp Read_ASN.1_Transform <name> <package> [options...] -layout <layout>` Class: MANUAL_REVIEW. Meaning: Decodes ASN.1-encoded data into records using `<package>`.
- `ASN.1_Decoder` `mp ASN.1_Decoder <name> [options...] -layout <layout>` Class: MANUAL_REVIEW. Meaning: Decodes ASN.1 (BER/DER) messages into records.
- `ASN.1_Splitter` `mp ASN.1_Splitter <name> [options...] -layout <layout>` Class: MANUAL_REVIEW. Meaning: Splits an ASN.1 stream into individual messages.
- `Extract_ASN.1_Message` `mp Extract_ASN.1_Message <name> <package> [options...] -layout <layout>` Class: MANUAL_REVIEW. Meaning: Extracts ASN.1 messages from a byte stream using `<package>`.
- `mvsdasd` `mp mvsdasd mvsdasd <options>` Class: MANUAL_REVIEW. Meaning: MVS DASD dataset.
- `read-shared` `mp read-shared [-startup-seconds <startup seconds>] [-startup-attempts <connection tries>] [-startup-retry-seconds <retry delay>] [-max-clients <clients per server>] [-n-buffers <n-buffers>] [-buffer-kbytes <buffer-kbytes>] [-file-mode <filemode>] [-progress-timeout-seconds <timeout-seconds>] [-io-method direct | conventional] [-group <alphanumeric group name>]` Class: RUNTIME_IGNORE. Meaning: Shared-memory reader runtime service (all options runtime).
- `List_Files` `mp List_Files <transform> -mode <files|files-and-directories|directories|hdfs-blocks> [-singledir <Summary|Expansion>] [-file-pattern <glob pattern>] [-directory-prune <glob pattern>] [-skip-count <count>] [-read-count <count>] -layout <layout> [-file-empty <Fail|Notify|Ignore|Accept>] [-file-unavailable <Fail|Notify|Ignore>] [-filename-error <Fail|Notify|Ignore>] [-input-error <Fail|Notify|Ignore>]` Class: IO_READ. Meaning: Lists files as records.
- `BusinessObjects_41_UnxUnload` `mp BusinessObjects_41_UnxUnload BusinessObjects_41_UnxUnload <options...>` Class: MANUAL_REVIEW. Meaning: Unloads data through a SAP BusinessObjects 4.1 universe (.unx).
- `sas-input-program` `mp sas-input-program sas-input-program <libname> <dataset> <outputFile> [-log <logfile>]` Class: MANUAL_REVIEW. Meaning: Reads SAS dataset `<dataset>` from library `<libname>` into `<outputFile>`.
- `sas-idataset-v2` `mp sas-idataset-v2 sas-idataset-v2 <libname> <dataset> <output_file> [-log <logfile>]` Class: MANUAL_REVIEW. Meaning: Reads a SAS dataset (v2 interface) into `<output_file>`.
- `sas-metadata` `mp sas-metadata sas-metadata <libname> <dataset> [-nullable <true|false>] [-case <as_is|all_upper|all_lower>] [-prefer <first_defined|informat_first|format_first|informat_only|format_only>] [-endianness <true|false>] [-utf8_aware <true|false>]` Class: MANUAL_REVIEW. Meaning: Derives a DML record format from a SAS dataset’s metadata.

### 9.15 Writers

Data targets. Business critical: path, format, mode, partitioning of output files. Default class: `IO_WRITE`.

- `writefiles` `mp writefiles <transform> [-compressed] [-overwrite] [-max-files <num>] [-max-core <num>] [-permissions <mode>] [-grouped] -layout <layout>` Class: IO_WRITE. Meaning: Write Multiple Files. Each record’s target file name comes from `<transform>`; `-overwrite` replaces files; `-grouped` writes one file per group.
- `continuous-writefiles` `mp continuous-writefiles <transform> [-publish-style <overwrite | files_after>] [-max-files <num>] [-permissions <mode>] [-grouped] -layout <layout>` Class: IO_WRITE. Meaning: Write Multiple Files in a continuous flow; `-publish-style` controls whether files are overwritten or published after completion.
- `Write_JSON` `mp Write_JSON <name> [options...] -layout <layout>` Class: IO_WRITE. Meaning: Writes records as JSON text.
- `Write_Avro` `mp Write_Avro <name> [options...] -layout <layout>` Class: IO_WRITE. Meaning: Writes records as Avro data.
- `Write_Excel` `mp Write_Excel Write_Excel <options...>` Class: IO_WRITE. Meaning: Writes records to an Excel workbook.
- `Write_Excel_Flow` `mp Write_Excel_Flow Write_Excel_Flow <options...>` Class: IO_WRITE. Meaning: Writes records to an Excel workbook produced as a flow.
- `Write_Nested_Sequence` `mp Write_Nested_Sequence <name> [options...] -layout <layout>` Class: IO_WRITE. Meaning: Writes hierarchical (nested) records as a sequence.
- `write-xml` `mp write-xml <name> [ -custom-format ] [ -root-element <name> ] [ -base-element <name> ] [ -leaf-element <name> ] [ -character-data-field <name> ] [ -add-bom ] -layout <layout>` Class: IO_WRITE. Meaning: Writes records as XML (`-root-element`, `-base-element`, `-leaf-element` control structure; `-add-bom` prepends a byte-order mark).
- `write-xml-xfr` `mp write-xml-xfr write-xml-xfr <options...>` Class: IO_WRITE. Meaning: Writes XML using a transform that maps fields to elements.
- `xml-combine` `mp xml-combine <name> [ -pretty-formatting on|off ] [ -indent-size <num> ] [ -document-header on|off ] [ -add-bom ] [ -document-per-record ] [ -xml-doc-field-name <field-name> ] [ -limit <limit> ] [ -ramp <ramp> ] [ -log <log_event_mask> ] [ -package <package> ] [ -i <port-name> ] [ -port <port-name> ] -layout <layout>` Class: BUSINESS. Meaning: Combines records into XML documents.
- `xml-reformat` `mp xml-reformat <name> <xfr> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: BUSINESS. Meaning: Transforms XML content with `<xfr>`.
- `Write_ASN.1_Transform` `mp Write_ASN.1_Transform <name> <package> [options...] -layout <layout>` Class: MANUAL_REVIEW. Meaning: Encodes records as ASN.1 using `<package>`.
- `ASN.1_Encoder` `mp ASN.1_Encoder <name> [options...] -layout <layout>` Class: MANUAL_REVIEW. Meaning: Encodes records as ASN.1 messages.
- `write-iceberg-files` `mp write-iceberg-files <transform> [-compressed] [-overwrite] [-max-files <num>] [-max-core <num>] [-permissions <mode>] [-grouped] -layout <layout>` Class: IO_WRITE. Meaning: Writes Iceberg data files.
- `Manage_Iceberg_Table` `mp Manage_Iceberg_Table <name> [options...] -layout <layout>` Class: SIDE_EFFECT. Meaning: Performs Apache Iceberg table management (create/alter/maintenance).
- `copy-to-dataset` `mp copy-to-dataset <dataset name> { <fopen option> }+` Class: IO_WRITE. Meaning: Copies stream to a named dataset.
- `sas-output-program` `mp sas-output-program sas-output-program <libname> <dataset> <inputFile> [-log <logfile>] [-key <keyspec>] [-composite_key <name>]` Class: MANUAL_REVIEW. Meaning: Writes `<inputFile>` into SAS dataset `<dataset>`; `-key`/`-composite_key` define keys.
- `sas-odataset-v2` `mp sas-odataset-v2 sas-odataset-v2 <libname> <dataset> <inputfile> [-log <logfile>]` Class: MANUAL_REVIEW. Meaning: Writes a SAS dataset (v2 interface).
- `sas-upsert-program` `mp sas-upsert-program sas-upsert-program <libname> <dataset> <inputFile> -key <key> [-do_insert] [-update_count <update-count>] [-composite_key <key_name>] [-log <logfile>]` Class: MANUAL_REVIEW. Meaning: Updates/inserts SAS dataset rows by `-key`; `-do_insert` allows inserts.
- `sas-upsert` `mp sas-upsert <name> <dataset> [-update_count <count>] [-layout <layout>]` Class: MANUAL_REVIEW. Meaning: Upserts into a SAS dataset; `-update_count` limits updates.
- `To_PGWire` `mp To_PGWire` Class: MANUAL_REVIEW. Meaning: Exposes/sends records over the PostgreSQL wire protocol.
- `Write_MSLI` `mp Write_MSLI <name> ...` Class: MANUAL_REVIEW. Meaning: Specialised writer; mp help gives no further detail.

### 9.16 Lookup files & block-compressed (BC) lookups

Lookup files are keyed reference datasets read by `lookup(...)` calls inside transforms. BC = block-compressed lookup format with separate index files. Default class: `BUSINESS`.

- `write-lookup` `mp write-lookup -key <key> [-block_size <desired uncompressed data bytes per indexed block>] [-bitmap_size <log(2) of bitmap size>] [-miss_rate <0 to 4>] [-skip_empty] [-package <transform>] [+<flag> ...] -layout <layout>` Class: IO_WRITE. Meaning: Writes an indexed lookup file. `-block_size`, `-bitmap_size`, `-miss_rate`, `-skip_empty`, `+<flag>` = index tuning (ignore). Output is a keyed reference file used by `lookup()` calls in transforms.
- `writelookups` `mp writelookups <transform> [-continuous] -key <collator> [-overwrite] [-max-files <num>] [-max-core <num>] [-permissions <mode>] [-grouped] -layout <layout>` Class: IO_WRITE. Meaning: Write Multiple Lookup Files.
- `write-bc-lookups` `mp write-bc-lookups <transform> [-continuous] -key <collator> [-block_size <desired uncompressed data bytes per indexed block>] [-bitmap_size <log(2) of bitmap size>] [-overwrite] [-max-files <num>] [-max-core <num>] [-permissions <mode>] [-grouped] -layout <layout>` Class: IO_WRITE. Meaning: Block-compressed lookup writer.
- `write-lookup-indexes` `mp write-lookup-indexes <name> "-key <key> [-key <key> ...] -layout <layout>` Class: RUNTIME_IGNORE. Meaning: Builds lookup indexes for repeated `-key`.
- `compute-bc-lookups` `mp compute-bc-lookups <transform> [-continuous] -key <collator> [-block_size <desired uncompressed data bytes per indexed block>] [-bitmap_size <log(2) of bitmap size>] [-overwrite] [-max-files <num>] [-max-core <num>] [-permissions <mode>] [-grouped] -layout <layout>` Class: IO_WRITE. Meaning: Computes BC lookup data + index; `-grouped`, `-overwrite`.
- `consolidate-bc-index` `mp consolidate-bc-index data-url index-url key [-keep_bitmap]` Class: RUNTIME_IGNORE. Meaning: Index maintenance.
- `read-bc-data` `mp read-bc-data data-url [data-url ...] index-url [index-url ...] key [-extra-data <abort|ignore|process>]` Class: IO_READ. Meaning: Reads block-compressed (BC) lookup data using its index files; `-extra-data` decides what happens to data not covered by the index.
- `read-multi-bc-data` `mp read-multi-bc-data <transform package> key [-input-error <abort|ignore|notify>] [-extra-data <abort|ignore|process>] [-limit <limit>] [-ramp <ramp>] [-log <logmask>]` Class: IO_READ. Meaning: `-input-error abort|ignore|notify`.
- `merge-bc-data` `mp merge-bc-data data-url [data-url ...] index-url [index-url ...] key [-extra-data <abort|ignore|process>]` Class: IO_READ. Meaning: Merges several BC data/index sets on `key`.
- `merge-multi-bc-data` `mp merge-multi-bc-data <transform package> key [-input-error <abort|ignore|notify>] [-extra-data <abort|ignore|process>] [-limit <limit>] [-ramp <ramp>] [-log <logmask>]` Class: IO_READ. Meaning: Merges multiple BC lookup sets with a transform package.
- `vsamlookup` `mp vsamlookup <vsam url> <transform-file> [-match_optional] [-maximum_matches <n>] [-limit <n>] [-ramp <n>] [-log <log-string>] [-reverse] [-read_only]` Class: MANUAL_REVIEW. Meaning: Looks up records in a mainframe VSAM file using `<transform-file>`; `-match_optional` allows no match; `-maximum_matches` caps matches.

### 9.17 Database components

`<db-config-file>` (.dbc) = connection definition. `<sql-select-clause>`/`-select` = source query; `<ablocal-expression>` = per-partition WHERE clause for parallel unload. Credentials belong in the .dbc/secret store, never in the answer.

- `db-unload` `mp db-unload <name> <db-config-file> <sql-select-clause> <ablocal-expression> [-itable <itable-name>]  [-max-rows-per-partition <num>] -layout <layout>` Class: IO_READ. Meaning: Generic unload (Input Table). `<sql-select-clause>` is the source query; `<ablocal-expression>` is the per-partition WHERE clause for parallel unload.
- `itable` `mp itable <name> <db-config-file> -select <sql-select-statement> [-layout <layout>]` Class: IO_READ. Meaning: Input Table.
- `itable-ims` `mp itable-ims <name> <db-config-file> <table> [-layout <layout>]` Class: MANUAL_REVIEW. Meaning: Reads an IMS table/segment.
- `itable-mongo` `mp itable-mongo <name> <db-config-file> <table> [-layout <layout>]` Class: IO_READ. Meaning: Reads a MongoDB collection as an input table.
- `otable` `mp otable <name> <db-config-file> -table <tablename> [-layout <layout>]` Class: IO_WRITE. Meaning: Output Table.
- `db-load` `mp db-load <name> <db-config-file> [-table <tablename> | -insert <sql>] ... -layout <layout>` Class: IO_WRITE. Meaning: Generic load. Inserts records into `-table` (or via `-insert <sql>`).
- `db-load-continuous` `mp db-load-continuous <name> <db-config-file> <update-statement-file> <insert-statement-file> [-commitnum <commit-block-size>] -layout <layout>` Class: IO_WRITE. Meaning: Loads records into a database continuously using update/insert statement files.
- `db-update` `mp db-update <name> <db-config-file> <update-statement-file> <insert-statement-file> [-commitnum <commit-block-size>] -layout <layout>` Class: IO_WRITE. Meaning: Update Table. Per record: run the update statement; if it affects no rows, run the insert statement (upsert).
- `db-update-continuous` `mp db-update-continuous <name> <db-config-file> <update-statement-file> <insert-statement-file> [-commitnum <commit-block-size>] -layout <layout>` Class: IO_WRITE. Meaning: Continuous Update Table: update-then-insert per record.
- `db-multi-update` `mp db-multi-update <name> db_config_file -commitTable <commit-table> [ -jobid <jobid> ] -batchSize <batch-size> ( -primarySqlN <sql1> [ -secondarySqlN <sql2> ] [ -filterN <expr>] [ -transformN <package> ]) ... [-continuous] [-no_actions_ok] [-ignoreDuplicates (compat | False | True)] -layout <layout>` Class: IO_WRITE. Meaning: Multi Update Table.
- `db-multi-update-continuous` `mp db-multi-update-continuous <name> db_config_file -commitTable <commit-table> [ -jobid <jobid> ] -batchSize <batch-size> ( -primarySqlN <sql1> [ -secondarySqlN <sql2> ] [ -filterN <expr>] [ -transformN <package> ]) ... [-continuous] [-no_actions_ok] [-ignoreDuplicates (compat | False | True)] -layout <layout>` Class: IO_WRITE. Meaning: Continuous Multi Update Table (numbered statement groups).
- `db-continuous-select-multi` `mp db-continuous-select-multi <name> db_config_file interval delay as_of -layout <layout>` Class: IO_READ. Meaning: Repeatedly runs selects every `interval` (after `delay`, `as_of` point) and emits new rows.
- `db-continuous-update-multi` `mp db-continuous-update-multi <name> db_config_file commit_table -layout <layout>` Class: IO_WRITE. Meaning: Applies multiple update statements continuously, tracked in `commit_table`.
- `db-lookup` `mp db-lookup <db-config-file> <esql-lookup-file> <transform-file> <execute-when-not-found-file> [-match_optional] -maximum_matches <n> -commit_number <n> [-limit <n>] [-ramp <n>] [-log <log-string>]` Class: BUSINESS. Meaning: DB lookup per record. Per record, runs `<esql-lookup-file>`; `-match_optional` allows records with no match; `-maximum_matches` caps matches; `<execute-when-not-found-file>` handles misses.
- `db-runsql` `mp db-runsql <name> <db-config-file> <sql-file> [-maximum_errors <num>] [-interface api] [-ignore_drop_table_errors] -layout <layout>` Class: SIDE_EFFECT. Meaning: Run SQL. Executes the statements in `<sql-file>`; changes database state.
- `db-runsql-continuous` `mp db-runsql-continuous <name> <db-config-file> <sql-file> [-maximum_errors <num>] [-interface api] [-ignore_drop_table_errors] -layout <layout>` Class: SIDE_EFFECT. Meaning: Runs a SQL file repeatedly inside a continuous flow.
- `db-truncate` `mp db-truncate <name> <db-config-file> <sql-file> -layout <layout>` Class: SIDE_EFFECT. Meaning: Truncate Table. Removes all rows of the target (via `<sql-file>`) — order-sensitive: normally runs before a load.
- `db-call-stored-procedure` `mp db-call-stored-procedure <name> db_config_file select_sql [<switches>]` Class: SIDE_EFFECT. Meaning: Calls a database stored procedure (`select_sql`).
- `db-controller` `mp db-controller <name> <db-config-file> [-select <sql>] [-itable <name>] [-layout <layout>]` Class: RUNTIME_IGNORE. Meaning: Coordinates parallel database unload/load partitions.
- `db-ims-lookup` `mp db-ims-lookup <name> <db-config-file> <segment_name> <package> -parent_segment_data <key_only | full_segment> -batch_mode <DLI | BMP> -ims_parameters <parameters> -match_required  -maximum_matches <num> -limit <num> -ramp <num> -layout <layout>` Class: MANUAL_REVIEW. Meaning: Looks up IMS segment `<segment_name>` per record.
- `db-ims-lookup-segments` `mp db-ims-lookup-segments <name> db_config_file target_segment [-batch_mode <DLI|BMP>] [-interface <local|remote>] [-match_required] -maximum_matches <max> [-generate_unique_key] [-ims_parameters <ims_parameters>] [-alternate_psbname <alternate_psbname>]` Class: MANUAL_REVIEW. Meaning: Looks up IMS target segments per record.
- `db-ims-unload` `mp db-ims-unload <name> <db-config-file> <tablename> <control-file> [-otable <name> -controller-node <node>] [-mode <insert|append|replace>] [-direct=true] [-directfile=false] [-directparallel=true] [-num-discards-allowed <num>] [-num-errors-allowed <num>] [-commitnum <num>] [-bindsize <num>] -layout <layout>` Class: MANUAL_REVIEW. Meaning: Unloads IMS data.
- `db-ora-sqlldr` `mp db-ora-sqlldr <name> <db-config-file> <tablename> <control-file> [-otable <name> -controller-node <node>] [-mode <insert|append|replace>] [-direct=true] [-directfile=false] [-directparallel=true] [-num-discards-allowed <num>] [-num-errors-allowed <num>] [-commitnum <num>] [-bindsize <num>] -layout <layout>` Class: IO_WRITE. Meaning: Oracle SQL*Loader.
- `db-mssql-ldr` `mp db-mssql-ldr <name> <db-config-file> <tablename> <control-file> [-otable <name> -controller-node <node>]  [-num_errors <num>] [-rows_per_commit <num>]  -layout <layout>` Class: IO_WRITE. Meaning: Bulk-loads records into SQL Server.
- `db-db2-ldr` `mp db-db2-ldr <name> <db-config-file> <tablename>` Class: IO_WRITE. Meaning: Bulk-loads records into DB2.
- `db-db2uss-load` `mp db-db2uss-load <name> <db-config-file> <jcl-tranform-file> <control-file> [-table_name <tablename>] [-unique_identifier <uid>] [-max_errors <num>] [-max_rejects <max>] -layout <layout>` Class: IO_WRITE. Meaning: DB2 z/OS load. `-max_errors`, `-max_rejects` -> thresholds.
- `db-db2uss-loader` `mp db-db2uss-loader <name> <db-config-file> <tablename> <control-file> [-otable <name> -controller-node <node>] -layout <layout>` Class: IO_WRITE. Meaning: Bulk-loads records into DB2 for z/OS (USS).
- `db-db2uss-unload` `mp db-db2uss-unload <name> <db-config-file> <sql-select-clause> <ablocal-expression> [-itable <itable-name>] -layout <layout>` Class: IO_READ. Meaning: Unloads (extracts) rows from DB2 for z/OS (USS).
- `db-db2-genheader` `mp db-db2-genheader <name> <db-config-file> <tablename>` Class: RUNTIME_IGNORE. Meaning: DB2 loader helper: generates load header.
- `db-db2-pad` `mp db-db2-pad <name> <db-config-file> <tablename>` Class: RUNTIME_IGNORE. Meaning: DB2 loader helper: pads fields.
- `db-db2-split` `mp db-db2-split <name> <db-config-file> <tablename> [[abcol1name=dbcol1name]..]` Class: RUNTIME_IGNORE. Meaning: Column mapping `abcol=dbcol` -> apply as renames before write.
- `db-db2-xfrpad` `mp db-db2-xfrpad <name> <db-config-file> <tablename>` Class: RUNTIME_IGNORE. Meaning: DB2 loader helper: pads via transform.
- `db-db2-xfrsplit` `mp db-db2-xfrsplit <name> <db-config-file> <tablename> [[abcol1name=dbcol1name]..]` Class: RUNTIME_IGNORE. Meaning: DB2 loader helper: splits columns via transform.
- `db-as400-load` `mp db-as400-load <name> <db-config-file> <tablename> <control-file> [-otable <name> -controller-node <node>]  [-num_errors <num>] [-rows_per_commit <num>] [-haostname <hostname>] [-rebuild_loader] [-ifs_data_path <path>] [-ifs_dml_path <path>] -layout <layout>` Class: IO_WRITE. Meaning: Bulk-loads records into IBM i (AS/400).
- `db-netezza-load` `mp db-netezza-load <name> <db-config-file> <tablename> <control-file> [-otable <name> -controller-node <node>]  [-num_errors <num>] [-rows_per_commit <num>]  -layout <layout>` Class: IO_WRITE. Meaning: Bulk-loads records into Netezza.
- `db-netezza-unload` `mp db-netezza-unload <name> <db-config-file> <sql-select-clause> <ablocal-expression> [-itable <itable-name>]  [-max-rows-per-partition <num>] -layout <layout>` Class: IO_READ. Meaning: Unloads (extracts) rows from Netezza.
- `db-nza-extload` `mp db-nza-extload <name> <db-config-file> <tablename> <control-file> [-otable <name> -controller-node <node>]  [-num_errors <num>] [-rows_per_commit <num>]  -layout <layout>` Class: IO_WRITE. Meaning: Bulk-loads records into Netezza.
- `db-redbrick-unload` `mp db-redbrick-unload <name> <db-config-file> <sql-select-clause> <ablocal-expression> [-itable <itable-name>]  [-max-rows-per-partition <num>] -layout <layout>` Class: IO_READ. Meaning: Unloads (extracts) rows from Red Brick.
- `db-sybase-bcp-ldr` `mp db-sybase-bcp-ldr <name> <db-config-file> <tablename> [-otable <name> -controller-node <node>]  [-num_errors <num>] [-rows_per_commit <num>]  -layout <layout>` Class: IO_WRITE. Meaning: Bulk-loads records into Sybase.
- `db-sybaseiq-dbisql-load` `mp db-sybaseiq-dbisql-load <name> <db-config-file> <tablename> [-otable <name> -controller-node <node>]  [-num_errors <num>] [-rows_per_commit <num>]  -layout <layout>` Class: IO_WRITE. Meaning: Bulk-loads records into Sybase IQ.
- `db-sybaseiq-load` `mp db-sybaseiq-load <name> <db-config-file> <tablename> [-otable <name> -controller-node <node>]  [-num_errors <num>] [-rows_per_commit <num>]  -layout <layout>` Class: IO_WRITE. Meaning: Bulk-loads records into Sybase IQ.
- `db-sybaseiq-unload` `mp db-sybaseiq-unload <name> <db-config-file> <sql-select-clause> <ablocal-expression> [-itable <name> -controller-node <node>] -layout <layout>` Class: IO_READ. Meaning: Unloads (extracts) rows from Sybase IQ.
- `db-ter-unload-data` `mp db-ter-unload-data <name> <db-config-file> -select <sql-select-statement> [-layout <layout>]` Class: IO_READ. Meaning: Unloads (extracts) rows from Teradata.
- `db-ter-fast-export` `mp db-ter-fast-export <name> <db-config-file> -select <sql-select-statement> [-layout <layout>]` Class: IO_READ. Meaning: Unloads (extracts) rows from Teradata.
- `db-ter-direct-unload` `mp db-ter-direct-unload <XXX usage file to be written later>` Class: IO_READ. Meaning: Usage text not documented in mp help.
- `db-ter-fexp` `mp db-ter-fexp <XXX usage file to be written later>` Class: IO_READ. Meaning: Usage text not documented in mp help.
- `db-ter-load-data` `mp db-ter-load-data <name> <db-config-file> -select <sql-select-statement> [-layout <layout>]` Class: IO_WRITE. Meaning: Bulk-loads records into Teradata.
- `db-ter-update-data` `mp db-ter-update-data <name> <db-config-file>` Class: IO_WRITE. Meaning: Updates/upserts records in Teradata.
- `db-ter-delete-data` `mp db-ter-delete-data <name> <db-config-file> -delete <delete stmt> [-brief] [-layout <layout>]` Class: SIDE_EFFECT. Meaning: `-brief` = less output.
- `db-ter-direct-load` `mp db-ter-direct-load This component is internal and is not intended for direct use by end users.` Class: RUNTIME_IGNORE. Meaning: Bulk-loads records into Teradata (internal; not for direct use).
- `db-ter-fmload` `mp db-ter-fmload This component is internal and is not intended for direct use by end users.` Class: RUNTIME_IGNORE. Meaning: Bulk-loads records into Teradata (internal; not for direct use).
- `continuous-db-ter-fmload` `mp continuous-db-ter-fmload This component is internal and is not intended for direct use by end users.` Class: RUNTIME_IGNORE. Meaning: Teradata FastLoad/MultiLoad helper for continuous flows (internal; not for direct use).
- `db-inf-unload-data` `mp db-inf-unload-data <name> <db-config-file> -select <sql-select-statement> [-layout <layout>]` Class: IO_READ. Meaning: Unloads (extracts) rows from Informix.
- `db-inf-load-data` `mp db-inf-load-data This component is internal and is not intended for direct use by end users.` Class: RUNTIME_IGNORE. Meaning: Bulk-loads records into Informix (internal; not for direct use).
- `db-inf-insert` `mp db-inf-insert This component is internal and is not intended for direct use by end users.` Class: RUNTIME_IGNORE. Meaning: Bulk-loads records into Informix (internal; not for direct use).
- `db-inf-phlp` `mp db-inf-phlp This component is internal and is not intended for direct use by end users.` Class: RUNTIME_IGNORE. Meaning: Operates on Informix (internal; not for direct use).
- `db-inf-var2delim` `mp db-inf-var2delim This component is internal and is not intended for direct use by end users.` Class: RUNTIME_IGNORE. Meaning: Operates on Informix (internal; not for direct use).
- `db-inf-xtract` `mp db-inf-xtract This component is internal and is not intended for direct use by end users.` Class: RUNTIME_IGNORE. Meaning: Operates on Informix (internal; not for direct use).
- `db-genutil-extract` `mp db-genutil-extract <XXX usage file to be written later>` Class: IO_READ. Meaning: Usage not documented in mp help.
- `db-genutil-extract-np` `mp db-genutil-extract-np <XXX usage file to be written later>` Class: IO_READ. Meaning: Usage not documented in mp help.
- `db-genutil-insert` `mp db-genutil-insert This component is internal and is not intended for direct use by end users.` Class: RUNTIME_IGNORE. Meaning: Internal helper component (not for direct use).
- `dbi-udl-01-controller` `mp dbi-udl-01-controller This component is internal and is not intended for direct use by end users.` Class: RUNTIME_IGNORE. Meaning: Internal helper component (not for direct use).
- `dbi-udl-01-datareader-via-file` `mp dbi-udl-01-datareader-via-file This component is internal and is not intended for direct use by end users.` Class: RUNTIME_IGNORE. Meaning: Internal helper component (not for direct use).
- `dbi-udl-01-datareader-via-npipe` `mp dbi-udl-01-datareader-via-npipe This component is internal and is not intended for direct use by end users.` Class: RUNTIME_IGNORE. Meaning: Internal helper component (not for direct use).
- `db-bigquery-load-gcs-util` `mp db-bigquery-load-gcs-util <name> <db-config-file> [options]` Class: IO_WRITE. Meaning: Bulk-loads records into Google BigQuery.
- `db-bigquery-load-storage-api-stream-writer` `mp db-bigquery-load-storage-api-stream-writer <name> <db-config-file> [options]` Class: IO_WRITE. Meaning: Bulk-loads records into Google BigQuery.
- `db-bigquery-unload-gcs-util` `mp db-bigquery-unload-gcs-util <name> <db-config-file> [options]` Class: IO_READ. Meaning: Unloads (extracts) rows from Google BigQuery.
- `db-bigquery-unload-storage-api-controller` `mp db-bigquery-unload-storage-api-controller <name> <db-config-file> [options]` Class: IO_READ. Meaning: Unloads (extracts) rows from Google BigQuery.
- `db-bigquery-unload-storage-api-worker` `mp db-bigquery-unload-storage-api-worker <name> <db-config-file> [options]` Class: IO_READ. Meaning: Unloads (extracts) rows from Google BigQuery.
- `db-cassandra-load` `mp db-cassandra-load <name> <db-config-file> [options]` Class: IO_WRITE. Meaning: Bulk-loads records into Cassandra.
- `db-dynamodb-read` `mp db-dynamodb-read <name> <db-config-file> [options]` Class: IO_READ. Meaning: Reads from Amazon DynamoDB.
- `db-hbase-load` `mp db-hbase-load <name> <db-config-file> -table <tablename> ... -layout <layout>` Class: IO_WRITE. Meaning: Bulk-loads records into HBase.
- `db-hbase-bulkload` `mp db-hbase-bulkload <name> <db-config-file> [options]` Class: IO_WRITE. Meaning: Bulk-loads records into HBase.
- `db-hbase-unload` `mp db-hbase-unload <name> <db-config-file> [options]` Class: IO_READ. Meaning: Unloads (extracts) rows from HBase.
- `db-hbase-lookup` `mp db-hbase-lookup <name> <db-config-file> [options]` Class: BUSINESS. Meaning: Looks up records in HBase.
- `db-hbase-update` `mp db-hbase-update <name> <db-config-file> [options]` Class: IO_WRITE. Meaning: Updates/upserts records in HBase.
- `db-hbase-update-continuous` `mp db-hbase-update-continuous <name> <db-config-file> [options]` Class: IO_WRITE. Meaning: Updates/upserts records in HBase (continuous flow).
- `db-kudu-update` `mp db-kudu-update <name> <db-config-file> [options]` Class: IO_WRITE. Meaning: Updates/upserts records in Apache Kudu.
- `db-kudu-update-continuous` `mp db-kudu-update-continuous <name> <db-config-file> [options]` Class: IO_WRITE. Meaning: Updates/upserts records in Apache Kudu (continuous flow).
- `db-mongo-unload` `mp db-mongo-unload <name> <db-config-file> [options]` Class: IO_READ. Meaning: Unloads (extracts) rows from MongoDB.
- `db-mongo-lookup` `mp db-mongo-lookup <name> <db-config-file> [options]` Class: BUSINESS. Meaning: Looks up records in MongoDB.
- `db-mongo-update` `mp db-mongo-update <name> <db-config-file> [options]` Class: IO_WRITE. Meaning: Updates/upserts records in MongoDB.
- `db-mongo-update-continuous` `mp db-mongo-update-continuous <name> <db-config-file> [-options]` Class: IO_WRITE. Meaning: Updates/upserts records in MongoDB (continuous flow).
- `db-mongo-trunc` `mp db-mongo-trunc <name> <db-config-file> -database <db> -collection <collection>` Class: SIDE_EFFECT. Meaning: Deletes all documents from a collection in MongoDB.
- `db-doc-store-unload` `mp db-doc-store-unload <name> <db-config-file> [options]` Class: IO_READ. Meaning: Unloads (extracts) rows from a document store.
- `db-doc-store-lookup` `mp db-doc-store-lookup <name> <db-config-file> [options]` Class: BUSINESS. Meaning: Looks up records in a document store.
- `db-doc-store-update` `mp db-doc-store-update <name> <db-config-file> [options]` Class: IO_WRITE. Meaning: Updates/upserts records in a document store.
- `db-doc-store-update-continuous` `mp db-doc-store-update-continuous <name> <db-config-file> [options]` Class: IO_WRITE. Meaning: Updates/upserts records in a document store (continuous flow).
- `db-key-value-store-lookup` `mp db-key-value-store-lookup <name> <db-config-file> [options]` Class: BUSINESS. Meaning: Looks up records in a key-value store.
- `db-key-value-store-update` `mp db-key-value-store-update <name> <db-config-file> [options]` Class: IO_WRITE. Meaning: Updates/upserts records in a key-value store.
- `db-key-value-store-update-continuous` `mp db-key-value-store-update-continuous <name> <db-config-file> [options]` Class: IO_WRITE. Meaning: Updates/upserts records in a key-value store (continuous flow).
- `CDC_Oracle_XStream` `mp CDC_Oracle_XStream <name> <db-config-file> <outbound-server-name>` Class: MANUAL_REVIEW. Meaning: Captures Oracle change data (CDC) from XStream outbound server `<outbound-server-name>`.
- `dblibunisub` `mp dblibunisub <args>` Class: MANUAL_REVIEW. Meaning: Database universal subscriber.
- `dblibunisub2` `mp dblibunisub2 <args>` Class: MANUAL_REVIEW. Meaning: Database universal subscriber, version 2.

### 9.18 Queues, publish/subscribe & messaging

Ab Initio queues and external message brokers. `Batch_*` = finite read; other subscribers = continuous input.

- `publish` `mp publish <name> <Queue URL> [-publish_style (appended | file_after | queue)] [-delete_file_if_exists] [-create_empty (True | False)] [-package <package>] -layout <layout> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] [-permissions <mode>]` Class: IO_WRITE. Meaning: Publishes records to an Ab Initio queue `<Queue URL>`; `-publish_style` appended | file_after | queue.
- `multipublish` `mp multipublish <name> <queue directory> -layout <layout> [-create-empty] [-log <logevents>] [-compressed] [-varstring] [-permissions <mode>] [-package <package>]` Class: IO_WRITE. Meaning: `-varstring`, `-compressed`, `-create-empty`.
- `publish_recycle` `mp publish_recycle <name> <Recycle_Queue_URL> -layout <layout>` Class: IO_WRITE. Meaning: Recycle queue.
- `unipublish` `mp unipublish <name> <command line> -layout <layout>` Class: IO_WRITE. Meaning: Universal publisher.
- `subscribe` `mp subscribe <name> <URL> <package_file> [-id <subscriber-id>] [-more (appended | files_after | rotating | none | files_in_directory | files_after_time)] [-checkpoint-trigger (time_interval record_count infile_boundary dml_driven service)] [-checkpoint_interval <n_records>] [-wait] [-nodelete] -layout <layout> [-log <logmask>] [-limit <limit>] [-ramp <ramp>]` Class: IO_READ. Meaning: Subscribes to a queue `<URL>` with `<package_file>`; `-more` says whether more data follows (none = finite).
- `batch-subscribe` `mp batch-subscribe <name> [-infile <local_URL>] [-id <subscriber-id>] [-more (files_after | none)] [-while <dml_expression>] [-wait] [-nodelete] [-package <package>] -layout <layout> [-log <logmask>] [-limit <limit>] [-ramp <ramp>]` Class: IO_READ. Meaning: Reads a finite batch from a queue; `-while` stops when the expression becomes false.
- `unisubscribe` `mp unisubscribe <name> <package_file> [-wait_for_commit] [-poll_delay <delay in seconds>] [-checkpoint-trigger (time_interval record_count dml_driven service)] [-checkpoint_interval <n_records>] -layout <layout> [-log <logmask>] [-limit <limit>] [-ramp <ramp>]` Class: IO_READ. Meaning: `-wait_for_commit`, `-poll_delay`.
- `subscribe_recycle` `mp subscribe_recycle <name> <Recycle_Queue_URL> -layout <layout>` Class: IO_READ. Meaning: Reads from a recycle queue.
- `uniadapter` `mp uniadapter <name> <command-line>` Class: MANUAL_REVIEW. Meaning: Universal adapter command line.
- `Kafka_Publish` `mp Kafka_Publish <name> ...` Class: IO_WRITE. Meaning: Publishes records as messages to Apache Kafka.
- `Kafka_Publish2` `mp Kafka_Publish2 <name> ...` Class: IO_WRITE. Meaning: Publishes records as messages to Apache Kafka (version 2 component).
- `Kafka_Subscribe` `mp Kafka_Subscribe <name> ...` Class: IO_READ. Meaning: Continuously consumes messages from Apache Kafka.
- `Kafka_Subscribe2` `mp Kafka_Subscribe2 <name> ...` Class: IO_READ. Meaning: Continuously consumes messages from Apache Kafka (version 2 component).
- `Batch_Kafka_Subscribe` `mp Batch_Kafka_Subscribe <name> ...` Class: IO_READ. Meaning: Reads a finite batch of messages from Apache Kafka.
- `Batch_Kafka_Subscribe2` `mp Batch_Kafka_Subscribe2 <name> ...` Class: IO_READ. Meaning: Reads a finite batch of messages from Apache Kafka (version 2 component).
- `Kinesis_Publish` `mp Kinesis_Publish <name> ...` Class: IO_WRITE. Meaning: Publishes records as messages to Amazon Kinesis.
- `Kinesis_Subscribe` `mp Kinesis_Subscribe <name> ...` Class: IO_READ. Meaning: Continuously consumes messages from Amazon Kinesis.
- `Batch_Kinesis_Subscribe` `mp Batch_Kinesis_Subscribe <name> ...` Class: IO_READ. Meaning: Reads a finite batch of messages from Amazon Kinesis.
- `Pulsar_Publish` `mp Pulsar_Publish <name> ...` Class: IO_WRITE. Meaning: Publishes records as messages to Apache Pulsar.
- `Pulsar_Subscribe` `mp Pulsar_Subscribe <name> ...` Class: IO_READ. Meaning: Continuously consumes messages from Apache Pulsar.
- `Batch_Pulsar_Subscribe` `mp Batch_Pulsar_Subscribe <name> ...` Class: IO_READ. Meaning: Reads a finite batch of messages from Apache Pulsar.
- `RabbitMQ_Publish` `mp RabbitMQ_Publish <name> ...` Class: IO_WRITE. Meaning: Publishes records as messages to RabbitMQ.
- `RabbitMQ_Subscribe` `mp RabbitMQ_Subscribe <name> ...` Class: IO_READ. Meaning: Continuously consumes messages from RabbitMQ.
- `Batch_RabbitMQ_Subscribe` `mp Batch_RabbitMQ_Subscribe <name> ...` Class: IO_READ. Meaning: Reads a finite batch of messages from RabbitMQ.
- `MQ_Publish` `mp MQ_Publish <args>` Class: IO_WRITE. Meaning: Publishes records as messages to IBM MQ.
- `MQ_Subscribe` `mp MQ_Subscribe <args>` Class: IO_READ. Meaning: Continuously consumes messages from IBM MQ.
- `Batch_MQ_Subscribe` `mp Batch_MQ_Subscribe <args>` Class: IO_READ. Meaning: Reads a finite batch of messages from IBM MQ.
- `JMS_Publish` `mp JMS_Publish <args>` Class: IO_WRITE. Meaning: Publishes records as messages to a JMS provider.
- `Batch_JMS_Publish` `mp Batch_JMS_Publish <args>` Class: IO_WRITE. Meaning: Publishes records as messages to a JMS provider (batch).
- `Batch_JMS_Subscribe` `mp Batch_JMS_Subscribe <args>` Class: IO_READ. Meaning: Reads a finite batch of messages from a JMS provider.
- `MQTT_Publish` `mp MQTT_Publish <args>` Class: IO_WRITE. Meaning: Publishes records as messages to an MQTT broker.
- `MQTT_Subscribe` `mp MQTT_Subscribe <args>` Class: IO_READ. Meaning: Continuously consumes messages from an MQTT broker.
- `Google_PubSub_Publish` `mp Google_PubSub_Publish <name> ...` Class: IO_WRITE. Meaning: Publishes records as messages to Google Cloud Pub/Sub.
- `Google_PubSub_Subscribe` `mp Google_PubSub_Subscribe <name> ...` Class: IO_READ. Meaning: Continuously consumes messages from Google Cloud Pub/Sub.
- `Batch_Google_PubSub_Subscribe` `mp Batch_Google_PubSub_Subscribe <name> ...` Class: IO_READ. Meaning: Reads a finite batch of messages from Google Cloud Pub/Sub.
- `SQS_Publish` `mp SQS_Publish <name> ...` Class: IO_WRITE. Meaning: Publishes records as messages to Amazon SQS.
- `SQS_Subscribe` `mp SQS_Subscribe <name> ...` Class: IO_READ. Meaning: Continuously consumes messages from Amazon SQS.
- `Batch_SQS_Subscribe` `mp Batch_SQS_Subscribe <name> ...` Class: IO_READ. Meaning: Reads a finite batch of messages from Amazon SQS.
- `S3_Publish` `mp S3_Publish <name> ...` Class: IO_WRITE. Meaning: Publishes records as messages to Amazon S3 (as a message/object queue).
- `S3_Subscribe` `mp S3_Subscribe <name> ...` Class: IO_READ. Meaning: Continuously consumes messages from Amazon S3 (as a message/object queue).
- `Batch_S3_Subscribe` `mp Batch_S3_Subscribe <name> ...` Class: IO_READ. Meaning: Reads a finite batch of messages from Amazon S3 (as a message/object queue).
- `HTTP_Publish` `mp HTTP_Publish <name> ...` Class: IO_WRITE. Meaning: Publishes records as messages to HTTP endpoints.
- `HTTP_Subscribe` `mp HTTP_Subscribe <name> ...` Class: IO_READ. Meaning: Continuously consumes messages from HTTP endpoints.
- `UDP_Publish` `mp UDP_Publish <name> [-hostname <host>] [-port <port>] [-varstring] [-package <file>]` Class: MANUAL_REVIEW. Meaning: Publishes records as messages to UDP sockets.
- `UDP_Subscribe` `mp UDP_Subscribe <name> ...` Class: MANUAL_REVIEW. Meaning: Continuously consumes messages from UDP sockets.
- `Multicast` `mp Multicast Multicast [-multicast_IP_address <ip_addr>] [-port <port>] [-max_memory <max_mem_size_in_MB>] [-udp_block_size <udp_block_size_in_bytes>] [-max_send_speed <blocks_per_sec>] [-max_send_speed_check_period <block_count>] [-resend_period <period_in_microseconds>] [-acknowledgement_period <period_in_microseconds>] [-acknowledgement_group_size <block_count>] [-report_file <URL>] [-report_period <period_in_seconds>]` Class: MANUAL_REVIEW. Meaning: All options are network tuning.
- `SMTP_Subscribe` `mp SMTP_Subscribe <args>` Class: MANUAL_REVIEW. Meaning: Continuously consumes messages from an SMTP mail server.
- `Simulator_Publish` `mp Simulator_Publish <name> [-path <path>]` Class: RUNTIME_IGNORE. Meaning: Test simulator publisher (no production effect).
- `Batch_Cluster_Subscribe` `mp Batch_Cluster_Subscribe Batch_Cluster_Subscribe <options...>` Class: MANUAL_REVIEW. Meaning: Reads a finite batch of messages from an Ab Initio cluster queue.
- `tiled-worker-publish` `mp tiled-worker-publish <args>` Class: RUNTIME_IGNORE. Meaning: Internal tiled-execution worker.
- `tiled-worker-subscribe` `mp tiled-worker-subscribe <args>` Class: RUNTIME_IGNORE. Meaning: Internal tiled-execution worker.

### 9.19 Service calls & external programs

Components that call external services or programs per record or per run. Default class: `MANUAL_REVIEW`.

- `call-web-service` `mp call-web-service internet-connector <options...>` Class: MANUAL_REVIEW. Meaning: Calls a web service per record (internet connector).
- `continuous-call-web-service` `mp continuous-call-web-service internet-connector <options...>` Class: MANUAL_REVIEW. Meaning: Calls a web service per record in a continuous flow.
- `Call_TCP` `mp Call_TCP call-tcp <package-name> [-reject-threshold <choice>] [-ramp <ramp>] [-limit <limit>] [-concurrency-mode <choice>] [-concurrent_requests-allowed] [-concurrent-requests-max-number <nn>] [-concurrent-requests-maintain-record-order] [-save-output-frequency <nn>] [-logging] [-log_input <choice>] [-log_output <choice>] [-log_reject<choice>] [-hostname <string>] [-port <nn>] [-timeout <nn>] [-always-reconnect] [-ssl] [-ssl_cert_file <file>] [-ssl_key_file <file>] [-ssl_key_type <PEM|ASN1>] [-ssl_key_password <xxx>] [-ssl_key_password_encrypted <xxx>] [-ssl_cacert_file <file>] [-ssl_cacert_path <dir>] [-ssl_verify_cert] [-ssl_ciphers <list>] [-ssl_random_file <file>] [-ssl_random_length <nn>]` Class: MANUAL_REVIEW. Meaning: Sends each record to a TCP service defined by `<package-name>` and receives the reply; `-reject-threshold` sets failure policy.
- `SMPP_Transmit` `mp SMPP_Transmit call-tcp <package-name> [-reject-threshold <choice>] [-ramp <ramp>] [-limit <limit>] [-concurrency-mode <choice>] [-concurrent_requests-allowed] [-concurrent-requests-max-number <nn>] [-concurrent-requests-maintain-record-order] [-save-output-frequency <nn>] [-asynchronous] [-logging] [-log_input <choice>] [-log_output <choice>] [-log_reject<choice>] [-hostname <string>] [-port <nn>] [-timeout <nn>] [-always-reconnect] [-ssl] [-ssl_cert_file <file>] [-ssl_key_file <file>] [-ssl_key_type <PEM|ASN1>] [-ssl_key_password <xxx>] [-ssl_key_password_encrypted <xxx>] [-ssl_cacert_file <file>] [-ssl_cacert_path <dir>] [-ssl_verify_cert] [-ssl_ciphers <list>] [-ssl_random_file <file>] [-ssl_random_length <nn>] [-ssl_configuration_string <openssl_config_string>]` Class: MANUAL_REVIEW. Meaning: Sends SMS messages via the SMPP protocol.
- `Call_Ab_RPC` `mp Call_Ab_RPC call-ab-rpc <options...>` Class: MANUAL_REVIEW. Meaning: Calls an Ab Initio RPC service per record.
- `Call_Ab_RPC_Transaction` `mp Call_Ab_RPC_Transaction Call_Ab_RPC_Transactions <options...>` Class: MANUAL_REVIEW. Meaning: Calls an Ab Initio RPC service transactionally.
- `Call_Active_Data` `mp Call_Active_Data Call_Active_Data <options...>` Class: MANUAL_REVIEW. Meaning: Calls an Active Data service.
- `Call_Cluster` `mp Call_Cluster Call_Cluster <options...>` Class: MANUAL_REVIEW. Meaning: Calls an Ab Initio cluster service.
- `Call_MQSeries_Service` `mp Call_MQSeries_Service Call_MQSeries_Service <options...>` Class: MANUAL_REVIEW. Meaning: Request/reply call to an IBM MQ service.
- `Call_MQSeries_Service_Transaction` `mp Call_MQSeries_Service_Transaction Call_MQSeries_Service_Transaction <options...>` Class: MANUAL_REVIEW. Meaning: Transactional request/reply call to an IBM MQ service.
- `ldap-reader` `mp ldap-reader ldap-reader <host> <port> <version> <base-dn> <anonymous> <user-dn> <password> <user-dn-is-relative> <object-class> <attr-filter> <multi-valued-attributes> <referral> <deref-aliases> <timeout> <size-limit>` Class: IO_READ. Meaning: Reads entries from an LDAP directory.
- `ldap-writer` `mp ldap-writer ldap-writer <host> <port> <version> <base-dn> <anonymous> <user-dn> <password> <user-dn-is-relative> <object-class> <multi-valued-attributes> <referral> <deref-aliases> <timeout> <size-limit>` Class: IO_WRITE. Meaning: Writes entries to an LDAP directory.
- `ldap-publisher` `mp ldap-publisher ldap-publisher <host> <port> <version> <base-dn> <anonymous> <user-dn> <password> <user-dn-is-relative> <object-class> <multi-valued-attributes> <referral> <deref-aliases> <timeout> <size-limit>` Class: IO_WRITE. Meaning: Publishes entries to an LDAP directory.
- `filter` `mp filter <name> <command> [<option> ...] -layout <layout>` Class: MANUAL_REVIEW. Meaning: Runs a shell command as a record filter.
- `custom` `mp custom <name> <template> [<option> ...] -layout <layout>` Class: MANUAL_REVIEW. Meaning: Runs a custom component built from `<template>`.
- `multi` `mp multi <arg> ... -layout <layout>` Class: MANUAL_REVIEW. Meaning: Generic multi-argument component.
- `sas-run-program` `mp sas-run-program <name> <sas_file> -layout <layout>` Class: MANUAL_REVIEW. Meaning: Runs SAS program `<sas_file>`.
- `sas-run-v2` `mp sas-run-v2 sas-run-v2 <inputs> <outputs> <sasfilename> [-full_url <url>] [-limit <limit>] [-ramp <ramp>] [-memport <port> <dml_file> <dml_name>]` Class: MANUAL_REVIEW. Meaning: `-memport` passes in-memory port with DML.
- `run-micrograph` `mp run-micrograph <name> ...` Class: MANUAL_REVIEW. Meaning: Runs a micrograph (reusable subgraph) per record or request.
- `Run_DMLScript` `mp Run_DMLScript <name> ...` Class: MANUAL_REVIEW. Meaning: Runs a DML script.
- `send-mail` `mp send-mail <name> <transform> -smtp-server-host <host> [-smtp-server-port <port>] [-validate-addresses] [-on-address-error (Reject | Log | Ignore)] [-debug-file-name <debug-file-name>] [-continuous] [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout_name>` Class: SIDE_EFFECT. Meaning: `-on-address-error Reject|Log|Ignore`, `-validate-addresses`.
- `continuous-send-mail` `mp continuous-send-mail <name> <transform> -smtp-server-host <host> [-smtp-server-port <port>] [-validate-addresses] [-on-address-error (Reject | Log | Ignore)] [-debug-file-name <debug-file-name>] [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout_name>` Class: SIDE_EFFECT. Meaning: Sends e-mail per record in a continuous flow.
- `send-to-ui` `mp send-to-ui <name> [ -grab-input <bool> ] -layout <layout>` Class: RUNTIME_IGNORE. Meaning: Sends data to GDE UI (debug). `-grab-input`.
- `ws-security-encode` `mp ws-security-encode <name> <package-file> [-user-name <username>] [-password <password>] [-password-encrypted <password>] [-password-auth-type {none, plaintext, digest, plaintext-with-expiration, digest-with-expiration}] [-pretty-formatting {off, on}] [-password-digest-type {SHA-1, SHA-256}] [-client-certificate <cert-path>] [-client-certificate-type {PEM, ASN1}] [-sign-body] [-sign-usernametoken] [-sign-timestamp] [-sign certificate] [-sign-xpointer] [-xpointer-expression <xpointer-expression>] [-private-key <key-path>] [-private-key-password <password>] [-private-key-password-encrypted <password>] [-private-key-type {PEM, ASN1, Binary}] [-signature-algorithm {RSA_SHA1, HMAC-SHA1}] [-error_group <group>] [-log_group <group>] [-limit <limit>] [-ramp <ramp>] [-log <logevents>]` Class: MANUAL_REVIEW. Meaning: All key/password options are secrets.
- `ws-security-decode` `mp ws-security-decode <name> <package-file> [-verify-username-password] [-verify-timestamp] [-verify-client-certificate] [-verify-signatures] [-public-key <path-to-key>] [-public-key-type {PEM, ASN1, Binary}] [-cacert-file <cacert-file>] [-cacert-path <cacert-path>] [-error_group <group>] [-log_group <group>] [-limit <limit>] [-ramp <ramp>] [-log <logevents>]` Class: MANUAL_REVIEW. Meaning: Verifies/decodes WS-Security SOAP headers (signatures, timestamps, credentials).

### 9.20 File-system & transfer operations

Side effects outside the data stream (file system, transfers, cloud storage administration). Their position in the run (before/after which phase) matters. Default class: `SIDE_EFFECT`.

- `delfile` `mp delfile <name> <file> -layout <layout>` Class: SIDE_EFFECT. Meaning: Deletes `<file>`.
- `Delete_Multiple_Files` `mp Delete_Multiple_Files -delete <transform> [-ignore_missing] -layout <layout>` Class: SIDE_EFFECT. Meaning: Deletes files whose paths are produced by `-delete <transform>`; `-ignore_missing` tolerates absent files.
- `Rename_Multiple_Files` `mp Rename_Multiple_Files -rename <transform> -layout <layout>` Class: SIDE_EFFECT. Meaning: Renames files per `-rename <transform>`.
- `Touch_Files` `mp Touch_Files -touch <transform> -layout <layout>` Class: SIDE_EFFECT. Meaning: Creates/updates files (e.g. markers) per `-touch <transform>`.
- `Set_File_Permissions` `mp Set_File_Permissions -chmod <transform> -mode <file_permission> -layout <layout>` Class: SIDE_EFFECT. Meaning: Changes file permissions per `-chmod <transform>` to `-mode`.
- `ftp-from` `mp ftp-from <name> -rfile <remote-file> -rhost <remote-host> [-ascii | -binary] [-user <remote username>] [-pw <remote password>] [-acct <remote account password>] [-site <string>] -layout <layout>` Class: SIDE_EFFECT. Meaning: Downloads `-rfile` from `-rhost` by FTP.
- `ftp-to` `mp ftp-to <name> -rfile <remote-file> -rhost <remote-host> [-ascii | -binary] [-user <remote username>] [-pw <remote password>] [-acct <remote account password>] [-appremote] [-site <string>] -layout <layout>` Class: SIDE_EFFECT. Meaning: Uploads to `-rfile` on `-rhost` by FTP; `-appremote` appends.
- `ftp-multiple-from` `mp ftp-multiple-from <name> <transform> -skip-count <count> -read-count <count> -layout <layout> -file-empty <Fail|Notify|Ignore|Accept> -file-unavailable <Fail|Notify|Ignore> -filename-error <Fail|Notify|Ignore> -read_as_single_record -rhost <remote-host> [-ascii | -binary] [-user <remote username>] [-pw <remote password>] [-acct <remote account password>] [-site <string>] -layout <layout>` Class: SIDE_EFFECT. Meaning: Downloads several files by FTP (names from `<transform>`).
- `ftp-multiple-to` `mp ftp-multiple-to <transform> -layout <layout> [-write_raw_data] [-session-per-blip] [-session-per-transfer] [-grouped] [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -rhost <remote-host> [-ascii | -binary] [-user <remote username>] [-pw <remote password>] [-acct <remote account password>] [-site <string>]` Class: SIDE_EFFECT. Meaning: Uploads several files by FTP.
- `sftp-from` `mp sftp-from <name> -sftp|-scp -host <remote hostname> -source-file <filename to copy> [-user <remote username>] [-passphrase <password or passphrase>] [-encrypted-passphrase <encrypted password or passphrase>] [-program <path to sftp program>] [-no-passphrase|-need-passphrase] [-extra-args <extra arguments to sftp>] [-tolerate-output|-dont-tolerate-output] [-sftp-script <path of template script>]` Class: SIDE_EFFECT. Meaning: Copies `-source-file` from `-host` by SFTP/SCP.
- `sftp-to` `mp sftp-to <name> -sftp|-scp -host <remote hostname> -target-file <destination of copy> [-temp-target <temporary destination>] [-user <remote username>] [-passphrase <password or passphrase>] [-encrypted-passphrase <encrypted password or passphrase>] [-program <path to sftp program>] [-no-passphrase|-need-passphrase] [-extra-args <extra arguments to sftp>] [-tolerate-output|-dont-tolerate-output] [-sftp-script <path of template script>]` Class: SIDE_EFFECT. Meaning: Copies to `-target-file` on `-host` by SFTP/SCP; `-temp-target` uploads then renames.
- `S3_Abort_Multipart_Upload` `mp S3_Abort_Multipart_Upload <name> ...` Class: SIDE_EFFECT. Meaning: Amazon S3 management operation: abort multipart upload.
- `S3_Copy_Object` `mp S3_Copy_Object <name> ...` Class: SIDE_EFFECT. Meaning: Amazon S3 management operation: copy object.
- `S3_Create_Bucket` `mp S3_Create_Bucket <name> ...` Class: SIDE_EFFECT. Meaning: Amazon S3 management operation: create bucket.
- `S3_Delete_Bucket` `mp S3_Delete_Bucket <name> ...` Class: SIDE_EFFECT. Meaning: Amazon S3 management operation: delete bucket.
- `S3_Delete_Bucket_Tags` `mp S3_Delete_Bucket_Tags <name> ...` Class: SIDE_EFFECT. Meaning: Amazon S3 management operation: delete bucket tags.
- `S3_Delete_Object_Tags` `mp S3_Delete_Object_Tags <name> ...` Class: SIDE_EFFECT. Meaning: Amazon S3 management operation: delete object tags.
- `S3_Delete_Objects` `mp S3_Delete_Objects <name> ...` Class: SIDE_EFFECT. Meaning: Amazon S3 management operation: delete objects.
- `S3_Delete_Version` `mp S3_Delete_Version <name> ...` Class: SIDE_EFFECT. Meaning: Amazon S3 management operation: delete version.
- `S3_Get_Bucket_ACL` `mp S3_Get_Bucket_ACL <name> ...` Class: SIDE_EFFECT. Meaning: Amazon S3 management operation: get bucket acl.
- `S3_Get_Bucket_Versioning` `mp S3_Get_Bucket_Versioning <name> ...` Class: SIDE_EFFECT. Meaning: Amazon S3 management operation: get bucket versioning.
- `S3_Get_Object_ACL` `mp S3_Get_Object_ACL <name> ...` Class: SIDE_EFFECT. Meaning: Amazon S3 management operation: get object acl.
- `S3_Get_Object_Metadata` `mp S3_Get_Object_Metadata <name> ...` Class: SIDE_EFFECT. Meaning: Amazon S3 management operation: get object metadata.
- `S3_List_Bucket_Tags` `mp S3_List_Bucket_Tags <name> ...` Class: SIDE_EFFECT. Meaning: Amazon S3 management operation: list bucket tags.
- `S3_List_Buckets` `mp S3_List_Buckets <name> ...` Class: SIDE_EFFECT. Meaning: Amazon S3 management operation: list buckets.
- `S3_List_Multipart_Uploads` `mp S3_List_Multipart_Uploads <name> ...` Class: SIDE_EFFECT. Meaning: Amazon S3 management operation: list multipart uploads.
- `S3_List_Objects` `mp S3_List_Objects <name> ...` Class: SIDE_EFFECT. Meaning: Amazon S3 management operation: list objects.
- `S3_List_Versions` `mp S3_List_Versions <name> ...` Class: SIDE_EFFECT. Meaning: Amazon S3 management operation: list versions.
- `S3_Set_Bucket_Canned_ACL` `mp S3_Set_Bucket_Canned_ACL <name> ...` Class: SIDE_EFFECT. Meaning: Amazon S3 management operation: set bucket canned acl.
- `S3_Set_Bucket_Tags` `mp S3_Set_Bucket_Tags <name> ...` Class: SIDE_EFFECT. Meaning: Amazon S3 management operation: set bucket tags.
- `S3_Set_Bucket_Versioning` `mp S3_Set_Bucket_Versioning <name> ...` Class: SIDE_EFFECT. Meaning: Amazon S3 management operation: set bucket versioning.
- `S3_Set_Object_Canned_ACL` `mp S3_Set_Object_Canned_ACL <name> ...` Class: SIDE_EFFECT. Meaning: Amazon S3 management operation: set object canned acl.
- `S3_Set_Object_Tags` `mp S3_Set_Object_Tags <name> ...` Class: SIDE_EFFECT. Meaning: Amazon S3 management operation: set object tags.
- `Google_Cloud_Storage_Create_Bucket` `mp Google_Cloud_Storage_Create_Bucket <name> ...` Class: SIDE_EFFECT. Meaning: Google Cloud Storage management operation: create bucket.
- `Google_Cloud_Storage_Delete_Bucket` `mp Google_Cloud_Storage_Delete_Bucket <name> ...` Class: SIDE_EFFECT. Meaning: Google Cloud Storage management operation: delete bucket.
- `Google_Cloud_Storage_Delete_Objects` `mp Google_Cloud_Storage_Delete_Objects <name> ...` Class: SIDE_EFFECT. Meaning: Google Cloud Storage management operation: delete objects.
- `Google_Cloud_Storage_List_Buckets` `mp Google_Cloud_Storage_List_Buckets <name> ...` Class: SIDE_EFFECT. Meaning: Google Cloud Storage management operation: list buckets.
- `Google_Cloud_Storage_List_Objects` `mp Google_Cloud_Storage_List_Objects <name> ...` Class: SIDE_EFFECT. Meaning: Google Cloud Storage management operation: list objects.
- `Azure_Blob_Storage_Create_Container` `mp Azure_Blob_Storage_Create_Container <name> ...` Class: SIDE_EFFECT. Meaning: Azure Blob Storage management operation: create container.
- `Azure_Blob_Storage_Delete_Blob` `mp Azure_Blob_Storage_Delete_Blob <name> ...` Class: SIDE_EFFECT. Meaning: Azure Blob Storage management operation: delete blob.
- `Azure_Blob_Storage_Delete_Container` `mp Azure_Blob_Storage_Delete_Container <name> ...` Class: SIDE_EFFECT. Meaning: Azure Blob Storage management operation: delete container.
- `Azure_Blob_Storage_List_Blobs` `mp Azure_Blob_Storage_List_Blobs <name> ...` Class: SIDE_EFFECT. Meaning: Azure Blob Storage management operation: list blobs.
- `Azure_Blob_Storage_List_Containers` `mp Azure_Blob_Storage_List_Containers <name> ...` Class: SIDE_EFFECT. Meaning: Azure Blob Storage management operation: list containers.
- `Write_File_Datasets` `mp Write_File_Datasets -sandbox <sandbox> [-lock] [-break-locks] [-validate] [-create-directories]` Class: RUNTIME_IGNORE. Meaning: Registers file datasets in the EME/sandbox.
- `Write_Sandbox_Files` `mp Write_Sandbox_Files -sandbox <sandbox> [-lock] [-break-locks] [-create-directories]` Class: RUNTIME_IGNORE. Meaning: Writes sandbox files.
- `Write_Table_Datasets` `mp Write_Table_Datasets -sandbox <sandbox> [-lock] [-break-locks] [-validate] [-create-directories]` Class: RUNTIME_IGNORE. Meaning: Registers table datasets in the EME/sandbox.

### 9.21 Compression & encryption

Byte-stream codecs and crypto. Default class: `BUSINESS`.

- `compress` `mp compress <name> -layout <layout>` Class: DATA_MOVEMENT. Meaning: Compresses the byte stream.
- `uncompress` `mp uncompress <name> -layout <layout>` Class: DATA_MOVEMENT. Meaning: Uncompresses a compressed stream.
- `gzip` `mp gzip <name> -layout <layout>` Class: DATA_MOVEMENT. Meaning: gzip-compresses the stream.
- `gunzip` `mp gunzip <name> -layout <layout>` Class: DATA_MOVEMENT. Meaning: gzip-decompresses the stream.
- `inflate` `mp inflate <name> -layout <layout>` Class: DATA_MOVEMENT. Meaning: Inflates (zlib/deflate) a compressed stream.
- `encrypt` `mp encrypt <name> <key>` Class: BUSINESS. Meaning: `<key>` is a secret.
- `decrypt` `mp decrypt <name> <key>` Class: BUSINESS. Meaning: Decrypts the stream with `<key>`.
- `aes_encrypt` `mp aes_encrypt ...` Class: BUSINESS. Meaning: AES-encrypts data.
- `aes_decrypt` `mp aes_decrypt ...` Class: BUSINESS. Meaning: AES-decrypts data.

### 9.22 Validation, testing & diagnostics

Quality checks and debugging. Never part of business output. Default class: `VALIDATION`.

- `compute-checksum` `mp compute-checksum <name> -layout <layout>` Class: VALIDATION. Meaning: Computes checksum of the stream.
- `compare-checksums` `mp compare-checksums <name> -layout <layout>` Class: VALIDATION. Meaning: Compares checksums of flows (reconciliation).
- `compare-flows` `mp compare-flows <name> <limit> -layout <layout>` Class: VALIDATION. Meaning: Compares two flows record by record; fails beyond `<limit>` differences.
- `dump` `mp dump <name> -layout <layout>` Class: RUNTIME_IGNORE. Meaning: Dumps records for inspection (debug).
- `format-for-dump` `mp format-for-dump <name> normal| hex-fields | hex-record <domversion> [ -print-metadata ] [ -print-functions ] [ -print-internals ] [ -progress <secs> ] [ -select <expression> ] [ -start <recordnum> ] [ -records <recordnums> ] [ -attribute <name>=<value> ] [ -protocol <version> ] [ -as-xml ] [ -select-is-encoded ]  [ -mark-invalid ] [ -easy-sort ] -layout <layout>` Class: RUNTIME_IGNORE. Meaning: Formats records for dump (`-print-metadata`, `-select`, `-records` ...). (internal; not for direct use).
- `logger` `mp logger <name> <file> <starttext> <endtext> -layout <layout>` Class: RUNTIME_IGNORE. Meaning: Writes `<starttext>`/`<endtext>` to `<file>`.
- `Handle_Errors` `mp Handle_Errors <name> [-package package] [options] -layout <layout>` Class: VALIDATION. Meaning: Collects and routes error records/messages using `-package`.
- `Handle_Logs` `mp Handle_Logs <name> [options] -layout <layout>` Class: RUNTIME_IGNORE. Meaning: Collects and routes log records.
- `Analyze_Graph` `mp Analyze_Graph analyze_graph <options...>` Class: RUNTIME_IGNORE. Meaning: Analyses a graph (tooling).
- `DML_Cross_Reference` `mp DML_Cross_Reference DML_Cross_Reference <options...>` Class: RUNTIME_IGNORE. Meaning: Cross-references DML usage (lineage tooling).
- `Summarize_Lineage` `mp Summarize_Lineage Summarize_Lineage <options...>` Class: RUNTIME_IGNORE. Meaning: Summarises lineage (tooling).
- `SQL_Analysis` `mp SQL_Analysis sqlanalysis [-case {Upper | Lower | Mixed}]  [-control-dependencies {True|False}] [-dialect <dialect>] [-dialect2 <dialect>]` Class: RUNTIME_IGNORE. Meaning: Analyses SQL text for lineage (tooling).
- `Unload_Analysis` `mp Unload_Analysis unload_analysis <options...>` Class: RUNTIME_IGNORE. Meaning: Analyses unload definitions (tooling).
- `Load_Collection` `mp Load_Collection Load_Collection <options...>` Class: RUNTIME_IGNORE. Meaning: Loads an EME collection.
- `Unload_Collection` `mp Unload_Collection Unload_Collection <options...>` Class: RUNTIME_IGNORE. Meaning: Unloads an EME collection.
- `Do_All_in_Collection` `mp Do_All_in_Collection Do_All_in_Collection <options...>` Class: RUNTIME_IGNORE. Meaning: Runs an operation over an EME collection.

### 9.23 Internal runtime, continuous-flow & tiled infrastructure

Infrastructure with no business meaning. Default class: `RUNTIME_IGNORE`.

- `tiled-connector-source` `mp tiled-connector-source <cmdline>` Class: RUNTIME_IGNORE. Meaning: Internal tiled-execution runtime command (`<cmdline>`); no business meaning.
- `tiled-connector-target` `mp tiled-connector-target <cmdline>` Class: RUNTIME_IGNORE. Meaning: Internal tiled-execution runtime command (`<cmdline>`); no business meaning.
- `tiled-hook` `mp tiled-hook <cmdline>` Class: RUNTIME_IGNORE. Meaning: Internal tiled-execution runtime command (`<cmdline>`); no business meaning.
- `tiled-run` `mp tiled-run <cmdline>` Class: RUNTIME_IGNORE. Meaning: Internal tiled-execution runtime command (`<cmdline>`); no business meaning.
- `tiled-worker` `mp tiled-worker <cmdline>` Class: RUNTIME_IGNORE. Meaning: Internal tiled-execution runtime command (`<cmdline>`); no business meaning.
- `to-tiled` `mp to-tiled <cmdline>` Class: RUNTIME_IGNORE. Meaning: Internal tiled-execution runtime command (`<cmdline>`); no business meaning.
- `from-tiled` `mp from-tiled <cmdline>` Class: RUNTIME_IGNORE. Meaning: Internal tiled-execution runtime command (`<cmdline>`); no business meaning.
- `copybomb` `mp copybomb <name> copybomb` Class: RUNTIME_IGNORE. Meaning: Internal runtime utility; no business meaning.
- `revind` `mp revind This component is internal and is not intended for direct use by end users.` Class: RUNTIME_IGNORE. Meaning: Internal component.
- `continuous_sequencer` `mp continuous_sequencer continuous_sequencer <name> -layout <layout>` Class: MANUAL_REVIEW. Meaning: Sequences records/transactions in a continuous flow.
- `gather-transactions` `mp gather-transactions <name> -layout <layout>` Class: MANUAL_REVIEW. Meaning: Gathers transactional records from partitions.
- `gather-transactions-from-pool` `mp gather-transactions-from-pool <name> -layout <layout>` Class: MANUAL_REVIEW. Meaning: Gathers transactions from a worker pool.
- `Transactional_Filter` `mp Transactional_Filter <name> -layout <layout>` Class: MANUAL_REVIEW. Meaning: Filters records within transactional continuous flows.
- `Free_Shared_Variables` `mp Free_Shared_Variables <name> <package> [-limit <limit>] [-ramp <ramp>] [-log <logevents>] -layout <layout>` Class: RUNTIME_IGNORE. Meaning: Frees shared variables used by `<package>`.


---

## 10. DML BASICS & KEY SPECIFIER

`mp metadata` carries DML: `record <field definitions> end;`. Enough to read a record format:

| DML | Meaning |
|---|---|
| `string("\|") name;` | Variable-length text ending at delimiter `\|` |
| `string(10) code;` | Fixed-width text, 10 characters |
| `string("\n") last;` | Last field; newline ends the record |
| `string(integer(4)) s;` | Text prefixed by a 4-byte length |
| `decimal(12.2) amt;` / `decimal(",") id;` | Numeric text: 12 characters with 2 after the point / delimited by `,` |
| `packed decimal(9.2)` | Mainframe packed decimal |
| `integer(1\|2\|4\|8)`, `unsigned integer(4)` | Binary integers of that byte length |
| `real(4)` / `real(8)` | Binary float / double |
| `date("YYYYMMDD")`, `datetime("YYYY-MM-DD HH24:MI:SS")` | Date / timestamp in that text format |
| `void(4)` | Filler bytes |
| `record ... end sub;` | Nested record |
| `type[n]`, `type[integer(4)]` | Vector (fixed count / count prefix) |
| `union ... end` | Alternative layouts of the same bytes |
| `= NULL`, `NULL("")` | Field may be NULL; `""` represents NULL |
| `if (cond) type field;` | Conditional field |
| `ebcdic` / `utf8` / `ascii` prefixes | Character set |

### Key specifier (`<key>`, `<collate>`, `<collator>`, `-key`)

| Key | Meaning |
|---|---|
| `{cust_id}` | One key field |
| `{cust_id; txn_dt}` | Composite key, in that order |
| `{cust_id; txn_dt descending}` | Ordering per field (ascending default) |
| `{}` | Empty key: the whole input is one group |

### Common DML expressions (`-select`, `<select>`, `<function>`)

| DML | Meaning |
|---|---|
| `==`, `!=`, `<`, `>`, `and`/`&&`, `or`/`\|\|`, `not`/`!` | Comparisons and logic |
| `is_null(x)` / `is_defined(x)` / `is_blank(s)` | NULL / not NULL / empty-or-spaces |
| `if (c) a else b` | Conditional value |
| `first_defined(a, b)` | First non-NULL |
| `string_substring(s, start, len)` | Substring (1-based) |
| `string_lrtrim`, `string_upcase`, `string_downcase`, `string_concat` | Trim, upper, lower, concatenate |
| `member [vector 'A', 'B']` | In-list test |
| `lookup("LKP", key).field` | Field from a lookup file (see lookup components) |
| `now()` / `today()` | Current timestamp / date |

---

## 11. INTERPRETATION ALGORITHM

1. Pre-parse the script with shell rules (§4); collect parameters in order.
2. Split jobs: each `mp job` … `mp run` block is one job (several run in script order).
3. Note shell pre/post steps outside the job (§14) and where they sit relative to `mp run`.
4. Register definitions: layouts (serial/parallel, partition count), metadata (record format), files (direction, URLs, format, codec, write mode), includes, errors.
5. Register components: command, name, positional args by signature, options; attach the §9 entry and §8 option meanings.
6. Register ports and flows; resolve macro ports; note `-metadata` on each flow.
7. Assign phases and checkpoints (§5).
8. Build the graph: for each phase, order components from sources to targets; check every input is connected and every flow has a record format (inherit from the producer when missing).
9. Classify every component and option (§7).
10. Describe business intent per component: what it reads, filters, joins (type, keys, NULL behaviour), aggregates, deduplicates (survivor rule), routes (out/deselect/reject), writes (target, mode), and its reject threshold.
11. List side effects in execution order.
12. Answer in the §12 format; run the §16 self-check.

---

## 12. OUTPUT CONTRACT

For a single command question: give the command's signature, what each positional argument and option given means, its class, ports, and whether it affects data.

For a script, produce:

### 12.1 Parsed job (YAML, fixed key order)

```yaml
job:
  name: <mp job name>
  source_script: <file name>
  parameters:
    - {name: RUN_DT, value: null, resolved: false, used_in: [Txn_File]}
layouts:
  - {name: layout_4way, form: partitions, partitions: 4, serial: false}
metadata:
  - name: md_txn
    source: string            # string | file
    record_format: {type: delimited, field_sep: ",", record_sep: "\n"}
    fields:
      - {name: cust_id, dml_type: 'decimal(",")', nullable: false}
datasets:
  - {name: Txn_File, kind: ifile, urls: ["file://etlhost/data/serial/txn_${RUN_DT}.dat"], format: delimited, codec: none, write_mode: null, multifile: false}
components:
  - name: Join_Cust
    command: hash-join
    family: Joins
    class: BUSINESS
    phase: "0"
    layout: layout_4way
    args: {key: "{cust_id}", xfr: "/xfr/join_cust.xfr"}
    data_options: {sql_null_comparisons: false}
    runtime_options: [-max-core, -layout]
    reject_threshold: {limit: 0, ramp: 0.0}
    ports: {in0: F3, in1: F4, out: F5, reject: null}
    meaning: "Joins transactions to customers on cust_id; NULL keys match each other"
    review_required: false
    review_reason: null
flows:
  - {name: F5, type: straight-flow, from: Join_Cust.out, to: Agg_Daily.in, metadata: md_joined}
phases:
  - {name: "0", order: 0, components: [Join_Cust], checkpoint: false}
side_effects:
  - {step: "m_rm output file", timing: before_job}
unresolved:
  - {item: RUN_DT, reason: "supplied at runtime ($1)"}
```

### 12.2 Explanation (plain language)

1. One-paragraph summary of what the job produces from what.
2. Data path per phase: source → steps → target, naming components.
3. What affects the data (business rules, keys, filters, thresholds) vs what is runtime only.
4. Side effects in order.
5. Assumptions and unresolved items.

---

## 13. WORKED EXAMPLE

Illustrative wrapper (signatures from mp help; names, paths and DML are examples):

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

Line-by-line reading of the key commands:

| Line | Interpretation |
|---|---|
| `m_rm -f …cust_daily_$RUN_DT.dat` | Side effect before the job: delete the previous output |
| `mp job cust_daily_agg` | Graph name |
| `mp layout layout_4way …p0 … p3` | 4-way parallel layout (4 partitions); `layout_serial` = 1 partition |
| `mp metadata md_txn -string '…'` | Record format: comma-delimited cust_id, date txn_dt (YYYYMMDD), amount (12 chars, 2 decimals), newline-terminated |
| `mp ifile Txn_File …txn_$RUN_DT.dat` | Input dataset; `$RUN_DT` = first script argument (unresolved until run) |
| `mp phase 0` | Everything below is phase 0 (single phase) |
| `mp hash-partition Part_Txn '{cust_id}'` | Distributes transactions over 4 partitions by cust_id — data unchanged |
| `mp select-transform Filter_Valid 'amount > 0' -limit 10 -ramp 0.01` | Keeps records with amount > 0; aborts if rejects > 10 + 1% of records; `deselect` not connected → records with amount ≤ 0 are dropped; `reject` connected → bad records written to Rej_File |
| `mp hash-join Join_Cust '{cust_id}' /xfr/join_cust.xfr …` | Joins filtered transactions (in0) with customers (in1) on cust_id; no `-sql-null-comparisons` → NULL keys match; join type and output fields come from the xfr; `-max-core` is memory only; limit 0 / ramp 0 → any reject aborts |
| `mp hash-rollup Agg_Daily '{cust_id; txn_dt}' …` | One record per customer per day with aggregates from the xfr |
| `mp gather Gather_Out -layout layout_serial` | Collects the 4 partitions into one stream (serial) — order across partitions not guaranteed |
| `fan-out-flow F4` / `all-to-all-flow F2` / `fan-in-flow F6` | Partition/repartition/departition edges — runtime only |
| `mp run -report flows -report times` | Executes; reports are monitoring only |
| `status=$?` / `exit $status` | Job failure propagates to the scheduler |

Summary: from the day's transaction file and the customer file, the job writes one file of daily totals per customer (`cust_daily_<RUN_DT>.dat`) and a reject file for unparseable transactions. Runtime-only elements: layouts, partitioning, gather, `-max-core`, `-auto-max-core`, `-log`, `-report`.

---

## 14. EDGE CASES & UNKNOWN HANDLING

| Situation | Rule |
|---|---|
| **Unknown command** (not in §9) | Say it is not in mp help; class `MANUAL_REVIEW`; keep the raw line in `unresolved`; never guess |
| Signature with `...` / `<options...>` | Report options verbatim; explain only well-known ones (`-limit`, `-ramp`, `-layout`, `-log`) |
| Internal components ("not intended for direct use") | `RUNTIME_IGNORE`; mention them; if on a data path, treat as pass-through |
| Deprecated `*-transform` forms | Positional `<xfr-file> <prefix> <limit> <ramp> <log-events>`; same meaning as the modern command |
| Usage "to be written later" (`db-genutil-extract`, `db-ter-fexp`, …) | Meaning from the name only; flag as review |
| Unresolved `$VAR` / `$(...)` | Keep symbolic; list in `unresolved` |
| Several `mp job` blocks | Separate jobs, run in script order |
| Shell `if`/`case`/`for` around mp lines | Describe the condition under which each job/step runs |
| Exit status checks (`$?`, `\|\| exit 1`) | Job failure handling |
| `m_*` utilities in the wrapper | `m_rm` delete, `m_mkdir` make directory, `m_touch` create file, `m_cp` copy, `m_mv` move/rename, `m_ls` list, `m_dump` view records (debug), `m_rollback`/`m_cleanup` recovery cleanup, `m_mkfs` create multifile system, `m_eval` evaluate an expression into a value |
| `air` commands | EME/sandbox repository management — no data effect |
| Flow without `-metadata` | Format inherited from the producer port; if unknown, unresolved |
| One out port feeding several flows | The same records go to every consumer |
| Same file written then read (iofile, cross-phase) | Read happens after the earlier phase finishes |
| `mfile:` URL or several URLs | Multifile: one physical file per partition |
| Credentials in script text | Report that credentials are present; do not repeat them |
| Continuous components (`continuous-*`, subscribe with `-more` other than `none`) | The job runs as a continuous (streaming) flow |

---

## 15. COMMON MISINTERPRETATIONS

| Wrong | Right |
|---|---|
| `layout` = record schema | `layout` = where/how many partitions; schema = `metadata` (DML) |
| `-limit` = maximum rows | `-limit`/`-ramp` = reject threshold `limit + ramp × records` |
| `-ramp` = gradual scaling | `-ramp` = tolerated reject rate per record |
| `checkpoint` changes data | Recovery point only |
| Partition components change data | They change placement/order only |
| `-sql-null-comparisons` is the default | Without it NULL keys match each other |
| `dedup` keeps an arbitrary record | `-keep first\|last\|unique-only` defines the survivor |
| `concat` = `gather` | `concat` keeps input order; `gather` does not |
| `reformat -select` is a separate component | It filters inside the reformat |
| `db-truncate`, `delfile`, `db-runsql` are runtime noise | They change external state; order matters |
| `-log`, `-report`, `-max-core` affect data | Never |
| `meta_pivot` = pivot | Unpivot: one record per field as name/value |
| normalize vs denormalize | normalize = 1→N; denormalize = N→1 |
| File errors are always fatal | `-file-*`/`-input-error` choose Fail, Notify, Ignore or Accept |

---

## 16. SELF-CHECK BEFORE ANSWERING

- [ ] Every `mp` line accounted for (explained, marked runtime-only, or unresolved).
- [ ] Each component's signature matched; positional args named correctly.
- [ ] Record formats taken from `metadata`, never from `layout`.
- [ ] `-limit`/`-ramp` explained as reject thresholds.
- [ ] Connected `reject`/`deselect`/extra `outN` ports mentioned.
- [ ] Join inputs (`in0` first), keys and NULL behaviour stated; join type source stated.
- [ ] Phase order and checkpoints stated.
- [ ] Side effects listed in order.
- [ ] Runtime-only items identified as such.
- [ ] Unresolved parameters and unknown commands listed; nothing invented; no credentials repeated.

---

## 17. CHANGELOG

| Version | Change |
|---|---|
| v1 (README.md) | Conceptual overview, small parameter table; mixed in conversion guidance; errors: layout as schema, limit as row limit |
| v2 (skill_file_v1) | Command inventory (~120 commands), layout as topology; no meanings per option |
| v3.x (mp_runtime_skill_v3.md) | Full inventory with conversion mappings |
| v1.2 | Interpretation-only help: all 438 commands with exact usage, class and meaning; all 308 options; execution, ports, phases, rejects, DML basics, worked reading of a script; no target-language code |

## 18. CONVERSION WORKFLOW & PARAMETERS
 
This section defines how a copilot or LLM chat should use this skill file to convert an Ab Initio artifact.
 
### 18.1 Conversion Methods
 
| Method | Outputs | When to use |
|--------|---------|-------------|
| `code_to_doc_to_code` | **Output 1:** Technical interpretation document (§18.2) → **wait for user confirmation** → **Output 2:** PySpark code | Complex artifacts, audit trail needed, user wants to review logic before code |
| `code_to_code` | **Output 1:** PySpark code (with business summary as top comments) | Simple artifacts, fast turnaround, user already understands the artifact |
 
### 18.2 Technical Document Structure (Output 1)
 
When method = `code_to_doc_to_code`, produce these 12 sections in order:
 
| # | Section | Content |
|---|---------|---------|
| 1 | Overview | One paragraph: what the job does, reads, produces |
| 2 | Parameters & Environment | Table: all `$VAR`, sourced files, script args — resolved vs unresolved |
| 3 | Pre-job Steps | Shell commands before `mp job` — purpose and order |
| 4 | Job Definition | Name, layouts, all metadata with DML parsed into field-level tables |
| 5 | Datasets | Table: name, direction (in/out/io), URLs, format, codec, write mode, multifile |
| 6 | Component Graph | Per phase, per component in data-flow order: command, class (§7), positional args, data-affecting options (§8), runtime-only options (marked), port connections, reject threshold formula, one-line business meaning |
| 7 | Flow Map | Every flow: name, type, from-port, to-port, metadata — grouped by phase |
| 8 | Data Lineage | Source-to-target field trace where transforms/xfr are available |
| 9 | Side Effects | Ordered list of all non-data operations (deletes, truncates, SQL, mail, renames) |
| 10 | Critical Semantics | NULL join behavior, dedup survivor rules, sort-dependent logic, reject routing, conditionals |
| 11 | Unresolved Items | Unknown commands, unresolved variables, missing transforms, credentials detected |
| 12 | Conversion Notes | What maps cleanly, what needs manual work, what gets dropped |
 
### 18.3 Effort Levels
 
| Aspect | Low | Medium (default) | High |
|--------|-----|-------------------|------|
| Schema | Comments only, `inferSchema` | Full `StructType` from DML | Full `StructType` + nullability + constraints |
| Reject/deselect | Skip unless port explicitly connected | Enforce thresholds; write deselect if connected | Full enforcement + per-field DQ checks |
| External transforms (`.xfr`) | `# TODO: implement` stub | Stub with field-level mapping comments | Parse `.xfr` bodies into Column expressions |
| Error handling | Minimal | Basic try/except | Comprehensive with cleanup |
| Logging | None | `logging.info` phase start/end | Structured with counts per step |
| Reconciliation | None | None | Count + checksum source vs target |
| Config | Hardcoded paths OK | `argparse` for unresolved vars | Config-driven, zero hardcoded strings |
| Write pattern | As-is | As-is | Idempotent (overwrite/merge) for safe reruns |
| Documentation | None | Inline comments | Docstrings on every function |
 
### 18.4 Configurable Parameters
 
The user fills these in before submitting. Defaults in **bold**.
 
**Required:**
 
| Parameter | Default | Options | Description |
|-----------|---------|---------|-------------|
| Artifact name | *(required)* | free text | Filename of the Ab Initio artifact being converted |
| Artifact type | **wrapper_script** | `wrapper_script`, `graph_export`, `graph_serialized`, `mp_fragment` | Input format so the LLM knows what to expect |
| Skill file name | **mp_runtime_skill_v3.md** | any skill file name | Which attached file is the command knowledge base |
| Conversion effort | **medium** | `low`, `medium`, `high` | Controls code completeness per §18.3 |
| Conversion method | **code_to_doc_to_code** | `code_to_doc_to_code`, `code_to_code` | Two outputs (doc → code) or one output (code only) |
 
**Optional:**
 
| Parameter | Default | Options | Description |
|-----------|---------|---------|-------------|
| Target Spark version | **3.3+** | `3.3+`, `3.5+`, `4.0+` | Controls which Spark APIs are available |
| Output format | **single_module** | `single_module`, `multi_file`, `notebook` | One `.py`, separate files per phase, or Jupyter |
| Naming convention | **snake_case** | `snake_case`, `camelCase`, `original_names` | How to name functions/variables/DataFrames |
| File format handling | **auto_detect** | `auto_detect`, `force_csv`, `force_parquet`, `force_delta` | Override output file formats |
| Write mode override | **preserve_original** | `preserve_original`, `overwrite`, `merge_upsert` | Override write modes from artifact |
| Reconciliation checks | **no** | `yes`, `no` | Add count/checksum validation at end |
| Logging | **basic** | `none`, `basic`, `structured` | Logging verbosity in generated code |
| Config management | **argparse** | `argparse`, `dict`, `dataclass`, `env_vars` | How unresolved params are managed in code |
| External transform handling | **stub_with_todo** | `stub_with_todo`, `inline_if_provided`, `raise_error` | What to do with `.xfr` references |
| Reject file output | **write_if_connected** | `write_if_connected`, `always_write`, `skip` | When to write reject DataFrames |
| Additional instructions | *(empty)* | free text | Extra requirements, naming patterns, target paths, business context |
 
### 18.5 Execution Steps for the LLM
 
```text
1. Read the ENTIRE skill file — §0 Golden Rules, §7 Classification, §9 Command Catalog
2. Read the artifact top to bottom
3. Look up EVERY mp command in §9; classify per §7
4. IF method = code_to_doc_to_code → produce Output 1 (§18.2), WAIT for confirmation
5. Generate PySpark per §12 algorithm, §13 output contract, effort level (§18.3)
6. Apply optional parameters where relevant
7. Run self-check (§17) before delivering
