# Validation report — mp_runtime_skill_v3.md

Validated against `mp help.txt`: 438 distinct commands, 308 distinct options (442 usage forms).

## Score by iteration (strict rubric, pass > 85)

| Version | Score | Main issues fixed in next version |
|---|---|---|
| v1 README.md (baseline) | ~7 | layout→StructType, limit→`limit()`, no command inventory |
| v2 skill_file_v1 (baseline) | ~8 | ~120 commands listed, no semantics/PySpark per command |
| v3.0 | 89.03 | 2 broken table rows; validator bug on example check; 159 KB |
| v3.1 | 95.03 | entry text ambiguous (meaning vs PySpark); NULL-join default needed verification note |
| v3.2 final | 94.8 | — |

## Final breakdown

| Check | Points | Max |
|---|---|---|
| Base rubric (command coverage, option coverage, per-command options, 18 required sections, 15 correctness traps, entry fields, usability) | 60.0 | 60 |
| Token budget (≤110 KB full marks) | 0.8 | 6 |
| Python example compiles | 5.0 | 5 |
| YAML contract parses | 4.0 | 4 |
| Classes and § references consistent | 5 | 5 |
| Markdown tables well formed | 5.0 | 5 |
| Every signature verbatim from mp help | 5.0 | 5 |
| Example script matches mp signatures | 5.0 | 5 |
| MANUAL_REVIEW entries actionable | 5.0 | 5 |
| **Total** | **94.8** | 100 |

## Known limits

- Size: 153,342 bytes (~38K tokens). Only check below target; driven by keeping all 442 signatures verbatim.
- Worked PySpark example is syntax-checked, not executed (no PySpark in the validation environment).
- mp help gives no semantics for 120+ commands whose usage is `<name> ...`/`<options...>`; their mappings come from component names and Ab Initio conventions and are flagged as SIDE_EFFECT, MANUAL_REVIEW or stub where uncertain.
- Two semantics are inferred, not stated in mp help, and flagged for reconciliation: default NULL-key matching in joins, and phase membership by declaration order.
