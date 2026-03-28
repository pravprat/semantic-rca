# Release Notes: v1.4.2, v1.4.3, v1.4.4

## v1.4.2

### Highlights

- Stabilized causal inference and grounded root-event extraction for incident RCA.
- Improved deterministic report generation with coherent incident-to-candidate linkage.
- Established reliable artifact chain from ingest to base RCA report outputs.

### User Impact

- More consistent root-cause ranking and reproducible RCA artifacts.
- Better baseline for forensic expansion in subsequent releases.

---

## v1.4.3

### Highlights

- Introduced stronger pipeline validation and stage contract checking.
- Added non-HTTP failure-signal fallback to reduce zero-trigger/zero-incident false negatives.
- Improved parser reliability and graceful diagnostics for no-incident paths.

### User Impact

- Better operational confidence in pipeline health.
- Fewer brittle failures when logs are sparse on explicit HTTP error codes.

---

## v1.4.4

### Highlights

- Added post-anomaly downstream impact analysis in evidence and detailed RCA reports.
- Added support-facing `what broke what` dependency impact section:
  - source service -> downstream dependency service
  - count, first-seen timestamp, domain/system/owner hint
- Added component-level breakdown in post-anomaly failure analysis.
- Fixed status-class reporting semantics:
  - `null` for missing `response_code`
  - `unknown` reserved for non-null unclassifiable cases
- Reduced actor identity noise in parser (filters code-path/function-like actor artifacts).
- Improved validation gating:
  - Step 11 timeline remains visible, but optional for overall release pass unless explicitly required.
- Strengthened component registry:
  - added `dcn_manager`/`dcn_mgr` aliases
  - added `rke2-server` and `rke2-agent`
  - removed ambiguous bare `manager` mapping to prevent false component attribution.

### User Impact

- Stronger operator-readable reports for support routing and triage ownership.
- Clearer separation of control-plane origin vs downstream dependency impact.
- More trustworthy artifact status and release gating.

### Known Constraints

- `all.logs` remains sparse in structured `verb/path/response_code` fields versus mature K8s audit logs.
- Domain attribution quality still depends on source logging richness for non-HTTP failures.
