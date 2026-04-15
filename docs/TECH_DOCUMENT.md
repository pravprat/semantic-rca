# SCRCA + SIP Technical Document

## 1) Purpose

This document defines the technical architecture, stage contracts, and evolution path for the SCRCA pipeline built on SIP semantic foundations.

It reflects:
- Existing implementation and observed outputs
- Current quality baseline from K8s audit logs
- Forward plan toward forensic and intelligence-grade RCA
- Multi-source log support with NetApp as the next source


## 2) Current Baseline (Validated)

Current pipeline score on K8s audit input: **90/100**

Validated strengths:
- End-to-end artifact generation is stable.
- Semantic enrichment and signature generation are consistent.
- Causal graph, root candidates, and rooted event evidence are coherent.
- Reporting includes incident window and root-cause narrative.

Known constraint:
- Cluster mapping coverage is about **93.72%** (`unmapped_events=1040/16564`).


## 3) Architecture (Current -> Target)

### SIP Platform (Semantic Foundation)
- Parsing and normalization
- Semantic extraction (`semantic` block)
- Signature generation (`signature`)
- Embedding representation
- Reusable knowledge primitives (component/domain/failure mode)

### SCRCA Core Engine (Deterministic Reasoning)
1. Event ingestion
2. Embedding
3. Clustering
4. Trigger analysis
5. Incident detection
6. Causal analysis
7. RCA reporting
8. Evidence bundle generation
9. Detailed RCA reporting (support-first + technical appendix)

Core outputs:
- Causal graph
- Root candidate ranking
- Event-grounded evidence

### Productization Tracks
- **Internal RCA (v1.4.x):** fast triage and readable reports
- **B2B / Forensic RCA (v1.5.x):** evidence chains, assertions, attribution
- **Future intelligence (v1.6+):** copilots, predictive RCA, auto-remediation hooks


## 4) Data Contracts (Current)

Primary outputs:
- `outputs/events.jsonl`
- `outputs/event_index.json`
- `outputs/clusters.json`
- `outputs/event_cluster_map.json`
- `outputs/cluster_trigger_stats.json`
- `outputs/incidents.json`
- `outputs/incident_causal_graph.json`
- `outputs/incident_root_candidates.json`
- `outputs/incident_root_events.json`
- `outputs/incident_rca_report.json`
- `outputs/incident_rca_report.md`
- `outputs/incident_evidence_bundle.json`
- `outputs/incident_rca_report_detailed.json`
- `outputs/incident_rca_report_detailed.md`
- `outputs/scorecard.json`
- `outputs/clusters_stats.json` (coverage/clustering stats)

Required operational invariants:
- `event_index.event_id` uniqueness = total index rows.
- `event_cluster_map` keys must be subset of indexed event IDs.
- All mapped cluster IDs must exist in `clusters.json`.
- Incident IDs must align across incidents/graph/candidates/roots/report outputs.
- Rooted events should represent failure-class evidence (`response_code >= 400` where available).
- Evidence bundle must include anomaly onset (`first_anomaly_timestamp`, `first_anomaly_event_id`).
- Detailed report must include support narrative and detection timeline.


## 5) Source-Specific Ingestion Strategy

### 5.1 Kubernetes (Current)

Supported format:
- K8s audit-style CSV rows (currently validated)
- JSON audit-style variants (code path exists)

### 5.2 NetApp Logs (Next)

Add a source-aware ingestion adapter layer:
- `source_type = "k8s_audit"` (existing)
- `source_type = "netapp"` (new)

NetApp onboarding approach:
1. Profile sample logs and define canonical fields.
2. Build parser adapter that maps NetApp fields into canonical event schema.
3. Add source-specific semantic extractor extensions.
4. Re-run stage gates and compare reliability metrics vs K8s baseline.

Canonical schema mapping target for NetApp:
- `timestamp`
- `actor` (user/process/system principal)
- `verb` (operation/action)
- `resource` (volume/svm/share/object/path)
- `response_code` or equivalent error/status code
- `raw_text`, `normalized_text`, `embedding_text`
- `semantic` + `signature`

If NetApp logs do not expose HTTP-style status codes:
- Add `status_family` abstraction:
  - `success`, `client_error`, `authz_error`, `server_error`, `unknown`
- Map vendor codes to canonical failure modes in `semantic/entity_extractor`.


## 6) Reliability Model

Reliability is measured at three levels:

1. **Schema reliability**
- Parse success rate
- Required fields availability
- Artifact schema completeness

2. **Pipeline reliability**
- Stage completion rate
- Cross-file consistency
- Coverage and grounding quality

3. **RCA reliability**
- Root-cause plausibility
- Evidence-chain coherence
- Assertion pass rates (future stage)

Recommended quality gates per run:
- Parse success >= 99.5%
- Cluster coverage >= 94% (short-term), >= 96% (target)
- Incident artifact alignment = 100%
- Non-failure rooted events = 0
- Null actor/resource in graph nodes = 0
- Evidence bundle anomaly-onset completeness = 100%
- Detailed report support sections present = 100%


## 7) Stage Extensions for Forensic RCA

### Stage 8: Evidence Bundle
New artifact:
- `outputs/incident_evidence_bundle.json`

Bundle content:
- Claim -> evidence links
- Event IDs and cluster lineage
- Temporal ordering proofs
- Why-selected metadata for top candidates
- Anomaly onset details (`first_anomaly_timestamp`, `first_anomaly_event_id`, `detection_rule`, `delta_to_primary_seconds`)

### Stage 9: Detailed RCA Reporting
New artifact:
- `outputs/incident_rca_report_detailed.json`
- `outputs/incident_rca_report_detailed.md` (optional for machine-only mode)

Detailed report structure:
- Support-facing narrative:
  - Incident narrative
  - Support impact summary
  - Detection timeline (first anomaly and delta to primary root event)
  - Suggested next actions
- Supplemental technical appendix:
  - Pattern ID
  - Primary cluster/event IDs
  - Causal edge counts and confidence labels

### Stage 10: Assertion Engine
New artifact:
- `outputs/incident_assertions.json`

Assertion examples:
- Root candidate precedes downstream clusters by configured threshold.
- Candidate has positive causal influence (out > in strength).
- Top candidate confidence exceeds minimum gate.

### Stage 11: Attribution Layer
New artifact:
- `outputs/incident_attribution.json`

Attribution labels:
- `rbac_policy_regression`
- `resource_missing`
- `service_unavailable`
- `dependency_failure`
- `unknown`


## 8) Non-Functional Requirements

- Deterministic stage outputs for same input and params.
- Stage-level runtime telemetry.
- Schema versioning on all major artifacts.
- Backward-compatible evolution of report schemas.
- Reproducible runs via parameter capture and environment metadata.


## 9) Testing and Validation Strategy

### Unit
- Parser normalization and semantic extraction edge cases.
- Signature determinism.
- Scoring formula sanity and monotonicity checks.

### Integration
- Full pipeline execution on golden K8s sample.
- Cross-artifact contract validation.
- Failure injection (missing fields, malformed timestamps).

### Multi-source (K8s + NetApp)
- Same quality gate suite for each source adapter.
- Compare source-specific reliability scorecards.
- Ensure RCA ranking stability under mixed-source workloads.


## 10) Deliverables for NetApp Readiness

Required implementation assets:
- `parsers/netapp_reader.py` (or adapter in existing parser)
- Source mapping spec (`docs/netapp_mapping.md`)
- Sample fixture pack (`fixtures/netapp/*.log`)
- Contract tests (`tests/test_netapp_ingest_contract.py`)
- Reliability scorecard script for K8s vs NetApp parity


## 11) Success Criteria (Program-Level)

Short term (v1.5 baseline):
- Preserve K8s score >= 90/100 while adding forensic artifacts.
- NetApp ingestion parse success >= 99%.
- NetApp run score >= 82/100 initial target.

Medium term:
- Both K8s and NetApp >= 90/100 with stable RCA quality.
- Assertion-backed reports become default for B2B mode.

Long term:
- Feedback loop improves attribution precision release-over-release.


## 12) Release Progress Through v1.4.4

### v1.4.2 (Stability Baseline)

Delivered:
- Stable causal inference and grounded root-event selection.
- Deterministic artifact chain from ingestion to RCA report.
- Reliable root-cause pattern classification baseline.

Operational value:
- Established a stable base for forensic and support-oriented extensions.

### v1.4.3 (Validation and Reliability Hardening)

Delivered:
- Expanded stage-wise validation and artifact contract checks.
- Improved handling for sparse HTTP code scenarios via fallback failure signals.
- Better no-incident and diagnostics path behavior.

Operational value:
- Reduced brittle pipeline behavior and improved run trust for operators.

### v1.4.4 (Forensic Support Readability + Parser/Registry Upgrades)

Delivered:
- Post-root impact timeline enriched with:
  - status-class counts
  - failure domain/component/service/resource breakdowns
  - pre/post degradation metrics
- Explicit downstream dependency impact extraction:
  - source service -> target dependency service
  - count, first-seen timestamp
  - target domain/system/owner hints for support routing
- Status class semantics correction:
  - `null` for missing response code
  - `unknown` only for non-null unclassifiable values
- Parser quality improvement:
  - actor sanitization to suppress code-path/function-like actor noise
- Validation policy update:
  - Step 11 remains visible but optional in overall release pass by default
- Component registry hardening:
  - added `dcn_manager`/`dcn_mgr` aliases
  - added `rke2-server`, `rke2-agent`
  - removed ambiguous bare `manager` mapping

Operational value:
- Reports now communicate "what broke what" at component/service/system level.
- Support engineers can map incidents to likely owning teams faster.

### Verified Run Snapshot (v1.4.4)

- Overall validation status: `PASS` (release-gated steps)
- Events processed: `67824`
- Incident decision: `incident_detected` (`incidents_count=1`)
- Root candidate confidence: high (`0.90813`)
- Assertions: `4 pass, 0 fail, 0 inconclusive`
- Dependency impact observed:
  - `kube-apiserver -> aide-system-milvus-operator-webhook-service` (`count=2380`)

### Companion Documents

- Run scorecard: `reports/RUN_VERIFICATION_SCORECARD_v1.4.4.md`
- Release notes: `reports/RELEASE_NOTES_v1.4.2_to_v1.4.4.md`
- all.logs maturity plan: `reports/ALL_LOGS_MATURITY_IMPROVEMENT_PLAN.md`

