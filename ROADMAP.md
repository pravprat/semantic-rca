# SCRCA Roadmap and Acceptance Criteria

## Scope

This roadmap covers:
- Current RCA pipeline hardening
- Forensic/B2B readiness
- NetApp log-source onboarding
- Future intelligence capabilities


## Stage Plan

### Stage 1-7 (Current): Core RCA Stabilization
Status: **Implemented, tuned**

Objective:
- Keep deterministic RCA flow stable for K8s audit logs.

Acceptance criteria:
- [ ] All stage outputs generated without runtime error.
- [ ] Cross-artifact incident ID alignment is 100%.
- [ ] Root events contain only failure-class evidence.
- [ ] `incident_rca_report.md` includes incident window section.
- [ ] `incident_causal_graph.json` has no null actor/resource in nodes.
- [ ] `clusters_stats.json` present and populated.

Exit target:
- Overall score >= **89/100** on K8s baseline dataset.


### Stage 8: Evidence Bundle (Forensic Foundation)
Target release: **v1.5.0**

Objective:
- Make every RCA conclusion audit-traceable.

Deliverables:
- `outputs/incident_evidence_bundle.json`
- Claim-to-evidence linking in report JSON.

Acceptance criteria:
- [ ] Every `root_cause_summary` claim references event IDs and cluster IDs.
- [ ] Evidence chain includes temporal relation metadata.
- [ ] Missing evidence links per incident = 0.
- [ ] Bundle schema validation passes for all incidents.

Exit target:
- Evidence completeness >= **98%**.


### Stage 9: Assertion Engine
Target release: **v1.5.0**

Objective:
- Convert heuristic reasoning into explicit machine-checkable assertions.

Deliverables:
- `outputs/incident_assertions.json`
- Assertion status in report JSON (`pass`/`fail`/`inconclusive`).

Acceptance criteria:
- [ ] At least 3 core assertions executed per incident.
- [ ] Assertion failure reasons are human-readable.
- [ ] Assertion payload includes confidence impact.
- [ ] No silent assertion failures (all assertion execution exceptions are surfaced).

Exit target:
- Assertion execution success >= **99%**.


### Stage 10: Attribution Layer
Target release: **v1.5.1**

Objective:
- Explain probable cause category, not just root node.

Deliverables:
- `outputs/incident_attribution.json`
- Attribution fields in RCA report JSON.

Acceptance criteria:
- [ ] Top incident has non-empty attribution label.
- [ ] Attribution maps to supported taxonomy.
- [ ] Attribution includes top supporting signals.
- [ ] Unknown attribution rate below threshold for benchmark dataset.

Exit target:
- Unknown attribution <= **20%** on curated benchmark.


### Stage 11: Report Contract v2 (B2B)
Target release: **v1.5.1**

Objective:
- Produce integration-friendly forensic reports.

Deliverables:
- `incident_rca_report_v2.json` (or versioned schema in existing report)
- Optional markdown rendering from v2 JSON.

Acceptance criteria:
- [ ] v2 includes: findings, evidence chains, assertions, confidence signals, recommended actions.
- [ ] Schema version embedded (`schema_version`).
- [ ] Backward compatibility strategy documented.
- [ ] Contract tests pass for required fields.

Exit target:
- 100% required-field completeness in v2 reports.


### Stage 12: NetApp Log Source Onboarding
Target release: **v1.5.2**

Objective:
- Validate pipeline reliability beyond K8s logs.

Deliverables:
- NetApp parser/adapter
- NetApp mapping spec
- NetApp fixtures + tests
- Source-aware scorecard report

Acceptance criteria:
- [ ] Parse success rate >= **99%** for NetApp fixtures.
- [ ] Canonical event schema completeness >= **95%**.
- [ ] Semantic/signature generation success >= **98%**.
- [ ] End-to-end pipeline run succeeds with NetApp inputs.
- [ ] NetApp scorecard produced with stage-wise metrics.
- [ ] No regressions to K8s baseline score.

Exit target:
- NetApp overall score >= **82/100** initial, >= **88/100** hardening target.


### Stage 13: Coverage and Noise Reduction
Target release: **v1.5.3**

Objective:
- Reduce unmapped events and improve clustering reliability.

Deliverables:
- Parameter tuning harness (`min_cluster_size`, threshold sweeps)
- Coverage trend report.

Acceptance criteria:
- [ ] Cluster coverage >= **96%** on K8s baseline.
- [ ] Cluster coverage >= **94%** on NetApp baseline.
- [ ] False incident inflation controlled (incident count variance within expected bound).

Exit target:
- Reduce unmapped events by >= **40%** from current baseline.


### Stage 14: Feedback Loop and Learning Hooks
Target release: **v1.6.0**

Objective:
- Continuously improve ranking/attribution quality from analyst feedback.

Deliverables:
- Feedback capture schema
- Weight-tuning pipeline
- Evaluation reports over time

Acceptance criteria:
- [ ] Feedback can be attached to incident IDs and candidate IDs.
- [ ] Model/weight updates are traceable and reversible.
- [ ] Quality metrics improve across at least two evaluation windows.

Exit target:
- Measurable precision lift over static baseline.


## Governance and Release Gates

Per release gate:
- [ ] Scorecard generated for K8s and (when available) NetApp.
- [ ] No critical schema regressions.
- [ ] No incident alignment regressions across artifact chain.
- [ ] Updated docs/spec and changelog published.


## Immediate Next Sprint (Recommended)

1. Implement Stage 8 evidence bundle scaffold.
2. Implement Stage 9 assertion engine with 3 baseline assertions.
3. Start Stage 12 NetApp adapter discovery with 1-2 sample log files.
4. Add automated scorecard command (`python main.py validate_run` or equivalent script).

