# SCRCA Run Verification Scorecard (v1.4.4)

## Run Context

- Dataset profile: `all.logs` style support bundle stream
- Outputs path: `outputs/`
- Validation mode: Step 11 optional for overall release gating

## Final Status

- Overall status: `PASS`
- Required release steps (1-10): `PASS`
- Step 11 timeline artifact: `FAIL` (non-blocking by design in `v1.4.4`)

## Artifact Completeness

- Present: `events.jsonl`, `event_index.json`, `event_embeddings.npy`
- Present: `clusters.json`, `event_cluster_map.json`, `clusters_stats.json`
- Present: `cluster_trigger_stats.json`, `incidents.json`, `incident_detection_status.json`
- Present: `incident_causal_graph.json`, `incident_root_candidates.json`, `incident_root_events.json`
- Present: `incident_rca_report.json`, `incident_rca_report_detailed.json`, `incident_rca_report_detailed.md`
- Present: `incident_evidence_bundle.json`, `incident_assertions.json`, `validation_report.json`, `validation_report.md`

## Quality Metrics (Current Run)

- Events parsed: `67824`
- Embedding rows: `67824` (dims=`384`)
- Index/embedding row match: `true`
- Clusters analyzed: `87`
- Trigger candidate clusters: `12`
- Incident decision: `incident_detected` (`incidents_count=1`)
- Top root candidate: `C24` (`candidate_score=0.808943`, `failure_domain=control_plane`)

## Data Coverage Snapshot

- `service` non-null: `100.0%`
- `actor` non-null: `100.0%`
- `verb` non-null: `5.7295%`
- `path` non-null: `8.379%`
- `response_code` non-null: `4.6621%`
- `status_family` non-null: `100.0%`

## RCA Outcome Snapshot

- Incident type: `Server / Control Plane Failure`
- First anomaly: `2025-10-24T23:50:02.790976961Z`
- Post-anomaly failures: `2726`
- Status class counts post-anomaly:
  - `2xx`: `780`
  - `5xx`: `2380`
  - `null`: `64653`
- New dependency impact section:
  - `kube-apiserver -> aide-system-milvus-operator-webhook-service` (`count=2380`)

## Assertions Summary

- Assertions: `4 pass`, `0 fail`, `0 inconclusive`
- Evidence coverage: `100%`

## Release Verdict

- `v1.4.4` run quality is acceptable for release close.
- Remaining non-blocking risk is source logging maturity (`verb/path/response_code` sparsity in `all.logs`).
