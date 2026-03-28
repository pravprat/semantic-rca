# all.logs Maturity Improvement Plan (Publishable)

## Objective

Bring `all.logs` observability quality closer to mature K8s audit-style logging so RCA accuracy, explainability, and support routing improve release-over-release.

## Current Gap (Observed from Artifacts)

Comparison basis:
- Mature reference: `outputs_k8s_raw_log/`
- Current all.logs: `outputs/`

| Signal | K8s Raw | all.logs | Gap |
|---|---:|---:|---|
| service non-null | 99.9491% | 100.0% | good |
| actor non-null | 99.9491% | 100.0% | good |
| verb non-null | 100.0% | 5.7295% | major |
| path non-null | 100.0% | 8.379% | major |
| response_code non-null | 100.0% | 4.6621% | major |
| http_class non-null | 100.0% | 4.6621% | major |

Interpretation:
- Core identity fields (`service`, `actor`) are strong.
- Action/endpoint/result fields (`verb`, `path`, `response_code`) are under-instrumented in all.logs.
- This directly limits trigger precision, root attribution confidence, and service-to-service causality explanation.

## Logging Contract Required for Maturity

Adopt and enforce structured fields on all app/service logs:

- `timestamp`
- `service`
- `component`
- `team_owner` (or owner group)
- `severity`
- `verb` or `method`
- `path` or `uri`
- `response_code` (or equivalent canonical status code)
- `status_family` (`normal`, `warning`, `failure`)
- `failure_mode` (stable enum)
- `dependency_target` (service or FQDN)
- `error_code` (stable service enum)
- `trace_id` and `request_id`

## Priority Improvements

### P0 (Immediate)

- Ensure every HTTP/gRPC handler logs `method`, `path`, `statusCode`.
- Ensure every dependency call failure logs `dependency_target` (service/FQDN), timeout/error class, and status code.
- Emit explicit `failure_mode` for known categories (authz, dependency, timeout, service_failure).

### P1 (Next Sprint)

- Add `team_owner` and `system` metadata per service.
- Add structured call-site fields for webhook and control-plane interactions.
- Add CI checks for log schema completeness on critical services.

### P2 (Hardening)

- Introduce source-specific parser metrics dashboard:
  - `verb/path/response_code` coverage trend
  - unknown component/domain rate
  - dependency-edge extraction rate
- Define SLO for logging completeness by service.

## Acceptance Gates (Proposed)

For all.logs maturity progress tracking:

- `verb` non-null >= 60% (phase-1), >= 85% (phase-2)
- `path` non-null >= 50% (phase-1), >= 80% (phase-2)
- `response_code` non-null >= 50% (phase-1), >= 80% (phase-2)
- Unknown component attribution <= 15%
- Dependency-impact extraction present for top incidents in >= 80% runs

## Why This Matters for Support

With the contract above, reports can consistently answer:

- What broke first?
- What did it break downstream?
- Which team/system owns each impacted component?
- Which errors are likely causal vs collateral?

This is the shortest path from technically correct RCA to operationally actionable RCA.
