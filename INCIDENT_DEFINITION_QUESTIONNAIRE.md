# Incident Definition Questionnaire (SCRCA)

Use this document with engineering teams to standardize how incidents should be detected and classified in company logs.

---

## 1) Current vs Desired Incident Criteria

| Area | Current Criteria in SCRCA | What Else to Consider | Team Input |
|---|---|---|---|
| Primary trigger signal | `response_code >= 400` drives error_count and trigger score | Non-HTTP protocol failures (RPC/DB/queue/timeouts/exceptions) |  |
| Candidate gating | Trigger score and error_count must pass thresholds | Severity-based fallback when HTTP is absent |  |
| Incident grouping | Time-window merge of trigger clusters | Multi-signal merge rules (errors + latency + restarts) |  |
| Evidence policy | Failure-grounded rooted events preferred | Weak-signal incident class with lower confidence |  |
| Source assumptions | Strong for K8s/audit-style logs | App/platform/storage/infra logs may be sparse/non-HTTP |  |
| No-incident output | Diagnostics produced when no trigger clusters | Required scan-report fields for operational value |  |
| Exclusion policy | Limited explicit suppression today | Maintenance/deploy/startup/noise suppression |  |
| Impact modeling | Mostly technical impact | Customer-facing/business impact tags |  |

---

## 2) Incident Types to Confirm

| Incident Type | Should SCRCA detect? (Y/N) | Required Signals | Minimum Threshold |
|---|---|---|---|
| Availability outage |  | Service down, connection refused, repeated timeout |  |
| Performance/latency degradation |  | p95/p99 spike, queue lag, throttling |  |
| Correctness/data integrity |  | write/read failure, corruption, inconsistency |  |
| Security/access |  | authn/authz failures, cert/token issues |  |
| Dependency propagation |  | downstream failure causing upstream errors |  |
| Capacity/resource |  | OOM, disk pressure, evictions, restart storm |  |

---

## 3) Trigger Rule Questions

1. Should incident detection require HTTP 4xx/5xx, or can severity/error-patterns also trigger incidents?
2. Which error classes are **critical enough** to raise an incident without HTTP status?
3. What minimum duration should a condition sustain before qualifying as an incident?
4. What minimum event volume/rate should be required?
5. Should a single high-severity event ever open an incident?

---

## 4) Incident Boundary Questions

1. When should multiple abnormal clusters be merged into one incident?
2. What should be the gap/cooldown window between incidents?
3. How should cascading incidents be represented (same incident vs child incident)?
4. Should cross-service spread (blast radius) be required for incident declaration?

---

## 5) Exclusion/Noise Questions

1. Which known benign patterns should never create incidents? (e.g., startup logs, deploy churn)
2. Should maintenance windows suppress incident creation?
3. Should retry/recovery logs reduce incident confidence?
4. Which components are known noisy emitters that need custom handling?

---

## 6) Confidence and RCA Output Questions

1. What confidence level is required to declare an incident?
2. Is a “possible incident” category acceptable for weak evidence?
3. What RCA outputs are mandatory for support?
   - root cause candidate
   - evidence links
   - timeline
   - blast radius
   - remediation hints

---

## 7) Suggested Policy Template (Fill In)

| Policy Item | Decision |
|---|---|
| Incident declaration requires |  |
| Allowed trigger families |  |
| Minimum severity mix |  |
| Minimum duration |  |
| Minimum volume/rate |  |
| Merge window (seconds) |  |
| Exclusions enabled |  |
| Weak-signal mode behavior |  |
| Customer-impact tagging rule |  |

---

## 8) Final Definition Statement (Draft)

An incident is a time-bounded abnormal system condition where one or more approved failure signals exceed configured thresholds for severity, volume, duration, and impact, after applying source-specific exclusion rules.

