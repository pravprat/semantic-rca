# SCRCA v1.2.1 — Presentation deck (copy into PowerPoint)

Treat each `###` heading block as one slide (split long slides in PPT if needed).

---

## PART A — Topic 1: Introduction

### Slide A1 — Title

**SCRCA — Semantic Root Cause Analysis**  
**Version v1.2.1** (public release; aligned with internal v1.4.x capability line)

*From raw logs → structured incidents → explainable RCA and evidence*

---

### Slide A2 — Problem statement

**The problem**

- **Volume & heterogeneity:** Modern platforms emit massive, mixed logs (e.g. Kubernetes audit, API gateways, application logs). Manual review does not scale.
- **Slow RCA:** Engineers spend hours grepping, correlating by hand, and rebuilding timelines—often under incident pressure.
- **Inconsistency:** Different people interpret the same logs differently; lessons learned rarely produce repeatable artifacts.
- **Weak semantic linkage:** Keyword search misses **similar failures** phrased differently and **cross-service** patterns that embeddings and clustering can surface.
- **Stakeholder gap:** Leadership and customers need **narrative, time-bounded explanations** and **traceable evidence**, not unstructured log exports alone.

---

### Slide A3 — Solution (what SCRCA is)

**The solution — SCRCA**

- A **deterministic, multi-stage pipeline** that turns logs into **events**, **semantic clusters**, **incidents**, **causal hypotheses**, and **reports**.
- **Semantic layer:** Transformer **embeddings** group log lines by meaning, not only by exact text match.
- **Operational layer:** **Trigger analysis** scores failure-rich behavior; **incident detection** merges activity into **time windows** suitable for RCA.
- **Reasoning layer:** **Causal analysis** proposes **root candidates** and **grounds** representative failure lines from the original logs.
- **Delivery layer:** **JSON + Markdown** for humans and systems; **evidence bundles** for sharing; **assertions** for automated quality checks.

---

### Slide A4 — Why it is needed (value creation)

**Value creation**

| Dimension | Value |
|-----------|--------|
| **Speed** | Faster path from log drop to **first structured RCA narrative** and incident boundaries. |
| **Accuracy & repeatability** | Same inputs and configuration yield the **same artifact set**—better for regression and audits than ad-hoc notes. |
| **Explainability** | Conclusions tie to **clusters, time windows, and grounded log lines**—not an opaque single score. |
| **Scale** | Supports **large inputs** via profiles (e.g. Parquet), batching, clustering caps, and optional **log triage / slicing** before ingest. |
| **Enterprise fit** | Outputs designed to support **engineering (2.1)**, **customer evidence (2.2)**, and future **authorized remediation (2.3)**. |

---

## PART B — Topic 2: Roadmap — three pathways

### Slide B1 — Roadmap title

**SCRCA strategic roadmap — three pathways**

1. **2.1** Engineering support — RCA with **speed and accuracy** vs. purely manual effort.  
2. **2.2** Enterprise evidence — **Bundles** for **compliance, understanding, and trust**.  
3. **2.3** Authorized remediation — Customer **opt-in** (**Yes/No**) for **self-heal / auto-remediation** using evidence from **2.2**.

---

### Slide B2 — Pathway 2.1 (engineering RCA)

**2.1 — Engineering support: determine RCA**

- **Objective:** Reduce time and guesswork for engineers triaging production issues from logs.
- **Capabilities (today):** Incident list, **root candidate ranking**, **symptom summaries**, **grounded failure events**, Markdown reports for handoff.
- **Vs. manual:** Less reliance on one-off greps; **consistent** steps 1–10 produce comparable artifacts run-over-run for retros and comparisons.
- **Accuracy note:** Quality depends on **log coverage** and **time window**; SCRCA highlights **statistically salient** failure episodes—domain expertise still validates conclusions.

---

### Slide B3 — Pathway 2.2 (enterprise evidence)

**2.2 — Evidence bundle for enterprise customers**

- **Objective:** Give customers **reviewable packages** so they can **comply** with internal process, **understand** what was analyzed, and **trust** vendor support conclusions.
- **What goes in:** Incident metadata, **causal graph** context, **candidates**, **grounded events**, links to base and **detailed** reports, structured JSON suitable for archival.
- **Why it matters:** Supports **security reviews**, **operational governance**, and **shared audit trail** between vendor and customer—beyond a single email summary.
- **Relationship to 2.1:** Same pipeline; **2.2** emphasizes **packaging, provenance, and narrative** for external consumption.

---

### Slide B4 — Pathway 2.3 (authorized self-heal)

**2.3 — Customer-authorized self-heal / remediation**

- **Objective:** Shorten **MTTR** when customers explicitly allow automated or guided fixes.
- **Mechanism:** **Explicit authorization** in product (e.g. **Yes/No** UI)—no silent changes. Remediation plans consume **evidence and narrative from 2.2** (and underlying artifacts).
- **Governance:** Policy hooks (who can approve, change windows, rollback), audit log of **what** was approved and **what** ran.
- **Maturity:** **Evolving capability**—depends on safe integration with **runbooks**, **GitOps**, **ticketing**, and customer **change** processes.

---

## PART C — Topic 3: Pipeline — Steps 1–10 (detail per step)

### Slide C1 — Pipeline overview

**SCRCA pipeline — ten steps + validation**

| Step | Name | One-line role |
|------|------|----------------|
| 1 | Ingest | Logs → structured **events** |
| 2 | Embed | Events → **vectors** + **index** |
| 3 | Cluster | Vectors → **pattern clusters** |
| 4 | Trigger analysis | Clusters → **failure stats** & candidates |
| 5 | Incident detection | Stats → **incidents** (time-bounded) |
| 6 | Causal analysis | Incidents → **graph**, **roots**, **grounded events** |
| 7 | RCA report | Artifacts → **JSON + Markdown** |
| 8 | Evidence bundle | Pack for **forensics / sharing** |
| 9 | Detailed report | **Merged** support-first narrative |
| 10 | Assertions | **Machine-checkable** quality gates |

*Post-run: **validate** checks artifact consistency.*

---

### Slide C2 — Step 1: Ingest

**Step 1 — Ingest**

- **Purpose:** Parse raw log files into a uniform **event** representation for downstream semantics and statistics.
- **Inputs:** Log **directory**, single file, optional **`--logfile-list`**, or **`--triage`** (pre-filter failure-heavy files before ingest).
- **Processing:** Log reading → **eventization** (fields, timestamps, actors, HTTP/metadata where present) → **enrichment** (semantic hints, signatures).
- **Outputs:** **`events.jsonl`** (profile v1) or **`events.parquet`** (profile v2 — better at scale, stable schema).
- **Why it matters:** Everything downstream assumes **clean, comparable events**; gzip and batching help **large** inputs.

---

### Slide C3 — Step 2: Embed

**Step 2 — Embed**

- **Purpose:** Represent each event’s text/meaning as a **dense vector** for similarity and clustering.
- **Inputs:** Events from Step 1.
- **Processing:** Chunked batches through a **sentence-transformer** model (e.g. MiniLM-class); builds parallel **event index** aligned with vectors.
- **Outputs:** **`event_embeddings.npy`**, **`event_index.json`** or **`event_index.parquet`** (v2).
- **Why it matters:** Enables **semantic** grouping—similar failures that **don’t share identical strings** still cluster together.

---

### Slide C4 — Step 3: Cluster

**Step 3 — Cluster**

- **Purpose:** Group events into **pattern clusters** to compress noise and expose recurring behaviors.
- **Inputs:** Embeddings + events (for sizing/metadata).
- **Processing:** Optional **PCA**; **HDBSCAN** (or fallback / fast modes); optional **downsample** when event count exceeds caps; **event ↔ cluster** assignment.
- **Outputs:** **`clusters.json`**, **`event_cluster_map.json`**; cluster tags (e.g. baseline vs contextual) for interpretation.
- **Why it matters:** RCA works on **patterns**, not on millions of raw lines individually.

---

### Slide C5 — Step 4: Trigger analysis

**Step 4 — Trigger analysis**

- **Purpose:** Quantify how **failure-like** each cluster is versus a **global baseline** (rates, signals, actors, resources).
- **Inputs:** Events, cluster definitions, event–cluster map.
- **Processing:** Per-cluster aggregates: errors, **HTTP classes**, **failure hints/modes**, services, actors, resources, timing; **candidate** gating for incident seeding.
- **Outputs:** **`cluster_trigger_stats.json`**.
- **Why it matters:** Separates **routine noise** from **actionable spikes**; feeds **deterministic** incident detection.

---

### Slide C6 — Step 5: Incident detection

**Step 5 — Incident detection**

- **Purpose:** Turn high-signal cluster activity into **incidents** with **start/end** and metadata.
- **Inputs:** Trigger stats (v2 path: **stats-only** detection—no re-reading raw logs for contract hygiene).
- **Processing:** Cluster time windows, **merge** overlapping/nearby activity, **episode** rules, optional **semantic similarity** between clusters; incident **classification** (e.g. security, capacity, latency).
- **Outputs:** **`incidents.json`**, **`incident_detection_status.json`**.
- **Why it matters:** Gives RCA a **bounded scope**—the right slice of time and pattern set to explain.

---

### Slide C7 — Step 6: Causal analysis

**Step 6 — Causal analysis**

- **Purpose:** Propose **which cluster patterns** likely **drive** others and attach **log-level evidence**.
- **Inputs:** Incidents, trigger stats, event–cluster map, full events.
- **Processing:** Cluster **profiles**; **edge inference** between clusters; **root candidate** extraction and ranking; **ground** top failure-like events per candidate (**multi-signal** failure detection: HTTP, severity, hints, text).
- **Outputs:** **`incident_causal_graph.json`**, **`incident_root_candidates.json`**, **`incident_root_events.json`**.
- **Why it matters:** Bridges **statistics** to a **narrative** engineers can challenge with concrete lines from logs.

---

### Slide C8 — Step 7: RCA report

**Step 7 — RCA report**

- **Purpose:** Produce **human-readable** and **machine-readable** RCA for each incident.
- **Inputs:** Incidents, candidates, grounded root events.
- **Processing:** Pattern classification, **explanations**, confidence, blast radius, **provenance** (e.g. log-grounded vs inferred).
- **Outputs:** **`incident_rca_report.json`**, **`incident_rca_report.md`**.
- **Why it matters:** Primary **handoff** artifact for engineers and support—summary + hypothesis + next checks.

---

### Slide C9 — Step 8: Evidence bundle

**Step 8 — Evidence bundle**

- **Purpose:** Assemble a **single forensic package** tying graph, candidates, evidence, and report pointers—optimized for **sharing and audit** (pathway **2.2**).
- **Inputs:** Incidents, candidates, grounded events, causal graph, base report, events store (for lookups).
- **Outputs:** **`incident_evidence_bundle.json`**.
- **Why it matters:** Customers and security teams can **inspect the chain** from claim to data without re-running the full pipeline.

---

### Slide C10 — Step 9: Detailed report

**Step 9 — Detailed report**

- **Purpose:** **Merge** base RCA with the evidence bundle into a **support-oriented** deep view.
- **Inputs:** Step 7 report + Step 8 bundle.
- **Outputs:** **`incident_rca_report_detailed.json`**, **`incident_rca_report_detailed.md`**.
- **Why it matters:** **Long-form** context for tickets, war rooms, and customer-facing explanations while keeping JSON for tools.

---

### Slide C11 — Step 10: Assertions

**Step 10 — Incident assertions**

- **Purpose:** Emit **checkable rules** over artifacts (counts, presence, consistency) for **CI, regression, and trust**.
- **Inputs:** Incidents, candidates, root events, evidence bundle.
- **Outputs:** **`incident_assertions.json`**.
- **Why it matters:** “Did the pipeline still produce coherent RCA?” becomes **automatable**—important as inputs and code evolve.

---

### Slide C12 — Validation & operational notes

**Validation & running the pipeline**

- **Validation:** **`validate`** subcommand runs **`validation/validate_pipeline_steps.py`** — file presence, schema-ish checks, optional legacy compatibility flags.
- **No-incident path:** If Step 5 finds **no incidents**, pipeline can emit **pre-incident diagnostics** and stop—still useful signal (“nothing salient in this slice”).
- **Minimal run (conceptual):** `python main.py all <logs> --pipeline-profile v2` (plus optional **`--triage`**, **`--logfile-list`**).
- **Deployment modes:** See **Part D (offline)** and **Part E (online)** — same steps 1–10; differ only in **how logs and context are collected** and how **remediation** is triggered.

---

## PART D — Offline deployment: log analysis, RCA, and evidence

### Slide D1 — Offline mode — overview

**Offline SCRCA — what it means**

- **Definition:** Logs are **exported or copied** to disk (or object storage mounted as files); an operator or batch job runs **`main.py`** locally or in a **worker container**.
- **No live coupling** required to production log streams for the **core engine** — only **files** (or lists) that match the ingest contract.
- **Best for:** Investigations, customer log drops, air-gapped analysis, **repeatable** runs from a **fixed evidence bundle** of raw logs.
- **Outputs:** Full artifact tree under **`outputs/<profile>/`** — RCA JSON/MD, **evidence bundle**, **detailed** reports, **assertions**, **validation** reports.

---

### Slide D2 — Offline — inputs and workflow

**Offline — typical workflow**

1. **Acquire:** Download / receive logs (e.g. zip, directory, Splunk export, K8s audit extract).  
2. **Optional triage:** **`--triage`** or pre-built **`--logfile-list`** when volume is high (failure-signal scoring, top-N files).  
3. **Run:** `python main.py all …` or stepwise (ingest → … → assertions).  
4. **Store:** Archive **`outputs/`** + **triage manifest** (if used) + **git commit hash** of pipeline version (for reproducibility).  
5. **Share:** Hand off **`incident_rca_report*.md`**, **`incident_evidence_bundle.json`**, and **`incident_assertions.json`** to support or customer.

---

### Slide D3 — Offline — RCA results (what you get)

**Offline — RCA results (engineering view)**

- **`incidents.json`** — Incident IDs, **time windows**, seed/trigger clusters, coarse classification.  
- **`incident_root_candidates.json`** — Ranked **root cluster** hypotheses with scores and graph context.  
- **`incident_root_events.json`** — **Grounded** log lines tied to candidates (evidence you can quote).  
- **`incident_causal_graph.json`** — Cluster-level **edges** and narrative support for “what drove what.”  
- **`incident_rca_report.json` / `.md`** — Executive + engineer narrative: symptoms, hypothesis, **where to look next**.  
- **`incident_detection_status.json`** — Transparency into **how** incidents were formed (useful for tuning).

---

### Slide D4 — Offline — evidence for compliance and trust

**Offline — evidence packaging (pathway 2.2 ready)**

- **`incident_evidence_bundle.json`** — **Single forensic object**: incidents, graph slice, candidates, grounded events, pointers into reports; suitable for **archival** and **customer review**.  
- **`incident_rca_report_detailed.json` / `.md`** — **Merged** view: narrative + bundle context for **long-form** audits.  
- **`incident_assertions.json`** — **Pass/fail-style checks** so a third party can see **structural quality** of the run.  
- **`validation_report.json` / `.md`** — Pipeline **QA** over expected artifacts.  
- **Provenance:** Pair outputs with **input file list**, **triage manifest**, **pipeline version** (`VERSION`), and **CLI parameters** — so conclusions are **defensible**.

---

### Slide D5 — Offline — limits and good practices

**Offline — limits & practices**

- **Coverage:** RCA quality ≤ **quality of logs + time window**; missing services = missing hypotheses.  
- **Scale:** Use **v2 Parquet profile**, **triage**, and **resource caps** (`--max-cluster-events`, embed batch sizes) for large corpora.  
- **Security:** Treat raw logs and artifacts as **sensitive**; redact before external share if needed.  
- **Repeatability:** Same inputs + same version → comparable outputs; ideal for **regression** when parsers change.

---

## PART E — Online deployment: live integration and remediation

### Slide E1 — Online mode — overview

**Online SCRCA — what it means**

- **Definition:** **Event-generating systems** (APM, K8s, Fluent Bit → store, ticketing, CI/CD) **trigger** jobs that **fetch or stream** logs into the **same ingest contract**, then run **steps 1–10** automatically or on approval.
- **Extra context:** **Sub-system metadata** (service, namespace, deployment), **change events**, and optionally **`git` commits** (SHA, author, touched paths) are **correlated** with incident windows for **richer RCA** and **safer remediation**.
- **Remediation:** Not a silent step — tied to pathway **2.3**: **customer authorization** (e.g. **Yes/No** UI), policies, and audit trail; uses **evidence from 2.2** as the **plan input**.

---

### Slide E2 — Online — integration with event-generating systems

**Online — integrations (examples)**

| Source | Role |
|--------|------|
| **Log pipelines (e.g. Fluent Bit)** | Central **store** (S3, OpenSearch, vendor); **query by time + scope** into staging files for ingest. |
| **Kubernetes / control plane** | Audit and API events; **namespace / workload** scope for fetch. |
| **Ticketing / ITSM** | **Trigger** on defect/severity; pass **incident window** + IDs into orchestrator. |
| **Alerting (PagerDuty, etc.)** | **Webhook** → job queue; attach **firing time** and **labels**. |
| **CI / deployments** | **Deploy events** as **correlation anchors** (before/after comparison). |

- **Orchestrator responsibility:** Auth to log backend, **quotas**, retries, **idempotency** (`defect_id + log_hash`), artifact **upload** and **link-back** to ticket.

---

### Slide E3 — Online — sub-systems and git commits as context

**Online — sub-systems & Git correlation**

- **Sub-systems:** Map logs and clusters to **service catalog** entries (owner, repo, on-call). Feeds **“where to look”** and **remediation routing**.  
- **Git commits:** In the **incident window** (and short **pre-window**), associate **SHAs**, **changed paths**, and **release tags** with top clusters or failure modes.  
- **Uses:**  
  - **RCA:** “Failure spike aligns with **commit X** touching **component Y**.”  
  - **Risk:** Avoid auto-remediation if **high-risk paths** (auth, billing) changed without approval.  
  - **Remediation:** Suggest or open **revert PR**, **config patch**, or **runbook** step tied to **known commit**.  
- **Implementation note:** v1.2.1 **core** is log-centric; **git/CMDB** enrichment is typically a **wrapper service** that **enriches** reports or evidence JSON — same pipeline, **augmented** output.

---

### Slide E4 — Online — path to remediation (authorized)

**Online — remediation using evidence + authorization**

- **Inputs:** **`incident_evidence_bundle.json`**, detailed report, **assertions**, optional **git diff** / **deploy record**.  
- **Decision:** **Human or policy gate** → UI **Yes/No** (pathway **2.3**): “Allow SCRCA/system to execute **remediation plan** P?”  
- **Actions (examples):** Rollback deployment, scale replica, restart workload, apply **GitOps** PR, execute **Ansible/Terraform** with limits, open **change request** with **pre-filled** evidence links.  
- **Safety:** **Dry-run** mode, **blast-radius** caps, **one-shot** vs **reversible** actions, full **audit log** (who approved, what ran, outcome).  
- **Principle:** **Evidence (2.2)** is the **mandate** for **action**; no remediation without **traceable** bundle + approval.

---

### Slide E5 — Offline vs online — summary table

**Offline vs online — at a glance**

| Aspect | Offline | Online |
|--------|---------|--------|
| **Log arrival** | Files / manual export | **Query or push** from live systems |
| **Trigger** | User runs CLI | **Ticket, alert, schedule, API** |
| **Git / CMDB** | Manual notes optional | **Automated correlation** (recommended) |
| **Remediation** | Human only | **Optional authorized** automation (2.3) |
| **Same engine?** | **Yes** — steps 1–10 | **Yes** — steps 1–10 + orchestration |

---

*End of deck source — SCRCA v1.2.1 (includes offline & online deployment narratives)*
