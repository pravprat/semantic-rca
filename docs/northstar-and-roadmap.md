# Northstar and roadmap

**Single canonical planning doc** for SCRCA: **Part I** is product **vision** (outcomes, goals, operating models, gaps, phases 0–6) plus **product milestones** (executable work that often **lives outside** this repo—e.g. alert → RCA). **Part II** is **execution in this repository**—`main.py` **pipeline steps 1–11**, **stages 1–15** (Stage **15** = **planned** baseline/contrast), acceptance criteria, release gates, **trust/release modes**, and **scorecard composition**. Phases and stages are linked in a **traceability matrix** so “what we build next” is not ambiguous.

**Living document** — update Part II checkboxes and “last reviewed” as the codebase changes.

> **Pipeline step numbers** (1–11 in Part II) ≠ **roadmap stage numbers** (e.g. Stage 9 vs step 9). Example: **pipeline step 11** = `validate`; **roadmap Stage 11** = B2B report contract v2.

---

## Part I — Product vision

### Northstar objective *(value creation)*

**Objective:** Turn raw operational signals into **defensible, reusable diagnoses** so organizations **spend less time searching logs** and **more time fixing the right thing**—with evidence that stands up to support, engineering, and audit, and a path (where policy allows) from diagnosis to **governed** action.

**Value created**

| For… | Value |
|------|--------|
| **Support & SRE** | Faster time to a **structured RCA** (narrative + artifacts), fewer dead ends in unstructured logs. |
| **The business** | Shorter incident drag, fewer **reopened** “we guessed wrong” tickets, **shareable** forensic packages for customers and compliance. |
| **The product** | One engine for **offline** and **online**; rising bar on **trust** (evidence, assertions) before **automation** (remediation). |

---

### Metrics *(supporting the objective)*

These metrics tie the Northstar objective to **measurable** outcomes. **Offline / engine** targets are anchored in Part II where stages exist; **online / product** targets apply once **Milestone P1** is in scope. **Promotion** of claims (demo vs production vs automation) follows **gates G0–G4** in Part II **Trust model**. Baselines and exact definitions live in the **scorecard** (Part II); refresh this table when targets change.

| Pillar | Metric | Target / direction | Primary anchor |
|--------|--------|-------------------|----------------|
| **Speed** | Time from **signal** to **structured RCA package** (reports + bundle + validation) | **Online:** product SLO toward **&lt;2 min** to first usable diagnosis (Phase 1). **Offline:** wall-clock and human effort on fixed **benchmark** runs. | Part I Phase **1**; Part II pipeline + `validate` |
| **Speed** | Engine quality / throughput on **K8s baseline** | **Overall score ≥89/100** (Part II Stage 1–7 exit target) until scorecard is revised. | Part II Stage 1–7 |
| **Trust** | **Evidence completeness** (claims linked to events/clusters, temporal metadata) | **≥ 98%** (Part II Stage 8); **0** missing evidence links per incident at exit. | Part II Stage 8 |
| **Trust** | **Validation** clean on baseline runs | **Strict mode:** step **11** `validate` passes with **no undocumented waivers** on the **golden path** (see **Trust model** in Part II). | Part II pipeline step 11; **Trust model** |
| **Trust** | **Assertion** reliability | **≥ 99%** assertion execution success; **≥ 3** core assertions per incident; statuses surfaced in customer JSON/MD (Part II Stage 9). | Part II Stage 9 |
| **Action** | **Attribution** (probable-cause taxonomy) coverage | **Unknown attribution ≤ 20%** on curated benchmark (Part II Stage 10). | Part II Stage 10 |
| **Action** | **Ownership / fix localization** | Tracked when Phase 3 product feeds exist (service/repo/on-call linkage)—**define scorecard** when in scope. | Part I Phase **3** |
| **Safety** | **Human-in-the-loop** before automated change | **100%** of automated actions behind policy (approval, blast-radius, audit)—**define per product**; engine today **recommends only**. | Part I **Goal 3**; Phases **4–5** |
| **Learning** | **Feedback → model/playbook** improvement | **Measurable precision lift** vs static baseline over **≥ 2** evaluation windows (Part II Stage 14). | Part II Stage 14 |

---

### Vision (one line)

Move from **“engineers read logs to find what broke”** to **“alerts and evidence become trusted diagnoses, then safe fixes—with humans in the loop until we earn full autonomy.”**

---

### North-star outcomes

| Outcome | What “good” looks like |
|--------|-------------------------|
| **Speed** | Minutes from signal to structured RCA, not hours of manual search |
| **Trust** | Conclusions tied to evidence, baselines, and clear “what changed” |
| **Action** | Owners, repos, and ranked fix paths—not just symptoms |
| **Safety** | Approvals, blast radius, audit trail before any automated change |
| **Learning** | Every incident improves models, playbooks, and baselines |

---

### 1. Product vision — three strategic goals

We are building a platform that turns **logs and operational signals** into **trusted, actionable outcomes** for support and SRE—starting with **offline** investigations and extending to **online**, **alert-driven** workflows and, eventually, **governed remediation**.

| # | Goal | Outcome for the business |
|---|------|---------------------------|
| **1** | **Engineering-based RCA** | Engineers and support get **fast, explainable root-cause analysis** from logs—**offline** (files, exports, triaged slices) and **online** (e.g. **EMS/alert-anchored** windows, integrated log backends). Same core engine; different **trigger and context**. |
| **2** | **Evidence & forensic bundle (enterprise)** | Customers and auditors get **packaged, traceable evidence**—not only a narrative—so they can **trust** our conclusions, **review** the chain from claim to log line, and **meet compliance** expectations (archival, provenance, structured exports). |
| **3** | **Automated remediation (with human-in-the-loop)** | Where policy allows, move from **diagnosis** to **safe action**: ranked fix hypotheses → **human approval** → **assisted** execution (PR, rollback, restart) → over time, **selective autonomy** for low-risk, reversible actions—with **blast radius**, **audit**, and **rollback**. |

#### Principles

- **One RCA engine** — ingest → embed → cluster → triggers → incidents → causal → reporting; **orchestration** and **product** layers wrap it for online and remediation.  
- **Offline stays first-class** — repeatability, customer log drops, air-gap, regression testing.  
- **Trust before autonomy** — forensic quality and **enforced** trust gates (Part II) before wider **product** phases (online at scale, ownership routing, remediation).

**Trust is not only measured—it is gated for certain claims:** See Part II **Trust model and release modes**. “Trust before autonomy” is **weak** if validation **waivers** are routine without a defined **strict** path for production and GTM.

---

### 2. Operating models: offline and online

The **same core RCA engine** (ingest → embed → cluster → triggers → incidents → causal → reports → evidence → assertions) runs in both modes. What differs is **how logs and context arrive**, **how jobs are triggered**, and **how far toward remediation** you go in product.

#### Offline (file-first, investigation-first)

| Aspect | Description |
|--------|-------------|
| **Definition** | Logs are **exported or copied** to disk/object storage as files; operators or batch jobs run the pipeline on demand |
| **Trigger** | Human request, ticket attachment, customer log drop, scheduled batch, air-gapped analysis |
| **Strengths** | Full control, **repeatable** runs, easy **regression** on fixed inputs, no production coupling for the core engine |
| **Typical use** | Post-incident review, customer support packages, compliance archives, development & benchmarking |
| **Outputs** | RCA JSON/MD, **evidence bundle**, detailed reports, assertions — packaged for **review and sharing** |

**PM framing:** Offline is **“bring your logs, get a defensible RCA package.”** It supports engineering RCA (**Goal 1**) and evidence packaging (**Goal 2**); remediation (**Goal 3**) is usually manual execution from playbooks.

#### Online (integration-first, incident-first)

| Aspect | Description |
|--------|-------------|
| **Definition** | **Event-generating systems** (EMS/alerting, K8s, log pipelines such as Fluent Bit → store, ITSM, CI/CD) **trigger** jobs that **query or receive** logs into the same ingest contract, then run the pipeline |
| **Trigger** | Alert webhook, ticket created, API, schedule — anchored on **service, time, severity** |
| **Context** | Optional enrichment: **dependency hints**, **deploy/change events**, **Git commits**, service catalog / ownership |
| **Strengths** | **Alert → RCA in minutes**, continuous alignment with production reality |
| **Typical use** | On-call, SRE workflows, proactive support, eventual assisted/autonomous remediation |
| **Outputs** | Same artifacts as offline, plus **links to tickets/alerts**, correlation IDs, and (later) **remediation proposals** |

**PM framing:** Online is **“something fired → we narrow the window → we diagnose (and later fix) with governance.”** Natural home for **Goal 1** (EMS-anchored RCA), enriched **Goal 2** (ticket-linked evidence), and **Goal 3** (approval-gated actions).

#### Offline vs online — at a glance

| Dimension | Offline | Online |
|-----------|---------|--------|
| **Log arrival** | Files / exports | Query or push from live systems |
| **Anchor** | User-chosen path & window | **Alert / ticket / API** defines window |
| **Enrichment** | Manual notes optional | **Automated** (deps, deploys, Git, CMDB) — recommended |
| **Remediation** | Human-only typical | **Assisted → selective autonomous** (when Goal 3 is productized) |
| **Same engine?** | **Yes** | **Yes** — plus **orchestration** around it |

#### Offline vs online — engineering tension *(not cosmetic)*

The **same** code path can behave **differently** in production than on a fat offline export: **partial windows**, **incomplete logs**, **latency budgets**, and **noisy** anchors change cluster/incident behavior. **Do not** assume offline benchmark excellence **implies** online SLO compliance without a **separate online golden path** and scorecard slice.

| Dimension | Offline (typical) | Online (typical) |
|-----------|-------------------|------------------|
| **Data** | Full or chosen export | Windowed, may be incomplete |
| **Latency** | Batch acceptable | **SLO-bound** (e.g. Phase 1) |
| **Determinism** | Easier to reproduce | More variance run-to-run |
| **Scorecard** | K8s/file baseline | **Plus** online golden path when Phase 1 ships |

**Implication:** Maintain **two** explicit tracks in the scorecard until online is proven: **engine (offline) quality** and **product (online) reliability**.

---

### 3. What the codebase already delivers *(built)*

The **semantic-rca** line implements a **full, deterministic pipeline** via `main.py`: **`PIPELINE_STEPS` = steps 1–10**, then **step 11 — `validate`** runs after `all` (or via the `validate` subcommand), plus **no-incident** diagnostics when step 5 finds nothing.

#### 3.1 Pipeline steps (summary)

**Full CLI labels and artifact filenames:** [Pipeline steps (`main.py`)](#pipeline-steps-mainpy) in Part II.

| Steps | What runs |
|-------|-----------|
| **1–2** | Ingest / normalize, embed, build event index |
| **3–5** | Cluster, trigger stats, incident detection |
| **6–7** | Causal graph and root candidates, primary RCA reports |
| **8–10** | Evidence bundle, detailed report, incident assertions |
| **11** | **`validate`** (post-run checks) |

If step **5** finds **no incidents**, steps **6–10** are skipped; **pre-incident diagnostics** run, then **step 11**. **Roadmap stage numbers** (e.g. Stage 9) are **not** the same as **pipeline step numbers** — see Part II callouts.

**PM takeaway:** The **offline engine** and forensic outputs through **validation** exist in-repo. Remaining vision gaps are often **wrapping** that engine (online orchestration, enterprise contract polish, remediation) — see §4 and Part II **[Vision ↔ repository stages](#vision--repository-stages)**.

#### 3.2 Operating mode in the repo today

- **Offline-first:** `python main.py all …` (or stepwise) on local/mounted logs.  
- **Online:** **Orchestration** (EMS → fetch logs → call pipeline) is **outside** this repo; the engine is ready to be **invoked** by that layer.

---

### 4. What needs to be built — gap to full vision

Grouped under the **same three goals** as §1.

#### Goal 1 — Engineering-based RCA *(offline + online, EMS)*

**Already have:** Full **offline** RCA (steps **1–11** including `validate`), triage, incident detection v2, multi-signal triggers, component-aware reporting.

**Still to build / productize**

| Area | Gap | Where tracked |
|------|-----|----------------|
| **Online orchestration** | Service: **EMS/alert** → log fetch + window policy → invoke `main.py` (or library)—**&lt;2 min RCA** as a product SLO. | **Milestone P1** (Part I **Product milestones**) — executable **outside** this repo; **invokes** Part II pipeline **1–11**. |
| **Context window policy** | Automated **T−Δ → T+Δ**, dependency expansion, baseline slice—**config-driven**. | **Milestone P1** (same). |
| **Baseline / contrast** | Productized **vs last good / rolling baseline** narrative (“what changed” / trust). | **Milestone P2** + planned **Stage 15** (Part II). |
| **Multi-source parity** | e.g. **NetApp** adapters. | **Stage 12** (Part II). |
| **Ownership & localization** | Service catalog / **Git** wrapper around the pipeline. | **Phase 3** (§5) — not a Part II stage yet. |

---

#### Goal 2 — Evidence & forensic bundle *(enterprise trust & compliance)*

**Already have:** `incident_evidence_bundle.json`, detailed reports, assertions, provenance in reporting, validation, JSON + Markdown.

**Still to build / harden**

| Area | Gap | Where tracked |
|------|-----|----------------|
| **B2B report contract** | Versioned report schema, `schema_version`, contract tests. | **Stage 11** (Part II; **not** pipeline step 11 `validate`). |
| **Attribution layer** | `incident_attribution.json` + taxonomy — **bridge from RCA → action** (routing, policies, learning). | **Stage 10** (Part II); **blocks G3** until credible (see **Trust model**). |
| **Evidence completeness** | Claim → event/cluster chains, temporal metadata, zero-missing-link targets. | **Stage 8** (Part II). |
| **Compliance packaging** | Retention labels, export formats, redaction hooks, audit manifest (who ran, inputs, version). | Vision only until scoped — **no** Part II stage; add when we split product vs repo. |
| **Assertion depth** | More assertions, failure semantics. | **Stage 9** (Part II; maps to **pipeline step 10** `incident_assertions`). |

---

#### Goal 3 — Automated remediation *(+ human-in-the-loop)*

**Already have:** Hypotheses and “where to look next”; evidence for human decisions. **No** automated execution in-repo.

**Still to build**

| Area | Gap | Where tracked |
|------|-----|----------------|
| **Hypothesis → action** | Remediation candidates with **risk** and **reversibility**. | **Phases 3–4**; **Milestones P3–P4** — see **Remediation prerequisites**; **not** defined as repo stages yet. |
| **Human-in-the-loop** | Approval UI/API, blast-radius preview, dry-run, audit log. | **Phase 4**; **Milestone P4**. |
| **Assisted execution** | Git PR, deploy, K8s, runbooks—one action behind approval. | **Phase 4**; **Milestone P4**. |
| **Selective autonomy** | Policy engine, auto-rollback, verify step, post-mortem artifact. | **Phase 5**; **Milestone P5**. |
| **Learning loop** | Fix-outcome feedback → weights/playbooks. | **Stage 14** (Part II); full closed loop also spans **Phase 6** / product. |

---

### 5. Strategic journey — phases 0–6 *(product evolution)*

These phases describe **how** we mature over time. They align with the **three goals** and **offline vs online** (§2); they complement §3–4 (“built” vs “gaps”).

**Phases vs Part II stages:** Phase **0** and parts of **6** line up with in-repo **stages** (see **[Vision ↔ repository stages](#vision--repository-stages)**). Phases **1–5** are **mostly product and orchestration** around the engine until we add matching stages.

#### Phase 0 — Foundation *(complete)*

**Status:** Done — matches §3 (pipeline through validation).

**User story:** *“A support engineer reads logs and finds what broke.”*

**Offline:** Exported logs; archive outputs + provenance.  
**Online:** Same pipeline once logs for a window are **materialized** to the ingest contract.

---

#### Phase 1 — EMS-triggered autonomous RCA *(next realistic product step)*

**Goal:** **Alert → root cause identified** without humans reading raw logs first — **diagnosis only** (no fixes). Aligns with **Goal 1** online.

**What we add:** EMS as **anchor** (service, time, severity); **automatic context window**; targeted pipeline run; **baseline** logs as reference where useful.

**Deliverable:** **“Alert → RCA in &lt; 2 minutes”** — root clusters, evidence logs, change vs baseline explanation.

**Offline equivalent:** Operator supplies **time range + scope** (or file list); same windowing, no EMS.  
**Online:** EMS drives window selection and orchestration end-to-end.

---

#### Phase 2 — Change detection & regression reasoning

**Goal:** **“This broke because X changed since last good”** — deepens **Trust** and **Goal 2**.

**What we add:** Healthy reference (last-known-good or rolling baseline); **temporal contrast** (appeared / increased / vanished); **root-cause class** (config, dependency, capacity, code path, …).

**Offline:** Incident export vs **saved baseline** export.  
**Online:** Baselines from scheduled/continuous reference; contrast **per alert**.

**Executable track:** **Milestone P2** (Part I, below) + **Stage 15** (Part II, planned)—so Phase 2 is **not** only “discovery” on paper.

---

#### Phase 3 — Ownership & fix localization

**Goal:** **Diagnosis → actionability** — who owns it, where in code/config, what to try first. Bridges **Goal 1** and **Goal 3**.

**What we add:** Cluster/issue → service, repo, config owner, on-call; log → code/config hints; **ranked fix hypotheses** (human-approved before action).

**Offline:** Ownership bundled in evidence package; hypotheses **recommendations only**.  
**Online:** CMDB/Git feeds hypotheses; links to **ticket/alert**.

---

#### Phase 4 — Assisted remediation *(human-in-the-loop)*

**Goal:** **“Click to apply fix”** with approvals, blast-radius, audit — **Goal 3**.

**Examples:** PR with config change, rollback, safe restart, workaround.

**Offline:** Usually **playbooks + evidence** only.  
**Online:** **proposal → approve → execute → verify**; evidence bundle as **mandate for action**.

---

#### Phase 5 — Selective full autonomy

**Goal:** **Low-risk, reversible** automation only; verify and rollback — **Goal 3** mature.

**Offline:** Simulate or recommend only.  
**Online:** Production automation with safeguards; post-mortem artifacts.

---

#### Remediation *(Goal 3) — design prerequisites*

Phases **4–5** are **not implementable** as engineering backlog until we define a **unit of action** and **failure/verify** semantics. Today this doc states **vision**, not **system design**.

**Must be specified before build:**

| Open design item | Why it matters |
|------------------|----------------|
| **Unit of action** | Is the smallest executable step a **K8s rollout restart**, **Git revert**, **config apply**, **runbook step**, **ticket-only** recommendation? Policies differ per customer. |
| **Execution interface** | Which **adapters** (cluster API, CI, Git provider, ITSM) and how **idempotency** and **timeouts** work. |
| **Blast radius & rollback** | How we **model** impact, **enforce** rollback, and **record** evidence of verify. |
| **Verification loop** | What **proves** the action worked (metrics, health checks, log silence window)—ties to Phase **2** contrast in production. |

**Dependency:** Meaningful **attribution** (Part II **Stage 10**) and usually **ownership** (Phase **3**) are **prerequisites** for safe routing and policy—not optional polish.

---

#### Phase 6 — Learning loop *(self-improving SRE)*

**Goal:** **Did it work?** Update weights, models, playbooks, baselines — **Learning** outcome.

**Offline:** Replay historical incidents; evaluate before rollout.  
**Online:** Closed-loop improvement from live feedback.

---

#### How to read phases vs goals

| Phases | Focus |
|--------|--------|
| **0–1** | **Speed to answer** (offline capable; online productized) |
| **2** | **Trust** (contrast & regression) |
| **3** | **Actionability** (ownership & hypotheses) |
| **4–5** | **Execution** (assisted → selective autonomous) — **online-first** |
| **6** | **Compounding value** — offline eval + online learning |

---

### Product milestones — executable track *(Phase 1 is product, not “just a wrapper”)*

**Problem this fixes:** Phase **1** (“alert → RCA in &lt;2 min”) is the **primary SRE entry point**. If it exists **only** as narrative in Part I with **no** milestone checklist, the **first sellable product slice** is **undefined in execution terms**. This section is the **product-side counterpart** to Part II **stages**: same planning doc, **different artifact surface** (service, APIs, runbooks—not only `main.py`).

| Milestone | Part I phase | What it is | Primary owner *(typical)* |
|-----------|--------------|------------|---------------------------|
| **P1** | Phase **1** | **Online RCA:** EMS/alert → window policy → log materialization → **invoke engine** → artifacts + ticket/alert link | Product / platform (service **outside** this repo **or** new repo) |
| **P2** | Phase **2** | Baseline capture + **contrast** in product UX and/or **new engine outputs** | Product + engine (may need **future repo stage** for contrast artifacts) |
| **P3** | Phase **3** | Ownership & localization feeds → **actionability** in reports | Product + data (CMDB/Git) |
| **P4–P5** | Phases **4–5** | Remediation proposals → approval → execution (see **Remediation prerequisites** above) | Product + governance |
| **P6** | Phase **6** | Closed-loop learning (pairs with Part II **Stage 14**) | Product + ML/engine |

#### Milestone **P1** — acceptance *(draft; refine with PM/eng)*

**Hard dependencies on this repo (Part II):** pipeline **1–11** callable with a **stable inputs contract** (ingest profile, paths/correlation IDs); **Stages 1–7** within agreed score band; **Stages 8–9** strongly recommended before claiming **enterprise** online RCA; **Stage 11** `validate` on the **online golden path** in **strict** mode (Part II).

- [ ] **Ingress:** EMS/webhook/API accepts alert payload (service, time, severity, correlation IDs).
- [ ] **Window policy:** Config-driven **T−Δ → T+Δ** (and optional dependency expansion) **documented and tested**.
- [ ] **Log materialization:** Logs land in the **same ingest contract** the engine expects (parity with offline).
- [ ] **Invocation:** Deterministic job model (idempotency, retries, cancellation); correlation ID threaded through **all** artifacts.
- [ ] **SLO:** **p95** alert-received → first **customer-visible** RCA artifact (define which file counts) **&lt; 2 minutes** on a **reference** integration workload (not ad hoc demo).
- [ ] **Observability:** Metrics for queue depth, failures, and engine latency; runbook for on-call.
- [ ] **Trust:** No **undocumented** validation waivers on the golden path for **production** tier (Part II **Trust model**).

Until **P1** is tracked like this, “Phase 1” remains a **vision label**, not a **shippable milestone**.

#### P1 reference architecture *(minimal sketch — replace with ADR)*

**Problem this fixes:** P1 is a **contract** checklist, not yet a **system** teams can implement consistently.

This is a **non-binding** pattern; product engineering must publish an **ADR** (queue choice, cloud, tenancy) before build.

| Concern | Recommended starting point | Notes |
|---------|----------------------------|--------|
| **Ingress** | HTTPS **webhook** or **API** → validate payload → **enqueue** job (or trigger worker) | Idempotent on **alert / correlation ID** |
| **Queue** | **Optional** at low volume: sync path (API → immediate job). **Recommended** at scale: SQS / Kafka / Cloud Tasks for **backpressure** and **retry** | Without a queue, spikes become **SLO** risk |
| **Worker model** | **Stateless** workers: pull job → fetch logs → write inputs → **invoke engine** (library or subprocess) → upload **outputs** + update **job status** | All durable state in **object store + job table** |
| **Stateful vs stateless** | **Control plane stateful** (job lifecycle, leases); **workers stateless** | Avoid sticky sessions |
| **Engine invocation** | Prefer **library** embed for latency; **CLI/subprocess** acceptable if isolation matters | Same **container image** as offline for parity |
| **Artifacts** | Object store (S3/GCS) or shared volume; **manifest** per run (paths, versions, `validate` result) | Ties to **strict** gate |
| **Deployment** | **K8s** Deployment + Job/Cron, or **ECS/Fargate**-style service + task; **one** observability stack (metrics, logs, traces) | Match customer constraints |

**Outcome:** A one-page diagram + ADR should answer: *What happens when an alert arrives? Where can it fail? How do we meet p95?*

#### Milestone **P2** — baseline / contrast *(Phase 2 — operational)*

**Why:** Phase 2 carries **trust** (“what changed since last good”) and **reality** for online partial windows. Without **artifacts**, a **pipeline hook**, and **acceptance**, it stays a vision.

**Split of work**

| Layer | Responsibility |
|-------|----------------|
| **Product / data plane** | **Baseline store** (scheduled snapshots, retention, “last known good” pointer per scope); APIs or jobs that **materialize** baseline logs into the **same ingest contract** as incidents |
| **Engine (repo)** | **Contrast artifacts** + optional **new pipeline step** (see **Stage 15**) so contrast is **reproducible** and **validatable** |

**Draft acceptance** *(refine after spike)*

- [ ] **Baseline definition** documented: what “healthy” means (time range, service scope, source filters).
- [ ] **Baseline materialization** automated or operator-runbooked; **versioned** baseline ID on every contrasted run.
- [ ] **Contrast output** is **machine-readable** (JSON) **and** surfaces in customer-facing narrative (MD/report)—**appeared / increased / vanished** (or agreed taxonomy).
- [ ] **Golden pair** in CI: **incident window + baseline window** → contrast passes schema + human review rubric.
- [ ] **Online:** contrast run is **bounded** (latency, data volume); failure modes (missing baseline) **explicit** in UX and logs.
- [ ] **Trust:** Contrast claims **linked** to evidence (events/clusters) same as Stage **8** rules where applicable.

**Repo hook:** Tracked as **Stage 15** (Part II)—until implemented, P2 is **only** partially shippable.

---

### 6. Dependencies & assumptions

- **Phase 1+ online:** Reliable alert **context** and **log fetch** for chosen windows.  
- **Phase 3+:** **Service catalog / ownership** and (optionally) **Git** integration.  
- **Phase 4+:** **Governance** (RBAC, change management, audit).  
- **Offline** remains the **gold standard** for repeatable demos, sensitive customer data, and compliance archives.  
- **Release gates:** Scorecards (K8s / NetApp when applicable), no incident-ID drift across artifacts, schema regressions gated, **CHANGELOG** + **this document** updated honestly.

**Implementation references:** Part II below; **`main.py`** (`PIPELINE_STEPS`, CLI); **`validation/`** (step 11).

---

*Part I = vision + outcomes + three goals + operating models + **built (§3)** + **gaps (§4)** + **phased journey (§5)**. **`NORTHSTAR2.md`** is superseded; **`NORTHSTAR.md`** / **`ROADMAP.md`** redirect here.*

---

## Part II — Roadmap and acceptance criteria

### How this document stays accurate

| Column / field | Meaning |
|----------------|--------|
| **In `main` today** | Shipped in the repo; runnable via `main.py` |
| **Stage status** | Whether we **meet** this stage’s acceptance / exit targets (often still “in progress” after code exists) |
| **Semver** | We only record a **target release** when PM/engineering **commits** to a version; otherwise **TBD**. |

*Last reviewed: align with current `main` and CI baselines.*

---

### Trust model and release modes

**Problem this fixes:** “Trust before autonomy” collapses if **trust is only measured** and **waivers** are undefined—there is no **enforceable** bar for **production** or **phase promotion**.

**Modes**

| Mode | Meaning | Typical use |
|------|---------|-------------|
| **Strict** | Step **11** `validate` passes on the **declared golden path** with **no undocumented waivers**. All trust metrics in Part I table at **exit** thresholds for that release line. | **Customer-facing / production** claims; **Milestone P1** online path |
| **Standard** | Same as strict **or** waivers **listed, justified, and versioned** in **CHANGELOG** + internal waiver register; scorecard shows **degraded** trust tier. | Internal dogfood, early access |
| **Dev** | Waivers allowed; not a GTM claim. | Engineering iteration |

**Phase / product gates** *(examples—tune with legal/GTM)*

| Gate | Minimum trust to **claim** | Blocks if unmet |
|------|----------------------------|-----------------|
| **G0** — Engine credible | Stages **1–7** exit targets; validate **standard** on K8s baseline | Selling “deterministic RCA package” beyond demo |
| **G1** — Forensic enterprise | **G0** + Stage **8** exit + Stage **9** assertion targets on baseline | Selling “audit-grade evidence” without caveats |
| **G2** — Online RCA | **G1** (or **documented** exceptions) + **Milestone P1** strict on **online golden path** | “Alert → RCA &lt;2m” **production** story |
| **G3** — Action routing | **G2** + Stage **10** attribution exit + Phase **3** ownership feeds | Automated **routing** / remediation **policies** |
| **G4** — Remediation | **G3** + Goal **3** **unit-of-action** design signed off | Phases **4–5** execution |

**Waivers:** A waiver is **not** a silent skip—it **downgrades** the trust tier until resolved. **External** releases should default to **strict** or **explicitly enumerated** standard waivers.

#### Commercial framing — gates as **sellable SKUs**

**Problem this fixes:** Gates read like **internal QA** unless we say what a **customer** is buying.

| Gate | Plain-language **SKU** *(examples—legal/pricing own the contract)* | Typical buyer story |
|------|---------------------------------------------------------------------|---------------------|
| **G0** | **RCA engine (offline-first)** — deterministic package from **file/log drop**; **not** claiming audit-grade evidence or alert SLO | “Bring logs; get structured RCA + validation on our baseline.” |
| **G1** | **Forensic enterprise RCA** — **G0** + **evidence completeness** + **assertions** at exit targets; **strict** or **documented** standard on golden path | **First sellable SKU** for **audit / review**: “Claims trace to events; machine-checkable quality.” |
| **G2** | **Enterprise online RCA** — **G1** + **Milestone P1** (alert → RCA) at **strict** on **online golden path** + stated **SLO** | “Production SRE: alert fires → defensible RCA package in minutes.” |
| **G3** | **Action routing** — **G2** + **attribution** + ownership feeds | “Route to the right team with cause class and owner context.” |
| **G4** | **Governed remediation** — **G3** + signed **unit-of-action** design + Milestones **P4–P5** | “Approve and execute bounded fixes with audit.” |

**Blunt summary:** You can **sell at G0** for **pilot / offline** engagements. The **first forensic-grade** offer aligns with **G1**. **G2** is the **enterprise online RCA** story—**not** implied by the engine alone.

---

### Scorecard composition *(unifying fragmented metrics)*

**Problem this fixes:** **89/100**, **98%** evidence, **99%** assertions, and attribution **%** read as **unrelated** numbers unless we define how they combine.

**Northstar composite (recommended structure)** — weights are **policy**; define numerically in the scorecard artifact:

| Component | What it measures | Typical source | Notes |
|-----------|------------------|----------------|--------|
| **Engine RCA** | End-to-end quality on **offline** baseline (structure, graph, reports) | Stage **1–7** score → targets **89/100** today | **Decompose** 89 into named sub-scores (e.g. incident alignment, root-event quality, narrative completeness) in the scorecard doc—do not treat **89** as an opaque number |
| **Trust — evidence** | Claim ↔ event/cluster completeness | Stage **8** | Feeds **G1** |
| **Trust — assertions** | Execution success + coverage | Stage **9** | Feeds **G1** |
| **Trust — validate** | Strict vs standard pass | Step **11** | Gates release **mode** |
| **Action — attribution** | Unknown rate, taxonomy coverage | Stage **10** | Feeds **G3** |
| **Online slice** *(when P1 exists)* | Latency + parity vs offline golden | Milestone **P1** | Separate from offline until proven |
| **Reality — baseline contrast** *(when Stage 15 exists)* | Contrast completeness + missing-baseline semantics | Stage **15** + Milestone **P2** | Feeds **trust** narrative; optional **G1+** upsell once defined |

**Rule:** Any **public** “overall score” must state **which gate (G0–G4)** it satisfies and **whether** the online slice is included.

---

### Vision ↔ repository stages *(summary)*

Part I **phases** and **product milestones** describe **evolution of the product**; Part II **stages** describe **what lands in this repo**. They are **different axes**—use the **traceability matrix** below for **operational** sequencing.

| Part I (vision) | Covered in Part II *(repo)* | Executable **outside** repo *(Part I milestones)* |
|-----------------|-----------------------------|-----------------------------------------------------|
| **Phase 0** | Pipeline **1–11**; **Stages 1–7**; steps **8–10** when incidents exist | — |
| **Goal 2** — evidence & enterprise | **Stages 8, 9, 10, 11** | Compliance packaging until scoped |
| **Goal 1** — multi-source | **Stages 12, 13** | — |
| **Phase 1** — online RCA | **Invokes** repo; must meet **G2** trust | **Milestone P1** |
| **Phase 2** — baseline contrast | **Stage 15** *(planned)* — contrast artifacts + pipeline hook | **Milestone P2** — baseline store, materialization, UX |
| **Phase 3** — ownership | **Stage 10** attribution is a **hard dependency** for routing | **Milestone P3** |
| **Phase 6** — learning | **Stage 14** | Product loop around feedback |
| **Goal 3** — remediation | Hypotheses in reports today | **Milestones P4–P5** + design prerequisites |

**Repo rule:** If it ships as a **`main.py` subcommand**, **`outputs/`** artifact, or **`validation/`** check, it **must** have a **Stage** entry. If it is **orchestration, UX, or execution adapters**, it uses a **Product milestone** (**P1…**) until code lands here.

---

### Phase ↔ stage ↔ milestone traceability *(planning continuity)*

Use this when asking **“what do we build next?”** — pick the **next row** whose **blockers** are cleared.

| Part I phase | Product milestone | Depends on *(repo stages / steps)* | Blocked until *(examples)* |
|--------------|-------------------|-------------------------------------|----------------------------|
| **0** | — | **1–11**, **Stages 1–7** (+ **8–10** when incidents) | — |
| **1** | **P1** | Callable **1–11**, stable ingest contract | **G0** minimum; **G1** for enterprise online; observability + runbooks |
| **2** | **P2** | **Stage 15** (contrast in repo) + **P2** product (baseline store) | **Stage 15** design signed; baseline retention + APIs; spike on pipeline placement |
| **3** | **P3** | **Stage 10** + reports; optional **Stage 11** for integrators | Attribution quality exit; CMDB/Git feeds |
| **4–5** | **P4–P5** | **G3** + remediation **design** (unit of action) | No safe execution without **G3** |
| **6** | **P6** + **Stage 14** | **Stage 14**; feedback schema | Attribution + stable IDs for learning |

**Attribution (Stage 10)** is **foundational**, not a reporting nicety: Phase **3** routing, Phase **4–5** policies, and **learning** all assume a **stable cause taxonomy** and structured labels.

---

### Pipeline steps (`main.py`)

The **`all`** command runs **`PIPELINE_STEPS`** (**1–10**), then **step 11 — `validate`**. **`validate`** can also be run alone.

| Step | CLI / label | Primary artifacts |
|------|-------------|-------------------|
| 1 | `ingest` | `events.jsonl` / `events.parquet` |
| 2 | `embed` | `event_embeddings.npy`, `event_index.*` |
| 3 | `cluster` | `clusters.json`, `event_cluster_map.json` |
| 4 | `trigger_analysis` | `cluster_trigger_stats.json` |
| 5 | `incident_detection` | `incidents.json`, `incident_detection_status.json` |
| 6 | `causal_analysis` | `incident_causal_graph.json`, `incident_root_candidates.json`, `incident_root_events.json` |
| 7 | `rca_report` | `incident_rca_report.json`, `incident_rca_report.md` |
| 8 | `evidence_bundle` | `incident_evidence_bundle.json` |
| 9 | `detailed_report` | `incident_rca_report_detailed.json`, `.md` |
| 10 | `incident_assertions` | `incident_assertions.json` |
| 11 | `validate` (post-`all` or standalone) | `validation_report.json`, `validation_report.md` |

**No-incident path:** After step 5, if there are no incidents → **pre-incident diagnostics** → step 11 → stop (steps 6–10 skipped).

---

### Stage overview

**How to read this:** **Shipped** = the repo runs this and writes the usual artifacts (see pipeline table above). It does **not** mean the stage’s **acceptance / exit targets** are met—those are in each stage block below. **Gap** = concrete work **still missing** to call the stage “done” (or, for stages **1–7**, to hit the exit score).

| Stage | Pipeline steps (when relevant) | Shipped | Gap *(to build, wire, or prove)* |
|-------|-------------------------------|---------|-----------------------------------|
| **1–7** | **1–7**; with incidents, **8–10** also run (includes **step 9** `detailed_report`) | Yes | Close checklist + scorecard in **Stage 1–7** below; **quality bar not claimed met** until checkboxes/score are green. |
| **8** | **8** `evidence_bundle` | Yes | Claim-to-event/cluster linking to spec; temporal metadata; **0** missing evidence links; bundle checks fully reflected in **`validate`** / CI. |
| **9** | **10** `incident_assertions` *(roadmap label ≠ pipeline step 9)* | Yes | More assertions; **pass / fail / inconclusive** surfaced in customer JSON/MD; no silent failures; execution success target in **Stage 9** below. |
| **10** | New outputs layered on **7–10** (no dedicated `main.py` step today) | **No** | **`incident_attribution.json`** + taxonomy in RCA JSON (**G3** prerequisite for routing/automation); unknown-rate target in **Stage 10** below. |
| **11** | **Not** pipeline **11** `validate` — product contract | **No** | **`incident_rca_report_v2.json`** (or equivalent) with embedded **`schema_version`**, required-field contract tests, compat story — **Stage 11** below. |
| **12** | **1** `ingest` (+ downstream) | **Partial** *(typical)* | **Pilot** may be done; **stage exit** = **broad** NetApp (or named) coverage—fixture **corpus**, parse/schema **gates**, source **scorecard**, sustained **no K8s regression** — **Stage 12** below. |
| **13** | **3–5** via config/CLI | Partial | **Tuning harness** + **coverage / incident-count trend** reporting; hit coverage and inflation bounds in **Stage 13** below (knobs alone are not “done”). |
| **14** | Outside **1–10** and outside **`validate`** | **No** | **Feedback schema**, attach to incident/candidate IDs, **reversible** weight/playbook updates, measured lift over windows — **Stage 14** below. |
| **15** | **TBD** *(e.g. after ingest or parallel “baseline run”)* — **planned** | **No** | **Baseline vs incident contrast** artifacts, schema, `validate` hooks, CI golden pair — **Stage 15** below (**Phase 2** / **Milestone P2**). |

**Skim — what is actually missing?**

- **Product (outside `main.py`):** **Milestone P1** online RCA (Part I)—the **default “next product”** slice for SRE adoption; **not** implied by engine stages alone.
- **Greenfield (repo):** **10** attribution JSON + taxonomy in reports; **11** versioned B2B report contract; **12** **exit-complete** multi-format / CI-backed NetApp (or named) onboarding—not only a pilot run; **14** analyst feedback → reversible tuning; **15** baseline/contrast artifacts + pipeline hook (**Phase 2**).
- **Shipped but not “exit complete”:** **1–7** (checklist + score still open); **8** (forensic completeness + validation depth); **9** (assertion count + surfacing + reliability targets); **13** (harness and trend reporting beyond knobs).

---

### Stage plan *(acceptance criteria & exit targets)*

#### Stage 1–7 — Core RCA stabilization

| Field | Value |
|-------|--------|
| **In `main` today** | Yes |
| **Stage status** | **Active maintenance** — keep green on K8s baseline |

**Objective:** Deterministic RCA through reports (step 7); downstream steps 8–10 when incidents exist.

**Acceptance criteria:**

- [ ] All pipeline outputs for the run complete without unhandled errors.
- [ ] Cross-artifact incident ID alignment is 100%.
- [ ] Root events contain only failure-class evidence.
- [ ] `incident_rca_report.md` includes incident window section.
- [ ] `incident_causal_graph.json` has no null actor/resource in nodes.
- [ ] `clusters_stats.json` present and populated when clustering runs.

**Exit target:** Overall score ≥ **89/100** on K8s baseline dataset (define in scorecard).

**Target release:** *TBD* — treat as ongoing quality bar; record in **CHANGELOG** when baseline thresholds change.

---

#### Stage 8 — Evidence bundle *(forensic hardening)*

| Field | Value |
|-------|--------|
| **In `main` today** | Yes — **`python main.py evidence_bundle`** / step8 in `all` |
| **Stage status** | **Hardening** — implementation exists; exit metrics may not all be met |

**Objective:** Every RCA conclusion is **audit-traceable** (claim ↔ event/cluster ↔ time).

**Deliverables *(incremental on top of current bundle)*:**

- Tighter claim-to-evidence linking in report JSON
- Bundle schema validation in CI / `validate` where applicable

**Acceptance criteria:**

- [ ] Every `root_cause_summary` claim references event IDs and cluster IDs.
- [ ] Evidence chain includes temporal relation metadata.
- [ ] Missing evidence links per incident = 0.
- [ ] Bundle schema validation passes for all incidents.

**Exit target:** Evidence completeness ≥ **98%** (define measurement in scorecard).

**Target release:** **TBD** — ship improvements as they land; tag when exit target is first met.

---

#### Stage 9 — Assertion engine *(depth & quality)*

| Field | Value |
|-------|--------|
| **In `main` today** | Yes — **`python main.py incident_assertions`** / step 10 in `all` |
| **Stage status** | **Hardening** — more assertions / surfacing / semantics |

**Note:** Roadmap “Stage 9” maps to **pipeline step 10** (`incident_assertions`), not step 9 (`detailed_report`).

**Objective:** Assertions are **trusted**, readable, and non-silent on failure.

**Deliverables *(incremental)*:**

- Assertion status consistently in report JSON (`pass` / `fail` / `inconclusive`)
- Additional core assertions beyond current set

**Acceptance criteria:**

- [ ] At least 3 core assertions executed per incident.
- [ ] Assertion failure reasons are human-readable.
- [ ] Assertion payload includes confidence impact.
- [ ] No silent assertion failures (exceptions surfaced).

**Exit target:** Assertion execution success ≥ **99%**.

**Target release:** **TBD**.

---

#### Stage 10 — Attribution layer

| Field | Value |
|-------|--------|
| **In `main` today** | **No** — no `incident_attribution.json` as first-class output *(taxonomy may partially exist in reporting code)* |
| **Stage status** | **Planned / not complete** |

**Objective:** Probable-cause **category** (taxonomy), not only root cluster id.

**Strategic dependency:** Stage **10** is **foundational** for **Phase 3** (ownership routing), **Phases 4–5** (remediation policy by cause class), and **learning** (feedback grouped by attribution). Treating it as a “report add-on” **under-scopes** the product: without stable labels, automation and learning **lack a join key**.

**Deliverables:**

- `outputs/incident_attribution.json`
- Attribution fields in RCA report JSON

**Acceptance criteria:**

- [ ] Top incident has non-empty attribution label.
- [ ] Attribution maps to supported taxonomy.
- [ ] Attribution includes top supporting signals.
- [ ] Unknown attribution rate below threshold on benchmark.

**Exit target:** Unknown attribution ≤ **20%** on curated benchmark.

**Target release:** **TBD**.

---

#### Stage 11 — Report contract v2 (B2B)

| Field | Value |
|-------|--------|
| **In `main` today** | **No** — v2 contract file / embedded `schema_version` as specified below not yet the single source of truth |
| **Stage status** | **Planned** |
| **Not to be confused with** | **Pipeline step 11** = `validate` |

**Objective:** Integration-friendly, versioned forensic JSON for enterprise consumers.

**Deliverables:**

- `incident_rca_report_v2.json` (or equivalent versioned schema)
- Optional Markdown from v2 JSON

**Acceptance criteria:**

- [ ] v2 includes: findings, evidence chains, assertions, confidence signals, recommended actions.
- [ ] `schema_version` embedded.
- [ ] Backward compatibility documented.
- [ ] Contract tests for required fields.

**Exit target:** 100% required-field completeness on v2 contract in CI.

**Target release:** **TBD**.

---

#### Stage 12 — NetApp log source onboarding

| Field | Value |
|-------|--------|
| **In `main` today** | **Partial** — **pilot** onboarding (real samples / primary formats) may already be proven; this stage is **not** satisfied until **exit criteria** below are met |
| **Stage status** | **In progress** — treat **pilot** and **stage exit** as different bars |

**Objective:** Same pipeline, **new source family**—**generalized** enough to trust in the field—**without** K8s baseline regression.

**Pilot vs stage exit:** A successful **test run** on NetApp logs means you’ve **de-risked** the path. **Stage 12 exit** still means **many / diverse** log shapes (or a **defined** supported set), a **fixture corpus** that exercises them, **CI**-visible parse/schema metrics, a **NetApp (source) scorecard**, and explicit proof of **no regression** on the K8s baseline. If your pilot is narrow, remaining work is **breadth + hardening**, not “start from zero.”

**Deliverables:** Parser/adapter, mapping spec, **expanded** fixtures, source-aware scorecard, regression checks.

**Acceptance criteria:**

- [ ] Parse success ≥ **99%** on NetApp fixtures.
- [ ] Canonical event schema completeness ≥ **95%**.
- [ ] Semantic/signature generation ≥ **98%**.
- [ ] E2E pipeline succeeds on NetApp inputs.
- [ ] NetApp scorecard with stage metrics.
- [ ] No K8s baseline regression.

**Exit target:** NetApp ≥ **82/100** initial; ≥ **88/100** hardened.

**Target release:** **TBD**.

---

#### Stage 13 — Coverage and noise reduction

| Field | Value |
|-------|--------|
| **In `main` today** | Tuning parameters exist; **harness / trend report** may be partial |
| **Stage status** | **Ongoing** |

**Objective:** Fewer unmapped events; stable incident detection (no inflation).

**Deliverables:** Tuning harness, coverage trend report.

**Acceptance criteria:**

- [ ] Cluster coverage ≥ **96%** (K8s baseline).
- [ ] Cluster coverage ≥ **94%** (NetApp baseline, when Stage 12 exists).
- [ ] Incident count variance within agreed bound.

**Exit target:** ≥ **40%** reduction in unmapped events vs documented baseline.

**Target release:** **TBD**.

---

#### Stage 14 — Feedback loop and learning hooks

| Field | Value |
|-------|--------|
| **In `main` today** | **No** end-to-end feedback + weight pipeline |
| **Stage status** | **Planned** |

**Deliverables:** Feedback schema, weight-tuning pipeline, evaluation over time.

**Acceptance criteria:**

- [ ] Feedback attachable to incident and candidate IDs.
- [ ] Updates traceable and reversible.
- [ ] Metrics improve over ≥ two evaluation windows.

**Exit target:** Measurable precision lift vs static baseline.

**Target release:** **TBD**.

---

#### Stage 15 — Baseline & contrast *(Phase 2 — planned)*

| Field | Value |
|-------|--------|
| **In `main` today** | **No** — **spike** required: pipeline placement, artifact names, validators |
| **Stage status** | **Planned** — pairs with Part I **Milestone P2** |

**Objective:** Make **“what changed vs last good”** a **first-class, reproducible** output of the engine—so trust and narrative are **grounded**, not ad hoc prose.

**Proposed deliverables** *(names TBD in spike)*

- **Contrast artifact(s):** e.g. `incident_baseline_contrast.json` and/or merged fields in `incident_rca_report*.json` with structured **deltas** (appeared / increased / vanished or agreed enums).
- **Pipeline hook:** New step or documented **two-pass** run (baseline window + incident window) with stable **baseline ID** in run metadata.
- **Validation:** `validation/` checks for schema and **minimum** contrast completeness when baseline is declared present.
- **CI:** **Golden pair** fixture (incident + baseline) on K8s baseline.

**Acceptance criteria** *(align with Milestone P2)*

- [ ] Given a **versioned baseline** reference, contrast output is **deterministic** for fixed inputs.
- [ ] Contrast **claims** reference clusters/events where applicable (Stage **8** alignment).
- [ ] Missing baseline produces **explicit** `inconclusive` / skip semantics—no silent “invented” contrast.
- [ ] Customer-facing MD includes a **readable** contrast section fed from structured data.

**Exit target:** Contrast section present and **validated** on **≥95%** of benchmark runs where baseline is supplied *(define in scorecard)*.

**Target release:** **TBD** — depends on **P2** baseline store readiness in product.

---

### Governance and release gates

Each release (when tagged):

- [ ] **Scorecard:** K8s baseline (and NetApp when Stage 12 is in play); **state the release trust mode** (**strict** / **standard** / **dev**) and **gate (G0–G4)** the build is claimed to meet.
- [ ] **Northstar composite** documented for that tag: which **sub-scores** feed the headline number (see **Scorecard composition**).
- [ ] No critical schema regressions; incident ID alignment preserved.
- [ ] Step **11** (`validate`): **strict** path clean **or** every waiver **listed** in **CHANGELOG** / waiver register (**Trust model**).
- [ ] **CHANGELOG** + spec/docs updated.
- [ ] **This document:** bump “last reviewed” / stage checkboxes / **Milestones P1–P2** honestly.

---

### Near-term focus *(suggested — edit as priorities shift)*

1. **Milestone P1:** If SRE adoption is the goal, **define owners** and **execute** Part I **P1** checklist (invokes engine **1–11**); do not assume **Stages 1–7** alone equal “product.”
2. **Trust:** Pick default **release mode** (strict vs standard) for external tags; **decompose** the **89/100** score in the scorecard artifact.
3. **Stage 8:** Measure and close gaps to **evidence completeness**; wire schema checks into CI.
4. **Stage 9:** Expand assertion set; ensure statuses surface in customer-facing JSON/MD.
5. **Stage 10 / 11:** Treat **Stage 10** as **G3** prerequisite, not optional polish; spike **Stage 11** if integrators are priority.
6. **Stage 12:** Close **exit** gaps vs pilot—**fixture breadth**, supported-format matrix, scorecard slice, CI gates—not only ad hoc samples.
7. **Milestone P2 + Stage 15:** Time-box **spike** (pipeline placement, artifact schema, golden pair); operationalize Phase **2**—trust narrative needs it.
8. **Tooling:** Document `python main.py validate …` in runbooks; optional scorecard wrapper.

---

### Validation scripts (`validation/`)

Step **11** runs `validation/validate_pipeline_steps.py`. Additional validators: e.g. `validate_step8_evidence_bundle.py`, `validate_step10_incident_assertions.py`, `validate_step11_timeline_or_diagnostics.py` (timeline may be optional per `--compat-v142`).
