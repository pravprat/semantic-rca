# NetApp System Diagnostics Engine (NSDE)

## AI-Driven Root Cause Intelligence for Infrastructure Systems

**Audience:** EVP / CTO and executive sponsors  
**Purpose:** Position NSDE as a **governed diagnostics vertical**—moving NetApp from **observability** to **intelligent, evidence-backed root cause intelligence** across ONTAP, Kubernetes, and hybrid estates.

---

## Executive summary

- **Gap:** Enterprises detect problems faster than they **prove** root cause. Diagnosis remains **manual, variable, and weakly evidenced**—raising MTTR, reopen rates, and eroding trust in conclusions.

- **NSDE:** An **AI-driven diagnostics engine** that turns raw operational signals into **structured incidents**, **relationships across behavior**, **evidence-grounded root-cause hypotheses**, and **consistent artifacts** for support and engineering—built for **repeatability** and **validation**, not chat-only demos.

- **Strategic fit:** Aligns with NetApp’s **layered data and AI platform** direction: ingestion and **log-structure awareness**, **intelligence** (normalization, correlation, patterns), **diagnostics** (incidents, RCA), and **integration** (support, alerting, internal tools, future agentic consumers).

- **Trajectory:** **Offline foundation** today; **EMS-triggered online diagnostics** as the priority product step; **change intelligence**, **ownership/routing**, and **assisted remediation** (with CI/CD awareness) as phased evolution—**automation** explicitly future-gated and policy-bound.

---

## 1. Problem statement

| Challenge | Effect |
|-----------|--------|
| **Manual RCA** — log exploration and heuristics | Slow resolution; **inconsistent** diagnosis quality |
| **Limited explainability** — inference without proof | Weak **linkage** between conclusions and evidence |
| **Fragmented telemetry** — logs, metrics, events in silos | **No unified reasoning** across signals |
| **Operational drag** | **Higher MTTR**, reopened cases, lower confidence in outcomes |

**Executive framing:** Visibility is not the bottleneck—**trusted, structured diagnosis** is.

---

## 2. Proposed solution: NetApp System Diagnostics Engine (NSDE)

NSDE adds a **diagnostics intelligence layer** that processes telemetry and produces **structured, actionable** outputs.

**Core outcomes**

- Structured **incident** representations and time-bounded episodes  
- **Relationships** between events and system behavior (including causal structure at cluster/incident level)  
- Root-cause **hypotheses grounded in evidence** (traceable to signals, not narrative-only)  
- **Consistent diagnostic artifacts** for support, engineering, and integration (reports, bundles, checks)

**Differentiators**

| Theme | What it means for NetApp |
|--------|---------------------------|
| **Deterministic spine** | Repeatable pipeline and **contract-style outputs**—suitable for regression, CI, and release discipline |
| **Evidence-linked insights** | Conclusions **tied** to supporting events/clusters and forensic-style packaging where required |
| **Validation-driven trust** | Built-in **quality checks** so “pass” means something operationally—not a one-off demo |

**Positioning:** NSDE is **semantic and structural intelligence** over infrastructure logs and related signals. Generative AI and **agents** can **consume** NSDE artifacts (summaries, APIs, tools) **above** this core without replacing **audited** diagnostics.

---

## 3. Architecture alignment

NSDE maps to NetApp’s **ingestion → intelligence → diagnostics → output/integration** direction and to a modern **Data Intelligence Platform** mental model (foundation, governance, content/semantic intelligence, presentation)—**without** requiring NSDE to own every layer.

**Telemetry and structure awareness**

- **Ingestion:** ONTAP systems, **Kubernetes** (namespaces, pods, containers, services), hybrid infrastructure.  
- **NetApp log organization:** Logs grouped by **sub-system** (subdirectories reflecting sub-system boundaries).  
- **Sub-system–level diagnostics (new capability):** Logs per sub-system are **analyzed locally** and **correlated** with system-wide context.

**Why sub-system granularity matters**

| Benefit | Description |
|---------|-------------|
| **Faster fault-domain isolation** | Localize failure before system-wide noise dominates |
| **Noise reduction** | Fewer unrelated components in the reasoning window |
| **Sharper root-cause summaries** | Explanations anchored to the **right** scope |

**Layered flow (conceptual)**

| Layer | NSDE role |
|-------|-----------|
| **Data foundation** | Consumes operational telemetry where it lands (exports, stores, hybrid)—NSDE is a **vertical engine**, not the data lake |
| **Governance** | Provenance, evidence packaging, policy hooks, validation posture—**trust as a product feature** |
| **Content intelligence** | Parsing, normalization, **understanding** log/audit streams; sub-system awareness |
| **Semantic intelligence** | Embeddings, clustering, incident semantics, causal reasoning; path to **hybrid** retrieval over events/baselines |
| **Diagnostics** | Incident detection, behavioral analysis, ranked root hypotheses **grounded in evidence** |
| **Output & integration** | Reports, APIs, support and alerting integration; future **agentic** workflows as **consumers** of structured outputs |

---

## 4. Event-driven (online) diagnostics — EMS integration

**Intent:** Move from **post-incident file analysis** to **alert-triggered** diagnostics in **near real time**.

**Flow (target operating model):**  
**Alert (e.g., EMS)** → **scoped log collection** (sub-systems identified) → **RCA computation** → **structured outputs** (minutes-level SLO as a **product commitment**, not implied by batch alone).

**Outcomes:** Less manual triage on the critical path; faster handoff to support; **repeatable** runbooks tied to **evidence-backed** outputs.

---

## 5. CI/CD integration — change-aware diagnostics and remediation (phased)

**Intent:** Close the loop from **incident** to **change context**—deployment, config, code—before wide automation.

**Flow (vision):**  
**Alert** → **logs** → **RCA** → **root hypothesis** → **CI/CD inspection** → **change correlation** → **remediation path**

**Capabilities (staged):** Link root cause hypotheses to **recent deployments, configuration drift, or code changes**; integrate with CI/CD views to **inspect and correlate** change with incident evidence.

**Remediation modes (explicitly phased)**

| Mode | Description |
|------|-------------|
| **Manual** | Engineer validates and applies fix (default for production trust) |
| **Assisted** | System proposes actions; human approves |
| **Automated (future)** | **Policy-bound**, low-risk, reversible actions only—requires governance and design sign-off |

**Outcome:** Shorter **cause-to-resolution** cycles, lower regression risk, **foundation** for intelligent automation—**without** over-promising autonomy today.

---

## 6. Product evolution roadmap (phased)

| Phase | Focus | Highlights |
|-------|--------|------------|
| **0 — Offline foundation** | Post-incident and batch excellence | Structured RCA; sub-system insights; regression discipline |
| **1 — Online diagnostics (priority)** | EMS-triggered RCA | Scoped collection; minutes-level path; support workflow integration |
| **2 — Change intelligence** | “What changed” | Baseline/contrast; higher diagnostic confidence |
| **3 — Ownership & routing** | Actionability | Map incidents to owning sub-systems/services |
| **4+ — Assisted remediation** | CI/CD-linked workflows | Assisted first; automation **future-gated** |

---

## 7. Business impact

| Lens | Value |
|------|--------|
| **Internal** | Faster root-cause identification; less manual debugging; improved support efficiency |
| **Customer** | Faster resolution; **clear, explainable** diagnostics; higher trust |
| **Strategic** | Positions NetApp in **AI-driven infrastructure diagnostics**; enables advanced support and a path to **governed** automation |

---

## 8. Success metrics (pilot → scale)

| Category | Examples |
|----------|----------|
| **Speed** | Alert → first structured RCA artifact within agreed **SLO** (defined per integration) |
| **Precision** | Correct **sub-system / fault-domain** isolation on pilot incident set |
| **Quality** | High **evidence completeness**; consistent outputs; validation posture met |
| **Adoption** | Reduced manual debugging time; increased use in **support workflows** |

---

## 9. Initial pilot scope

**Focus**

- ONTAP **sub-system** logs (structured collection)  
- **Kubernetes** contexts (namespaces, pods, containers, services)

**Deliverables**

- Sub-system–level **RCA summaries**  
- **Event-driven** RCA outputs (as Phase 1 matures)  
- **CI/CD-linked** diagnostics for a **bounded** pilot use case

---

## 10. Ask and next steps

| Function | Actions |
|----------|---------|
| **Product management** | PRD for EMS-triggered diagnostics, sub-system RCA scope, CI/CD pilot boundaries |
| **Engineering** | Align on ingestion pipelines, **sub-system mapping**, orchestration, execution, and **validation** gates |
| **Pilot** | Select high-impact incidents; measure **speed, precision, usability**; document trust tier (strict vs documented exceptions) |

---

## Closing

NSDE represents a deliberate shift: **from observability alone to intelligent, evidence-backed diagnostics**—with a **deterministic core**, **governed outputs**, and a **credible path** to online operation and change-aware remediation. Investment here **accelerates resolution**, **improves diagnostic accuracy**, and **differentiates** NetApp’s AI and platform story for infrastructure intelligence.

---

*Confidential — align timelines, SLOs, and “shipped vs roadmap” claims with official product planning and release documentation before external distribution.*
