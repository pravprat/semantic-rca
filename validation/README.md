# Validation Scripts (QA Ready)

This folder provides step-wise validation for the SCRCA pipeline outputs.

## What it does

- Prints exactly what is being compared in each check
- Prints check result (`PASS`/`FAIL`) with details
- Produces a final summary for QA sign-off

## Scripts

- `validation/validate_pipeline_steps.py`  
  Full validation across steps 1-11
- `validation/validate_step1_ingest.py`  
  Step 1 only (ingest)
- `validation/validate_step2_embed.py`  
  Step 2 only (embeddings/index)
- `validation/validate_step3_cluster.py`  
  Step 3 only (cluster artifacts)
- `validation/validate_step4_trigger_analysis.py`  
  Step 4 only (trigger stats)
- `validation/validate_step5_incident_detection.py`  
  Step 5 only (incident detection contract)
- `validation/validate_step6_causal_analysis.py`  
  Step 6 only (causal graph/candidates/grounded roots)
- `validation/validate_step7_report.py`  
  Step 7 only (RCA report JSON)
- `validation/validate_step8_evidence_bundle.py`  
  Step 8 only (evidence bundle JSON)
- `validation/validate_step9_detailed_report.py`  
  Step 9 only (detailed report JSON)
- `validation/validate_step10_incident_assertions.py`  
  Step 10 only (assertions JSON)
- `validation/validate_step11_timeline_or_diagnostics.py`  
  Step 11 only (incident timeline or no-incident diagnostics)

## Run

From the project root:

```bash
python validation/validate_pipeline_steps.py --outputs-dir outputs --raw-log /path/to/raw.log
```

If you do not want raw-log line sanity checks:

```bash
python validation/validate_pipeline_steps.py --outputs-dir outputs
```

Compatibility mode for legacy v1.4.2-style outputs:

```bash
python validation/validate_pipeline_steps.py --outputs-dir outputsk8s_1to9 --compat-v142
```

Individual steps:

```bash
python validation/validate_step1_ingest.py --outputs-dir outputs --raw-log /path/to/raw.log
python validation/validate_step2_embed.py --outputs-dir outputs
python validation/validate_step3_cluster.py --outputs-dir outputs
python validation/validate_step4_trigger_analysis.py --outputs-dir outputs
python validation/validate_step5_incident_detection.py --outputs-dir outputs
python validation/validate_step6_causal_analysis.py --outputs-dir outputs
python validation/validate_step7_report.py --outputs-dir outputs
python validation/validate_step8_evidence_bundle.py --outputs-dir outputs
python validation/validate_step9_detailed_report.py --outputs-dir outputs
python validation/validate_step10_incident_assertions.py --outputs-dir outputs
python validation/validate_step11_timeline_or_diagnostics.py --outputs-dir outputs
```

## Notes

- For no-incident runs, the validator expects diagnostics artifacts (`preincident_diagnostics.json/.md`).
- For incident runs, it expects downstream RCA/evidence/assertion/timeline outputs.
- In `--compat-v142` mode, `incident_detection_status.json` and timeline summary are treated as optional.
