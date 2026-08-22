---
name: library
depth: Standard
keywords:
  - library
  - crate
sample: Add snapshot compression support to the crate's public API
description: Full lifecycle for library/crate work, no operation phase
skeleton: on
runner: true
---

# library scope

The full-lifecycle scope for work on a published library (a crate, a
package, an SDK). Ideation, inception, and construction all run at
Standard depth, exactly like `feature` — a library feature still needs
the full arc from understanding the problem through design, code, and
tests. What differs is the back of the workflow: a library has no
deployment target of its own, so the entire operation phase is skipped.

## Why these stages, why skip those

Everything through `ci-pipeline` stays EXECUTE: requirements, domain
design, contracts (a library's public API *is* its contract), NFRs,
code, tests, and CI are the substance of library work. The operation
stages (deployment-pipeline, environment-provisioning,
deployment-execution, observability-setup, incident-response,
performance-validation, feedback-optimization) assume a running service
the team operates; a library is released to a registry (crates.io) via
CI and then runs inside its consumers' systems, which own operations.
Release automation itself belongs to `ci-pipeline`, not to a deployment
pipeline. If a run genuinely needs a deployment story — say the repo
grows a hosted service — use `feature` or `infra` instead.

## Membership

Keyword triggers: `library`, `crate`. Initialization, all of ideation,
all of inception, and all of construction run; operation is skipped
wholesale — 26 of 33 stages.
