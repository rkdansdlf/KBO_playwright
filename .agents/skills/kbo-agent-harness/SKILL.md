---
name: kbo-agent-harness
description: Use for KBO_playwright crawler bugs, features, refactors, research, analytics, or architecture work that should be routed through the project Harness and existing verification gates.
license: MIT
compatibility: Requires Python 3.12 and repository-local tools.agent_harness.
metadata:
  project: KBO_playwright
  harness-version: "1"
---

# KBO Agent Harness

Treat `AGENTS.md` as policy, `.agent-harness/` as configuration,
`tools/agent_harness/` as the control plane, and existing pytest/Ruff/certification
commands as the source of truth.

1. Run `python -m tools.agent_harness plan "<task>"` to select a profile.
2. Use only the context, workflow, guard, verification, and output roles selected in the plan.
3. Keep Graphify as the default context engine. Add Understand Anything only for the
   `refactor` and `architecture` profiles.
4. Use Caveman only for subagent communication and i-have-adhd only for human output.
5. Treat all external adapters as `reference_only`; do not claim they executed and do
   not fetch or install third-party code without explicit approval.
6. Deny secret and wallet reads. Keep generated evidence under
   `artifacts/agent-harness/<run-id>/`.
7. Run the plan's existing project verification commands before reporting completion.
