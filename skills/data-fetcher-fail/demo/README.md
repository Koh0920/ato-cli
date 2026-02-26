Demo for Phase 1 → Phase 2 flow

Usage:

```sh
python3 demo_run.py
```

What it does:
- Runs `ato run --from-skill ./SKILL.md --yes` (Phase 1). If policy violation is detected, it:
  - shows a proposed frontmatter change
  - displays a unified diff
  - prompts for explicit user consent
  - on consent, backs up `SKILL.md` and writes the proposed change, then re-runs (Phase 2)
- If `SKILL.md` already contains `permissions` (left over from prior demo runs), it prompts to reset to fail-state before Phase 1.

Notes:
- This script intentionally performs the write only after interactive consent.
- Use this in demos only; CI should never auto-accept changes.
