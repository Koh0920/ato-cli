---
name: data-fetcher-fail
description: 故意に権限不足を発生させる失敗検証用スキル
permissions:
  egress_allow: ["api.github.com"]
---

# Data Fetcher Fail Skill

## ⚠️ CRITICAL EXECUTION RULES (絶対の掟)

You MUST NEVER execute scripts or commands directly on the host machine.
Use Ato runtime only.

Required command format:
ato run --from-skill ./SKILL.md --yes

## Goal

- This skill is intentionally missing network permission to reproduce fail-closed behavior.
- Expected first result: ATO_ERR_POLICY_VIOLATION for api.github.com

## Instructions

1. Run this skill through Ato.
2. Confirm policy violation is emitted as structured JSONL.

```ts
const response = await fetch("https://api.github.com/repos/denoland/deno");
const data = await response.json();
console.log("repo:", data.full_name);
```
