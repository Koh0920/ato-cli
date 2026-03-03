# ato-cli

English | [日本語](README_JA.md)

`ato` is a meta-CLI that interprets `capsule.toml` to execute, distribute, and install capsules.
It is designed around a Zero-Trust / fail-closed model: normal runs stay quiet, while consent prompts and policy violations are surfaced explicitly.

## Key Commands

```bash
ato run [path|publisher/slug] [--registry <url>]
ato open [path] [--watch]                 # compatibility command (deprecated; prefer run)
ato ps
ato close --id <capsule-id> | --name <name> [--all] [--force]
ato logs --id <capsule-id> [--follow]
ato install <publisher/slug> [--registry <url>]
ato build [dir] [--force-large-payload]
ato publish [--registry <url>] [--artifact <file.capsule>] [--scoped-id <publisher/slug>] [--allow-existing] [--prepare] [--build] [--deploy] [--legacy-full-publish] [--force-large-payload]
ato publish --dry-run
ato publish --ci
ato search [query]
ato config engine install --engine nacelle [--version <ver>]
ato setup --engine nacelle [--version <ver>] # compatibility command (deprecated)
ato registry serve --host 127.0.0.1 --port 18787 [--auth-token <token>]
```

## Quick Start (Local)

```bash
# build
cargo build -p ato-cli

# install nacelle engine if not installed (recommended)
./target/debug/ato config engine install --engine nacelle

# compatibility: setup subcommand
./target/debug/ato setup --engine nacelle

# run
./target/debug/ato run .

# hot reload during development
./target/debug/ato open . --watch

# background process management
./target/debug/ato run . --background
./target/debug/ato ps
./target/debug/ato logs --id <capsule-id> --follow
./target/debug/ato close --id <capsule-id>
```

## Publish Model (Official / Local)

- Official registries (`https://api.ato.run`, `https://staging.api.ato.run`):
  `ato publish` is CI-first (OIDC). Direct local uploads are not allowed.
  Default phase selection is `deploy` only (handoff/diagnostics). If you need local build checks, explicitly add `--build` (or `--prepare --build`) before `--deploy`.
- Local/private registries (any other `--registry`):
  `ato publish --registry ...` performs direct uploads. `--artifact` is recommended to avoid re-packing.
  `--artifact` supports standalone artifact flow (no local `capsule.toml` required).
  `--allow-existing` is available only on deploy phase (`--deploy`) for private/local registries.

`ato publish` runs 3 internal phases in fixed order: `prepare -> build -> deploy`.

- Default (official registries): run `deploy` only.
- Default (private/local registries): run all phases.
- If any of `--prepare/--build/--deploy` is specified: run only selected phases.
- `--artifact` always skips build phase.
- `official + deploy` returns handoff only (no local upload).
- `--legacy-full-publish` (official only) temporarily restores legacy default (`prepare -> build -> deploy`), is deprecated, and is scheduled for removal in the next major release.
- `--ci` / `--dry-run` cannot be combined with phase flags.

```bash
# pre-build + direct publish to a private registry (recommended)
ato build .
ATO_TOKEN=pwd ato publish --registry http://127.0.0.1:18787 --artifact ./<name>.capsule

# phase-filtered execution examples
ato publish --prepare
ato publish --build
ATO_TOKEN=pwd ato publish --deploy --artifact ./<name>.capsule --registry http://127.0.0.1:18787
ato publish --registry https://api.ato.run           # default: deploy only
ato publish --registry https://api.ato.run --build   # explicit local build + official handoff
ato publish --deploy --registry https://api.ato.run

# temporary compatibility flag (official only; deprecated and will be removed in next major)
ato publish --registry https://api.ato.run --legacy-full-publish

# idempotent retry for the same version/content (CI retry best practice)
ATO_TOKEN=pwd ato publish --registry http://127.0.0.1:18787 --artifact ./<name>.capsule --allow-existing
```

## Proto Regeneration (Maintenance Only)

`protoc` is not required for normal builds.
Run this only when `core/proto/tsnet/v1/tsnet.proto` changes.

```bash
./core/scripts/gen_tsnet_proto.sh
```

## Local Registry E2E

```bash
# Terminal 1: start local HTTP registry
ato registry serve --host 127.0.0.1 --port 18787

# Terminal 2: build -> publish(artifact) -> install -> run
ato build .
ATO_TOKEN=pwd ato publish --artifact ./<name>.capsule --registry http://127.0.0.1:18787
ato install <publisher>/<slug> --registry http://127.0.0.1:18787
ato run <publisher>/<slug> --registry http://127.0.0.1:18787 --yes
```

Notes:
- Write operations (`publish`) require `ATO_TOKEN` when `registry serve --auth-token` is enabled.
- Read operations (search/install/download) can remain unauthenticated.
- Use `18787` in local verification to avoid collision with app services that also use `8787` (for example, worker HTTP ports).
- `publish --artifact` is the recommended path for local/private workflows.
- `--scoped-id` can override publisher/slug for artifact upload.
- `--allow-existing` is not a blind conflict ignore; it is an idempotent operation gated by artifact hash/manifest consistency checks.
- In enterprise CI, attach `--allow-existing` to retry paths to make reruns deterministic and safe.
- Version conflict is reported as `E202` with next actions (`bump version`, `--allow-existing`, or reset local registry).

## Cross-Device Publish (VPN / Tailscale)

```bash
# Server side: non-loopback exposure requires --auth-token
ato registry serve --host 0.0.0.0 --port 18787 --auth-token pwd

# Client side: install/run do not require token (read APIs)
ato install <publisher>/<slug> --registry http://100.x.y.z:18787
ato run <publisher>/<slug> --registry http://100.x.y.z:18787

# Token required only for publish
ATO_TOKEN=pwd ato publish --registry http://100.x.y.z:18787 --artifact ./<name>.capsule
```

## Required Environment Variable Checks (Pre-Run)

`ato run` validates required environment variables before startup.
If missing or empty, execution stops fail-closed.

- `targets.<label>.required_env = ["KEY1", "KEY2"]` (recommended)
- Backward compatibility: `targets.<label>.env.ATO_ORCH_REQUIRED_ENVS = "KEY1,KEY2"`

## Dynamic App Capsule Recipe (Web + Services Supervisor)

For multi-service apps (for example: dashboard + API + worker), use a single `web/deno` target with top-level `[services]`. `ato run` starts services in DAG order, waits on readiness probes, prefixes logs, and fail-fast stops all services when one exits.

1. Pre-bundle artifacts before packing (for example: `next build`, worker build, lockfiles).
2. Include only runtime artifacts via `[pack].include` (do not package raw `node_modules`, `.venv`, caches).
3. Build once, then publish with `--artifact` to avoid re-packing.

Minimal `capsule.toml` pattern:

```toml
schema_version = "0.2"
name = "my-dynamic-app"
version = "0.1.0"
default_target = "default"

[pack]
include = [
  "capsule.toml",
  "capsule.lock",
  "apps/dashboard/.next/standalone/**",
  "apps/dashboard/.next/static/**",
  "apps/control-plane/src/**",
  "apps/control-plane/pyproject.toml",
  "apps/control-plane/uv.lock",
  "apps/worker/src/**",
  "apps/worker/wrangler.dev.jsonc"
]
exclude = [
  ".deno/**",
  "node_modules/**",
  "**/__pycache__/**",
  "apps/dashboard/.next/cache/**"
]

[targets.default]
runtime = "web"
driver = "deno"
runtime_version = "1.46.3"
runtime_tools = { node = "20.11.0", python = "3.11.10", uv = "0.4.30" }
port = 4173
required_env = ["CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ACCOUNT_ID"]

[services.main]
entrypoint = "node apps/dashboard/.next/standalone/server.js"
depends_on = ["api"]
readiness_probe = { http_get = "/health", port = "PORT" }

[services.api]
entrypoint = "python apps/control-plane/src/main.py"
env = { API_PORT = "8000" }
readiness_probe = { http_get = "/health", port = "API_PORT" }
```

Recommended flow:

```bash
# 1) pre-bundle app artifacts
npm run capsule:prepare

# 2) package once
ato build .

# 3) publish artifact (private/local registry)
ATO_TOKEN=pwd ato publish --registry http://127.0.0.1:18787 --artifact ./my-dynamic-app.capsule

# 4) install + run
ato install <publisher>/<slug> --registry http://127.0.0.1:18787
ato run <publisher>/<slug> --registry http://127.0.0.1:18787
```

Notes:
- For Next.js standalone, copy `.next/static` (and `public` if used) into standalone output before `ato build`.
- `ato run` stops before startup if `required_env` keys are missing.
- `services.main` is required in services mode and receives `PORT=<targets.<label>.port`.
- `targets.<label>.entrypoint = "ato-entry.ts"` is deprecated and rejected.
- If a service command starts with `node`, `python`, or `uv`, pin the matching version in `runtime_tools`.

## Runtime Isolation Policy (Tiers)

- `web/static`: Tier1 (`driver = "static"` + `targets.<label>.port` required; no `capsule.lock` needed)
- `web/deno`: Tier1 (`capsule.lock` + `deno.lock` or `package-lock.json`)
- `web/node`: Tier1 (Deno compat execution; requires `capsule.lock` + `package-lock.json`)
- `web/python`: Tier2 (requires `uv.lock`; `--sandbox` recommended)
- `source/deno`: Tier1 (`capsule.lock` + `deno.lock` or `package-lock.json`)
- `source/node`: Tier1 (Deno compat execution; requires `capsule.lock` + `package-lock.json`)
- `source/python`: Tier2 (requires `uv.lock`; `--sandbox` recommended)
- `source/native`: Tier2 (`--sandbox` recommended)

Notes:
- Node is Tier1 and does not require `--unsafe`.
- Tier2 (`source/native|python`, `web/python`) requires the `nacelle` engine.
  If not configured, execution stops fail-closed. Configure via `ato engine register`, `--nacelle`, or `NACELLE_PATH`.
- Legacy compatibility flags (`--unsafe`, `--unsafe-bypass-sandbox`) remain but are discouraged.
- Unsupported or out-of-policy Node/Python behavior does not auto-fallback; it stops fail-closed.
- `runtime=web` requires `driver` (`static|node|deno|python`).
- `public` is deprecated for `runtime=web`.
- For `runtime=web`, CLI prints the URL and does not auto-open a browser.

## SKILL Execution

```bash
# Resolve by skill name (default search paths)
ato run --skill <skill-name>

# Point to a specific SKILL.md
ato run --from-skill /path/to/SKILL.md
```

`--skill` and `--from-skill` are mutually exclusive.

## UX Policy (Silent Runner)

- Minimal output on success (tool stdout-first)
- Prompt only when explicit consent is required
- In non-interactive environments, `-y/--yes` auto-approves consent
- Policy violations and unmet requirements are emitted as `ATO_ERR_*` JSONL to `stderr`

## Security and Execution Policy (Zero-Trust / Fail-closed)

- Required env validation: startup fails if `targets.<label>.required_env` (or `ATO_ORCH_REQUIRED_ENVS`) is missing/empty
- Dangerous flag guard: `--dangerously-skip-permissions` is rejected unless `CAPSULE_ALLOW_UNSAFE=1`
- Local registry write auth: when `registry serve --auth-token` is enabled, `publish` requires `ATO_TOKEN`
- Engine auto-install: checksum retrieval/verification failures stop execution fail-closed

## Environment Variable Reference (Core)

- `CAPSULE_WATCH_DEBOUNCE_MS`: debounce interval for `open --watch` (ms, default: `300`)
- `CAPSULE_ALLOW_UNSAFE`: explicit allow for `--dangerously-skip-permissions` (only `1` is valid)
- `ATO_TOKEN`: auth token for local/private registry publish
- `ATO_STORE_API_URL`: API base URL for `ato search` / install flows (default: `https://api.ato.run`)
- `ATO_STORE_SITE_URL`: store web base URL (default: `https://store.ato.run`)
- `ATO_SESSION_TOKEN`: session token (`CAPSULE_SESSION_TOKEN` is supported for compatibility)

## Search and Auth

```bash
ato search ai
ato login
ato whoami
```

Default endpoints:
- `ATO_STORE_API_URL` (default: `https://api.ato.run`)
- `ATO_STORE_SITE_URL` (default: `https://store.ato.run`)
- `ATO_SESSION_TOKEN` (`CAPSULE_SESSION_TOKEN` is compatibility alias)

## Development Tests

```bash
cargo test -p capsule-core execution_plan:: --lib
cargo test -p ato-cli --test local_registry_e2e -- --nocapture
```

## License

Apache License 2.0 (SPDX: Apache-2.0). See [LICENSE](LICENSE).
