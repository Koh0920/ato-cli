#!/usr/bin/env bash
set -euo pipefail

# Upload ato release archives to an R2 bucket in both versioned and latest paths.
#
# Example:
#   VERSION="0.2.0" \
#   DEPLOY_ENV="staging" \
#   TARGETS="x86_64-apple-darwin aarch64-apple-darwin x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu" \
#   ./scripts/upload_release_to_r2.sh
#
# Env:
#   VERSION             required
#   BUCKET              optional (if omitted, inferred from DEPLOY_ENV)
#   DEPLOY_ENV          optional: staging|stg|production|prod
#   SOURCE_DIR          default: /tmp/ato-release/$VERSION
#   TARGETS             default: infer from SOURCE_DIR/ato-*.tar.gz
#   UPDATE_LATEST       default: 1
#   PREFIX              default: ato
#   WRANGLER_CONFIG     optional (if empty, use wrangler default resolution)
#   WRANGLER_ENV        optional
#   REMOTE              default: 1 (set 0 to omit --remote)
#   DRY_RUN             default: 0

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "error: required command not found: $1" >&2
    exit 1
  }
}

resolve_bucket_from_env() {
  case "${DEPLOY_ENV:-}" in
    staging|stg) echo "ato-releases-stg" ;;
    production|prod) echo "ato-releases-prod" ;;
    *) echo "" ;;
  esac
}

put_object() {
  local object_key="$1"
  local source_file="$2"
  local cache_control="${3:-}"
  local object_ref="${BUCKET}/${object_key}"
  local -a cmd

  cmd=(wrangler)
  if [[ -n "$WRANGLER_CONFIG" ]]; then
    cmd+=(--config "$WRANGLER_CONFIG")
  fi
  if [[ -n "$WRANGLER_ENV" ]]; then
    cmd+=(--env "$WRANGLER_ENV")
  fi
  cmd+=(r2 object put "$object_ref" --file "$source_file")
  if [[ -n "$cache_control" ]]; then
    cmd+=(--cache-control "$cache_control")
  fi
  if [[ "$REMOTE" == "1" ]]; then
    cmd+=(--remote)
  fi

  if [[ "$DRY_RUN" == "1" ]]; then
    echo "[dry-run] ${cmd[*]}"
    return
  fi

  "${cmd[@]}"
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

need_cmd wrangler
need_cmd find

VERSION="${VERSION:-}"
DEPLOY_ENV="${DEPLOY_ENV:-}"
BUCKET="${BUCKET:-$(resolve_bucket_from_env)}"
if [[ -z "$VERSION" ]]; then
  echo "error: VERSION is required" >&2
  exit 1
fi
if [[ -z "$BUCKET" ]]; then
  echo "error: BUCKET is required (or set DEPLOY_ENV=staging|production)" >&2
  exit 1
fi

SOURCE_DIR="${SOURCE_DIR:-/tmp/ato-release/${VERSION}}"
TARGETS="${TARGETS:-}"
UPDATE_LATEST="${UPDATE_LATEST:-1}"
PREFIX="${PREFIX:-ato}"
DEFAULT_WRANGLER_CONFIG="$WORKSPACE_ROOT/apps/ato-store/wrangler.toml"
if [[ "${WRANGLER_CONFIG+x}" == "x" ]]; then
  WRANGLER_CONFIG="${WRANGLER_CONFIG}"
elif [[ -f "$DEFAULT_WRANGLER_CONFIG" ]]; then
  WRANGLER_CONFIG="$DEFAULT_WRANGLER_CONFIG"
else
  WRANGLER_CONFIG=""
fi
WRANGLER_ENV="${WRANGLER_ENV:-}"
REMOTE="${REMOTE:-1}"
DRY_RUN="${DRY_RUN:-0}"
RELEASE_CACHE_CONTROL="${RELEASE_CACHE_CONTROL:-public, max-age=31536000, immutable}"
LATEST_CACHE_CONTROL="${LATEST_CACHE_CONTROL:-no-store, max-age=0}"

if [[ -n "$WRANGLER_CONFIG" && ! -f "$WRANGLER_CONFIG" ]]; then
  echo "error: WRANGLER_CONFIG not found: $WRANGLER_CONFIG" >&2
  exit 1
fi

if [[ ! -d "$SOURCE_DIR" ]]; then
  echo "error: SOURCE_DIR not found: $SOURCE_DIR" >&2
  exit 1
fi

target_list=()
if [[ -n "$TARGETS" ]]; then
  read -r -a target_list <<<"$TARGETS"
else
  while IFS= read -r archive; do
    archive_name="$(basename "$archive")"
    target="${archive_name#ato-}"
    target="${target%.tar.gz}"
    target_list+=("$target")
  done < <(find "$SOURCE_DIR" -maxdepth 1 -type f -name 'ato-*.tar.gz' | sort)
fi

if [[ "${#target_list[@]}" -eq 0 ]]; then
  echo "error: no targets detected. Set TARGETS or place ato-<target>.tar.gz in $SOURCE_DIR" >&2
  exit 1
fi

checksum_file="$SOURCE_DIR/SHA256SUMS"
if [[ ! -f "$checksum_file" ]]; then
  echo "error: SHA256SUMS not found: $checksum_file" >&2
  exit 1
fi

for target in "${target_list[@]}"; do
  archive_file="$SOURCE_DIR/ato-$target.tar.gz"
  if [[ ! -f "$archive_file" ]]; then
    echo "error: archive not found for target '$target': $archive_file" >&2
    exit 1
  fi

  put_object "$PREFIX/releases/$VERSION/ato-$target.tar.gz" "$archive_file" "$RELEASE_CACHE_CONTROL"

  if [[ "$UPDATE_LATEST" == "1" ]]; then
    put_object "$PREFIX/latest/ato-$target.tar.gz" "$archive_file" "$LATEST_CACHE_CONTROL"
  fi
done

put_object "$PREFIX/releases/$VERSION/SHA256SUMS" "$checksum_file" "$RELEASE_CACHE_CONTROL"
if [[ "$UPDATE_LATEST" == "1" ]]; then
  put_object "$PREFIX/latest/SHA256SUMS" "$checksum_file" "$LATEST_CACHE_CONTROL"
fi

echo "==> upload completed"
echo "    bucket : $BUCKET"
echo "    env    : ${DEPLOY_ENV:-<manual>}"
echo "    version: $VERSION"
echo "    prefix : $PREFIX"
echo "    targets: ${target_list[*]}"
echo "    latest : $UPDATE_LATEST"
echo "    remote : $REMOTE"
echo "    config : ${WRANGLER_CONFIG:-<wrangler default>}"
echo "    cache(versioned): $RELEASE_CACHE_CONTROL"
echo "    cache(latest)   : $LATEST_CACHE_CONTROL"
