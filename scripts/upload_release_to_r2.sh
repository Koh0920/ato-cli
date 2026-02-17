#!/usr/bin/env bash
set -euo pipefail

# Upload ato release archives to an R2 bucket in both versioned and latest paths.
#
# Example:
#   VERSION="0.2.0" \
#   BUCKET="ato-store-artifacts-stg" \
#   TARGETS="x86_64-apple-darwin aarch64-apple-darwin x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu" \
#   ./scripts/upload_release_to_r2.sh
#
# Env:
#   VERSION             required
#   BUCKET              required
#   SOURCE_DIR          default: /tmp/ato-release/$VERSION
#   TARGETS             default: infer from SOURCE_DIR/ato-*.tar.gz
#   UPDATE_LATEST       default: 1
#   PREFIX              default: ato
#   WRANGLER_CONFIG     default: ../../ato-store/wrangler.toml (from this script dir)
#   WRANGLER_ENV        optional
#   DRY_RUN             default: 0

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "error: required command not found: $1" >&2
    exit 1
  }
}

put_object() {
  local object_key="$1"
  local source_file="$2"
  local object_ref="${BUCKET}/${object_key}"

  if [[ "$DRY_RUN" == "1" ]]; then
    echo "[dry-run] wrangler r2 object put ${object_ref} --file ${source_file}"
    return
  fi

  if [[ -n "$WRANGLER_ENV" ]]; then
    wrangler --config "$WRANGLER_CONFIG" --env "$WRANGLER_ENV" r2 object put "$object_ref" --file "$source_file"
  else
    wrangler --config "$WRANGLER_CONFIG" r2 object put "$object_ref" --file "$source_file"
  fi
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

need_cmd wrangler
need_cmd find

VERSION="${VERSION:-}"
BUCKET="${BUCKET:-}"
if [[ -z "$VERSION" ]]; then
  echo "error: VERSION is required" >&2
  exit 1
fi
if [[ -z "$BUCKET" ]]; then
  echo "error: BUCKET is required" >&2
  exit 1
fi

SOURCE_DIR="${SOURCE_DIR:-/tmp/ato-release/${VERSION}}"
TARGETS="${TARGETS:-}"
UPDATE_LATEST="${UPDATE_LATEST:-1}"
PREFIX="${PREFIX:-ato}"
WRANGLER_CONFIG="${WRANGLER_CONFIG:-$WORKSPACE_ROOT/apps/ato-store/wrangler.toml}"
WRANGLER_ENV="${WRANGLER_ENV:-}"
DRY_RUN="${DRY_RUN:-0}"

if [[ ! -f "$WRANGLER_CONFIG" ]]; then
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

  put_object "$PREFIX/releases/$VERSION/ato-$target.tar.gz" "$archive_file"

  if [[ "$UPDATE_LATEST" == "1" ]]; then
    put_object "$PREFIX/latest/ato-$target.tar.gz" "$archive_file"
  fi
done

put_object "$PREFIX/releases/$VERSION/SHA256SUMS" "$checksum_file"
if [[ "$UPDATE_LATEST" == "1" ]]; then
  put_object "$PREFIX/latest/SHA256SUMS" "$checksum_file"
fi

echo "==> upload completed"
echo "    bucket : $BUCKET"
echo "    version: $VERSION"
echo "    prefix : $PREFIX"
echo "    targets: ${target_list[*]}"
echo "    latest : $UPDATE_LATEST"
