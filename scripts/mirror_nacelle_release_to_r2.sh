#!/usr/bin/env bash
set -euo pipefail

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "error: required command not found: $1" >&2
    exit 1
  }
}

resolve_bucket_from_env() {
  case "${DEPLOY_ENV:-}" in
    staging|stg) echo "${DEFAULT_BUCKET_STAGING:-}" ;;
    production|prod) echo "${DEFAULT_BUCKET_PRODUCTION:-}" ;;
    *) echo "" ;;
  esac
}

put_object() {
  local object_key="$1"
  local source_file="$2"
  local object_ref="${BUCKET}/${object_key}"
  local -a cmd

  cmd=(wrangler)
  if [[ -n "${WRANGLER_CONFIG:-}" ]]; then
    cmd+=(--config "$WRANGLER_CONFIG")
  fi
  if [[ -n "${WRANGLER_ENV:-}" ]]; then
    cmd+=(--env "$WRANGLER_ENV")
  fi
  cmd+=(r2 object put "$object_ref" --file "$source_file")
  if [[ "${REMOTE:-1}" == "1" ]]; then
    cmd+=(--remote)
  fi

  "${cmd[@]}"
}

need_cmd awk
need_cmd curl
need_cmd mktemp
need_cmd wrangler

NACELLE_VERSION="${NACELLE_VERSION:-}"
UPSTREAM_BASE_URL="${UPSTREAM_BASE_URL:-https://releases.capsule.dev/nacelle}"
DEFAULT_BUCKET_STAGING="${DEFAULT_BUCKET_STAGING:-}"
DEFAULT_BUCKET_PRODUCTION="${DEFAULT_BUCKET_PRODUCTION:-}"
BUCKET="${BUCKET:-$(resolve_bucket_from_env)}"

if [[ -z "$NACELLE_VERSION" ]]; then
  echo "error: NACELLE_VERSION is required" >&2
  exit 1
fi

if [[ -z "$BUCKET" ]]; then
  echo "error: BUCKET is required (or set DEPLOY_ENV plus DEFAULT_BUCKET_STAGING/DEFAULT_BUCKET_PRODUCTION)" >&2
  exit 1
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT INT HUP TERM

platforms=(
  "darwin-x64"
  "darwin-arm64"
  "linux-x64"
  "linux-arm64"
)

checksum_file="$TMP_DIR/SHA256SUMS"
: > "$checksum_file"

for platform in "${platforms[@]}"; do
  binary_name="nacelle-${NACELLE_VERSION}-${platform}"
  version_dir="${UPSTREAM_BASE_URL%/}/${NACELLE_VERSION}"
  binary_url="${version_dir}/${binary_name}"
  sha_url="${binary_url}.sha256"
  binary_path="$TMP_DIR/${binary_name}"
  sha_path="$TMP_DIR/${binary_name}.sha256"

  echo "==> Mirroring ${binary_name}" >&2
  curl -fsSL --retry 3 --connect-timeout 15 --max-time 300 "$binary_url" -o "$binary_path"
  curl -fsSL --retry 3 --connect-timeout 15 --max-time 60 "$sha_url" -o "$sha_path"

  hash="$(awk '/[[:xdigit:]]{64}/ { print $1; exit }' "$sha_path" | tr '[:upper:]' '[:lower:]')"
  if [[ ! "$hash" =~ ^[[:xdigit:]]{64}$ ]]; then
    echo "error: invalid sha256 content in ${sha_url}" >&2
    exit 1
  fi

  printf '%s  %s\n' "$hash" "$binary_name" >> "$checksum_file"

  put_object "nacelle/${NACELLE_VERSION}/${binary_name}" "$binary_path"
  put_object "nacelle/${NACELLE_VERSION}/${binary_name}.sha256" "$sha_path"
done

printf '%s\n' "$NACELLE_VERSION" > "$TMP_DIR/latest.txt"

put_object "nacelle/${NACELLE_VERSION}/SHA256SUMS" "$checksum_file"
put_object "nacelle/latest.txt" "$TMP_DIR/latest.txt"

echo "Mirrored nacelle ${NACELLE_VERSION} to ${BUCKET}" >&2
