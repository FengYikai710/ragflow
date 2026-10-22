#!/usr/bin/env bash
# One-shot pipeline (run on the target host): extract -> upload images -> pack -> apply
#
# Resume-safe: each step leaves a completion marker and is skipped on re-run,
# so after an interruption the script continues where it stopped:
#   - extract:  $WORK_DIR/.extract_done
#   - upload:   $WORK_DIR/.upload_done
#   - pack:     $PKG_DIR/manifest.json (pack writes it atomically last)
#   - apply:    idempotent by content hash; --resume is passed automatically
#               when re-applying an existing package
# Force a full re-run from scratch (re-extract, re-upload, re-pack): FORCE=1
# NOTE: editing files inside $WORK_DIR is NOT picked up unless you FORCE=1
# (or delete $PKG_DIR) — the markers below assume untouched inputs.
#
# Usage:
#   ./one_shot.sh /path/to/vastdata_docs.tar.gz
#   ./one_shot.sh                        # no arg: use vastdata_docs.tar.gz next to this script
#
# Override defaults via environment variables (see config block below):
#   PICGO_URL / DST_HOST / DST_PORT / DST_USER / DST_PASSWORD /
#   MINIO_HOST / MINIO_USER / MINIO_PASSWORD / JOBS /
#   UPLOADER (fallback path of markdown_image_uploader.py; one shipped inside
#            the tarball wins over this)
#   APPLY_EXTRA_ARGS (extra args passed to apply, e.g. "--dry-run")
#   SKIP_UPLOAD_CHECK=1 (do not abort on image-upload failures)
set -euo pipefail

# ========== Config (edit for your environment) ==========
DST_HOST="${DST_HOST:-vastbase}"
DST_PORT="${DST_PORT:-5432}"
DST_USER="${DST_USER:-ragflow}"
DST_PASSWORD="${DST_PASSWORD:-Ragflow@123}"
MINIO_HOST="${MINIO_HOST:-minio:9000}"
MINIO_USER="${MINIO_USER:-rag_flow}"
MINIO_PASSWORD="${MINIO_PASSWORD:-infini_rag_flow}"
JOBS="${JOBS:-8}"
PICGO_URL="${PICGO_URL:-http://172.16.105.105:36677}"
UPLOADER="${UPLOADER:-./markdown_image_uploader.py}"
WORK_ROOT="${WORK_ROOT:-./}"
APPLY_EXTRA_ARGS="${APPLY_EXTRA_ARGS:-}"
# =========================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INJECT="$SCRIPT_DIR/inject.py"
[ -f "$INJECT" ] || { echo "inject.py not found (this script must live in the offline-inject dir): $INJECT"; exit 1; }

# First arg = tarball path; default to vastdata_docs.tar.gz next to this script
TARBALL="${1:-$SCRIPT_DIR/vastdata_docs.tar.gz}"
[ -f "$TARBALL" ] || { echo "Tarball not found: $TARBALL"; exit 1; }

LOG_DIR="$WORK_ROOT/offline_inject_logs"
mkdir -p "$LOG_DIR"
STAMP="$(date +%Y%m%d_%H%M%S)"

NAME="$(basename "$TARBALL" | sed -E 's/\.(tar\.gz|tgz|tar)$//')"
WORK_DIR="$WORK_ROOT/${NAME}_extracted"
PKG_DIR="$WORK_ROOT/inject_${NAME}"
SRC_DIR="$WORK_DIR/vastdata_docs"

echo "==== [1/4] Extracting $TARBALL ===="
if [ -f "$WORK_DIR/.extract_done" ] && [ -z "${FORCE:-}" ]; then
    echo "Already extracted, skipping ($WORK_DIR/.extract_done present; FORCE=1 to redo)"
else
    rm -rf "$WORK_DIR"
    mkdir -p "$WORK_DIR"
    tar xzf "$TARBALL" -C "$WORK_DIR"
    # macOS tarballs carry ._* AppleDouble files; drop them or the uploader
    # would treat them as markdown
    find "$WORK_DIR" -name "._*" -delete
    MD_COUNT="$(find "$SRC_DIR" -name '*.md' | wc -l | tr -d ' ')"
    echo "Extracted to: $WORK_DIR ($MD_COUNT md files)"
    echo "One KB per first-level directory:"
    find "$SRC_DIR" -mindepth 1 -maxdepth 1 -type d ! -name '.*' -exec basename {} \;
    [ "$MD_COUNT" -gt 0 ] || { echo "No .md files found after extraction; check the tarball layout first"; exit 1; }
    touch "$WORK_DIR/.extract_done"
fi

echo "==== [2/4] Uploading images to $PICGO_URL ===="
if [ -f "$WORK_DIR/.upload_done" ] && [ -z "${FORCE:-}" ]; then
    echo "Images already uploaded, skipping ($WORK_DIR/.upload_done present)"
else
    if [ ! -f "$UPLOADER" ]; then
        found="$(find "$WORK_DIR" -name markdown_image_uploader.py -type f | head -1 || true)"
        [ -n "$found" ] && UPLOADER="$found"
    fi
    [ -f "$UPLOADER" ] || { echo "markdown_image_uploader.py not found (UPLOADER=$UPLOADER)"; exit 1; }
    echo "uploader: $UPLOADER"
    python3 "$UPLOADER" --dir "$SRC_DIR" --picgo-url "$PICGO_URL" 2>&1 | tee "$LOG_DIR/upload_${STAMP}.log"
    # Abort on any uploader failure diagnostic (matches every failure message
    # markdown_image_uploader.py can print; success paths never match).
    if [ -z "${SKIP_UPLOAD_CHECK:-}" ] && grep -qE 'upload failed|request error|decode failed|not found|invalid data URI|unsupported data URI' "$LOG_DIR/upload_${STAMP}.log"; then
        echo "!! Image upload reported failures (see $LOG_DIR/upload_${STAMP}.log); aborting. Re-run with SKIP_UPLOAD_CHECK=1 to ignore."
        exit 1
    fi
    touch "$WORK_DIR/.upload_done"
fi

echo "==== [3/4] pack -> $PKG_DIR ===="
RESUME_FLAG=""
if [ -f "$PKG_DIR/manifest.json" ] && [ -z "${FORCE:-}" ]; then
    echo "Package already built, skipping ($PKG_DIR/manifest.json present)"
    # Same package re-applied: let apply consult its progress file too
    RESUME_FLAG="--resume"
else
    rm -rf "$PKG_DIR"
    python3 "$INJECT" pack --src-dir "$SRC_DIR" --out "$PKG_DIR" --by-dir --ext md 2>&1 | tee "$LOG_DIR/pack_${STAMP}.log"
fi

echo "==== [4/4] apply (writing to $DST_HOST:$DST_PORT + $MINIO_HOST) ===="
# shellcheck disable=SC2086
python3 "$INJECT" apply --pkg "$PKG_DIR" \
    --dst-host "$DST_HOST" --dst-port "$DST_PORT" \
    --dst-user "$DST_USER" --dst-password "$DST_PASSWORD" \
    --minio-host "$MINIO_HOST" --minio-user "$MINIO_USER" --minio-password "$MINIO_PASSWORD" \
    --jobs "$JOBS" $RESUME_FLAG $APPLY_EXTRA_ARGS 2>&1 | tee "$LOG_DIR/apply_${STAMP}.log"

echo "==== Done. Logs: $LOG_DIR/ (prefix ${STAMP}), package: $PKG_DIR ===="
