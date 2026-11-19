#!/usr/bin/env bash
# One-shot pipeline (run on the target host): extract -> upload images -> pack -> apply
# (+ optional step 5: queue document parsing via parse_docs.py)
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
#   PICGO_URL / DST_HOST / DST_PORT / DST_USER / DST_PASSWORD / DST_DB /
#   S3_HOST / S3_PORT / S3_AK / S3_SK / JOBS / TENANT_EMAIL
#   (TENANT_EMAIL: bind created KBs to this user's owner tenant, e.g.
#    admin@ragflow.io; baked at pack, resolved on the target at apply)
#   UPLOADER (fallback path of markdown_image_uploader.py; one shipped inside
#            the tarball wins over this)
#   SKIP_UPLOAD=1 skip step 2 (picgo upload); markdown keeps its original
#                image links and .upload_done is NOT written, so a later run
#                without SKIP_UPLOAD=1 still performs the upload
#   APPLY_EXTRA_ARGS (extra args passed to apply, e.g. "--dry-run")
#   PARSE=1      run step 5: queue parsing of unstarted docs via parse_docs.py
#   PARSE_WAIT=1 with PARSE=1: also wait for parsing to finish (default: exit
#                once everything is queued)
#   API_URL      ragflow server base URL for step 5
#                (default http://127.0.0.1:9380; from the host use
#                http://172.16.105.105:9380)
set -euo pipefail

# ========== Config (edit for your environment) ==========
# Defaults follow main's docker/.env (VB_* / MINIO_* / SVR_HTTP_PORT).
DST_HOST="${DST_HOST:-vastbase}"
DST_PORT="${DST_PORT:-5432}"
DST_USER="${DST_USER:-ragflow}"
DST_PASSWORD="${DST_PASSWORD:-Infini_Rag@123}"
DST_DB="${DST_DB:-rag_flow}"
S3_HOST="${S3_HOST:-minio}"
S3_PORT="${S3_PORT:-9000}"
S3_AK="${S3_AK:-rag_flow}"
S3_SK="${S3_SK:-infini_rag_flow}"
# Single-bucket mode (optional; empty = one bucket per KB). Also set
# S3_LAYOUT=s3 only when the server runs STORAGE_IMPL=AWS_S3 without
# prefix_path (s3_conn.py drops the kb_id/ segment in that case).
S3_BUCKET="${S3_BUCKET:-}"
S3_PREFIX_PATH="${S3_PREFIX_PATH:-}"
S3_SIGNATURE="${S3_SIGNATURE:-}"
S3_LAYOUT="${S3_LAYOUT:-}"
S3_ADDRESSING="${S3_ADDRESSING:-path}"   # path (default, for MinIO/gateways) or virtual
JOBS="${JOBS:-8}"
TENANT_EMAIL="${TENANT_EMAIL:-}"
PICGO_URL="${PICGO_URL:-http://172.16.105.105:36677}"
UPLOADER="${UPLOADER:-}"   # empty = markdown_image_uploader.py next to this script (resolved after SCRIPT_DIR)
SKIP_UPLOAD="${SKIP_UPLOAD:-true}"
WORK_ROOT="${WORK_ROOT:-./}"
APPLY_EXTRA_ARGS="${APPLY_EXTRA_ARGS:-}"
PARSE="${PARSE:-}"
PARSE_WAIT="${PARSE_WAIT:-}"
API_URL="${API_URL:-http://127.0.0.1:9380}"
# =========================================================

# Compose the object-store endpoint the SDK wants:
#   bare host          -> host:$S3_PORT (port skipped when S3_PORT is empty)
#   host:port          -> as-is
#   http(s)://... URL  -> as-is; but a URL of exactly scheme://host (no port,
#                         no path) gets :$S3_PORT appended, so
#                         S3_HOST=http://s3.example.com + S3_PORT=9000 works
case "$S3_HOST" in
    http://*|https://*)
        S3_ENDPOINT="$S3_HOST"
        if [ -n "$S3_PORT" ] && [[ "$S3_HOST" =~ ^https?://[^/:]+$ ]]; then
            S3_ENDPOINT="$S3_HOST:$S3_PORT"
        fi
        ;;
    *:*) S3_ENDPOINT="$S3_HOST" ;;
    *)   if [ -n "$S3_PORT" ]; then S3_ENDPOINT="$S3_HOST:$S3_PORT"; else S3_ENDPOINT="$S3_HOST"; fi ;;
esac

# Optional object-store flags for apply (single bucket / prefix / signer / layout)
OS_EXTRA=""
[ -n "$S3_BUCKET" ] && OS_EXTRA="$OS_EXTRA --minio-bucket $S3_BUCKET"
[ -n "$S3_PREFIX_PATH" ] && OS_EXTRA="$OS_EXTRA --minio-prefix-path $S3_PREFIX_PATH"
[ -n "$S3_SIGNATURE" ] && OS_EXTRA="$OS_EXTRA --os-signature $S3_SIGNATURE"
[ -n "$S3_LAYOUT" ] && OS_EXTRA="$OS_EXTRA --os-layout $S3_LAYOUT"
[ -n "$S3_ADDRESSING" ] && OS_EXTRA="$OS_EXTRA --os-addressing $S3_ADDRESSING"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INJECT="$SCRIPT_DIR/inject.py"
[ -f "$INJECT" ] || { echo "inject.py not found (this script must live in the offline-inject dir): $INJECT"; exit 1; }
# Anchor the default uploader to the script dir: a literal ./ would resolve
# against the caller's working directory and break "bash /path/one_shot.sh"
# launched from elsewhere. An explicitly set UPLOADER is used verbatim.
UPLOADER="${UPLOADER:-$SCRIPT_DIR/markdown_image_uploader.py}"

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

echo "==== [1/5] Extracting $TARBALL ===="
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

if [ -n "$SKIP_UPLOAD" ]; then
    echo "==== [2/5] image upload skipped (SKIP_UPLOAD set; markdown keeps original links) ===="
elif [ -f "$WORK_DIR/.upload_done" ] && [ -z "${FORCE:-}" ]; then
    echo "Images already uploaded, skipping ($WORK_DIR/.upload_done present)"
else
    if [ ! -f "$UPLOADER" ]; then
        found="$(find "$WORK_DIR" -name markdown_image_uploader.py -type f | head -1 || true)"
        [ -n "$found" ] && UPLOADER="$found"
    fi
    [ -f "$UPLOADER" ] || { echo "markdown_image_uploader.py not found (UPLOADER=$UPLOADER)"; exit 1; }
    echo "uploader: $UPLOADER"
    python3 "$UPLOADER" --dir "$SRC_DIR" --picgo-url "$PICGO_URL" 2>&1 | tee "$LOG_DIR/upload_${STAMP}.log"
    touch "$WORK_DIR/.upload_done"
fi

echo "==== [3/5] pack -> $PKG_DIR ===="
RESUME_FLAG=""
if [ -f "$PKG_DIR/manifest.json" ] && [ -z "${FORCE:-}" ]; then
    echo "Package already built, skipping ($PKG_DIR/manifest.json present)"
    # Same package re-applied: let apply consult its progress file too
    RESUME_FLAG="--resume"
else
    rm -rf "$PKG_DIR"
    if [ -n "$TENANT_EMAIL" ]; then
        echo "tenant binding: $TENANT_EMAIL"
        python3 "$INJECT" pack --src-dir "$SRC_DIR" --out "$PKG_DIR" --by-dir --ext md \
            --tenant-email "$TENANT_EMAIL" 2>&1 | tee "$LOG_DIR/pack_${STAMP}.log"
    else
        python3 "$INJECT" pack --src-dir "$SRC_DIR" --out "$PKG_DIR" --by-dir --ext md 2>&1 | tee "$LOG_DIR/pack_${STAMP}.log"
    fi
fi

echo "==== [4/5] apply (writing to $DST_HOST:$DST_PORT + $S3_ENDPOINT) ===="
# shellcheck disable=SC2086
python3 "$INJECT" apply --pkg "$PKG_DIR" \
    --dst-host "$DST_HOST" --dst-port "$DST_PORT" \
    --dst-user "$DST_USER" --dst-password "$DST_PASSWORD" --dst-meta-db "$DST_DB" \
    --minio-host "$S3_ENDPOINT" --minio-user "$S3_AK" --minio-password "$S3_SK" \
    --jobs "$JOBS" $OS_EXTRA $RESUME_FLAG $APPLY_EXTRA_ARGS 2>&1 | tee "$LOG_DIR/apply_${STAMP}.log"

if [ -n "$PARSE" ]; then
    echo "==== [5/5] queueing parse (unstarted docs -> $API_URL) ===="
    PARSE_SCRIPT="$SCRIPT_DIR/parse_docs.py"
    [ -f "$PARSE_SCRIPT" ] || { echo "parse_docs.py not found next to this script: $PARSE_SCRIPT"; exit 1; }
    TENANT_FLAG=""
    [ -n "$TENANT_EMAIL" ] && TENANT_FLAG="--tenant-email $TENANT_EMAIL"
    WAIT_FLAG=""
    [ -n "$PARSE_WAIT" ] && WAIT_FLAG="--wait"
    # shellcheck disable=SC2086
    python3 "$PARSE_SCRIPT" \
        --dst-host "$DST_HOST" --dst-port "$DST_PORT" \
        --dst-user "$DST_USER" --dst-password "$DST_PASSWORD" --dst-meta-db "$DST_DB" \
        --api-url "$API_URL" $TENANT_FLAG $WAIT_FLAG 2>&1 | tee "$LOG_DIR/parse_${STAMP}.log"
else
    echo "==== [5/5] parse skipped (set PARSE=1 to queue document parsing) ===="
fi

echo "==== Done. Logs: $LOG_DIR/ (prefix ${STAMP}), package: $PKG_DIR ===="
