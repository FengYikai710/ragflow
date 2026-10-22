#!/usr/bin/env bash
# 一键流水线（在目标环境跑）：解压交付包 → 图片上图床 → pack → apply
#
# 用法:
#   ./one_shot.sh /path/to/vastdata_docs.tar.gz
#   ./one_shot.sh                        # 不传参时自动找脚本同目录下的 vastdata_docs.tar.gz
#
# 可用环境变量覆盖默认值（见下方配置区）：
#   PICGO_URL / DST_HOST / DST_USER / DST_PASSWORD /
#   MINIO_HOST / MINIO_USER / MINIO_PASSWORD / JOBS /
#   UPLOADER（markdown_image_uploader.py 兜底路径，包里带了就用包里的）
#   APPLY_EXTRA_ARGS（透传给 apply，如 "--dry-run"）
#   SKIP_UPLOAD_CHECK=1（图片有失败项时不中止）
set -euo pipefail

# ========== 配置区（按环境改这里）==========
DST_HOST="${DST_HOST:-vastbase}"
DST_USER="${DST_USER:-ragflow}"
DST_PASSWORD="${DST_PASSWORD:-Ragflow@123}"
MINIO_HOST="${MINIO_HOST:-minio:9000}"
MINIO_USER="${MINIO_USER:-rag_flow}"
MINIO_PASSWORD="${MINIO_PASSWORD:-infini_rag_flow}"
JOBS="${JOBS:-8}"
PICGO_URL="${PICGO_URL:-http://172.16.105.105:36677}"
UPLOADER="${UPLOADER:-/tmp/vds_upload/markdown_image_uploader.py}"
WORK_ROOT="${WORK_ROOT:-/tmp}"
APPLY_EXTRA_ARGS="${APPLY_EXTRA_ARGS:-}"
# =========================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INJECT="$SCRIPT_DIR/inject.py"
[ -f "$INJECT" ] || { echo "找不到 inject.py（脚本应放在 offline-inject 目录里）: $INJECT"; exit 1; }

# 第一个参数 = 压缩包路径；不传则默认脚本同目录下的 vastdata_docs.tar.gz
TARBALL="${1:-$SCRIPT_DIR/vastdata_docs.tar.gz}"
[ -f "$TARBALL" ] || { echo "压缩包不存在: $TARBALL"; exit 1; }

LOG_DIR="$WORK_ROOT/offline_inject_logs"
mkdir -p "$LOG_DIR"
STAMP="$(date +%Y%m%d_%H%M%S)"

echo "==== [1/4] 解压 $TARBALL ===="
NAME="$(basename "$TARBALL" | sed -E 's/\.(tar\.gz|tgz|tar)$//')"
WORK_DIR="$WORK_ROOT/${NAME}_extracted"
PKG_DIR="$WORK_ROOT/inject_${NAME}"
rm -rf "$WORK_DIR" "$PKG_DIR"
mkdir -p "$WORK_DIR"
tar xzf "$TARBALL" -C "$WORK_DIR"
# macOS 打包会带 ._* AppleDouble 文件，清掉，免得被当作 md 处理
find "$WORK_DIR" -name "._*" -delete

SRC_DIR="$WORK_DIR"
MD_COUNT="$(find "$SRC_DIR" -name '*.md' | wc -l | tr -d ' ')"
echo "解压到: $WORK_DIR （${MD_COUNT} 个 md）"
echo "按目录分库，一级子目录（每个 = 一个 KB）:"
find "$SRC_DIR" -mindepth 1 -maxdepth 1 -type d ! -name '.*' -exec basename {} \;
[ "$MD_COUNT" -gt 0 ] || { echo "解压后一个 .md 都没有，先确认包内容/层级再跑"; exit 1; }

echo "==== [2/4] 图片上图床 $PICGO_URL ===="
if [ ! -f "$UPLOADER" ]; then
    found="$(find "$WORK_DIR" -name markdown_image_uploader.py -type f | head -1 || true)"
    [ -n "$found" ] && UPLOADER="$found"
fi
[ -f "$UPLOADER" ] || { echo "找不到 markdown_image_uploader.py（UPLOADER=$UPLOADER）"; exit 1; }
echo "uploader: $UPLOADER"
python3 "$UPLOADER" --dir "$SRC_DIR" --picgo-url "$PICGO_URL" 2>&1 | tee "$LOG_DIR/upload_${STAMP}.log"
if [ -z "${SKIP_UPLOAD_CHECK:-}" ] && grep -qE '上传失败|请求异常|文件不存在|路径无法访问|无法解析' "$LOG_DIR/upload_${STAMP}.log"; then
    echo "!! 图片上传存在失败项（见 $LOG_DIR/upload_${STAMP}.log），中止。确认无碍可 SKIP_UPLOAD_CHECK=1 重跑"
    exit 1
fi

echo "==== [3/4] pack → $PKG_DIR ===="
python3 "$INJECT" pack --src-dir "$SRC_DIR" --out "$PKG_DIR" --by-dir --ext md 2>&1 | tee "$LOG_DIR/pack_${STAMP}.log"

echo "==== [4/4] apply（写入 $DST_HOST + $MINIO_HOST）===="
# shellcheck disable=SC2086
python3 "$INJECT" apply --pkg "$PKG_DIR" \
    --dst-host "$DST_HOST" --dst-user "$DST_USER" --dst-password "$DST_PASSWORD" \
    --minio-host "$MINIO_HOST" --minio-user "$MINIO_USER" --minio-password "$MINIO_PASSWORD" \
    --jobs "$JOBS" $APPLY_EXTRA_ARGS 2>&1 | tee "$LOG_DIR/apply_${STAMP}.log"

echo "==== 完成。日志在 $LOG_DIR/（${STAMP} 前缀），包在 $PKG_DIR ===="
