#!/usr/bin/env python3
"""
RAGFlow offline document injector (pack / apply, no parsing).

Replicates the write side of FileService.upload_document
(api/db/services/file_service.py) so a delivery directory can be handed
over as an offline package and injected into a target deployment:

  pack  (deliverer, offline)  local dir -> package (objects/ + manifest.json)
  apply (target side)         package   -> MinIO objects + rag_flow rows

Documents are created in UNSTART state (run="0"): visible in the KB, no
task rows, no Redis messages, nothing parsed. Parsing is triggered later
from the UI or via POST /api/v1/datasets/<kb_id>/documents/parse.

What apply writes (same footprint as the upload API):
  - object bytes -> MinIO, key layout identical to RAGFlowMinio
                    (rag/utils/minio_conn.py: default bucket + kb_id prefix)
  - one row      -> document  (name/location deduped, xxh128 content hash)
  - one row      -> file      (leaf under the KB folder chain)
  - one row      -> file2document

Naming: by explicit requirement, a document is named after its ORIGINAL
relative directory path + filename (non-ASCII preserved), e.g.
"版本发布说明/VDS V3.9.md" — NOT the upload API's ASCII sanitize_path
folding. name == location; MinIO object keys are UTF-8 so Chinese path
segments round-trip as-is. Because real filesystem paths are unique per
KB, path+filename names cannot collide the way folded names did.

Throughput: apply runs --jobs concurrent workers (hash-check SELECTs on
per-thread autocommit connections, MinIO uploads) and batches every 20
files into ONE transaction (3 batched INSERTs + a single COMMIT) —
per-file commits made apply I/O-bound on WAL fsyncs.

Idempotency: a file whose content_hash already exists in the target KB
is skipped whatever its name (missing file/file2document rows get
repaired), so re-running apply is safe. Same-content-different-name
packages therefore never duplicate documents either.

Usage:
    # 1a. pack a delivery directory (single KB): document names are the
    #     original relative path + filename, e.g. "子目录/文件.md"
    python inject.py pack --src-dir ./docs --out ./init_pkg

    # 1b. pack per-product: each first-level sub-directory is its own KB,
    #     only .md files, optionally scoped to selected directories;
    #     --template-kb bakes the KB-clone config source into the manifest
    python inject.py pack --src-dir ~/Repository/dev420/001export --out ./init_pkg \\
        --by-dir --ext md --only VDS_v3.9.2 --template-kb <kb_id>

    # 2a. apply a single-KB package into the target (Vastbase + MinIO reachable)
    python inject.py apply --pkg ./init_pkg --kb-id <kb_id> \\
        --dst-host vb-dst --dst-user rag_flow --dst-password '***' \\
        --minio-host minio:9000 --minio-user rag_flow --minio-password '***'

    # 2b. apply a --by-dir package: KBs are reused by name or created
    #     cloned from --template-kb (embd/parser/tenant config source)
    python inject.py apply --pkg ./init_pkg --template-kb <existing_kb_id> \\
        --dst-host vb-dst --dst-user rag_flow --dst-password '***' \\
        --minio-host minio:9000 --minio-user rag_flow --minio-password '***' \\
        [--dry-run]

    # 3. later, start parsing (UI, or):
    curl -X POST -H "Authorization: Bearer <API-KEY>" \\
         -H "Content-Type: application/json" \\
         -d '{"document_ids": ["..."]}' \\
         http://<target>/api/v1/datasets/<kb_id>/documents/parse

Dependencies: psycopg2 / minio / xxhash / PyYAML (repo .venv has them).
Standalone script — the minimal Vastbase writer is embedded below
(trimmed from ../vb-to-vb-migration/vb_writer.py).
"""

import argparse
import json
import logging
import os
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from io import BytesIO
from pathlib import Path

import psycopg2  # noqa: E402
from psycopg2 import sql as pgsql  # noqa: E402
from psycopg2.extras import execute_batch, execute_values  # noqa: E402

import xxhash  # noqa: E402
from minio import Minio  # noqa: E402
from minio.error import S3Error  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("inject")


# ── embedded minimal Vastbase writer ────────────────────────────────────
# Standalone-script copy, trimmed from ../vb-to-vb-migration/vb_writer.py:
# connection management, introspection, and the one upsert path inject
# needs. Migration-only parts (schema creation, vector/fulltext indexes,
# chunk bulk-insert) are deliberately not carried over.


class VBWriter:
    """Vastbase metadata writer: connect/reconnect + introspection + upsert."""

    MAX_CONNECT_RETRIES = 24
    CONNECT_RETRY_DELAY = 5  # seconds

    def __init__(self, host: str = "localhost", port: int = 5432, user: str = "rag_flow", password: str = "", database: str = "rag_flow"):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._connect()

    def _connect(self):
        """(Re)connect to Vastbase, retrying while the server may still be
        coming up (e.g. restarting after a data load)."""
        last_error = None
        for attempt in range(1, self.MAX_CONNECT_RETRIES + 1):
            try:
                self.conn = psycopg2.connect(
                    host=self._host,
                    port=self._port,
                    user=self._user,
                    password=self._password,
                    dbname=self._database,
                    keepalives=1,
                    keepalives_idle=60,
                    keepalives_interval=30,
                    keepalives_count=10,
                    options="-c standard_conforming_strings=on -c backslash_quote=off",
                )
                self.conn.autocommit = False
                with self.conn.cursor() as cur:
                    cur.execute("SET standard_conforming_strings = on")
                    cur.execute("SET backslash_quote = off")
                    cur.execute("SET escape_string_warning = off")
                    cur.execute("SET client_encoding = 'UTF8'")
                    self.conn.commit()
                logger.info(f"Connected to Vastbase at {self._host}:{self._port}, database: {self._database}")
                return
            except Exception as e:
                last_error = e
                logger.warning(f"Vastbase connection attempt {attempt}/{self.MAX_CONNECT_RETRIES} failed: {e}. Retrying in {self.CONNECT_RETRY_DELAY}s...")
                time.sleep(self.CONNECT_RETRY_DELAY)
        raise last_error

    def _ensure_connection(self):
        """Reconnect if the connection is closed or stale. Checks both
        client-side conn.closed and a SELECT 1 probe to catch server-side
        disconnections the client hasn't noticed (idle timeout, restart)."""
        if self.conn.closed:
            logger.warning("Vastbase connection was closed, reconnecting...")
            self._connect()
            return
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1")
        except Exception:
            logger.warning("Vastbase connection is stale, reconnecting...")
            try:
                self.conn.close()
            except Exception:
                pass
            self._connect()

    def health_check(self) -> bool:
        try:
            self._ensure_connection()
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"Vastbase health check failed: {e}")
            return False

    def table_exists(self, table_name: str) -> bool:
        self._ensure_connection()
        # Clear any aborted transaction from previous errors
        try:
            self.conn.rollback()
        except Exception:
            pass
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)",
                (table_name,),
            )
            return cur.fetchone()[0]

    def get_columns(self, table_name: str) -> list[str]:
        """Ordered column names of a table. Opens with conn.rollback() —
        never call mid-transaction: staged writes would be discarded."""
        self._ensure_connection()
        try:
            self.conn.rollback()
        except Exception:
            pass
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = %s ORDER BY ordinal_position",
                    (table_name,),
                )
                return [r[0] for r in cur.fetchall()]
        except Exception as e:
            logger.warning(f"Cannot query columns for {table_name}: {e}")
            return []

    def upsert_batch(self, table_name: str, rows: list[dict], pk_cols: list[str]) -> int:
        """INSERT ... ON CONFLICT (pk_cols) DO UPDATE via execute_values.
        No RETURNING clause: Vastbase B mode rejects it combined with
        ON CONFLICT. Retries transient errors up to 3 times."""
        max_retries = 3
        for attempt in range(max_retries):
            result = self._upsert_batch_attempt(table_name, rows, pk_cols)
            if result >= 0:
                return result
            logger.warning(f"  Transient error on upsert attempt {attempt + 1}/{max_retries}, retrying...")
            time.sleep(0.5)
        logger.error(f"  All {max_retries} upsert attempts failed for batch into {table_name}")
        return 0

    def _upsert_batch_attempt(self, table_name: str, rows: list[dict], pk_cols: list[str]) -> int:
        """Single upsert attempt. Returns rows in batch, or -1 on transient error."""
        self._ensure_connection()
        if not rows:
            return 0

        # Ordered union of all row keys (dict.fromkeys preserves order + dedups).
        all_columns = list(dict.fromkeys(k for row in rows for k in row.keys()))
        col_identifiers = pgsql.SQL(", ").join(pgsql.Identifier(c) for c in all_columns)

        if pk_cols:
            pk_set = set(pk_cols)
            conflict_target = pgsql.SQL(", ").join(pgsql.Identifier(c) for c in pk_cols)
            non_pk = [c for c in all_columns if c not in pk_set]
            if non_pk:
                set_clause = pgsql.SQL(", ").join(pgsql.SQL("{col} = EXCLUDED.{col}").format(col=pgsql.Identifier(c)) for c in non_pk)
                conflict_action = pgsql.SQL("DO UPDATE SET {set_clause}").format(set_clause=set_clause)
            else:
                conflict_action = pgsql.SQL("DO NOTHING")
            conflict_clause = pgsql.SQL(" ON CONFLICT ({conflict}) {action}").format(conflict=conflict_target, action=conflict_action)
        else:
            # No primary key -> plain INSERT; caller must handle idempotency.
            conflict_clause = pgsql.SQL("")

        stmt = pgsql.SQL("INSERT INTO {table} ({columns}) VALUES %s{conflict}").format(table=pgsql.Identifier(table_name), columns=col_identifiers, conflict=conflict_clause)
        values = [tuple(row.get(c) for c in all_columns) for row in rows]

        try:
            with self.conn.cursor() as cur:
                execute_values(cur, stmt, values, page_size=100)
        except Exception as e:
            logger.warning(f"  Batch upsert failed for {table_name}: {str(e)[:200]}")
            self.conn.rollback()
            return -1

        self.conn.commit()
        return len(rows)

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("Vastbase connection closed")


MANIFEST_FILE = "manifest.json"
OBJECTS_DIRNAME = "objects"
PROGRESS_FILE = ".offline_inject_progress.json"

# ── constants mirrored from the server (pointers included) ─────────────
KNOWLEDGEBASE_FOLDER_NAME = ".knowledgebase"  # api/db/__init__.py:76
FILE_NAME_LEN_LIMIT = 255  # api/constants.py:26
UNSTART = "0"  # common/constants.py TaskStatus.UNSTART
FOLDER_TYPE = "folder"  # FileType.FOLDER
FILE_SOURCE_KB = "knowledgebase"  # FileSource.KNOWLEDGEBASE

# Extension lists mirror api/utils/file_utils.py:58-80 (filename_type).
_DOC_EXTS = (
    r"msg|eml|doc|docx|ppt|pptx|yml|xml|htm|json|jsonl|ldjson|csv|txt|ini|xls|xlsx"
    r"|wps|rtf|hlp|pages|numbers|key|md|mdx|py|js|java|c|cpp|h|php|go|ts|sh|cs|kt"
    r"|html|sql|epub"
)
_AURAL_EXTS = r"wav|flac|ape|alac|wavpack|wv|mp3|aac|ogg|vorbis|opus"
_VISUAL_EXTS = (
    r"jpg|jpeg|png|tif|gif|pcx|tga|exif|fpx|svg|psd|cdr|pcd|dxf|ufo|eps|ai|raw|WMF"
    r"|webp|avif|apng|icon|ico|mpg|mpeg|avi|rm|rmvb|mov|wmv|asf|dat|asx|wvx|mpe|mpa"
    r"|mp4|mkv"
)


def filename_type(filename: str) -> str:
    """Mirror of api/utils/file_utils.py:58 filename_type()."""
    base = os.path.basename(filename).strip().lower()
    if not base or len(base) > FILE_NAME_LEN_LIMIT:
        return "other"
    if re.match(r".*\.pdf$", base):
        return "pdf"
    if re.match(rf".*\.({_DOC_EXTS})$", base):
        return "doc"
    if re.match(rf".*\.({_AURAL_EXTS})$", base):
        return "aural"
    if re.match(rf".*\.({_VISUAL_EXTS})$", base):
        return "visual"
    return "other"


def get_parser(doc_type: str, filename: str, default: str) -> str:
    """Mirror of FileService.get_parser (api/db/services/file_service.py:593)."""
    if doc_type == "visual":
        return "picture"
    if doc_type == "aural":
        return "audio"
    if re.search(r"\.(ppt|pptx|pages)$", filename):
        return "presentation"
    if re.search(r"\.(msg|eml)$", filename):
        return "email"
    return default


def now_ms() -> int:
    return int(time.time() * 1000)  # common/time_utils.py current_timestamp()


def get_uuid() -> str:
    return uuid.uuid1().hex  # common/misc_utils.py get_uuid()


def xxh128_file(path: Path, chunk: int = 1 << 20) -> str:
    h = xxhash.xxh128()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def read_manifest(pkg_dir: str) -> dict:
    path = os.path.join(pkg_dir, MANIFEST_FILE)
    if not os.path.exists(path):
        raise SystemExit(f"{path} not found — incomplete or invalid package (re-run pack; the manifest is written last on purpose)")
    with open(path, encoding="utf-8") as f:
        manifest = json.load(f)
    if manifest.get("kind") != "ragflow-offline-inject":
        raise SystemExit(f"{path}: not a ragflow-offline-inject manifest")
    return manifest


# ── pack: local dir -> package ─────────────────────────────────────────


def _collect_under(base: Path, exts: set | None):
    """Collect packable files under base -> (entries, unsupported, skipped_ext).

    Document names are the ORIGINAL relative path + filename (non-ASCII
    preserved), e.g. "版本发布说明/VDS V3.9.md" — a deliberate divergence
    from the upload API's ASCII sanitize_path, so the KB shows real
    delivery paths. kb_name/staged are filled in by the caller (they
    depend on the --by-dir layout).
    """
    entries, unsupported, skipped_ext = [], [], 0
    for dirpath, dirnames, filenames in os.walk(base):
        dirnames[:] = sorted(d for d in dirnames if not d.startswith("."))
        rel = os.path.relpath(dirpath, base)
        parent = "" if rel == "." else rel.replace(os.sep, "/")
        for fn in sorted(filenames):
            if fn.startswith("."):
                continue
            if exts is not None and os.path.splitext(fn)[1].lstrip(".").lower() not in exts:
                skipped_ext += 1
                continue
            ftype = filename_type(fn)
            if ftype == "other":
                unsupported.append(os.path.relpath(os.path.join(dirpath, fn), base))
                continue
            name = f"{parent}/{fn}" if parent else fn
            if len(name) > FILE_NAME_LEN_LIMIT:  # document.name/location are varchar(255)
                unsupported.append(f"{name} (path+filename longer than {FILE_NAME_LEN_LIMIT})")
                continue
            path = Path(dirpath) / fn
            entries.append(
                {
                    "name": name,
                    "parent_path": parent,
                    "type": ftype,
                    "size": path.stat().st_size,
                    "xxh128": xxh128_file(path),
                    "src_path": str(path),
                }
            )
    return entries, unsupported, skipped_ext


def fetch_template_config(args) -> dict | None:
    """With --template-kb plus --src-meta-password, read the template KB row
    from the SOURCE metadata DB and bake its config into the manifest. apply
    can then import the template itself when the target lacks it, instead of
    failing with 'not found on target'."""
    if not (args.template_kb and args.src_meta_password):
        return None
    conn = psycopg2.connect(
        host=args.src_meta_host,
        port=args.src_meta_port,
        user=args.src_meta_user,
        password=args.src_meta_password,
        dbname=args.src_meta_db,
    )
    with conn.cursor() as cur:
        cur.execute(f"SELECT {','.join(TEMPLATE_KB_CONFIG_COLS)} FROM knowledgebase WHERE id = %s", (args.template_kb,))
        cols = [d[0] for d in cur.description]
        row = cur.fetchone()
    conn.close()
    if not row:
        raise SystemExit(f"--template-kb {args.template_kb} not found on the source database (--src-meta-*)")
    cfg = dict(zip(cols, row))
    cfg["language"] = KB_LANGUAGE  # delivery KBs are English, whatever the source KB says
    return cfg


def cmd_pack(args):
    src = Path(args.src_dir).resolve()
    if not src.is_dir():
        raise SystemExit(f"--src-dir {src} is not a directory")
    out = Path(args.out)
    exts = {x.strip().lstrip(".").lower() for x in args.ext.split(",") if x.strip()} if args.ext else None
    only = set(args.only or [])

    entries, unsupported, skipped_ext = [], [], 0
    if args.by_dir:
        # each first-level sub-directory becomes its own KB (named after the dir)
        root_files = []
        for entry in sorted(os.listdir(src)):
            p = src / entry
            if p.is_file():
                root_files.append(entry)
                continue
            if entry.startswith(".") or (only and entry not in only):
                continue
            sub_entries, sub_unsup, sub_skip = _collect_under(p, exts)
            unsupported += [f"{entry}/{u}" for u in sub_unsup]
            skipped_ext += sub_skip
            for e in sub_entries:
                e["kb_name"] = entry
                e["staged"] = f"{entry}/{e['name']}"
                entries.append(e)
        if root_files:
            logger.warning(f"{len(root_files)} root-level file(s) ignored (--by-dir packs sub-directories only)")
        if only:
            missing = only - {e["kb_name"] for e in entries}
            if missing:
                raise SystemExit(f"--only: no packable files under {', '.join(sorted(missing))}")
    else:
        entries, unsupported, skipped_ext = _collect_under(src, exts)
        for e in entries:
            e["kb_name"] = None
            e["staged"] = e["name"]

    # staged-path uniqueness is guaranteed by construction (real filesystem
    # paths are unique per KB), but keep a suffixer as a safety net so pack
    # can never silently overwrite one staged file with another.
    seen: dict = {}
    collisions = []
    for e in entries:
        staged = e["staged"]
        if staged in seen:
            stem, ext = os.path.splitext(staged)
            n = 0
            while True:
                n += 1
                candidate = f"{stem}_{n}{ext}"
                if candidate not in seen and candidate not in {x["staged"] for x in entries}:
                    break
            collisions.append(f"{e.get('src_path', '?')} -> {candidate}")
            e["staged"] = candidate
        seen[e["staged"]] = True
    if collisions:
        logger.warning(f"{len(collisions)} staged-path collision(s), suffixed automatically:")
        for c in collisions[:10]:
            logger.warning(f"  {c}")
        if len(collisions) > 10:
            logger.warning(f"  ... and {len(collisions) - 10} more")

    if unsupported:
        for u in unsupported:
            logger.error(f"unsupported file: {u}")
        raise SystemExit(f"{len(unsupported)} file(s) are unsupported — remove them, check extensions, or narrow --ext, then re-run pack")
    if skipped_ext:
        logger.info(f"{skipped_ext} file(s) skipped by --ext filter")
    if not entries:
        raise SystemExit("no packable files found")

    kb_count = len({e["kb_name"] for e in entries})
    logger.info(f"Packing {len(entries)} file(s) into {kb_count} KB(s) from {src} -> {out}")
    if args.dry_run:
        for e in entries:
            kb = f"[{e['kb_name']}] " if args.by_dir else ""
            logger.info(f"  [DRY-RUN] {kb}{e['staged']} ({e['size']:,}B, {e['type']})")
        return

    objects_dir = out / OBJECTS_DIRNAME
    for e in entries:
        dest = objects_dir / e["staged"]
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(Path(e["src_path"]).read_bytes())

    manifest = {
        "kind": "ragflow-offline-inject",
        "created_at": datetime.now().isoformat(),
        "source_dir": str(src),
        "by_dir": bool(args.by_dir),
        "template_kb": args.template_kb,
        "file_count": len(entries),
        "files": [{k: v for k, v in e.items() if k != "src_path"} for e in entries],
    }
    template_config = fetch_template_config(args)
    if template_config:
        manifest["template_kb_config"] = template_config
        logger.info(f"template_kb_config baked into the manifest (from {args.src_meta_host}:{args.src_meta_port}/{args.src_meta_db})")
    tmp = out / (MANIFEST_FILE + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    os.replace(tmp, out / MANIFEST_FILE)
    logger.info(f"Package ready: {out} ({len(entries)} files, {kb_count} KB(s)) — manifest written last")


# ── object store: replicates RAGFlowMinio key layout ───────────────────


class ObjectStore:
    """MinIO writer with the exact key layout of RAGFlowMinio
    (rag/utils/minio_conn.py:52-90):

      default bucket set -> physical bucket = <bucket>,
                            key = [<prefix_path>/]<kb_id>/<location>
      no default bucket  -> physical bucket = <kb_id> (created on demand),
                            key = <location>
    """

    def __init__(self, host, user, password, bucket=None, prefix_path=None, secure=False, verify=True):
        import ssl

        import urllib3

        http_client = None if verify else urllib3.PoolManager(cert_reqs=ssl.CERT_NONE)
        self.bucket = bucket or None
        self.prefix_path = (prefix_path or "").strip("/") or None
        self.client = Minio(host, access_key=user, secret_key=password, secure=secure, http_client=http_client)

    def _resolve(self, kb_id: str, location: str) -> tuple[str, str]:
        if self.bucket:
            key = f"{kb_id}/{location}"
            if self.prefix_path:
                key = f"{self.prefix_path}/{key}"
            return self.bucket, key
        return kb_id, location

    def health(self) -> bool:
        try:
            if self.bucket:
                return self.client.bucket_exists(self.bucket)
            self.client.list_buckets()
            return True
        except Exception as e:
            logger.error(f"MinIO unreachable: {e}")
            return False

    def obj_exist(self, kb_id: str, location: str) -> bool:
        bucket, key = self._resolve(kb_id, location)
        try:
            return self.client.stat_object(bucket, key) is not None
        except Exception:
            return False

    def put(self, kb_id: str, location: str, blob: bytes):
        bucket, key = self._resolve(kb_id, location)
        if not self.bucket and not self.client.bucket_exists(bucket):
            try:
                self.client.make_bucket(bucket)  # mirrors RAGFlowMinio.put
            except S3Error as ex:
                if ex.code != "BucketAlreadyOwnedByYou":  # lost the create race to another worker
                    raise
        self.client.put_object(bucket, key, BytesIO(blob), len(blob))


# ── apply: package -> MinIO + rag_flow ─────────────────────────────────


def wconn(writer: VBWriter):
    """Main-thread connection accessor for the q_one/dedup helpers."""
    writer._ensure_connection()
    return writer.conn


_tls = threading.local()
_worker_conns: list = []


def worker_conn(params: dict):
    """Per-thread autocommit connection for concurrent reads: SELECTs never
    leave an open transaction, so skip-checks cost one roundtrip, no COMMIT."""
    conn = getattr(_tls, "conn", None)
    if conn is None:
        conn = psycopg2.connect(**params)
        conn.autocommit = True
        _tls.conn = conn
        _worker_conns.append(conn)
    return conn


def close_worker_conns():
    for conn in _worker_conns:
        try:
            conn.close()
        except Exception:
            pass
    _worker_conns.clear()


def q_one(conn, sql: str, params: tuple) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        row = cur.fetchone()
    if not conn.autocommit:
        conn.commit()  # close the read txn
    return dict(zip(cols, row)) if row else None


def insert_row(writer: VBWriter, table: str, row: dict):
    """Column-intersected idempotent upsert of a single row (PK = id)."""
    target_columns = set(table_columns(writer, table))
    if not target_columns:
        raise RuntimeError(f"table {table} has no columns on target")
    # Column-intersected upsert (PK = id): metadata tables carry no vector
    # fields, so intersecting against the introspected columns is all the
    # old identity_batch() transform did here.
    rows = [{k: v for k, v in row.items() if k in target_columns}]
    inserted = writer.upsert_batch(table, rows, ["id"])
    if not inserted:
        raise RuntimeError(f"upsert into {table} failed")


_COLUMNS_CACHE: dict[str, list[str]] = {}


def table_columns(writer: VBWriter, table: str) -> list[str]:
    """Target table column list, introspected once per process (each
    introspection is a remote catalog roundtrip — per-file calls dominated
    apply runtime over slow links)."""
    if table not in _COLUMNS_CACHE:
        cols = writer.get_columns(table)
        if not cols:
            raise RuntimeError(f"table {table} has no columns on target")
        _COLUMNS_CACHE[table] = cols
    return _COLUMNS_CACHE[table]


def fetch_knowledgebase(writer: VBWriter, kb_id: str) -> dict:
    kb = q_one(
        wconn(writer),
        "SELECT id, tenant_id, name, parser_id, pipeline_id, parser_config, created_by FROM knowledgebase WHERE id = %s",
        (kb_id,),
    )
    if not kb:
        raise SystemExit(f"knowledgebase '{kb_id}' not found on target — apply --load-meta (or create the KB) first")
    return kb


def create_kb_from_template(writer: VBWriter, template: dict, name: str) -> dict:
    """Create a KB row by cloning an existing one (embd/parser/tenant config),
    mirroring what the create-KB API would set."""
    row = dict(template)
    ts, dt = now_ms(), datetime.now()
    row.update(
        {
            "id": get_uuid(),
            "name": name,
            "description": "",  # match a UI/API-created KB; provenance stays in the log line below
            "language": KB_LANGUAGE,  # delivery KBs are English regardless of the template's value
            "doc_num": 0,
            "token_num": 0,
            "chunk_num": 0,
            "create_time": ts,
            "create_date": dt,
            "update_time": ts,
            "update_date": dt,
        }
    )
    for col in (
        "graphrag_task_id",
        "graphrag_task_finish_at",
        "raptor_task_id",
        "raptor_task_finish_at",
        "mindmap_task_id",
        "mindmap_task_finish_at",
    ):
        row.pop(col, None)  # never clone another KB's in-flight task ids
    insert_row(writer, "knowledgebase", row)
    logger.info(f"  KB '{name}' created ({row['id']}), cloned from '{template['name']}'")
    return q_one(wconn(writer), "SELECT * FROM knowledgebase WHERE id = %s", (row["id"],))


# config columns baked into the manifest when packing with --src-meta-*; enough
# for apply to import the template KB itself on a fresh target
TEMPLATE_KB_CONFIG_COLS = (
    "name",
    "tenant_id",
    "created_by",
    "embd_id",
    "language",
    "permission",
    "parser_id",
    "parser_config",
    "pagerank",
    "similarity_threshold",
    "vector_similarity_weight",
    "status",
    "description",
)


def import_template_kb(writer: VBWriter, kb_id: str, cfg: dict) -> dict:
    """Create the template KB on the target from the config baked into the
    package manifest (pack --template-kb --src-meta-*), so a fresh deployment
    doesn't need a hand-built template first. Config columns only — id stays
    the baked template id, counters/timestamps are set fresh."""
    missing = [c for c in ("name", "tenant_id", "embd_id") if not cfg.get(c)]
    if missing:
        raise SystemExit(f"manifest template_kb_config lacks {missing} — re-pack with --src-meta-* against the source database")
    if not q_one(wconn(writer), "SELECT id FROM tenant WHERE id = %s", (cfg["tenant_id"],)):
        raise SystemExit(f"template tenant '{cfg['tenant_id']}' missing on target — deploy RAGFlow (or --load-meta) first")
    row = {c: cfg[c] for c in TEMPLATE_KB_CONFIG_COLS if c in cfg}
    row.setdefault("language", KB_LANGUAGE)  # manifests baked before the language column existed
    ts, dt = now_ms(), datetime.now()
    row.update(
        {
            "id": kb_id,
            "doc_num": 0,
            "token_num": 0,
            "chunk_num": 0,
            "create_time": ts,
            "create_date": dt,
            "update_time": ts,
            "update_date": dt,
        }
    )
    insert_row(writer, "knowledgebase", row)
    logger.info(f"  template KB '{row['name']}' ({kb_id}) imported from the package manifest")
    return q_one(wconn(writer), "SELECT * FROM knowledgebase WHERE id = %s", (kb_id,))


# language stamped on every KB this tool creates: the template config baked
# at pack time, the imported/synthesized template, and cloned KBs at apply.
# Same-name KBs already on the target are reused untouched.
KB_LANGUAGE = "Chinese"

# parser_config used when a template has to be synthesized from a bare tenant:
# this deployment's delivery values (newline delimiter, 512-token chunks)
SYNTHETIC_PARSER_CONFIG = '{"pages":[[1,1000]],"chunk_token_num":512,"delimiter":"\\n","layout_recognize":"DeepDOC","raptor":{"use_raptor":false}}'


def _synthetic_from_tenant(t: dict) -> dict:
    """Template-shaped config dict derived from a bare tenant row."""
    return {
        "id": "(synthetic)",
        "name": f"tenant default ({t.get('name') or t['id']})",
        "tenant_id": t["id"],
        "created_by": t["id"],
        "embd_id": t.get("embd_id") or "BAAI/bge-m3___VLLM@VLLM",
        "language": KB_LANGUAGE,
        "permission": "team",
        "parser_id": "naive",
        "parser_config": SYNTHETIC_PARSER_CONFIG,
        "pagerank": 0,
        "similarity_threshold": 0.2,
        "vector_similarity_weight": 0.3,
        "status": "1",
    }


def synthesize_template(writer: VBWriter) -> dict | None:
    """No --template-kb given anywhere but a KB must be created: derive a
    config source instead of failing — any existing KB on the target works
    (same rule as --template-kb); else the single tenant, else the admin
    tenant that actually has an embedding model configured."""
    kb = q_one(wconn(writer), "SELECT * FROM knowledgebase ORDER BY create_time LIMIT 1", ())
    if kb:
        logger.info(f"  no --template-kb given: cloning from existing KB '{kb['name']}' ({kb['id']})")
        return kb
    n = q_one(wconn(writer), "SELECT count(*) AS n FROM tenant", ())["n"]
    if n == 1:
        t = q_one(wconn(writer), "SELECT * FROM tenant LIMIT 1", ())
        logger.info(f"  no --template-kb given: synthesizing from the only tenant '{t['id']}' (embd {t.get('embd_id')})")
        return _synthetic_from_tenant(t)
    admin = q_one(
        wconn(writer),
        "SELECT * FROM tenant WHERE name LIKE %s AND embd_id IS NOT NULL AND embd_id <> '' ORDER BY create_time LIMIT 1",
        ("admin%",),
    )
    if admin:
        logger.info(f"  no --template-kb given: synthesizing from admin tenant '{admin['id']}' ({admin['name']}, embd {admin['embd_id']})")
        return _synthetic_from_tenant(admin)
    return None


def resolve_kbs(writer: VBWriter, args, kb_names: set) -> dict:
    """Resolve every kb_name in the package to a KB row: reuse the existing
    KB with the same name, or create one cloned from --template-kb (derived
    automatically when absent — see synthesize_template)."""
    groups: dict = {}
    if None in kb_names:  # single-KB package (packed without --by-dir)
        if not args.kb_id:
            raise SystemExit("--kb-id is required for single-KB packages")
        groups[None] = fetch_knowledgebase(writer, args.kb_id)
    named = sorted(n for n in kb_names if n is not None)
    template = None
    if named and args.template_kb:
        template = q_one(wconn(writer), "SELECT * FROM knowledgebase WHERE id = %s", (args.template_kb,))
        if not template and getattr(args, "template_kb_config", None):
            if args.dry_run:
                template = dict(args.template_kb_config)
                logger.info(f"  template KB '{template.get('name')}' missing on target: would IMPORT from the package")
            else:
                template = import_template_kb(writer, args.template_kb, args.template_kb_config)
        if not template:
            raise SystemExit(
                f"--template-kb {args.template_kb} not found on target, and the package carries no template_kb_config "
                "(pack with --template-kb --src-meta-* to embed one, or create any KB on the target and pass its id)"
            )
    for name in named:
        hits = q_one(wconn(writer), "SELECT count(*) AS n FROM knowledgebase WHERE name = %s", (name,))["n"]
        if hits > 1:
            raise SystemExit(f"KB name '{name}' exists under {hits} tenants — pass --template-kb to disambiguate")
        existing = q_one(wconn(writer), "SELECT * FROM knowledgebase WHERE name = %s LIMIT 1", (name,)) if hits else None
        if existing:
            logger.info(f"  KB '{name}': reuse existing ({existing['id']})")
            groups[name] = existing
            continue
        if template is None:
            template = synthesize_template(writer)
            if template is None:
                raise SystemExit(f"KB '{name}' must be created but there is no config source: no --template-kb, no existing KB on the target, and multiple tenants — pass --template-kb")
        if args.dry_run:
            logger.info(f"  KB '{name}': would CREATE, cloned from '{template['name']}'")
            groups[name] = {**template, "id": f"(new:{name})", "name": name}
        else:
            groups[name] = create_kb_from_template(writer, template, name)
    return groups


def get_or_create_root_folder(writer: VBWriter, tenant_id: str) -> dict:
    # mirror of FileService.get_root_folder (file_service.py:229)
    row = q_one(wconn(writer), "SELECT * FROM file WHERE tenant_id = %s AND parent_id = id LIMIT 1", (tenant_id,))
    if row:
        return row
    root = {
        "id": get_uuid(),
        "parent_id": None,
        "tenant_id": tenant_id,
        "created_by": tenant_id,
        "name": "/",
        "type": FOLDER_TYPE,
        "size": 0,
        "location": "",
        "source_type": "",
    }
    root["parent_id"] = root["id"]  # self-referencing root
    insert_row(writer, "file", root)
    return root


def get_or_create_folder(writer: VBWriter, tenant_id: str, parent_id: str, name: str) -> dict:
    # mirror of FileService.new_a_file_from_kb (file_service.py:270)
    row = q_one(wconn(writer), "SELECT * FROM file WHERE tenant_id = %s AND parent_id = %s AND name = %s LIMIT 1", (tenant_id, parent_id, name))
    if row:
        return row
    folder = {
        "id": get_uuid(),
        "parent_id": parent_id,
        "tenant_id": tenant_id,
        "created_by": tenant_id,
        "name": name,
        "type": FOLDER_TYPE,
        "size": 0,
        "location": "",
        "source_type": FILE_SOURCE_KB,
    }
    insert_row(writer, "file", folder)
    return folder


def ensure_kb_folder(writer: VBWriter, kb: dict) -> dict:
    """Ensure root -> .knowledgebase -> <kb name> chain, return the KB folder."""
    tenant = kb["tenant_id"]
    root = get_or_create_root_folder(writer, tenant)
    kb_root = get_or_create_folder(writer, tenant, root["id"], KNOWLEDGEBASE_FOLDER_NAME)
    return get_or_create_folder(writer, tenant, kb_root["id"], kb["name"])


def doc_name_exists(conn, kb_id: str, name: str) -> bool:
    return q_one(conn, "SELECT id FROM document WHERE kb_id = %s AND name = %s LIMIT 1", (kb_id, name)) is not None


def doc_location_exists(conn, kb_id: str, location: str) -> bool:
    return q_one(conn, "SELECT id FROM document WHERE kb_id = %s AND location = %s LIMIT 1", (kb_id, location)) is not None


def dedup_name(conn, kb_id: str, name: str) -> str:
    # mirror of api/db/services/__init__.py duplicate_name(): stem(1).ext, stem(2).ext ...
    if not doc_name_exists(conn, kb_id, name):
        return name
    stem, ext = os.path.splitext(name)
    m = re.match(r"^(.*)\((\d+)\)$", stem)
    base, n = (m.group(1), int(m.group(2))) if m else (stem, 0)
    while n < 1000:
        n += 1
        candidate = f"{base}({n}){ext}"
        if not doc_name_exists(conn, kb_id, candidate):
            return candidate
    raise RuntimeError(f"cannot dedup name for {name} after 1000 tries")


def build_object_store(args) -> ObjectStore:
    conf = {}
    if args.service_conf:
        import yaml

        with open(args.service_conf, encoding="utf-8") as f:
            conf = (yaml.safe_load(f) or {}).get("MINIO") or {}
        raw = [k for k, v in conf.items() if isinstance(v, str) and "${" in v]
        if raw:
            raise SystemExit(f"{args.service_conf} looks like an unsubstituted template ({', '.join(raw)}); pass explicit --minio-* flags instead")
    host = args.minio_host or conf.get("host")
    user = args.minio_user or conf.get("user")
    password = args.minio_password or conf.get("password")
    if not (host and user and password is not None):
        raise SystemExit("MinIO config incomplete: need --minio-host/user/password or a --service-conf with a MINIO section")
    secure = args.minio_secure if args.minio_secure is not None else str(conf.get("secure", False)).lower() in ("true", "1", "yes")
    return ObjectStore(
        host,
        user,
        password,
        bucket=args.minio_bucket or conf.get("bucket") or None,
        prefix_path=args.minio_prefix_path or conf.get("prefix_path") or None,
        secure=secure,
    )


def load_progress() -> dict:
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE) as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not read progress file {PROGRESS_FILE}: {e}")
    return {}


def save_progress(progress: dict):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


def reconcile_doc_num(writer: VBWriter, kb_id: str) -> int:
    """Re-sync knowledgebase.doc_num with the live document count.

    The upload API bumps this denormalized counter per insert
    (DocumentService.insert -> atomic_increase_doc_num_by_id); direct SQL
    injection bypasses that, so apply reconciles at the end — which is
    also self-healing after partial runs.
    """
    writer._ensure_connection()
    with writer.conn.cursor() as cur:
        cur.execute(
            "UPDATE knowledgebase SET doc_num = (SELECT count(*) FROM document WHERE kb_id = %s), update_time = %s WHERE id = %s",
            (kb_id, now_ms(), kb_id),
        )
        cur.execute("SELECT doc_num FROM knowledgebase WHERE id = %s", (kb_id,))
        doc_num = cur.fetchone()[0]
    writer.conn.commit()
    return doc_num


def cmd_apply(args):
    manifest = read_manifest(args.pkg)
    if not args.dst_host:
        raise SystemExit("--dst-host is required for apply")

    writer = VBWriter(
        host=args.dst_host,
        port=args.dst_port,
        user=args.dst_user,
        password=args.dst_password,
        database=args.dst_meta_db,
    )
    if not writer.health_check():
        raise SystemExit("Cannot connect to target metadata Vastbase")
    for table in ("knowledgebase", "document", "file", "file2document"):
        if not writer.table_exists(table):
            raise SystemExit(f"table '{table}' missing on target — deploy RAGFlow on the target (or run --load-meta) before injecting documents")
    logger.info(f"Target metadata ({args.dst_meta_db}) connection OK")

    kb_names = {e.get("kb_name") for e in manifest["files"]}
    if not args.template_kb:  # CLI flag wins over the manifest (pack --template-kb)
        args.template_kb = manifest.get("template_kb")
    args.template_kb_config = manifest.get("template_kb_config")
    groups_kb = resolve_kbs(writer, args, kb_names)
    store = build_object_store(args)
    if not store.health():
        raise SystemExit("Cannot connect to target MinIO")

    files_by_kb: dict = {}
    for e in manifest["files"]:
        files_by_kb.setdefault(e.get("kb_name"), []).append(e)

    stats = {"injected": 0, "skipped": 0, "repaired": 0, "failed": 0}
    progress = load_progress()
    objects_dir = Path(args.pkg) / OBJECTS_DIRNAME

    for kb_name, files in files_by_kb.items():
        kb = groups_kb[kb_name]
        created_by = args.created_by or kb["created_by"]
        parser_config = kb["parser_config"]
        if isinstance(parser_config, dict):  # JSON column: store the string form
            parser_config = json.dumps(parser_config, ensure_ascii=False)
        logger.info(f"=== KB '{kb['name']}' ({kb['id']}): {len(files)} file(s), parser={kb['parser_id']} ===")

        kb_folder = None if args.dry_run else ensure_kb_folder(writer, kb)

        pending = files
        if args.resume:
            pending = [e for e in files if not progress.get(e["staged"], {}).get("completed")]
            resumed = len(files) - len(pending)
            if resumed:
                logger.info(f"  [SKIP] {resumed} file(s) marked completed in progress file (--resume)")
                stats["skipped"] += resumed

        conn_params = dict(host=args.dst_host, port=args.dst_port, user=args.dst_user, password=args.dst_password, dbname=args.dst_meta_db)

        def work(e):
            staged = objects_dir / e["staged"]
            if not staged.is_file():
                raise FileNotFoundError(f"missing staged file {staged}")
            blob = staged.read_bytes()
            return process_one(worker_conn(conn_params), store, kb, kb_folder, e, blob, args, created_by, parser_config)

        buffer: list[dict] = []

        def drain(force: bool = False):
            while len(buffer) >= FLUSH_EVERY or (force and buffer):
                batch = buffer[:FLUSH_EVERY]
                del buffer[:FLUSH_EVERY]
                try:
                    flush_bundles(writer, batch, progress)
                except Exception as ex:
                    lost = sum(1 for b in batch if b["kind"] == "rows")
                    logger.error(f"  flush failed ({ex}); {lost} file(s) not written — they retry on the next run")
                    stats["failed"] += lost
                    continue
                save_progress(progress)
                for b in batch:
                    if b["kind"] == "rows":
                        stats["injected"] += 1
                        logger.info(f"  {b['label']} -> {b['doc']['id']} (parser={b['doc']['parser_id']}, run=UNSTART)")
                    elif b["kind"] == "repair":
                        stats["repaired"] += 1
                        logger.info(f"  repaired file/file2document rows for {b['label']}")

        with ThreadPoolExecutor(max_workers=args.jobs) as pool:
            futures = {pool.submit(work, e): e for e in pending}
            for fut in as_completed(futures):
                e = futures[fut]
                try:
                    b = fut.result()
                except Exception as ex:
                    logger.error(f"  {e['staged']}: FAILED ({ex})")
                    stats["failed"] += 1
                    continue
                if b["kind"] == "skip":
                    stats["skipped"] += 1
                    logger.info(f"  [SKIP] {b['label']}: already injected ({b['doc_id']})")
                elif b["kind"] == "dry":
                    stats["injected"] += 1
                    logger.info(f"  [DRY-RUN] {b['label']} -> doc {b['doc_id']} name={b['name']} parser={b['parser_id']} object={b['bucket']}/{b['key']} ({b['size']:,}B)")
                else:
                    buffer.append(b)
                    drain()
        drain(force=True)

        if not args.dry_run:
            save_progress(progress)  # flush any tail entries the %20 throttle deferred
            doc_num = reconcile_doc_num(writer, kb["id"])
            logger.info(f"knowledgebase.doc_num reconciled to {doc_num}")

    logger.info(f"INJECT SUMMARY: injected={stats['injected']} skipped={stats['skipped']} repaired={stats['repaired']} failed={stats['failed']}" + (" [DRY RUN]" if args.dry_run else ""))
    if not args.dry_run:
        logger.info('Documents are in UNSTART state — parse them from the UI or: POST /api/v1/datasets/<kb_id>/documents/parse {"document_ids": [...]}')
    close_worker_conns()
    writer.close()


FLUSH_EVERY = 20


def process_one(conn, store: ObjectStore, kb: dict, kb_folder: dict | None, e: dict, blob: bytes, args, created_by: str, parser_config) -> dict:
    """Worker body for one package entry: verify hash, skip/repair check,
    name/location dedup, MinIO upload, build the rows. Returns a bundle for
    the flusher — never writes document/file/file2document itself (same
    skip/repair/dedup semantics as FileService.upload_document, no tasks)."""
    label = e["staged"]
    if xxhash.xxh128(blob).hexdigest() != e["xxh128"]:
        raise RuntimeError("content hash mismatch — package corrupted")

    # Idempotency: same content already in this KB (under any name) -> skip.
    # Keyed by content_hash, not (name, hash): different directories can
    # carry same-basename files, and a name-keyed skip would rename-and-
    # reinject them on every rerun.
    existing = q_one(conn, "SELECT id, name, location FROM document WHERE kb_id = %s AND content_hash = %s LIMIT 1", (kb["id"], e["xxh128"]))
    if existing:
        # repair a half-written previous run: file/file2document rows
        link = q_one(conn, "SELECT id FROM file2document WHERE document_id = %s LIMIT 1", (existing["id"],))
        if link is None and not args.dry_run:
            ts, dt = now_ms(), datetime.now()
            file_id = get_uuid()
            return {
                "kind": "repair",
                "label": label,
                "file": {
                    "id": file_id,
                    "parent_id": kb_folder["id"],
                    "tenant_id": kb["tenant_id"],
                    "created_by": kb["tenant_id"],
                    "name": existing["name"],
                    "type": e["type"],
                    "size": e["size"],
                    "location": existing["location"],
                    "source_type": FILE_SOURCE_KB,
                    "create_time": ts,
                    "create_date": dt,
                    "update_time": ts,
                    "update_date": dt,
                },
                "f2d": {"id": get_uuid(), "file_id": file_id, "document_id": existing["id"]},
            }
        return {"kind": "skip", "label": label, "doc_id": existing["id"]}

    # mirror of the upload path: dedup name, then dedup location.
    # e["name"] already carries the original relative directory path
    # (e.g. "版本发布说明/VDS V3.9.md"), so location == name.
    name = dedup_name(conn, kb["id"], e["name"])
    location = name
    while doc_location_exists(conn, kb["id"], location):
        location += "_"
    parser_id = get_parser(e["type"], name, kb["parser_id"])
    doc_id, file_id = get_uuid(), get_uuid()
    ts, dt = now_ms(), datetime.now()

    if args.dry_run:
        bucket, key = store._resolve(kb["id"], location)
        return {"kind": "dry", "label": label, "doc_id": doc_id, "name": name, "parser_id": parser_id, "bucket": bucket, "key": key, "size": e["size"]}

    doc = {
        "id": doc_id,
        "kb_id": kb["id"],
        "parser_id": parser_id,
        "pipeline_id": kb["pipeline_id"],
        "parser_config": parser_config,
        "source_type": "local",
        "type": e["type"],
        "created_by": created_by,
        "name": name,
        "location": location,
        "size": e["size"],
        "thumbnail": "",
        "suffix": os.path.splitext(name)[1].lstrip("."),
        "content_hash": e["xxh128"],
        "token_num": 0,
        "chunk_num": 0,
        "retrieval_count": 0,
        "progress": 0.0,
        "progress_msg": "",
        "process_duration": 0.0,
        "run": UNSTART,
        "status": "1",
        "create_time": ts,
        "create_date": dt,
        "update_time": ts,
        "update_date": dt,
    }
    file_row = {
        "id": file_id,
        "parent_id": kb_folder["id"],
        "tenant_id": kb["tenant_id"],
        "created_by": kb["tenant_id"],
        "name": name,
        "type": e["type"],
        "size": e["size"],
        "location": location,
        "source_type": FILE_SOURCE_KB,
        "create_time": ts,
        "create_date": dt,
        "update_time": ts,
        "update_date": dt,
    }
    f2d = {"id": get_uuid(), "file_id": file_id, "document_id": doc_id}

    store.put(kb["id"], location, blob)
    return {"kind": "rows", "label": label, "staged": e["staged"], "doc": doc, "file": file_row, "f2d": f2d}


def flush_bundles(writer: VBWriter, bundles: list[dict], progress: dict, _retried: bool = False):
    """Insert a batch of prepared bundles in ONE transaction: three batched
    INSERTs (document/file/file2document; repair bundles merge into the
    file/f2d batches) and a single COMMIT. Per-file commits dominated apply
    runtime — every COMMIT is a WAL fsync on the metadata server.

    The writer connection can idle out while workers run; a dead-connection
    failure reconnects and retries the batch once (the txn rolled back, so
    the retry is safe)."""
    conn = wconn(writer)
    try:
        # Introspect every table's columns BEFORE the first INSERT: VBWriter's
        # get_columns() opens with conn.rollback() (clears aborted txns), which
        # mid-transaction would silently discard the inserts already staged.
        plans = []
        for table, key in (("document", "doc"), ("file", "file"), ("file2document", "f2d")):
            table_rows = [b[key] for b in bundles if key in b]
            if not table_rows:
                continue
            cols = [c for c in table_columns(writer, table) if c in table_rows[0]]
            plans.append((table, table_rows, cols))
        with conn.cursor() as cur:
            for table, table_rows, cols in plans:
                sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join(['%s'] * len(cols))}) ON CONFLICT (id) DO NOTHING"
                execute_batch(cur, sql, [tuple(r.get(c) for c in cols) for r in table_rows], page_size=200)
        conn.commit()
    except (psycopg2.InterfaceError, psycopg2.OperationalError):
        try:
            conn.rollback()
        except Exception:
            pass  # rollback on a dead connection raises its own error; the retry reconnects
        if not _retried:
            writer._ensure_connection()  # conn.closed is now set -> reconnects
            return flush_bundles(writer, bundles, progress, _retried=True)
        raise
    ts_iso = datetime.now().isoformat()
    for b in bundles:
        if b["kind"] == "rows":
            progress[b["staged"]] = {"doc_id": b["doc"]["id"], "completed": True, "completed_at": ts_iso}


# ── CLI ────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="RAGFlow offline document injector (pack / apply, no parsing)")
    sub = parser.add_subparsers(dest="command", required=True)

    p_pack = sub.add_parser("pack", help="Stage a local directory into an offline package")
    p_pack.add_argument("--src-dir", required=True, help="Directory of delivery files")
    p_pack.add_argument("--out", required=True, help="Package directory to create")
    p_pack.add_argument("--by-dir", action="store_true", help="Each first-level sub-directory becomes its own knowledge base, named after the directory (apply reuses/creates KBs by name)")
    p_pack.add_argument("--ext", default=None, metavar="EXTS", help="Comma-separated extension whitelist, e.g. 'md' or '.md,.txt' (default: all supported types)")
    p_pack.add_argument("--only", action="append", default=None, metavar="DIR", help="With --by-dir, only pack this sub-directory (repeatable)")
    p_pack.add_argument("--template-kb", default=None, metavar="ID", help="Record the template KB id in the manifest so apply needs no --template-kb flag (KBs to create are cloned from it)")
    p_pack.add_argument("--src-meta-host", default="localhost", help="With --template-kb: source metadata DB to read the template KB config from (baked as template_kb_config)")
    p_pack.add_argument("--src-meta-port", type=int, default=5432)
    p_pack.add_argument("--src-meta-user", default="ragflow")
    p_pack.add_argument("--src-meta-db", default="rag_flow")
    p_pack.add_argument("--src-meta-password", default=None, help="Triggers the template config read; without it only the template KB id is recorded")
    p_pack.add_argument("--dry-run", action="store_true", help="List packable files only")

    p_apply = sub.add_parser("apply", help="Inject a package into a target RAGFlow")
    p_apply.add_argument("--pkg", required=True, help="Package directory from pack")
    p_apply.add_argument("--kb-id", default=None, help="Target knowledgebase id (single-KB packages)")
    p_apply.add_argument(
        "--template-kb",
        default=None,
        metavar="ID",
        help="--by-dir packages: existing KB to clone embd/parser/tenant config from when a KB must be created by name (defaults to the template_kb recorded by pack)",
    )
    p_apply.add_argument("--created-by", default=None, help="document.created_by (default: KB owner)")
    p_apply.add_argument("--dst-host", default=None)
    p_apply.add_argument("--dst-port", type=int, default=5432)
    p_apply.add_argument("--dst-user", default="rag_flow")
    p_apply.add_argument("--dst-password", default="")
    p_apply.add_argument("--dst-meta-db", default="rag_flow")
    p_apply.add_argument("--service-conf", default=None, help="service_conf.yaml to read the MINIO section from")
    p_apply.add_argument("--minio-host", default=None, help="e.g. minio:9000")
    p_apply.add_argument("--minio-user", default=None)
    p_apply.add_argument("--minio-password", default=None)
    p_apply.add_argument("--minio-bucket", default=None, help="Default physical bucket (single-bucket mode); empty = one bucket per KB")
    p_apply.add_argument("--minio-prefix-path", default=None)
    p_apply.add_argument("--minio-secure", default=None, type=lambda v: v.lower() in ("1", "true", "yes"))
    p_apply.add_argument("--jobs", type=int, default=8, help="Concurrent workers for hash-check/upload; row inserts are batched into one transaction per %d files (default 8)" % FLUSH_EVERY)
    p_apply.add_argument("--resume", action="store_true", help="Skip files marked completed in the progress file")
    p_apply.add_argument("--dry-run", action="store_true", help="Resolve names/parsers/object keys, write nothing")
    p_apply.add_argument("-v", "--verbose", action="store_true")

    args = parser.parse_args()
    if getattr(args, "verbose", False):
        logging.getLogger().setLevel(logging.DEBUG)
    if args.command == "apply" and not 1 <= args.jobs <= 64:
        raise SystemExit("--jobs must be between 1 and 64")

    if args.command == "pack":
        cmd_pack(args)
    else:
        cmd_apply(args)


if __name__ == "__main__":
    main()
