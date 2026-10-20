"""
Offline local package for the rag_flow metadata DB.

Splits the VB -> VB metadata migration into two offline phases so a
private-deployment handover never needs source and target reachable at
the same time:

  dump (source side)  rag_flow -> <pkg>/<table>.jsonl + manifest.json
  load (target side)  <pkg>     -> rag_flow (idempotent upserts, FK order)

Row encoding is JSON Lines with default=str: datetime/Decimal become
strings, which Vastbase casts back on parameterized insert; integer[]
columns round-trip as native lists (psycopg2 adapts list -> PG array),
and text-stored JSON columns are already strings.

Column intersection still goes through identity.identity_batch, so a
package dumped from an older schema loads into a newer target without
referencing columns the target does not have (columns that exist only
on the target are left to their defaults).

The manifest is written last and atomically: a package directory whose
manifest.json is absent is an incomplete dump and the loader refuses it.
"""

import json
import logging
import os
import time
from datetime import datetime

from identity import identity_batch
from meta_migrator import MetaMigrator

logger = logging.getLogger(__name__)

MANIFEST_FILE = "manifest.json"
LOAD_PROGRESS_FILE = ".vb_meta_load_progress.json"


# ── shared helpers ─────────────────────────────────────────────────────

def _read_manifest(pkg_dir: str) -> dict:
    path = os.path.join(pkg_dir, MANIFEST_FILE)
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"{path} not found — incomplete or invalid package "
            f"(re-run --dump-meta; the manifest is written last on purpose)"
        )
    with open(path, encoding="utf-8") as f:
        manifest = json.load(f)
    if manifest.get("kind") != "vb-meta-package":
        raise ValueError(f"{path}: not a vb-meta-package manifest")
    return manifest


def _filter_entries(entries: list[dict], include, exclude) -> list[dict]:
    inc = {t.strip() for t in (include or []) if t.strip()}
    exc = {t.strip() for t in (exclude or []) if t.strip()}
    out = [e for e in entries if (not inc or e["table"] in inc) and e["table"] not in exc]
    for m in sorted(inc - {e["table"] for e in out} - exc):
        logger.warning(f"--meta-include-tables: '{m}' not found in package")
    return out


def _iter_batches(path: str, batch_size: int):
    """Yield lists of row dicts from a JSONL file, batch_size at a time."""
    batch = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            batch.append(json.loads(line))
            if len(batch) >= batch_size:
                yield batch
                batch = []
    if batch:
        yield batch


def _load_load_progress() -> dict:
    if os.path.exists(LOAD_PROGRESS_FILE):
        try:
            with open(LOAD_PROGRESS_FILE) as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not read {LOAD_PROGRESS_FILE}: {e}")
    return {}


def _save_load_progress(progress: dict):
    with open(LOAD_PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


# ── dump: rag_flow -> local package ────────────────────────────────────

def dump_meta(reader, out_dir: str, include=None, exclude=None,
              batch_size: int = 1000, dry_run: bool = False) -> dict | None:
    """Dump every metadata table (FK parent-first order) to JSONL files.

    Returns the manifest dict, or None when nothing was dumped.
    """
    mm = MetaMigrator(reader, None)
    tables = mm.apply_filters(mm.list_tables(), include, exclude)
    if not tables:
        logger.error("No metadata tables to dump")
        return None
    ordered = mm.ordered_tables(tables)

    logger.info(f"Metadata dump: {len(ordered)} table(s) in FK order")
    if not dry_run:
        os.makedirs(out_dir, exist_ok=True)

    entries = []
    total_rows = 0
    start = time.time()
    for t in ordered:
        try:
            pk = reader.get_primary_keys(t) or []
            columns = reader.get_columns(t)
            src_count = reader.count_rows(t)
        except Exception as e:
            logger.error(f"  {t}: introspection failed, skipping ({e})")
            continue

        if dry_run:
            logger.info(f"  [DRY-RUN] {t}: {src_count:,} rows")
            entries.append({"table": t, "pk": pk, "columns": columns, "rows": src_count})
            continue

        path = os.path.join(out_dir, f"{t}.jsonl")
        n = 0
        with open(path, "w", encoding="utf-8") as f:
            for batch in reader.scroll_rows(t, batch_size):
                for row in batch:
                    f.write(json.dumps(row, ensure_ascii=False, default=str))
                    f.write("\n")
                    n += 1
                if n % (batch_size * 20) == 0:
                    logger.info(f"    {t}: {n:,} rows written")
        if n != src_count:
            logger.warning(
                f"  {t}: row count drifted while dumping ({src_count} -> {n}); "
                f"the source was written to during the dump"
            )
        logger.info(f"  {t}: {n:,} rows -> {path}")
        entries.append({
            "table": t, "pk": pk, "columns": columns, "rows": n,
            "file": f"{t}.jsonl",
        })
        total_rows += n

    manifest = {
        "kind": "vb-meta-package",
        "created_at": datetime.now().isoformat(),
        "tables": entries,
    }
    if not dry_run:
        tmp = os.path.join(out_dir, MANIFEST_FILE + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)
        os.replace(tmp, os.path.join(out_dir, MANIFEST_FILE))
        logger.info(
            f"Package: {out_dir} ({len(entries)} tables, {total_rows:,} rows, "
            f"{time.time() - start:.1f}s) — manifest written last"
        )
    return manifest


# ── load: local package -> rag_flow ────────────────────────────────────

def load_meta(writer, pkg_dir: str, batch_size: int = 1000,
              dry_run: bool = False, clear: bool = False,
              include=None, exclude=None, resume: bool = False) -> dict:
    """Load a dumped package into the target rag_flow, FK order preserved.

    Idempotent: PK tables upsert via ON CONFLICT; PK-less tables are
    TRUNCATEd once then plain-INSERTed (same contract as MetaMigrator).
    """
    manifest = _read_manifest(pkg_dir)
    entries = _filter_entries(manifest["tables"], include, exclude)
    if not entries:
        logger.error("No tables in package to load")
        return {"tables": 0, "rows": 0, "failed": 0}

    logger.info(f"Package {pkg_dir}: created {manifest.get('created_at')}, "
                f"{len(entries)} table(s) in FK order")
    for i, e in enumerate(entries, 1):
        logger.info(f"  {i:>2}. {e['table']} ({e['rows']:,} rows)")

    # Tables missing on target can't be loaded (peewee must create them).
    loadable = []
    for e in entries:
        if not writer.table_exists(e["table"]):
            logger.warning(
                f"  SKIP {e['table']}: missing on target — deploy RAGFlow on "
                f"the target to create metadata tables first"
            )
            continue
        loadable.append(e)

    if dry_run:
        for e in loadable:
            target_cols = set(writer.get_columns(e["table"]))
            common = [c for c in e["columns"] if c in target_cols]
            dropped = [c for c in e["columns"] if c not in target_cols]
            logger.info(
                f"  [DRY-RUN] {e['table']}: {e['rows']:,} rows, "
                f"{len(common)}/{len(e['columns'])} columns match"
                + (f", drop {dropped}" if dropped else "")
            )
        logger.info("DRY RUN COMPLETE (no data written)")
        return {"tables": len(loadable), "rows": 0, "failed": 0}

    if clear:
        logger.info("Clearing target metadata tables (--clear-meta) ...")
        for e in reversed(loadable):  # children before parents
            try:
                writer.truncate(e["table"])
            except Exception as ex:
                logger.warning(f"  truncate {e['table']} failed (non-fatal): {ex}")

    progress = _load_load_progress()
    total = {"tables": 0, "rows": 0, "failed": 0}
    start = time.time()
    for e in loadable:
        t = e["table"]
        pk = e.get("pk") or []
        path = os.path.join(pkg_dir, e.get("file") or f"{t}.jsonl")

        if resume and progress.get(t, {}).get("completed"):
            logger.info(f"  [SKIP] {t} already completed ({e['rows']} rows)")
            total["tables"] += 1
            total["rows"] += e["rows"]
            continue

        logger.info(f"\n{'='*60}\n  Loading table: {t}\n{'='*60}")
        if not pk:
            logger.warning(
                f"  {t}: no primary key in package — TRUNCATE+INSERT "
                f"(not mid-table resumable)"
            )
            try:
                writer.truncate(t)
            except Exception as ex:
                logger.error(f"  Cannot truncate {t} before insert: {ex}")
                continue

        target_columns = set(writer.get_columns(t))
        if not target_columns:
            logger.error(f"  Target {t} has no columns, skipping")
            continue

        migrated = 0
        failed = 0
        batch_no = 0
        for batch in _iter_batches(path, batch_size):
            batch_no += 1
            try:
                # vector_fields=set(): meta tables have no q_*_vec columns,
                # so identity_batch does pure column-intersection.
                rows = identity_batch(batch, target_columns, set())
                inserted = writer.upsert_batch(t, rows, pk)
                migrated += inserted
                if batch_no % 5 == 0:
                    logger.info(f"    {t}: {migrated}/{e['rows']:,}")
                if batch_no % 100 == 0:
                    progress[t] = {"migrated": migrated, "total": e["rows"]}
                    _save_load_progress(progress)
            except Exception as ex:
                logger.error(f"    Batch {batch_no} failed for {t}: {ex}")
                failed += len(batch)

        progress[t] = {
            "migrated": migrated, "total": e["rows"], "completed": True,
            "completed_at": datetime.now().isoformat(),
        }
        _save_load_progress(progress)
        logger.info(f"  {t}: {migrated}/{e['rows']:,} loaded, {failed} failed")
        total["tables"] += 1
        total["rows"] += migrated
        total["failed"] += failed

    logger.info(f"\n{'='*60}")
    logger.info("LOAD SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"  Tables:   {total['tables']}")
    logger.info(f"  Rows:     {total['rows']}")
    logger.info(f"  Failed:   {total['failed']}")
    logger.info(f"  Duration: {time.time() - start:.1f}s")
    return total


# ── verify: package vs target ──────────────────────────────────────────

def verify_package(writer, pkg_dir: str, include=None, exclude=None) -> dict:
    """Compare per-table row counts in the package against the target."""
    manifest = _read_manifest(pkg_dir)
    entries = _filter_entries(manifest["tables"], include, exclude)
    matches = 0
    mismatches = 0
    for e in entries:
        t = e["table"]
        if not writer.table_exists(t):
            logger.warning(f"  - {t}: missing on target (pkg={e['rows']})")
            mismatches += 1
            continue
        try:
            dst = writer.count_rows(t)
        except Exception as ex:
            logger.warning(f"  ? {t}: cannot count target ({ex})")
            mismatches += 1
            continue
        if e["rows"] == dst:
            logger.info(f"  OK {t}: pkg={e['rows']} dst={dst} match")
            matches += 1
        else:
            logger.warning(f"  XX {t}: pkg={e['rows']} dst={dst} mismatch")
            mismatches += 1
    logger.info(f"Package verify: {matches} match, {mismatches} mismatch")
    return {"matches": matches, "mismatches": mismatches}
