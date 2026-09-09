"""
rag_flow metadata DB migrator for the Vastbase -> Vastbase tool.

Copies ALL tables of the rag_flow metadata database from a source Vastbase
instance to a target, in foreign-key (parent-first) topological order, using
idempotent ON CONFLICT upserts. Complements the vector-data migration in
migrate.py: chunk vectors live in the `ragflow` DB; business metadata
(knowledgebases, documents, users, dialogs, ...) lives in `rag_flow`. Both
must be migrated for the target instance to be usable.

Design (see plan elegant-honking-adleman.md):
  - Whole-DB copy, NOT per-tenant: ~30 peewee tables with cascading FKs make
    per-tenant carving error-prone. Copy everything; prune tenants afterwards.
  - Target tables must already exist (peewee create_table on RAGFlow init).
    Tables present in source but missing on target are skipped with a warning.
  - Idempotent upsert via INSERT ... ON CONFLICT (pk) DO UPDATE (B mode).
  - FK topo-sort via pg_constraint (Kahn's algorithm; fallback to name order).
    Foreign keys are NOT disabled (avoids superuser-privilege dependence).
  - Tables without a PK: TRUNCATE then plain INSERT (not mid-table resumable).
"""

import json
import logging
import os
import time
from datetime import datetime

from identity import identity_batch

logger = logging.getLogger(__name__)

PROGRESS_FILE = ".vb_to_vb_meta_progress.json"


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


def topo_sort(tables: list[str], fk_deps: dict[str, set[str]]) -> list[str]:
    """Order tables so parents precede children (Kahn's algorithm).

    fk_deps maps child -> {parent, ...}. Only edges whose endpoints are both
    in `tables` are considered. Tables with no inbound dependency come first
    (ties broken by name for determinism). Cycles are broken by appending the
    remaining tables in name order, with a warning.
    """
    table_set = set(tables)
    parents_of: dict[str, set[str]] = {t: set() for t in tables}
    children_of: dict[str, set[str]] = {t: set() for t in tables}
    for child, parents in fk_deps.items():
        if child not in table_set:
            continue
        for parent in parents:
            if parent in table_set and parent != child:
                parents_of[child].add(parent)
                children_of[parent].add(child)

    result: list[str] = []
    placed: set[str] = set()
    queued: set[str] = set(t for t in tables if not parents_of[t])
    ready: list[str] = sorted(queued)
    while ready:
        node = ready.pop(0)
        placed.add(node)
        result.append(node)
        for child in sorted(children_of[node]):
            if child in placed or child in queued:
                continue
            # all parents placed -> safe to emit after this node's level
            if parents_of[child] <= placed:
                queued.add(child)
                ready.append(child)

    remaining = [t for t in tables if t not in placed]
    if remaining:
        logger.warning(
            f"FK cycle detected among {remaining}; appending in name order "
            f"(ordering may not satisfy all FK constraints)"
        )
        result.extend(sorted(remaining))
    return result


class MetaMigrator:
    """Copy the rag_flow metadata DB from a source to a target Vastbase."""

    def __init__(self, reader, writer):
        # reader points at the SOURCE rag_flow, writer at the TARGET rag_flow.
        self.reader = reader
        self.writer = writer

    # ── discovery + ordering ──────────────────────────────────────────────

    def list_tables(self) -> list[str]:
        return self.reader.list_all_tables()

    @staticmethod
    def apply_filters(tables, include, exclude) -> list[str]:
        inc = {t.strip() for t in (include or []) if t.strip()}
        exc = {t.strip() for t in (exclude or []) if t.strip()}
        out = [t for t in tables if (not inc or t in inc) and t not in exc]
        for m in sorted(inc - set(out) - exc):
            logger.warning(f"--meta-include-tables: '{m}' not found in source")
        return out

    def ordered_tables(self, tables: list[str]) -> list[str]:
        deps = self.reader.get_foreign_key_deps()
        return topo_sort(tables, deps)

    def describe(self, include=None, exclude=None) -> list[dict]:
        """Ordered per-table introspection for plan/listing output."""
        tables = self.apply_filters(self.list_tables(), include, exclude)
        ordered = self.ordered_tables(tables)
        out = []
        for t in ordered:
            try:
                pk = self.reader.get_primary_keys(t)
            except Exception:
                pk = []
            try:
                src = self.reader.count_rows(t)
            except Exception as e:
                src = -1
                logger.warning(f"cannot count {t}: {e}")
            try:
                dst_exists = self.writer.table_exists(t)
            except Exception:
                dst_exists = "?"
            out.append({"table": t, "pk": pk, "src": src, "dst_exists": dst_exists})
        return out

    def describe_source(self, include=None, exclude=None) -> list[dict]:
        """Source-only introspection (no target connection needed).
        Used by --list-meta-tables."""
        tables = self.apply_filters(self.list_tables(), include, exclude)
        ordered = self.ordered_tables(tables)
        out = []
        for t in ordered:
            try:
                pk = self.reader.get_primary_keys(t)
            except Exception:
                pk = []
            try:
                src = self.reader.count_rows(t)
            except Exception as e:
                src = -1
                logger.warning(f"cannot count {t}: {e}")
            out.append({"table": t, "pk": pk, "src": src})
        return out

    # ── migration ─────────────────────────────────────────────────────────

    def migrate(
        self,
        batch_size: int = 1000,
        dry_run: bool = False,
        clear: bool = False,
        include=None,
        exclude=None,
        resume: bool = False,
    ) -> dict:
        tables = self.apply_filters(self.list_tables(), include, exclude)
        if not tables:
            logger.error("No metadata tables to migrate")
            return {"tables": 0, "rows": 0, "failed": 0}
        ordered = self.ordered_tables(tables)

        logger.info(f"Metadata migration: {len(ordered)} table(s) in FK order")
        for i, t in enumerate(ordered, 1):
            logger.info(f"  {i:>2}. {t}")

        if dry_run:
            for t in ordered:
                try:
                    src = self.reader.count_rows(t)
                except Exception:
                    src = -1
                logger.info(f"  [DRY-RUN] {t}: {src:,} rows")
            return {"tables": len(ordered), "rows": 0, "failed": 0}

        # Tables missing on target can't be migrated (peewee must create them).
        migratable = []
        for t in ordered:
            if not self.writer.table_exists(t):
                logger.warning(
                    f"  SKIP {t}: missing on target — deploy RAGFlow on the "
                    f"target to create metadata tables first"
                )
                continue
            migratable.append(t)

        if clear:
            logger.info("Clearing target metadata tables (--clear-meta) ...")
            # reverse FK order: children truncated before parents
            for t in reversed(migratable):
                try:
                    self.writer.truncate(t)
                except Exception as e:
                    logger.warning(f"  truncate {t} failed (non-fatal): {e}")

        total = {"tables": 0, "rows": 0, "failed": 0}
        start = time.time()
        for t in migratable:
            logger.info(f"\n{'='*60}\n  Metadata table: {t}\n{'='*60}")
            try:
                s = self._migrate_table(t, batch_size=batch_size, resume=resume)
            except Exception as e:
                logger.error(f"  Table {t} aborted: {e}", exc_info=True)
                continue
            total["tables"] += 1
            total["rows"] += s["migrated"]
            total["failed"] += s["failed"]

        dur = time.time() - start
        logger.info(f"\n{'='*60}")
        logger.info("METADATA MIGRATION SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"  Tables:   {total['tables']}")
        logger.info(f"  Rows:     {total['rows']}")
        logger.info(f"  Failed:   {total['failed']}")
        logger.info(f"  Duration: {dur:.1f}s")
        return total

    def _migrate_table(self, table: str, batch_size: int = 1000,
                       resume: bool = False) -> dict:
        stats = {"table": table, "src_rows": 0, "migrated": 0, "failed": 0}
        try:
            src_count = self.reader.count_rows(table)
        except Exception as e:
            logger.error(f"  Cannot count {table}: {e}")
            return stats
        stats["src_rows"] = src_count

        if src_count == 0:
            logger.info(f"  {table}: empty, skipping")
            return stats

        progress = load_progress()
        tp = progress.get(table, {})
        if resume and tp.get("completed"):
            logger.info(f"  [SKIP] {table} already completed ({src_count} rows)")
            stats["migrated"] = src_count
            return stats

        pk = self.reader.get_primary_keys(table)
        if pk:
            logger.info(f"  {table}: {src_count:,} rows, pk={pk}")
        else:
            logger.warning(
                f"  {table}: {src_count:,} rows, NO primary key — using "
                f"TRUNCATE+INSERT (not mid-table resumable)"
            )

        target_columns = set(self.writer.get_columns(table))
        if not target_columns:
            logger.error(f"  Target {table} has no columns, skipping")
            return stats

        # No-PK path: clear first so plain INSERTs are idempotent as a unit.
        if not pk:
            try:
                self.writer.truncate(table)
            except Exception as e:
                logger.error(f"  Cannot truncate {table} before insert: {e}")
                return stats

        migrated = 0
        failed = 0
        batch_no = 0
        for batch in self.reader.scroll_rows(table, batch_size):
            batch_no += 1
            try:
                # vector_fields=set(): meta tables have no q_*_vec columns,
                # so identity_batch does pure column-intersection.
                rows = identity_batch(batch, target_columns, set())
                inserted = self.writer.upsert_batch(table, rows, pk)
                migrated += inserted
                if batch_no % 5 == 0:
                    logger.info(f"    {table}: {migrated}/{src_count}")
                if batch_no % 100 == 0:
                    tp["migrated"] = migrated
                    tp["total"] = src_count
                    progress[table] = tp
                    save_progress(progress)
            except Exception as e:
                logger.error(f"    Batch {batch_no} failed for {table}: {e}")
                failed += len(batch)

        tp["migrated"] = migrated
        tp["total"] = src_count
        tp["completed"] = True
        tp["completed_at"] = datetime.now().isoformat()
        progress[table] = tp
        save_progress(progress)

        stats["migrated"] = migrated
        stats["failed"] = failed
        logger.info(f"  {table}: {migrated}/{src_count} migrated, {failed} failed")
        return stats

    # ── verify ────────────────────────────────────────────────────────────

    def verify(self, include=None, exclude=None) -> dict:
        tables = self.apply_filters(self.list_tables(), include, exclude)
        matches = 0
        mismatches = 0
        for t in tables:
            try:
                src = self.reader.count_rows(t)
            except Exception as e:
                logger.warning(f"  ? {t}: cannot count source ({e})")
                mismatches += 1
                continue
            if not self.writer.table_exists(t):
                logger.warning(f"  - {t}: missing on target (src={src})")
                mismatches += 1
                continue
            try:
                dst = self.writer.count_rows(t)
            except Exception as e:
                logger.warning(f"  ? {t}: cannot count target ({e})")
                mismatches += 1
                continue
            if src == dst:
                logger.info(f"  OK {t}: src={src} dst={dst} match")
                matches += 1
            else:
                logger.warning(f"  XX {t}: src={src} dst={dst} mismatch")
                mismatches += 1
        logger.info(f"Meta verify: {matches} match, {mismatches} mismatch")
        return {"matches": matches, "mismatches": mismatches}
