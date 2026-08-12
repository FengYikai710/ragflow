#!/usr/bin/env python3
"""
RAGFlow Vastbase → Vastbase migration tool.

Migrates chunk vector data and doc metadata from a *source* Vastbase instance
to a *target* Vastbase instance (both must be the `ragflow` vector database).

Use cases: cluster/server relocation, data relocation, sync between instances.

Usage:
    # List tenants in the source instance
    python migrate.py --src-host vb-src --src-user rag_flow --src-password '***' \\
        --src-db ragflow --list-tenants

    # Migrate one tenant, excluding two datasets (dry-run first)
    python migrate.py \\
        --src-host vb-src --src-port 5432 \\
        --src-user rag_flow --src-password '***' --src-db ragflow \\
        --dst-host vb-dst --dst-port 5432 \\
        --dst-user rag_flow --dst-password '***' --dst-db ragflow \\
        --tenant d253f468394111f1b41e53bb8d88db1c \\
        --exclude-kb-id <kb-to-exclude-1> --exclude-kb-id <kb-to-exclude-2> \\
        --dry-run

    # Real migration with resume
    python migrate.py ... --tenant <t> --exclude-kb-id <kb> --resume

    # Verify row counts after migration
    python migrate.py ... --tenant <t> --exclude-kb-id <kb> --verify

Environment variables:
    VB_DBCOMPATIBILITY   "PG" or "B" — controls target fulltext index syntax
    VB_STATEMENT_TIMEOUT index-build safety timeout (default 10min)
    VB_INDEX_RETRIES     index-build retry count (default 3)
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime

from vb_reader import VBChunkReader
from vb_writer import VBWriter, DOC_META_MAPPING
from identity import identity_batch
from meta_migrator import MetaMigrator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("migrate")

PROGRESS_FILE = ".vb_to_vb_progress.json"


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


def is_doc_meta_table(table_name: str) -> bool:
    return table_name.startswith("ragflow_doc_meta_")


# ── Introspection commands ─────────────────────────────────────────────


def list_tenants(reader: VBChunkReader):
    """List all tenants found in the source, with chunk/doc_meta table counts."""
    chunk_tables = reader.list_chunk_tables()
    doc_meta_tables = reader.list_doc_meta_tables()

    tenant_kbs: dict[str, set] = {}
    tenant_meta: dict[str, int] = {}
    for t in chunk_tables:
        p = VBChunkReader.parse_table_name(t)
        if p:
            tenant_kbs.setdefault(p[1], set()).add(p[2])
    for t in doc_meta_tables:
        p = VBChunkReader.parse_table_name(t)
        if p:
            tenant_meta[p[1]] = tenant_meta.get(p[1], 0) + 1

    all_tenants = sorted(set(tenant_kbs) | set(tenant_meta))
    if not all_tenants:
        print("No RAGFlow tables found in source Vastbase.")
        return

    DASH = "-" * 78
    print(f"\n{DASH}")
    print(f"  Tenants in source Vastbase ({len(all_tenants)})")
    print(DASH)
    print(f"  {'Tenant ID':<42s} {'Chunk KBs':>10s} {'DocMeta':>10s}")
    print(f"  {'-'*42} {'-'*10} {'-'*10}")
    for tid in all_tenants:
        print(f"  {tid:<42s} {len(tenant_kbs.get(tid, set())):>10d} "
              f"{tenant_meta.get(tid, 0):>10d}")
    print(f"  {'-'*42} {'-'*10} {'-'*10}")
    total_kb = sum(len(v) for v in tenant_kbs.values())
    print(f"  {'TOTAL':<42s} {total_kb:>10d} {len(doc_meta_tables):>10d}")
    print(DASH + "\n")


def list_tables(reader: VBChunkReader):
    """List all RAGFlow tables in the source with row counts."""
    chunk_tables = reader.list_chunk_tables()
    doc_meta_tables = reader.list_doc_meta_tables()

    if not chunk_tables and not doc_meta_tables:
        print("No RAGFlow tables found in source Vastbase.")
        return

    DASH = "-" * 78
    print(f"\n{DASH}")
    print(f"  Chunk tables ({len(chunk_tables)})")
    print(DASH)
    print(f"  {'Table':<78s}")
    print(f"  {'-'*78}")
    for t in chunk_tables:
        p = VBChunkReader.parse_table_name(t)
        try:
            rows = reader.count_rows(t)
            dim = reader.get_vector_dim(t)
        except Exception as e:
            rows, dim = -1, -1
            logger.warning(f"  cannot introspect {t}: {e}")
        tag = f"tenant={p[1][:8]}.. kb={p[2][:8]}.." if p else "?"
        print(f"  {t}")
        print(f"    rows={rows}, dim={dim}, {tag}")
    if doc_meta_tables:
        print(f"\n{DASH}")
        print(f"  Doc metadata tables ({len(doc_meta_tables)})")
        print(DASH)
        for t in doc_meta_tables:
            try:
                rows = reader.count_rows(t)
            except Exception:
                rows = -1
            print(f"  {t}  (rows={rows})")
    print(DASH + "\n")


# ── Work-list construction ─────────────────────────────────────────────


def build_table_list(reader: VBChunkReader, args) -> list[str]:
    """Resolve the set of source tables to migrate, applying tenant/kb filters."""
    if args.table:
        tables = [args.table]
        # attach doc_meta sibling for a chunk table, unless --no-meta
        if not args.no_meta:
            p = VBChunkReader.parse_table_name(args.table)
            if p and p[0] == "chunk":
                sibling = f"ragflow_doc_meta_{p[1]}"
                if reader.table_exists(sibling) and sibling not in tables:
                    tables.append(sibling)
        return tables

    chunk_tables = reader.list_chunk_tables(args.tenant)

    exclude_kb = set(args.exclude_kb_id or [])
    target_kbs = {args.kb_id} if args.kb_id else None

    filtered = []
    excluded_count = 0
    for t in chunk_tables:
        p = VBChunkReader.parse_table_name(t)
        if not p:
            continue
        kb = p[2]
        if target_kbs is not None and kb not in target_kbs:
            continue
        if kb in exclude_kb:
            excluded_count += 1
            continue
        filtered.append(t)

    if args.kb_id and not filtered:
        logger.warning(
            f"KB '{args.kb_id}' not found"
            + (f" under tenant {args.tenant}" if args.tenant else "")
        )
    if excluded_count:
        logger.info(f"  Excluded {excluded_count} chunk table(s) by --exclude-kb-id")

    tables = list(filtered)

    # attach doc_meta table(s) for the migrated tenants, unless --no-meta
    if not args.no_meta:
        if args.tenant:
            tenants = {args.tenant}
        else:
            tenants = {p[1] for t in filtered if (p := VBChunkReader.parse_table_name(t))}
        for dmt in reader.list_doc_meta_tables():
            p = VBChunkReader.parse_table_name(dmt)
            if p and p[1] in tenants and dmt not in tables:
                tables.append(dmt)

    return tables


# ── Migration plan + per-table migration ───────────────────────────────


def print_migration_plan(reader: VBChunkReader, vb: VBWriter, tables: list[str]):
    DASH = "-" * 78
    print(f"\n{DASH}")
    print(f"  MIGRATION PLAN — {len(tables)} table(s)")
    print(DASH)
    print(f"  {'Table':<58s} {'Rows':>8s} {'Dim':>5s} {'Dst?':>5s}")
    print(f"  {'-'*58} {'-'*8} {'-'*5} {'-'*5}")
    for t in tables:
        try:
            rows = reader.count_rows(t)
        except Exception:
            rows = -1
        is_meta = is_doc_meta_table(t)
        dim = 0 if is_meta else reader.get_vector_dim(t)
        try:
            dst_exists = vb.table_exists(t)
        except Exception:
            dst_exists = "?"
        print(f"  {t[:58]:<58s} {rows:>8,} {dim:>5d} {'Y' if dst_exists else 'N':>5s}")
    print(DASH + "\n")


def migrate_table(
    reader: VBChunkReader,
    vb: VBWriter,
    table_name: str,
    batch_size: int,
    dry_run: bool,
    resume: bool,
    no_index: bool,
) -> dict:
    """Migrate one table (chunk or doc_meta) from source to target."""
    stats = {"table": table_name, "src_rows": 0, "migrated": 0, "failed": 0}
    is_meta = is_doc_meta_table(table_name)

    try:
        src_count = reader.count_rows(table_name)
    except Exception as e:
        logger.error(f"  Cannot count {table_name}: {e}")
        return stats
    stats["src_rows"] = src_count

    if src_count == 0:
        logger.info(f"  {table_name}: empty, skipping")
        return stats

    progress = load_progress()
    tbl_progress = progress.get(table_name, {})
    if resume and tbl_progress.get("completed"):
        logger.info(f"  [SKIP] {table_name} already completed ({src_count} rows)")
        stats["migrated"] = src_count
        return stats

    vector_dim = 0 if is_meta else reader.get_vector_dim(table_name)
    if not is_meta and vector_dim == 0:
        logger.warning(
            f"  {table_name}: no vector column detected, skipping (not a chunk table?)"
        )
        return stats

    logger.info(
        f"  Processing {table_name} ({src_count:,} rows, dim={vector_dim}, meta={is_meta})"
    )

    if dry_run:
        logger.info(f"  [DRY-RUN] Would migrate {src_count:,} rows to {table_name}")
        return stats

    # ensure target table exists
    table_was_empty = False
    if not vb.table_exists(table_name):
        vb.create_table(
            table_name, vector_dim, DOC_META_MAPPING if is_meta else None
        )
        table_was_empty = True
    else:
        vb.widen_columns(table_name)
        try:
            existing = vb.count_rows(table_name)
            if resume and existing > 0:
                logger.info(f"  Resuming: {existing}/{src_count} rows already in target")
        except Exception:
            pass

    # column intersection (compute once)
    target_columns = set(vb.get_columns(table_name))
    vector_fields = {v["name"] for v in reader.get_vector_columns(table_name)}
    if not target_columns:
        logger.error(f"  Target table {table_name} has no columns, skipping")
        return stats

    migrated = 0
    failed = 0
    batch_count = 0
    for batch in reader.scroll_rows(table_name, batch_size):
        batch_count += 1
        try:
            rows = identity_batch(batch, target_columns, vector_fields)
            inserted = vb.insert_batch(table_name, rows, skip_delete=table_was_empty)
            migrated += inserted
            if batch_count % 5 == 0:
                logger.info(f"    {table_name}: {migrated}/{src_count} migrated")
            if batch_count % 100 == 0:
                tbl_progress["migrated"] = migrated
                tbl_progress["total"] = src_count
                progress[table_name] = tbl_progress
                save_progress(progress)
        except Exception as e:
            logger.error(f"    Batch {batch_count} failed for {table_name}: {e}")
            failed += len(batch)

    # build indexes after bulk load (chunk tables only)
    if not is_meta and not no_index:
        try:
            vb.create_indexes(table_name, vector_dim)
            logger.info(f"  Indexes created for {table_name}")
        except Exception as e:
            logger.warning(f"  Index creation failed for {table_name} (non-fatal): {e}")

    tbl_progress["migrated"] = migrated
    tbl_progress["total"] = src_count
    tbl_progress["completed"] = True
    tbl_progress["completed_at"] = datetime.now().isoformat()
    progress[table_name] = tbl_progress
    save_progress(progress)

    stats["migrated"] = migrated
    stats["failed"] = failed
    logger.info(f"  {table_name}: {migrated}/{src_count} migrated, {failed} failed")
    return stats


def verify_migration(reader: VBChunkReader, vb: VBWriter, tables: list[str]):
    matches = 0
    mismatches = 0
    for t in tables:
        try:
            src = reader.count_rows(t)
        except Exception as e:
            logger.warning(f"  ? {t}: cannot count source ({e})")
            mismatches += 1
            continue
        if not vb.table_exists(t):
            logger.warning(f"  ✗ {t}: table missing in target (src={src})")
            mismatches += 1
            continue
        try:
            dst = vb.count_rows(t)
        except Exception as e:
            logger.warning(f"  ? {t}: cannot count target ({e})")
            mismatches += 1
            continue
        if src == dst:
            logger.info(f"  ✓ {t}: src={src} dst={dst} match")
            matches += 1
        else:
            logger.warning(f"  ✗ {t}: src={src} dst={dst} count_mismatch")
            mismatches += 1
    logger.info(f"Verification complete: {matches} match, {mismatches} mismatch")


# ── Metadata DB (rag_flow) migration ───────────────────────────────────


def _split_csv(s: str | None) -> list[str] | None:
    return [x.strip() for x in s.split(",") if x.strip()] if s else None


def _print_meta_tables(rows: list[dict]):
    DASH = "-" * 78
    print(f"\n{DASH}")
    print(f"  rag_flow metadata tables ({len(rows)}) -- FK topo order")
    print(DASH)
    print(f"  {'#':>3}  {'Table':<40} {'Rows':>10}  {'PK':<20}")
    print(f"  {'-'*3}  {'-'*40} {'-'*10}  {'-'*20}")
    for i, r in enumerate(rows, 1):
        pk = ",".join(r["pk"]) if r["pk"] else "(none)"
        src = r["src"] if r["src"] >= 0 else "?"
        print(f"  {i:>3}  {r['table'][:40]:<40} {str(src):>10}  {pk[:20]:<20}")
    print(DASH + "\n")


def run_meta_mode(args):
    """Handle --migrate-meta / --list-meta-tables (operates on rag_flow)."""
    include = _split_csv(args.meta_include_tables)
    exclude = _split_csv(args.meta_exclude_tables)

    src_reader = VBChunkReader(
        host=args.src_host, port=args.src_port,
        user=args.src_user, password=args.src_password,
        database=args.src_meta_db,
    )
    if not src_reader.health_check():
        logger.error("Cannot connect to source metadata Vastbase")
        sys.exit(1)
    logger.info(f"Source metadata ({args.src_meta_db}) connection OK")

    if args.list_meta_tables:
        mm = MetaMigrator(src_reader, None)
        _print_meta_tables(mm.describe_source(include, exclude))
        src_reader.close()
        return

    writer = VBWriter(
        host=args.dst_host, port=args.dst_port,
        user=args.dst_user, password=args.dst_password,
        database=args.dst_meta_db,
    )
    if not writer.health_check():
        logger.error("Cannot connect to target metadata Vastbase")
        sys.exit(1)
    logger.info(f"Target metadata ({args.dst_meta_db}) connection OK")

    mm = MetaMigrator(src_reader, writer)
    try:
        if args.verify:
            mm.verify(include, exclude)
        else:
            mm.migrate(
                batch_size=args.batch_size,
                dry_run=args.dry_run,
                clear=args.clear_meta,
                include=include,
                exclude=exclude,
                resume=args.resume,
            )
    finally:
        src_reader.close()
        writer.close()


# ── CLI ────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="RAGFlow Vastbase to Vastbase migration tool"
    )

    # Source Vastbase (read)
    parser.add_argument("--src-host", default="localhost", help="Source Vastbase host")
    parser.add_argument("--src-port", type=int, default=5432, help="Source Vastbase port")
    parser.add_argument("--src-user", default="rag_flow", help="Source Vastbase user")
    parser.add_argument("--src-password", default="", help="Source Vastbase password")
    parser.add_argument("--src-db", default="ragflow", help="Source vector database name")

    # Target Vastbase (write)
    parser.add_argument("--dst-host", default=None, help="Target Vastbase host")
    parser.add_argument("--dst-port", type=int, default=5432, help="Target Vastbase port")
    parser.add_argument("--dst-user", default="rag_flow", help="Target Vastbase user")
    parser.add_argument("--dst-password", default="", help="Target Vastbase password")
    parser.add_argument("--dst-db", default="ragflow", help="Target vector database name")

    # Scope
    parser.add_argument("--tenant", default=None,
                        help="Only migrate this tenant_id (omit to migrate all tenants)")
    parser.add_argument("--kb-id", default=None,
                        help="Only migrate this single kb_id (can combine with --tenant)")
    parser.add_argument("--exclude-kb-id", action="append", default=None,
                        help="Exclude a kb_id/dataset (repeatable). Core feature.")
    parser.add_argument("--table", default=None,
                        help="Migrate one specific table directly (bypasses tenant/kb filters)")
    parser.add_argument("--no-meta", action="store_true",
                        help="Skip doc_meta table migration (default: migrate doc_meta)")

    # Execution
    parser.add_argument("--batch-size", type=int, default=1000, help="Rows per batch")
    parser.add_argument("--resume", action="store_true", help="Skip tables already completed")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    parser.add_argument("--verify", action="store_true", help="Compare source vs target row counts")
    parser.add_argument("--no-index", action="store_true",
                        help="Skip vector/fulltext index creation on target")

    # Introspection
    parser.add_argument("--list-tenants", action="store_true", help="List tenants in source")
    parser.add_argument("--list-tables", action="store_true", help="List source tables with row counts")

    # Metadata DB (rag_flow) migration
    parser.add_argument("--migrate-meta", action="store_true",
                        help="Migrate the rag_flow metadata DB (whole-DB copy, NOT per-tenant)")
    parser.add_argument("--src-meta-db", default="rag_flow",
                        help="Source metadata database name (default: rag_flow)")
    parser.add_argument("--dst-meta-db", default="rag_flow",
                        help="Target metadata database name (default: rag_flow)")
    parser.add_argument("--clear-meta", action="store_true",
                        help="Clear target metadata tables before migration (clean mirror)")
    parser.add_argument("--meta-include-tables", default=None,
                        help="Comma-separated metadata tables to include")
    parser.add_argument("--meta-exclude-tables", default=None,
                        help="Comma-separated metadata tables to exclude")
    parser.add_argument("--list-meta-tables", action="store_true",
                        help="List rag_flow tables + row counts + FK order (source only)")

    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

    args = parser.parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Metadata DB mode: operates on rag_flow, independent of vector reader/vb.
    if args.migrate_meta or args.list_meta_tables:
        if args.migrate_meta and not args.dst_host:
            parser.error("--dst-host is required for --migrate-meta")
        run_meta_mode(args)
        return

    needs_target = not (args.list_tenants or args.list_tables)
    if needs_target and not args.dst_host:
        parser.error("--dst-host is required for migration/verify (use --list-tenants/--list-tables for source-only)")

    reader = VBChunkReader(
        host=args.src_host, port=args.src_port,
        user=args.src_user, password=args.src_password,
        database=args.src_db,
    )
    if not reader.health_check():
        logger.error("Cannot connect to source Vastbase")
        sys.exit(1)
    logger.info("Source Vastbase connection OK")

    vb = None
    try:
        if args.list_tenants:
            list_tenants(reader)
            return
        if args.list_tables:
            list_tables(reader)
            return

        # target connection
        vb = VBWriter(
            host=args.dst_host, port=args.dst_port,
            user=args.dst_user, password=args.dst_password,
            database=args.dst_db,
        )
        if not vb.health_check():
            logger.error("Cannot connect to target Vastbase")
            sys.exit(1)
        logger.info("Target Vastbase connection OK")

        tables = build_table_list(reader, args)
        if not tables:
            logger.error("No tables to migrate")
            sys.exit(1)

        if args.verify:
            verify_migration(reader, vb, tables)
            return

        print_migration_plan(reader, vb, tables)

        total = {"tables": 0, "src_rows": 0, "migrated": 0, "failed": 0}
        start = time.time()
        for t in tables:
            logger.info(f"\n{'='*60}\n  Table: {t}\n{'='*60}")
            try:
                s = migrate_table(
                    reader, vb, t,
                    batch_size=args.batch_size,
                    dry_run=args.dry_run,
                    resume=args.resume,
                    no_index=args.no_index,
                )
            except Exception as e:
                logger.error(f"  Table {t} aborted: {e}", exc_info=args.verbose)
                continue
            total["tables"] += 1
            total["src_rows"] += s["src_rows"]
            total["migrated"] += s["migrated"]
            total["failed"] += s["failed"]

        duration = time.time() - start
        logger.info(f"\n{'='*60}")
        logger.info("DRY RUN COMPLETE (no data written)" if args.dry_run else "MIGRATION SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"  Tables:    {total['tables']}")
        logger.info(f"  Src rows:  {total['src_rows']}")
        logger.info(f"  Migrated:  {total['migrated']}")
        logger.info(f"  Failed:    {total['failed']}")
        logger.info(f"  Duration:  {duration:.1f}s")
    finally:
        reader.close()
        if vb is not None:
            vb.close()


if __name__ == "__main__":
    main()
