#!/usr/bin/env python3
"""
RAGFlow ES → Vastbase migration tool.

Migrates document chunk data from Elasticsearch to Vastbase.

Usage:
    # Discover indices and migrate all
    python migrate.py --es-host localhost --es-port 9200 \\
        --vb-host localhost --vb-port 5432 \\
        --vb-user vastbase --vb-password 'Vastdata@123' \\
        --vb-db vastbase \\
        --mysql-host localhost --mysql-password 'infini_rag_flow'

    # Migrate a specific ES index
    python migrate.py --es-host localhost --es-port 9200 \\
        --vb-host localhost --vb-port 5432 \\
        --vb-user vastbase --vb-password 'Vastdata@123' \\
        --vb-db vastbase \\
        --mysql-host localhost --mysql-password 'infini_rag_flow' \\
        --index ragflow_xxx

    # Migrate only a specific knowledge base
    python migrate.py ... --index ragflow_xxx --kb-id <kb_uuid>

    # Dry-run (preview only)
    python migrate.py ... --dry-run

    # Resume interrupted migration
    python migrate.py ... --resume

    # Verify data consistency after migration
    python migrate.py ... --verify

    # Migrate all ragflow_* indices in ES, no MySQL needed
    python migrate.py --no-mysql ...

    # Read metadata from Vastbase (same schema as MySQL) instead of MySQL
    python migrate.py --es-host localhost --es-port 9200 \\
        --vb-host localhost --vb-port 5432 \\
        --vb-user vastbase --vb-password 'Vastdata@123' \\
        --vb-db rag_flow \\
        --use-vb-meta ...

Environment variables:
    VB_DBCOMPATIBILITY  Set to "PG" or "B" for fulltext index creation

--no-mysql mode:
    Skips MySQL validation entirely. KB list comes from ES aggregation
    (terms agg on kb_id) and chunk filtering uses only the kb_id term
    filter (no MySQL doc_id whitelist). Use this when MySQL is
    unreachable but you still want to migrate ES data.
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime

from es_reader import ESReader
from vb_writer import VBWriter, DOC_META_MAPPING
from mysql_reader import MySQLReader
from vb_meta_reader import VBMetaReader
from converter import detect_vector_size, convert_batch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("migrate")

# Suppress verbose HTTP trace logging from ES client libraries.
# These log every POST at INFO level (e.g. "POST http://.../_count [status:200 duration:0.004s]").
# Called again in main() after imports are resolved.
def _silence_noisy_loggers():
    for name in ("elasticsearch", "elastic_transport", "urllib3", "httpx"):
        logging.getLogger(name).setLevel(logging.WARNING)


_silence_noisy_loggers()

PROGRESS_FILE = ".es_to_vb_progress.json"


def load_progress() -> dict:
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {}


def save_progress(progress: dict):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


def list_indices(args, mysql: MySQLReader | VBMetaReader | None = None):
    """List all ragflow_* indices in ES, optionally cross-referenced with metadata."""
    es = ESReader(
        host=args.es_host,
        port=args.es_port,
        username=args.es_user,
        password=args.es_password,
    )
    indices = es.list_ragflow_indices()
    if not indices:
        print("No ragflow_* indices found in Elasticsearch.")
        es.close()
        return

    # Pre-load all metadata KBs if available
    meta_all_kbs: list[dict] | None = None
    meta_label = ""
    if mysql is not None:
        meta_all_kbs = mysql.list_all_knowledge_bases()
        meta_label = mysql.source_label

    DASH = "-" * 78
    print(f"\nRAGFlow indices ({len(indices)} found):")
    for idx in indices:
        count = es.count_documents(idx)
        kbs = es.list_knowledge_bases(idx)
        kb_count = len(kbs)

        print(f"\n  Index: {idx}")
        print(f"  Total: {count:,} docs, {kb_count} KB(s)")
        print(f"  {DASH}")

        if not kbs:
            continue

        if meta_all_kbs is None or is_doc_meta_index(idx):
            # Simple list — no metadata cross-reference
            print(f"  {'KB ID':<42s} {'Docs':>10s}")
            print(f"  {'─'*42} {'─'*10}")
            for kb in kbs:
                print(f"  {kb['kb_id']:<42s} {kb['doc_count']:>10,d}")
            print(f"  {'─'*42} {'─'*10}")
            print(f"  {'Total':<42s} {count:>10,d}")
        else:
            # Cross-reference with metadata
            tenant_id = idx.replace("ragflow_", "")
            meta_kbs = [kb for kb in meta_all_kbs if kb["tenant_id"] == tenant_id]
            meta_kb_ids = {kb["kb_id"] for kb in meta_kbs}

            print(f"  {'KB ID':<42s} {'Status':<20s} {'Docs':>10s}")
            print(f"  {'─'*42} {'─'*20} {'─'*10}")
            for kb in kbs:
                kb_id = kb["kb_id"]
                if kb_id in meta_kb_ids:
                    status = f"✓ in {meta_label}"
                else:
                    status = f"✗ not in {meta_label}"
                print(f"  {kb_id:<42s} {status:<20s} {kb['doc_count']:>10,d}")
            print(f"  {'─'*42} {'─'*20} {'─'*10}")

            # Per-index summary
            matched = {kb["kb_id"] for kb in kbs if kb["kb_id"] in meta_kb_ids}
            orphaned = {kb["kb_id"] for kb in kbs if kb["kb_id"] not in meta_kb_ids}
            matched_docs = sum(kb["doc_count"] for kb in kbs if kb["kb_id"] in matched)
            orphaned_docs = sum(kb["doc_count"] for kb in kbs if kb["kb_id"] in orphaned)
            print(f"  Total KBs in ES: {kb_count}  ({matched_docs:,} docs will migrate, "
                  f"{orphaned_docs:,} docs orphaned)")
        print()

    es.close()


def is_doc_meta_index(index_name: str) -> bool:
    """Check if an index name is a doc_metadata index."""
    return index_name.startswith("ragflow_doc_meta_")


def print_migration_plan(
    es: ESReader,
    mysql: MySQLReader | VBMetaReader | None,
    indices_to_migrate: list[str],
    no_mysql: bool = False,
    exclude_kb_ids: set | None = None,
):
    """Before migration, cross-reference ES KBs against metadata KBs and
    print a plan showing what will be migrated, what is orphaned, and what
    exists only in metadata."""

    chunk_indices = [i for i in indices_to_migrate if not is_doc_meta_index(i)]
    if not chunk_indices:
        return

    DASH = "-" * 78
    print(f"\n{DASH}")
    print(f"  MIGRATION PLAN — ES ↔ Metadata Cross-Reference")
    print(f"{DASH}")

    total_es_kbs = 0
    total_meta_kbs = 0
    total_match = 0
    total_orphan = 0
    total_no_data = 0

    for idx in chunk_indices:
        tenant_id = idx.replace("ragflow_", "")

        # KBs found in ES (terms aggregation)
        es_kbs = es.list_knowledge_bases(idx)
        es_kb_map = {kb["kb_id"]: kb["doc_count"] for kb in es_kbs}

        if exclude_kb_ids:
            es_kbs = [kb for kb in es_kbs if kb["kb_id"] not in exclude_kb_ids]

        print(f"\n  Index: {idx}  (tenant: {tenant_id})")
        print(f"  {DASH}")

        if no_mysql:
            print(f"  {'KB ID':<42s} {'ES Docs':>10s}")
            print(f"  {'─'*42} {'─'*10}")
            for kb in es_kbs:
                print(f"  {kb['kb_id']:<42s} {kb['doc_count']:>10,d}")
            total_es_docs = sum(kb["doc_count"] for kb in es_kbs)
            print(f"  {'─'*42} {'─'*10}")
            print(f"  {'ES total':<42s} {total_es_docs:>10,d}")
            print(f"  {'ES KB count':<42s} {len(es_kbs):>10d}")
            total_es_kbs += len(es_kbs)
            print(f"  {DASH}")
            continue

        # KBs found in metadata (MySQL / Vastbase)
        all_meta_kbs = mysql.list_all_knowledge_bases()
        meta_kbs = [kb for kb in all_meta_kbs if kb["tenant_id"] == tenant_id]
        meta_kb_map = {kb["kb_id"]: kb["doc_count"] for kb in meta_kbs}

        es_kb_ids = set(es_kb_map.keys())
        meta_kb_ids = set(meta_kb_map.keys())

        matched = es_kb_ids & meta_kb_ids
        orphaned = es_kb_ids - meta_kb_ids
        no_data = meta_kb_ids - es_kb_ids

        if exclude_kb_ids:
            matched -= exclude_kb_ids
            orphaned -= exclude_kb_ids

        label = mysql.source_label

        # Table header
        print(f"  {'KB ID':<42s} {'Status':<14s} {'ES Docs':>10s} {label + ' Docs':>10s}")
        print(f"  {'─'*42} {'─'*14} {'─'*10} {'─'*10}")

        # All KB rows sorted together (matched first, then orphaned, then meta-only)
        all_kb_ids = sorted(matched) + sorted(orphaned) + sorted(no_data)
        for kb_id in all_kb_ids:
            if kb_id in matched:
                status = "✓ migrate"
                es_d = es_kb_map[kb_id]
                meta_d = meta_kb_map[kb_id]
                es_str = f"{es_d:>10,}"
                meta_str = f"{meta_d:>10,}"
            elif kb_id in orphaned:
                status = "✗ orphan"
                es_d = es_kb_map[kb_id]
                es_str = f"{es_d:>10,}"
                meta_str = f"{'─':>10}"
            else:
                status = "⚠ meta-only"
                meta_d = meta_kb_map[kb_id]
                es_str = f"{'─':>10}"
                meta_str = f"{meta_d:>10,}"
            print(f"  {kb_id:<42s} {status:<14s} {es_str} {meta_str}")

        # Per-index subtotals
        matched_docs = sum(es_kb_map[kb] for kb in matched)
        orphaned_docs = sum(es_kb_map[kb] for kb in orphaned)
        no_data_docs = sum(meta_kb_map[kb] for kb in no_data)
        total_es_docs = sum(es_kb_map.values())

        print(f"  {'─'*42} {'─'*14} {'─'*10} {'─'*10}")
        print(f"  {'Total KBs in ES':<42s} {len(es_kb_ids):>3d} KBs, {total_es_docs:>9,d} ES docs")
        print(f"  {'Total KBs in ' + label:<42s} {len(meta_kb_ids):>3d} KBs")
        print(f"  {'─'*78}")
        print(f"  ✓ migrate: {len(matched)} KBs, {matched_docs:,} docs"
              f"  |  ✗ orphan: {len(orphaned)} KBs, {orphaned_docs:,} docs"
              f"  |  ⚠ {label} only: {len(no_data)} KBs, {no_data_docs:,} docs")
        print(f"  {DASH}")

        total_match += len(matched)
        total_orphan += len(orphaned)
        total_no_data += len(no_data)
        total_es_kbs += len(es_kb_ids)
        total_meta_kbs += len(meta_kb_ids)

    # Global summary
    print(f"\n{'='*78}")
    print(f"  PLAN SUMMARY")
    print(f"{'='*78}")
    print(f"  Chunk indices to process:  {len(chunk_indices)}")
    if no_mysql:
        print(f"  ES KBs (all will migrate): {total_es_kbs}")
    else:
        print(f"  Matched (will migrate):    {total_match} KBs")
        print(f"  Orphaned (skipped):        {total_orphan} KBs")
        print(f"  In {mysql.source_label} only:           {total_no_data} KBs")
        print(f"  Total unique ES KBs:       {total_es_kbs}")
        print(f"  Total unique {mysql.source_label} KBs:  {total_meta_kbs}")
    print()


def migrate_index(
    es: ESReader,
    vb: VBWriter,
    mysql: MySQLReader | VBMetaReader | None,
    index_name: str,
    target_kb_id: str | None,
    batch_size: int,
    dry_run: bool,
    resume: bool,
    no_mysql: bool = False,
    exclude_kb_ids: set | None = None,
) -> dict:
    """Migrate one ES index to Vastbase. Returns stats dict."""
    stats = {
        "index": index_name,
        "kb_count": 0,
        "total_es": 0,
        "total_migrated": 0,
        "total_failed": 0,
        "tables": [],
    }

    is_meta = is_doc_meta_index(index_name)

    if is_meta:
        table_name = index_name
        total_docs = es.count_documents(index_name)
        kbs = es.list_knowledge_bases(index_name)

        progress_data = load_progress()
        index_progress = progress_data.get(index_name, {})

        kb_progress = index_progress.get("__all__", {})
        if resume and kb_progress.get("completed"):
            logger.info(
                f"  [SKIP] {table_name} already completed ({total_docs} docs)"
            )
            stats["total_es"] = total_docs
            stats["total_migrated"] = total_docs
            stats["kb_count"] = len(kbs)
            stats["tables"].append(table_name)
            return stats

        logger.info(
            f"  Processing doc_meta: {index_name} ({total_docs} docs) → table: {table_name}"
        )

        table_created = vb.table_exists(table_name)
        if not table_created and not dry_run:
            vb.create_table(table_name, vector_size=0, mapping=DOC_META_MAPPING)
            table_was_empty = True
        elif not table_created and dry_run:
            logger.info(f"  [DRY-RUN] Would create table: {table_name}")
            table_was_empty = True
        else:
            table_was_empty = False

        # Widen columns for existing doc_meta tables
        if table_created and not dry_run:
            vb.widen_columns(table_name)


        already_migrated = 0
        if not dry_run and vb.table_exists(table_name):
            already_migrated = vb.count_rows(table_name)
        if resume and already_migrated > 0:
            logger.info(f"  Resuming: {already_migrated}/{total_docs} already in table")

        if dry_run:
            logger.info(f"  [DRY-RUN] Would migrate {total_docs} docs to {table_name}")
            stats["total_es"] = total_docs
            stats["kb_count"] = len(kbs)
            stats["tables"].append(table_name)
            return stats

        migrated = 0
        failed = 0
        batch_count = 0

        for batch in es.scroll_documents(index_name, batch_size):
            batch_count += 1
            try:
                rows = convert_batch(batch)
                # Doc_meta table only has DOC_META_MAPPING fields.
                # Remove any extra fields (e.g. available_int added by
                # convert_document) that would cause INSERT failure.
                rows = [
                    {k: v for k, v in row.items() if k in DOC_META_MAPPING}
                    for row in rows
                ]
                skip_delete = table_was_empty
                inserted = vb.insert_batch(table_name, rows, skip_delete=skip_delete)
                migrated += inserted

                if batch_count % 100 == 0:
                    index_progress.setdefault("__all__", {})
                    index_progress["__all__"]["migrated"] = migrated
                    index_progress["__all__"]["total"] = total_docs
                    progress_data[index_name] = index_progress
                    save_progress(progress_data)

                if batch_count % 5 == 0:
                    logger.info(f"    {table_name}: {migrated}/{total_docs} migrated")
            except Exception as e:
                logger.error(f"    Batch insert failed for {table_name}: {e}")
                failed += len(batch)

        if migrated >= total_docs:
            index_progress.setdefault("__all__", {})
            index_progress["__all__"]["completed"] = True
            index_progress["__all__"]["completed_at"] = datetime.now().isoformat()
            progress_data[index_name] = index_progress
            save_progress(progress_data)

        stats["kb_count"] = len(kbs)
        stats["total_es"] = total_docs
        stats["total_migrated"] = migrated
        stats["total_failed"] = failed
        stats["tables"].append(table_name)

        logger.info(
            f"    {table_name}: {migrated}/{total_docs} migrated, {failed} failed"
        )
        return stats

    # ---- Chunk data ----
    # Extract tenant_id from ES index name: ragflow_{tenant_id}
    tenant_id = index_name.replace("ragflow_", "")

    # Get the list of KBs to migrate. By default we read MySQL so we only
    # migrate KBs that still exist in metadata (skipping orphans in ES).
    # When --no-mysql is set, we fall back to ES aggregation on kb_id.
    if no_mysql:
        kbs = es.list_knowledge_bases(index_name)
        logger.info(
            f"  [no-mysql] Discovered {len(kbs)} KB(s) in ES index {index_name}"
        )
    else:
        mysql_kbs = mysql.list_all_knowledge_bases()
        # Filter KBs belonging to this tenant (by matching tenant_id)
        kbs = [kb for kb in mysql_kbs if kb["tenant_id"] == tenant_id]

    if target_kb_id:
        kbs = [kb for kb in kbs if kb["kb_id"] == target_kb_id]
        if not kbs:
            source = "ES" if no_mysql else mysql.source_label
            logger.warning(
                f"KB '{target_kb_id}' not found in {source} for tenant {tenant_id}"
            )
            return stats

    if exclude_kb_ids:
        before = len(kbs)
        kbs = [kb for kb in kbs if kb["kb_id"] not in exclude_kb_ids]
        skipped = before - len(kbs)
        if skipped:
            logger.info(f"  Excluded {skipped} KB(s) by --exclude-kb-id")

    if not kbs:
        source = "ES" if no_mysql else mysql.source_label
        logger.warning(
            f"No active KBs found in {source} for tenant {tenant_id} (index {index_name})"
        )
        return stats

    # Load progress for resume
    progress_data = load_progress()
    index_progress = progress_data.get(index_name, {})

    for kb_info in kbs:
        kb_id = kb_info["kb_id"]
        doc_count = kb_info["doc_count"]
        table_name = f"{index_name}_{kb_id}"

        # Check resume skip
        kb_progress = index_progress.get(kb_id, {})
        if resume and kb_progress.get("completed"):
            logger.info(f"  [SKIP] {table_name} already completed ({doc_count} docs)")
            stats["total_es"] += doc_count
            stats["total_migrated"] += doc_count
            stats["kb_count"] += 1
            stats["tables"].append(table_name)
            continue

        logger.info(
            f"  Processing KB: {kb_id} ({doc_count} docs) → table: {table_name}"
        )

        # Build the ES filter query. By default we restrict to the
        # authoritative doc_id whitelist from MySQL/Vastbase; in --no-mysql
        # or --use-vb-meta mode we filter by kb_id only (the metadata source
        # is already authoritative so the doc_id terms filter is redundant
        # and extremely slow with many thousands of IDs).
        if no_mysql or isinstance(mysql, VBMetaReader):
            filter_query = {"bool": {"filter": [{"term": {"kb_id": kb_id}}]}}
        else:
            # Get valid doc_ids from MySQL — these are the authoritative set
            valid_doc_ids = mysql.get_doc_ids_by_kb(kb_id)
            if not valid_doc_ids:
                logger.warning(
                    f"  No active documents in {mysql.source_label} for KB {kb_id}, skipping"
                )
                continue

            logger.info(
                f"  Found {len(valid_doc_ids)} valid document IDs in {mysql.source_label}"
            )

            filter_query = {
                "bool": {
                    "filter": [
                        {"term": {"kb_id": kb_id}},
                        {"terms": {"doc_id": valid_doc_ids}},
                    ]
                }
            }

        # Get a sample doc from ES to detect vector size
        sample_iter = es.scroll_documents(
            index_name, batch_size=1, query=filter_query
        )
        sample_batch = next(sample_iter, None)
        if not sample_batch or not sample_batch[0]:
            logger.warning(
                f"  No sample doc found in ES for KB {kb_id}, skipping"
            )
            continue

        vector_size = detect_vector_size(sample_batch[0])
        if vector_size == 0:
            logger.error(f"  Cannot detect vector size for KB {kb_id}, skipping")
            continue

        logger.info(f"  Detected vector size: {vector_size}")

        # Get actual chunk count from ES for accurate progress tracking
        chunk_count = es.count_documents(index_name, query=filter_query)
        if no_mysql:
            logger.info(f"  ES chunks: {chunk_count}")
        else:
            logger.info(f"  {mysql.source_label} documents: {doc_count}, ES chunks: {chunk_count}")

        # Create table
        table_was_empty = False
        table_created = vb.table_exists(table_name)
        if not table_created and not dry_run:
            vb.create_table(table_name, vector_size)
            table_was_empty = True
        elif not table_created and dry_run:
            logger.info(f"  [DRY-RUN] Would create table: {table_name}")
            logger.info(f"  [DRY-RUN] Would create table: {table_name}")

        # Widen existing table columns that may have been created
        # with too-narrow varchar(256) → text
        if table_created and not dry_run:
            vb.widen_columns(table_name)
        already_migrated = 0
        if not dry_run and vb.table_exists(table_name):
            already_migrated = vb.count_rows(table_name, kb_id)

        if resume and already_migrated > 0:
            logger.info(
                f"  Resuming: {already_migrated}/{chunk_count} chunks already in table"
            )

        if dry_run:
            logger.info(
                f"  [DRY-RUN] Would migrate {chunk_count} chunks to {table_name}"
            )
            stats["kb_count"] += 1
            stats["total_es"] += chunk_count
            stats["tables"].append(table_name)
            continue

        # Migrate data
        migrated = 0
        failed = 0
        batch_count = 0

        for batch in es.scroll_documents(
            index_name, batch_size, query=filter_query, total_hint=chunk_count
        ):
            batch_count += 1
            try:
                before = time.time()
                rows = convert_batch(batch)
                convert_ms = (time.time() - before) * 1000
                skip_delete = table_was_empty
                before = time.time()
                inserted = vb.insert_batch(table_name, rows, skip_delete=skip_delete)
                insert_ms = (time.time() - before) * 1000
                migrated += inserted

                logger.info(
                    f"    Batch {batch_count}: {table_name}: "
                    f"{migrated}/{chunk_count} chunks migrated, "
                    f"convert={convert_ms:.0f}ms insert={insert_ms:.0f}ms"
                )

                if batch_count % 100 == 0:
                    index_progress.setdefault(kb_id, {})
                    index_progress[kb_id]["migrated"] = migrated
                    index_progress[kb_id]["total"] = doc_count
                    progress_data[index_name] = index_progress
                    save_progress(progress_data)
            except Exception as e:
                logger.error(f"    Batch {batch_count} insert failed for {table_name}: {e}")
                failed += len(batch)

        if migrated + failed >= chunk_count:
            # Create indexes after all data is migrated
            if not dry_run:
                vb.create_indexes(table_name, vector_size)
                logger.info(f"  Indexes created for {table_name}")

            index_progress.setdefault(kb_id, {})
            index_progress[kb_id]["completed"] = True
            index_progress[kb_id]["completed_at"] = datetime.now().isoformat()
            progress_data[index_name] = index_progress
            save_progress(progress_data)

        stats["kb_count"] += 1
        stats["total_es"] += chunk_count
        stats["total_migrated"] += migrated
        stats["total_failed"] += failed
        stats["tables"].append(table_name)

        if failed:
            logger.info(
                f"    {table_name}: {migrated}/{doc_count} migrated, {failed} failed"
            )
        else:
            logger.info(f"    {table_name}: {migrated}/{doc_count} migrated")

    return stats


def verify_migration(
    es: ESReader, vb: VBWriter, mysql: MySQLReader | VBMetaReader | None,
    index_name: str, kb_id: str | None,
    no_mysql: bool = False,
    exclude_kb_ids: set | None = None,
) -> dict:
    """Compare source-of-truth counts with Vastbase row counts.

    By default MySQL document counts are used as the source of truth for
    chunk indices. With --no-mysql, ES is queried directly (terms agg on
    kb_id) instead.
    """
    result = {"index": index_name, "kb_id": kb_id, "matches": [], "mismatches": []}

    if is_doc_meta_index(index_name):
        table_name = index_name
        es_count = es.count_documents(index_name)

        if not vb.table_exists(table_name):
            result["mismatches"].append({
                "kb_id": "__all__", "es_count": es_count,
                "vb_count": 0, "status": "table_missing",
            })
            return result

        vb_count = vb.count_rows(table_name)
        status = "match" if es_count == vb_count else "count_mismatch"
        entry = {"kb_id": "__all__", "es_count": es_count, "vb_count": vb_count}
        (result["matches"] if status == "match" else result["mismatches"]).append(entry)
        return result

    # Chunk data: pick a source of truth for the count
    if no_mysql:
        # In --no-mysql mode the ES aggregation is the only available count
        kbs = es.list_knowledge_bases(index_name)
    else:
        mysql_kbs = mysql.list_all_knowledge_bases()
        tenant_id = index_name.replace("ragflow_", "")
        kbs = [kb for kb in mysql_kbs if kb["tenant_id"] == tenant_id]

    if kb_id:
        kbs = [kb for kb in kbs if kb["kb_id"] == kb_id]

    if exclude_kb_ids:
        kbs = [kb for kb in kbs if kb["kb_id"] not in exclude_kb_ids]

    for kb in kbs:
        kb_id_val = kb["kb_id"]
        table_name = f"{index_name}_{kb_id_val}"
        source_count = kb["doc_count"]

        if not vb.table_exists(table_name):
            result["mismatches"].append({
                "kb_id": kb_id_val, "es_count": source_count,
                "vb_count": 0, "status": "table_missing",
            })
            continue

        vb_count = vb.count_rows(table_name, kb_id_val)
        if source_count == vb_count:
            result["matches"].append({
                "kb_id": kb_id_val, "es_count": source_count, "vb_count": vb_count,
            })
        else:
            result["mismatches"].append({
                "kb_id": kb_id_val, "es_count": source_count,
                "vb_count": vb_count, "status": "count_mismatch",
            })

    return result


def main():
    parser = argparse.ArgumentParser(
        description="RAGFlow ES → Vastbase migration tool"
    )

    # ES connection
    parser.add_argument("--es-host", default="localhost", help="Elasticsearch host")
    parser.add_argument("--es-port", type=int, default=9200, help="Elasticsearch port")
    parser.add_argument("--es-user", default=None, help="Elasticsearch username")
    parser.add_argument("--es-password", default=None, help="Elasticsearch password")

    # MySQL connection
    parser.add_argument("--mysql-host", default="localhost", help="MySQL host")
    parser.add_argument("--mysql-port", type=int, default=3306, help="MySQL port")
    parser.add_argument("--mysql-user", default="root", help="MySQL user")
    parser.add_argument(
        "--mysql-password", default="infini_rag_flow", help="MySQL password"
    )
    parser.add_argument("--mysql-db", default="rag_flow", help="MySQL database")

    # VB connection
    parser.add_argument("--vb-host", default="localhost", help="Vastbase host")
    parser.add_argument("--vb-port", type=int, default=5432, help="Vastbase port")
    parser.add_argument("--vb-user", default="rag_flow", help="Vastbase user")
    parser.add_argument("--vb-password", default="", help="Vastbase password")
    parser.add_argument("--vb-db", default="rag_flow", help="Vastbase database")

    # Migration options
    parser.add_argument("--index", "-i", default=None, help="ES index to migrate (omit to migrate all)")
    parser.add_argument("--kb-id", default=None, help="Specific KB ID to migrate")
    parser.add_argument("--batch-size", type=int, default=500, help="Documents per batch")
    parser.add_argument("--resume", action="store_true", help="Resume from previous progress")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    parser.add_argument("--no-mysql", action="store_true",
                        help="Skip MySQL validation. KB list comes from ES aggregation "
                             "and chunk filtering uses only the kb_id term filter. "
                             "Useful when MySQL is unreachable.")
    parser.add_argument("--use-vb-meta", action="store_true",
                        help="Read metadata from Vastbase instead of MySQL "
                             "(uses --vb-* connection parameters)")
    parser.add_argument("--exclude", action="append", default=None,
                        help="Exclude an ES index from migration (can be specified multiple times). "
                             "Default excludes ragflow_d253f468394111f1b41e53bb8d88db1c")
    parser.add_argument("--exclude-kb-id", action="append", default=None,
                        help="Exclude a specific KB ID from migration (can be specified multiple times)")

    # Commands
    parser.add_argument("--list-indices", action="store_true", help="List RAGFlow indices")
    parser.add_argument("--verify", action="store_true", help="Verify migration data")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # ES client (shared)
    es = ESReader(
        host=args.es_host,
        port=args.es_port,
        username=args.es_user,
        password=args.es_password,
    )

    try:
        health = es.health_check()
        es_status = health.get("status", "unknown")
        if es_status not in ("green", "yellow"):
            logger.error(f"Elasticsearch cluster unhealthy: {es_status}")
            sys.exit(1)
        logger.info(f"Elasticsearch cluster status: {es_status}")

        # Suppress noisy HTTP trace logging from ES/urllib3 now that imports
        # are resolved and all sub-loggers exist.
        _silence_noisy_loggers()

        # Metadata client (optional — skipped when --no-mysql).
        # Set up early so --list-indices can show cross-reference.
        mysql: MySQLReader | VBMetaReader | None = None
        if args.no_mysql:
            if not args.list_indices:
                logger.info("Running in --no-mysql mode: metadata will not be used")
        elif args.use_vb_meta:
            logger.info("Reading metadata from Vastbase (database: rag_flow)...")
            try:
                mysql = VBMetaReader(
                    host=args.vb_host,
                    port=args.vb_port,
                    user=args.vb_user,
                    password=args.vb_password,
                )
                if not mysql.health_check():
                    raise ConnectionError("health check failed")
                logger.info("Vastbase metadata connection OK")
            except Exception as e:
                logger.warning(f"Cannot connect to Vastbase metadata: {e}")
                if not args.list_indices:
                    sys.exit(1)
                mysql = None
        else:
            logger.info("Reading metadata from MySQL...")
            try:
                mysql = MySQLReader(
                    host=args.mysql_host,
                    port=args.mysql_port,
                    user=args.mysql_user,
                    password=args.mysql_password,
                    database=args.mysql_db,
                )
                if not mysql.health_check():
                    raise ConnectionError("health check failed")
                logger.info("MySQL connection OK")
            except Exception as e:
                logger.warning(f"Cannot connect to MySQL: {e}")
                if not args.list_indices:
                    sys.exit(1)
                mysql = None

        if args.list_indices:
            list_indices(args, mysql)
            return

        # Verify metadata is available for migration
        if not args.no_mysql and mysql is None:
            logger.error("Metadata connection required for migration. Use --no-mysql to skip.")
            sys.exit(1)

        # VB client
        vb = VBWriter(
            host=args.vb_host,
            port=args.vb_port,
            user=args.vb_user,
            password=args.vb_password,
            database=args.vb_db,
        )

        if not vb.health_check():
            logger.error("Cannot connect to Vastbase")
            sys.exit(1)
        logger.info("Vastbase connection OK")

        # --verify
        if args.verify:
            indices = [args.index] if args.index else es.list_ragflow_indices()
            all_matches = 0
            all_mismatches = 0

            for idx in indices:
                logger.info(f"Verifying index: {idx}")
                result = verify_migration(es, vb, mysql, idx, args.kb_id,
                                          no_mysql=args.no_mysql,
                                          exclude_kb_ids=exclude_kb_ids)

                source_label = "ES" if args.no_mysql else mysql.source_label
                for m in result["matches"]:
                    logger.info(
                        f"  ✓ {m['kb_id']}: {source_label}={m['es_count']}, VB={m['vb_count']}"
                    )
                    all_matches += 1

                for m in result["mismatches"]:
                    logger.warning(
                        f"  ✗ {m['kb_id']}: {source_label}={m['es_count']}, VB={m['vb_count']} ({m['status']})"
                    )
                    all_mismatches += 1

            logger.info(
                f"Verification complete: {all_matches} match, {all_mismatches} mismatch"

            )
            return

        # Determine indices to migrate
        all_indices = es.list_ragflow_indices()

        # Build mapping: chunk_hash → doc_meta index name
        doc_meta_map: dict[str, str] = {}
        for idx in all_indices:
            if idx.startswith("ragflow_doc_meta_"):
                hash_part = idx[len("ragflow_doc_meta_"):]
                doc_meta_map[hash_part] = idx

        # Excluded indices (can be overridden via --exclude)
        EXCLUDED_INDICES = set(args.exclude) if args.exclude else {"ragflow_d253f468394111f1b41e53bb8d88db1c"}
        exclude_kb_ids = set(args.exclude_kb_id) if args.exclude_kb_id else None

        # Separate chunk indices (exclude doc_meta indices)
        chunk_indices = [idx for idx in all_indices if not is_doc_meta_index(idx)]
        for excluded in EXCLUDED_INDICES & set(chunk_indices):
            logger.info(f"Skipping excluded index: {excluded}")

        if args.index:
            if args.index in EXCLUDED_INDICES:
                logger.warning(f"Index is excluded, skipping: {args.index}")
                indices_to_migrate = []
            elif is_doc_meta_index(args.index):
                indices_to_migrate = [args.index]
            else:
                # Chunk index: also include its doc_meta sibling
                indices_to_migrate = [args.index]
                hash_part = args.index[len("ragflow_"):]
                if hash_part in doc_meta_map:
                    indices_to_migrate.append(doc_meta_map[hash_part])
        else:
            # Migrate all chunk indices, each immediately followed by its doc_meta
            indices_to_migrate = []
            for idx in chunk_indices:
                if idx in EXCLUDED_INDICES:
                    continue
                indices_to_migrate.append(idx)
                hash_part = idx[len("ragflow_"):]
                if hash_part in doc_meta_map:
                    indices_to_migrate.append(doc_meta_map[hash_part])

        if not indices_to_migrate:
            logger.error("No indices to migrate")
            sys.exit(1)

        logger.info(f"Indices to migrate: {indices_to_migrate}")

        # Print cross-reference plan before starting
        print_migration_plan(
            es, mysql, indices_to_migrate,
            no_mysql=args.no_mysql,
            exclude_kb_ids=exclude_kb_ids,
        )

        total_stats = {
            "indices": 0,
            "kbs": 0,
            "es_docs": 0,
            "migrated": 0,
            "failed": 0,
        }
        start_time = time.time()

        for idx in indices_to_migrate:
            logger.info(f"\n{'='*60}")
            logger.info(f"Migrating index: {idx}")
            logger.info(f"{'='*60}")

            stats = migrate_index(
                es, vb, mysql, idx, args.kb_id,
                args.batch_size, args.dry_run, args.resume,
                no_mysql=args.no_mysql,
                exclude_kb_ids=exclude_kb_ids,
            )

            total_stats["indices"] += 1
            total_stats["kbs"] += stats["kb_count"]
            total_stats["es_docs"] += stats["total_es"]
            total_stats["migrated"] += stats["total_migrated"]
            total_stats["failed"] += stats["total_failed"]

        duration = time.time() - start_time

        if args.dry_run:
            logger.info(f"\n{'='*60}")
            logger.info("DRY RUN COMPLETE (no data written)")
            logger.info(f"{'='*60}")
        else:
            logger.info(f"\n{'='*60}")
            logger.info("MIGRATION SUMMARY")
            logger.info(f"{'='*60}")
            logger.info(f"  Indices:     {total_stats['indices']}")
            logger.info(f"  KBs:         {total_stats['kbs']}")
            doc_label = "ES docs:" if args.no_mysql else f"{mysql.source_label} docs:"
            logger.info(f"  {doc_label:13s}{total_stats['es_docs']}")
            logger.info(f"  Migrated:    {total_stats['migrated']}")
            logger.info(f"  Failed:      {total_stats['failed']}")
            logger.info(f"  Duration:    {duration:.1f}s")

    finally:
        es.close()
        if "vb" in locals():
            vb.close()
        if "mysql" in locals() and mysql is not None:
            mysql.close()


if __name__ == "__main__":
    main()