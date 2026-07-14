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

Environment variables:
    VB_DBCOMPATIBILITY  Set to "PG" or "B" for fulltext index creation
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
from converter import detect_vector_size, convert_batch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("migrate")

PROGRESS_FILE = ".es_to_vb_progress.json"


def load_progress() -> dict:
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {}


def save_progress(progress: dict):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


def list_indices(args):
    """List all ragflow_* indices in ES."""
    es = ESReader(
        host=args.es_host,
        port=args.es_port,
        username=args.es_user,
        password=args.es_password,
    )
    indices = es.list_ragflow_indices()
    if not indices:
        print("No ragflow_* indices found in Elasticsearch.")
        return

    print(f"\nRAGFlow indices ({len(indices)} found):")
    for idx in indices:
        count = es.count_documents(idx)
        kbs = es.list_knowledge_bases(idx)
        kb_count = len(kbs)
        print(f"  {idx:50s}  {count:>8,d} docs  {kb_count} KB(s)")

        # Show KB breakdown
        for kb in kbs:
            print(f"    └─ kb_id: {kb['kb_id']:30s}  {kb['doc_count']:>8,d} docs")

    es.close()


def is_doc_meta_index(index_name: str) -> bool:
    """Check if an index name is a doc_metadata index."""
    return index_name.startswith("ragflow_doc_meta_")


def migrate_index(
    es: ESReader,
    vb: VBWriter,
    mysql: MySQLReader,
    index_name: str,
    target_kb_id: str | None,
    batch_size: int,
    dry_run: bool,
    resume: bool,
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
                skip_delete = (table_was_empty and batch_count == 1)
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

    # ---- Chunk data (MySQL-based approach) ----
    # Extract tenant_id from ES index name: ragflow_{tenant_id}
    tenant_id = index_name.replace("ragflow_", "")

    # Get all KBs from MySQL instead of ES aggregation.
    # This ensures we only migrate KBs that still exist in metadata.
    mysql_kbs = mysql.list_all_knowledge_bases()

    # Filter KBs belonging to this tenant (by matching tenant_id)
    kbs = [kb for kb in mysql_kbs if kb["tenant_id"] == tenant_id]

    if target_kb_id:
        kbs = [kb for kb in kbs if kb["kb_id"] == target_kb_id]
        if not kbs:
            logger.warning(
                f"KB '{target_kb_id}' not found in MySQL or has no active documents "
                f"for tenant {tenant_id}"
            )
            return stats

    if not kbs:
        logger.warning(
            f"No active KBs found in MySQL for tenant {tenant_id} (index {index_name})"
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
            f"  Processing KB: {kb_id} ({doc_count} docs from MySQL) → table: {table_name}"
        )

        # Get valid doc_ids from MySQL — these are the authoritative set
        valid_doc_ids = mysql.get_doc_ids_by_kb(kb_id)
        if not valid_doc_ids:
            logger.warning(
                f"  No active documents in MySQL for KB {kb_id}, skipping"
            )
            continue

        logger.info(
            f"  Found {len(valid_doc_ids)} valid document IDs in MySQL"
        )

        # Get a sample doc from ES (any doc from any valid doc_id) to detect vector size
        sample_query = {
            "bool": {
                "filter": [
                    {"term": {"kb_id": kb_id}},
                    {"terms": {"doc_id": valid_doc_ids[:100]}},
                ]
            }
        }
        sample_docs = list(
            es.scroll_documents(index_name, batch_size=1, query=sample_query)
        )
        if not sample_docs or not sample_docs[0]:
            logger.warning(
                f"  No sample doc found in ES for KB {kb_id}, skipping"
            )
            continue

        vector_size = detect_vector_size(sample_docs[0][0])
        if vector_size == 0:
            logger.error(f"  Cannot detect vector size for KB {kb_id}, skipping")
            continue

        logger.info(f"  Detected vector size: {vector_size}")

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
                f"  Resuming: {already_migrated}/{doc_count} already in table"
            )

        if dry_run:
            logger.info(
                f"  [DRY-RUN] Would migrate {doc_count} docs to {table_name}"
            )
            stats["kb_count"] += 1
            stats["total_es"] += doc_count
            stats["tables"].append(table_name)
            continue

        # Build the ES filter query using MySQL doc_ids
        # Use terms filter to only migrate chunks belonging to valid documents
        filter_query = {
            "bool": {
                "filter": [
                    {"term": {"kb_id": kb_id}},
                    {"terms": {"doc_id": valid_doc_ids}},
                ]
            }
        }

        # Migrate data
        migrated = 0
        failed = 0
        batch_count = 0

        for batch in es.scroll_documents(
            index_name, batch_size, query=filter_query
        ):
            batch_count += 1
            try:
                rows = convert_batch(batch)
                skip_delete = (table_was_empty and batch_count == 1)
                inserted = vb.insert_batch(table_name, rows, skip_delete=skip_delete)
                migrated += inserted

                if batch_count % 100 == 0:
                    index_progress.setdefault(kb_id, {})
                    index_progress[kb_id]["migrated"] = migrated
                    index_progress[kb_id]["total"] = doc_count
                    progress_data[index_name] = index_progress
                    save_progress(progress_data)

                if batch_count % 10 == 0:
                    logger.info(
                        f"    {table_name}: {migrated}/{doc_count} migrated"
                    )
            except Exception as e:
                logger.error(f"    Batch insert failed for {table_name}: {e}")
                failed += len(batch)

        if migrated + failed >= doc_count:
            index_progress.setdefault(kb_id, {})
            index_progress[kb_id]["completed"] = True
            index_progress[kb_id]["completed_at"] = datetime.now().isoformat()
            progress_data[index_name] = index_progress
            save_progress(progress_data)

        stats["kb_count"] += 1
        stats["total_es"] += doc_count
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
    es: ESReader, vb: VBWriter, mysql: MySQLReader,
    index_name: str, kb_id: str | None,
) -> dict:
    """Compare MySQL document counts with Vastbase row counts."""
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

    # Chunk data: use MySQL as source of truth
    mysql_kbs = mysql.list_all_knowledge_bases()
    tenant_id = index_name.replace("ragflow_", "")
    kbs = [kb for kb in mysql_kbs if kb["tenant_id"] == tenant_id]

    if kb_id:
        kbs = [kb for kb in kbs if kb["kb_id"] == kb_id]

    for kb in kbs:
        kb_id_val = kb["kb_id"]
        table_name = f"{index_name}_{kb_id_val}"
        mysql_count = kb["doc_count"]

        if not vb.table_exists(table_name):
            result["mismatches"].append({
                "kb_id": kb_id_val, "es_count": mysql_count,
                "vb_count": 0, "status": "table_missing",
            })
            continue

        vb_count = vb.count_rows(table_name, kb_id_val)
        if mysql_count == vb_count:
            result["matches"].append({
                "kb_id": kb_id_val, "es_count": mysql_count, "vb_count": vb_count,
            })
        else:
            result["mismatches"].append({
                "kb_id": kb_id_val, "es_count": mysql_count,
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
    parser.add_argument("--exclude", action="append", default=None,
                        help="Exclude an ES index from migration (can be specified multiple times). "
                             "Default excludes ragflow_d253f468394111f1b41e53bb8d88db1c")

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

        if args.list_indices:
            list_indices(args)
            return

        # MySQL client
        mysql = MySQLReader(
            host=args.mysql_host,
            port=args.mysql_port,
            user=args.mysql_user,
            password=args.mysql_password,
            database=args.mysql_db,
        )
        if not mysql.health_check():
            logger.error("Cannot connect to MySQL")
            sys.exit(1)
        logger.info("MySQL connection OK")

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
                result = verify_migration(es, vb, mysql, idx, args.kb_id)

                for m in result["matches"]:
                    logger.info(
                        f"  ✓ {m['kb_id']}: MySQL={m['es_count']}, VB={m['vb_count']}"
                    )
                    all_matches += 1

                for m in result["mismatches"]:
                    logger.warning(
                        f"  ✗ {m['kb_id']}: MySQL={m['es_count']}, VB={m['vb_count']} ({m['status']})"
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
            logger.info(f"  MySQL docs:  {total_stats['es_docs']}")
            logger.info(f"  Migrated:    {total_stats['migrated']}")
            logger.info(f"  Failed:      {total_stats['failed']}")
            logger.info(f"  Duration:    {duration:.1f}s")

    finally:
        es.close()
        if "vb" in locals():
            vb.close()
        if "mysql" in locals():
            mysql.close()


if __name__ == "__main__":
    main()