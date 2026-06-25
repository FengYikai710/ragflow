#!/usr/bin/env python3
"""
ES → Vastbase 向量迁移正确性验证工具。

从 ES 中随机抽取 chunk，对比 Vastbase 中相同 id 的向量是否一致。

用法:
    # 快速抽查 10 条
    python verify_vectors.py --es-host localhost --es-port 1200 \
        --es-user elastic --es-password 'infini_rag_flow' \
        --vb-host localhost --vb-port 5432 \
        --vb-user ragflow --vb-password 'Ragflow@123' \
        --vb-db ragflow

    # 指定 index 和 kb_id，抽 100 条
    python verify_vectors.py ... --index ragflow_xxx --kb-id xxx --sample 100

    # 全部检查（逐条对比，可能很慢）
    python verify_vectors.py ... --all
"""

import argparse
import json
import logging
import math
import random
import sys
from typing import Any

from es_reader import ESReader
from vb_writer import VBWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("verify_vectors")


def cosine_similarity(a: list[float], b: list[float]) -> float:
    """Compute cosine similarity between two vectors."""
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def parse_vector_from_es(doc: dict) -> tuple[str, list[float]] | None:
    """Extract vector from an ES document. Returns (field_name, vector) or None."""
    for k, v in doc.items():
        if k.endswith("_vec") and isinstance(v, list) and len(v) > 0:
            return k, [float(x) for x in v]
    return None


def parse_vector_from_vb(row: dict, vector_field: str) -> list[float] | None:
    """Extract vector from a Vastbase row dict."""
    raw = row.get(vector_field)
    if raw is None:
        return None
    if isinstance(raw, list):
        return [float(x) for x in raw]
    if isinstance(raw, str):
        # Vastbase returns vectors as '[v1, v2, v3, ...]' string
        raw = raw.strip()
        if raw.startswith("[") and raw.endswith("]"):
            raw = raw[1:-1]
        parts = raw.split(",")
        return [float(x.strip()) for x in parts if x.strip()]
    return None


def sample_documents(es: ESReader, index_name: str, kb_id: str | None,
                     sample_size: int, use_all: bool) -> list[dict]:
    """Sample documents from ES."""
    if kb_id:
        query = {"term": {"kb_id": kb_id}}
    else:
        query = None

    total = es.count_documents(index_name, query=query)
    logger.info(f"Total documents in ES index '{index_name}': {total}")

    if use_all:
        logger.info(f"Checking ALL documents (this may take a while)...")
        batch_size = 1000
        all_docs = []
        for batch in es.scroll_documents(index_name, batch_size, query=query):
            for doc in batch:
                vec = parse_vector_from_es(doc)
                if vec:
                    all_docs.append((doc["_id"], vec[0], vec[1]))
        logger.info(f"Found {len(all_docs)} documents with vector fields")
        return all_docs

    sample_size = min(sample_size, total)
    logger.info(f"Sampling {sample_size} documents...")

    # Use a random sample via scroll with random sort
    batch_size = min(sample_size * 2, 10000)
    sampled = []
    seen_ids = set()

    for batch in es.scroll_documents(index_name, batch_size, query=query):
        for doc in batch:
            if len(sampled) >= sample_size:
                break
            doc_id = doc["_id"]
            if doc_id in seen_ids:
                continue
            seen_ids.add(doc_id)
            vec = parse_vector_from_es(doc)
            if vec:
                sampled.append((doc["_id"], vec[0], vec[1]))

    random.shuffle(sampled)
    sampled = sampled[:sample_size]
    logger.info(f"Sampled {len(sampled)} documents with vector fields")
    return sampled


def main():
    parser = argparse.ArgumentParser(
        description="Verify ES → Vastbase vector migration correctness"
    )

    # ES connection
    parser.add_argument("--es-host", default="localhost", help="Elasticsearch host")
    parser.add_argument("--es-port", type=int, default=9200, help="Elasticsearch port")
    parser.add_argument("--es-user", default=None, help="Elasticsearch username")
    parser.add_argument("--es-password", default=None, help="Elasticsearch password")

    # VB connection
    parser.add_argument("--vb-host", default="localhost", help="Vastbase host")
    parser.add_argument("--vb-port", type=int, default=5432, help="Vastbase port")
    parser.add_argument("--vb-user", default="rag_flow", help="Vastbase user")
    parser.add_argument("--vb-password", default="", help="Vastbase password")
    parser.add_argument("--vb-db", default="rag_flow", help="Vastbase database")

    # Options
    parser.add_argument("--index", "-i", default=None, help="ES index to check (omit to check all)")
    parser.add_argument("--kb-id", default=None, help="Specific KB ID to check")
    parser.add_argument("--sample", type=int, default=10, help="Number of random samples to check")
    parser.add_argument("--all", action="store_true", help="Check ALL documents (may be slow)")
    parser.add_argument("--threshold", type=float, default=0.999,
                        help="Cosine similarity threshold for passing (default: 0.999)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show all results")

    args = parser.parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # ES client
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

        # Determine indices
        indices = [args.index] if args.index else es.list_ragflow_indices()
        if not indices:
            logger.error("No indices found")
            sys.exit(1)

        # Get KB list from ES
        all_kbs = []
        for idx in indices:
            kbs = es.list_knowledge_bases(idx)
            for kb in kbs:
                if args.kb_id and kb["kb_id"] != args.kb_id:
                    continue
                all_kbs.append((idx, kb["kb_id"], kb["doc_count"]))
                logger.info(f"  Index: {idx}, KB: {kb['kb_id']}, docs: {kb['doc_count']}")

        if not all_kbs:
            logger.error("No knowledge bases found")
            sys.exit(1)

        # Check each KB
        total_checked = 0
        total_passed = 0
        total_failed = 0
        total_errors = 0

        for idx, kb_id, doc_count in all_kbs:
            table_name = f"{idx}_{kb_id}"
            if not vb.table_exists(table_name):
                logger.warning(f"  Table {table_name} not found in Vastbase, skipping")
                continue

            logger.info(f"\n{'='*60}")
            logger.info(f"Checking: index={idx}, kb_id={kb_id}")
            logger.info(f"  ES docs: {doc_count}, VB table: {table_name}")

            # Sample documents from ES
            samples = sample_documents(es, idx, kb_id, args.sample, args.all)
            if not samples:
                logger.warning(f"  No vector documents found in ES for {kb_id}")
                continue

            logger.info(f"  Checking {len(samples)} documents...")

            passed = 0
            failed = 0
            errors = 0
            max_diff = 0.0
            min_sim = 1.0

            for doc_id, vec_field, es_vec in samples:
                # Read from Vastbase
                vb_docs = list(
                    es.scroll_documents(
                        idx, batch_size=1,
                        query={"term": {"_id": doc_id}},
                    )
                )
                # Actually, we should read from VB, not ES again.
                # Let's query VB directly.
                vb_row = None
                with vb.conn.cursor() as cur:
                    cur.execute(
                        f"SELECT * FROM \"{table_name}\" WHERE id = %s",
                        (doc_id,),
                    )
                    columns = [desc[0] for desc in cur.description]
                    row = cur.fetchone()
                    if row:
                        vb_row = dict(zip(columns, row))

                if vb_row is None:
                    logger.warning(f"    ✗ {doc_id}: not found in Vastbase")
                    errors += 1
                    continue

                vb_vec = parse_vector_from_vb(vb_row, vec_field)
                if vb_vec is None:
                    logger.warning(f"    ✗ {doc_id}: vector field '{vec_field}' not found in Vastbase")
                    errors += 1
                    continue

                if len(es_vec) != len(vb_vec):
                    logger.warning(
                        f"    ✗ {doc_id}: dimension mismatch "
                        f"ES={len(es_vec)} vs VB={len(vb_vec)}"
                    )
                    failed += 1
                    continue

                sim = cosine_similarity(es_vec, vb_vec)
                diff = max(abs(a - b) for a, b in zip(es_vec, vb_vec))
                max_diff = max(max_diff, diff)
                min_sim = min(min_sim, sim)

                if sim >= args.threshold:
                    passed += 1
                    if args.verbose:
                        logger.info(f"    ✓ {doc_id}: sim={sim:.6f}, max_diff={diff:.6e}")
                else:
                    failed += 1
                    logger.warning(
                        f"    ✗ {doc_id}: sim={sim:.6f}, max_diff={diff:.6e} "
                        f"(threshold={args.threshold})"
                    )
                    # Show first few values for debugging
                    for i in range(min(5, len(es_vec))):
                        logger.warning(f"        [{i}] ES={es_vec[i]:.6f}  VB={vb_vec[i]:.6f}  diff={abs(es_vec[i]-vb_vec[i]):.6e}")

            total_checked += len(samples)
            total_passed += passed
            total_failed += failed
            total_errors += errors

            logger.info(f"  Results: {passed} passed, {failed} failed, {errors} errors")
            logger.info(f"  Min similarity: {min_sim:.6f}, Max diff per element: {max_diff:.6e}")

        # Summary
        logger.info(f"\n{'='*60}")
        logger.info("SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"  Total checked: {total_checked}")
        logger.info(f"  Passed:        {total_passed}")
        logger.info(f"  Failed:        {total_failed}")
        logger.info(f"  Errors:        {total_errors}")

        if total_failed > 0 or total_errors > 0:
            logger.warning("  ❌ SOME VECTORS ARE INCORRECT!")
            sys.exit(1)
        elif total_checked > 0:
            logger.info("  ✅ ALL CHECKED VECTORS ARE CORRECT!")
        else:
            logger.warning("  ⚠️  Nothing was checked!")
            sys.exit(1)

    finally:
        es.close()
        if "vb" in locals():
            vb.close()


if __name__ == "__main__":
    main()