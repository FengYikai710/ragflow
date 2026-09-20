"""
Test: insert a single document into Vastbase using the migration pipeline.

This test:
1. Reads one document from ES (optional, or uses a hardcoded sample)
2. Converts it via converter.py
3. Creates a Vastbase table
4. Inserts the single row
5. Cleans up
"""

import json
import logging
import os
import sys

# Add parent dirs so imports work when run from tools/es-to-vastbase-migration/
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from converter import convert_document
from vb_writer import VBWriter

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ── A realistic sample document (as it would come from ES) ──────────────
SAMPLE_DOC = {
    "_id": "test_doc_001",
    "id": "test_doc_001",
    "kb_id": "test_kb_001",
    "doc_id": "doc_xyz_123",
    "docnm_kwd": "测试文档.pdf",
    "title_kwd": "测试标题",
    "content": "This is a test document content for Vastbase insertion testing.",
    "content_with_weight": {"text": "test", "weight": 1.0},
    "content_ltks": "this is a test",
    "content_sm_ltks": "this is a test",
    "important_kwd": ["keyword1", "keyword2", "keyword3"],
    "tag_kwd": ["tag_a", "tag_b"],
    "page_num_int": [1, 2, 3],
    "position_int": [[1, 2, 3, 4, 5], [6, 7, 8, 9, 10]],
    "top_int": [100, 200],
    "from_page": 1,
    "to_page": 3,
    "create_time": "2025-01-01 00:00:00",
    "create_timestamp_flt": 1735689600.0,
    "created_by": "test_user",
    "status": "1",
    "chunk_order_int": 0,
    "pagerank_fea": 0.5,
    "tag_feas": {"category": "test", "priority": "high"},
    "url": "https://example.com/test",
    "source_id": "source_001",
    "img_id": "",
    "knowledge_graph_kwd": "",
    "toc_kwd": "",
    "raptor_kwd": "",
    "raptor_layer_int": 0,
    "mom_id": "",
    "mom": "",
    "mom_with_weight": "",
    "available_int": 1,
    # Optional vector field — uncomment if your table has one
    # "q_768_vec": [0.1, 0.2, 0.3, 0.4, 0.5],
}


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Test single-document insert to Vastbase")
    parser.add_argument("--vb-host", default="localhost", help="Vastbase host")
    parser.add_argument("--vb-port", type=int, default=5432, help="Vastbase port")
    parser.add_argument("--vb-user", default="ragflow", help="Vastbase user")
    parser.add_argument("--vb-password", default="Ragflow@123", help="Vastbase password")
    parser.add_argument("--vb-db", default="ragflow", help="Vastbase database")
    parser.add_argument("--table", default="test_migration_single", help="Table name (will be dropped after test)")
    parser.add_argument("--vector-size", type=int, default=0, help="Vector dimension (0 = no vector column)")
    parser.add_argument("--es-host", default=None, help="ES host (if set, reads a real doc from ES)")
    parser.add_argument("--es-port", type=int, default=1200, help="ES port")
    parser.add_argument("--es-user", default="elastic", help="ES username")
    parser.add_argument("--es-password", default="infini_rag_flow", help="ES password")
    parser.add_argument("--es-index", default=None, help="ES index to read from (requires --es-host)")
    parser.add_argument("--keep-table", action="store_true", help="Don't drop the table after test")
    args = parser.parse_args()

    # ── 1. Get a document ──────────────────────────────────────────────────
    if args.es_host and args.es_index:
        from es_reader import ESReader

        es = ESReader(
            host=args.es_host,
            port=args.es_port,
            username=args.es_user,
            password=args.es_password,
        )
        logger.info(f"Reading one document from ES index '{args.es_index}'...")
        batches = list(es.scroll_documents(args.es_index, batch_size=1))
        if not batches or not batches[0]:
            logger.error("No documents found in ES index")
            es.close()
            sys.exit(1)
        doc = batches[0][0]
        logger.info(f"Read document _id={doc.get('_id', 'N/A')}")
        es.close()
    else:
        doc = SAMPLE_DOC
        logger.info("Using hardcoded sample document")

    # ── 2. Convert ─────────────────────────────────────────────────────────
    row = convert_document(doc)
    logger.info(f"Converted document: {len(row)} fields")
    logger.debug(f"Row keys: {list(row.keys())}")

    # ── 3. Connect to Vastbase ─────────────────────────────────────────────
    vb = VBWriter(
        host=args.vb_host,
        port=args.vb_port,
        user=args.vb_user,
        password=args.vb_password,
        database=args.vb_db,
    )

    table_name = args.table
    vector_size = args.vector_size
    if vector_size == 0:
        # Auto-detect from document
        from converter import detect_vector_size
        vector_size = detect_vector_size(doc)

    # ── 4. Create table ────────────────────────────────────────────────────
    if vb.table_exists(table_name):
        logger.info(f"Table '{table_name}' already exists, using it")
    else:
        logger.info(f"Creating table '{table_name}' (vector_size={vector_size})...")
        vb.create_table(table_name, vector_size)

    # ── 5. Insert ──────────────────────────────────────────────────────────
    logger.info("Inserting single row...")
    inserted = vb.insert_batch(table_name, [row])
    if inserted == 1:
        logger.info("✅ Successfully inserted 1 row")
    else:
        logger.error(f"❌ Insert failed (inserted={inserted})")

    # ── 6. Verify ──────────────────────────────────────────────────────────
    count = vb.count_rows(table_name)
    logger.info(f"Row count in '{table_name}': {count}")

    # ── 7. Cleanup ─────────────────────────────────────────────────────────
    if not args.keep_table:
        logger.info(f"Dropping table '{table_name}'...")
        with vb.conn.cursor() as cur:
            cur.execute(
                f"DROP TABLE IF EXISTS {table_name} CASCADE"
            )
            vb.conn.commit()
        logger.info(f"Table '{table_name}' dropped")

    vb.close()
    logger.info("Test complete ✅")


if __name__ == "__main__":
    main()
