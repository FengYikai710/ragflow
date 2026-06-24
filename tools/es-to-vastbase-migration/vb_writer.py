"""
Vastbase writer for RAGFlow data migration.

Handles table creation (matching Vastbase's schema) and batch INSERT.
Uses pure psycopg2 — no RAGFlow code dependency.
"""

import json
import logging
import os
import re
from typing import Any

import psycopg2
from psycopg2 import sql

logger = logging.getLogger(__name__)

# Default Vastbase mapping (fields and their types/defaults).
# Mirrors conf/vastbase_mapping.json but embedded here for independence.
VASTBASE_MAPPING = {
    "id": {"type": "varchar(128)", "default": ""},
    "create_time": {"type": "varchar(32)", "default": ""},
    "create_timestamp_flt": {"type": "double precision", "default": 0.0},
    "created_by": {"type": "varchar(128)", "default": ""},
    "dataset_id": {"type": "varchar(128)", "default": ""},
    "doc_id": {"type": "varchar(128)", "default": ""},
    "docnm_kwd": {"type": "varchar(256)", "default": ""},
    "doc_type_kwd": {"type": "varchar(32)", "default": ""},
    "from_page": {"type": "integer", "default": 0},
    "img_id": {"type": "varchar(128)", "default": ""},
    "important_kwd": {"type": "varchar(1024)", "default": ""},
    "important_tks": {"type": "varchar(1024)", "default": ""},
    "kb_id": {"type": "varchar(128)", "default": ""},
    "knowledge_graph_kwd": {"type": "varchar(1024)", "default": ""},
    "page_num_int": {"type": "varchar(256)", "default": ""},
    "position_int": {"type": "varchar(2048)", "default": ""},
    "source_id": {"type": "varchar(1024)", "default": ""},
    "status": {"type": "varchar(32)", "default": "0"},
    "tag_kwd": {"type": "varchar(128)", "default": ""},
    "title_kwd": {"type": "varchar(256)", "default": ""},
    "title_sm_tks": {"type": "varchar(256)", "default": ""},
    "title_tks": {"type": "varchar(256)", "default": ""},
    "to_page": {"type": "integer", "default": 0},
    "top_int": {"type": "varchar(256)", "default": ""},
    "url": {"type": "varchar(512)", "default": ""},
    "content": {"type": "text", "default": ""},
    "content_with_weight": {"type": "text", "default": ""},
    "content_ltks": {"type": "text", "default": ""},
    "content_sm_ltks": {"type": "text", "default": ""},
    "mom_id": {"type": "varchar(128)", "default": ""},
    "mom": {"type": "text", "default": ""},
    "mom_with_weight": {"type": "text", "default": ""},
    "chunk_order_int": {"type": "integer", "default": 0},
    "pagerank_fea": {"type": "double precision", "default": 0.0},
    "tag_feas": {"type": "text", "default": ""},
    "toc_kwd": {"type": "varchar(32)", "default": ""},
    "raptor_kwd": {"type": "varchar(32)", "default": ""},
    "raptor_layer_int": {"type": "integer", "default": 0},
    "name_kwd": {"type": "varchar(256)", "default": ""},
    "entities_kwd": {"type": "varchar(1024)", "default": ""},
    "entity_kwd": {"type": "varchar(256)", "default": ""},
    "entity_type_kwd": {"type": "varchar(128)", "default": ""},
    "from_entity_kwd": {"type": "varchar(256)", "default": ""},
    "to_entity_kwd": {"type": "varchar(256)", "default": ""},
    "removed_kwd": {"type": "varchar(8)", "default": ""},
    "n_hop_with_weight": {"type": "text", "default": ""},
    "weight_int": {"type": "integer", "default": 0},
    "weight_flt": {"type": "double precision", "default": 0.0},
    "rank_int": {"type": "integer", "default": 0},
    "rank_flt": {"type": "double precision", "default": 0.0},
    "important_kwd_empty_count": {"type": "integer", "default": 0},
    "question_kwd": {"type": "varchar(1024)", "default": ""},
    "question_tks": {"type": "text", "default": ""},
    "available_int": {"type": "integer", "default": 1},
}

# Doc metadata table mapping (mirrors conf/doc_meta_vastbase_mapping.json)
DOC_META_MAPPING = {
    "id": {"type": "varchar(128)", "default": ""},
    "kb_id": {"type": "varchar(128)", "default": ""},
    "doc_id": {"type": "varchar(128)", "default": ""},
    "meta_fields": {"type": "text", "default": "{}"},
    "create_time": {"type": "varchar(32)", "default": ""},
    "create_timestamp_flt": {"type": "double precision", "default": 0.0},
}

VECTOR_PATTERN = re.compile(r"^q_(\d+)_vec$")


class VBWriter:
    """Write RAGFlow data to Vastbase."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        user: str = "rag_flow",
        password: str = "",
        database: str = "rag_flow",
    ):
        self.conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=database,
            keepalives=1,
            keepalives_idle=60,
            keepalives_interval=30,
            keepalives_count=10,
            options="-c standard_conforming_strings=on -c backslash_quote=off",
        )
        self.conn.autocommit = False
        # Ensure standard_conforming_strings is ON to avoid backslash escaping issues
        with self.conn.cursor() as cur:
            cur.execute("SET standard_conforming_strings = on")
            cur.execute("SET backslash_quote = off")
            cur.execute("SET escape_string_warning = off")
            cur.execute("SET client_encoding = 'UTF8'")
            self.conn.commit()
        logger.info(
            f"Connected to Vastbase at {host}:{port}, database: {database}"
        )

    def health_check(self) -> bool:
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"Vastbase health check failed: {e}")
            return False

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
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

    def create_table(self, table_name: str, vector_size: int, mapping: dict | None = None):
        """
        Create a Vastbase table matching RAGFlow's schema.
        """
        # Clear any aborted transaction from previous errors
        try:
            self.conn.rollback()
        except Exception:
            pass

        if mapping is None:
            mapping = VASTBASE_MAPPING

        columns = []
        for field_name, field_info in mapping.items():
            field_type = field_info["type"]
            field_default = field_info["default"]
            columns.append(
                sql.SQL("{field_name} {field_type} DEFAULT {field_default}").format(
                    field_name=sql.Identifier(field_name),
                    field_type=sql.SQL(field_type),
                    field_default=sql.Literal(field_default),
                )
            )

        if vector_size > 0:
            # Add vector field only for chunk tables
            vector_name = f"q_{vector_size}_vec"
            columns.append(
                sql.SQL("{vector_name} floatvector({vector_size})").format(
                    vector_name=sql.Identifier(vector_name),
                    vector_size=sql.SQL(str(vector_size)),
                )
            )

        create_sql = sql.SQL(
            "CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        ).format(
            table_name=sql.Identifier(table_name),
            columns=sql.SQL(", ").join(columns),
        )

        with self.conn.cursor() as cur:
            try:
                cur.execute(create_sql)
                logger.debug(f"Create table: {create_sql.as_string(self.conn)}")
            except Exception as e:
                logger.warning("Create table failed: {create_sql.as_string(self.conn)}")


            # Create vector index only if vector column exists
            if vector_size > 0:
                vector_name = f"q_{vector_size}_vec"
                vec_idx_sql = sql.SQL(
                    "CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name} "
                    "USING graph_index ({vector_name} floatvector_cosine_ops) "
                    "WITH (m=16, ef_construction=50)"
                ).format(
                    idx_name=sql.Identifier(f"q_vec_idx_{table_name}"),
                    table_name=sql.Identifier(table_name),
                    vector_name=sql.Identifier(vector_name),
                )
                try:
                    cur.execute(vec_idx_sql)
                    logger.debug(f"Create vector index: {vec_idx_sql.as_string(self.conn)}")
                except Exception as e:
                    logger.warning(f"Vector index creation failed (non-fatal): {e}")

            # ── Fulltext indexes (chunk tables only, skip for doc_meta) ────
            if vector_size > 0:
                db_compatibility = os.environ.get("VB_DBCOMPATIBILITY", "B").upper()
                text_idx_fields = [
                    "title_tks",
                    "title_sm_tks",
                    "important_kwd",
                    "important_tks",
                    "question_tks",
                    "content_ltks",
                    "content_sm_ltks",
                ]

                if db_compatibility == "PG":
                    try:
                        field_list = sql.SQL(", ").join(
                            sql.SQL("to_tsvector('cn_tokenizer', {})").format(sql.Identifier(f))
                            for f in text_idx_fields
                        )
                        pg_fts_sql = sql.SQL("""
                            CREATE INDEX IF NOT EXISTS {index_name}
                            ON {table_name} USING gin({field_list})
                        """).format(
                            index_name=sql.Identifier(f"text_gin_idx_{table_name}"),
                            table_name=sql.Identifier(table_name),
                            field_list=field_list,
                        )
                        cur.execute(pg_fts_sql)
                        logger.info(f"Created PG fulltext index: {pg_fts_sql.as_string(self.conn)}")
                    except Exception as e:
                        logger.warning(f"PG fulltext index creation failed (non-fatal): {e}")

                elif db_compatibility == "B":
                    for f in text_idx_fields:
                        try:
                            mysql_fts_sql = sql.SQL("""
                                ALTER TABLE {table_name}
                                ADD INDEX {index_name} USING "fulltext" ({field_name})
                            """).format(
                                table_name=sql.Identifier(table_name),
                                index_name=sql.Identifier(f"{f}_fulltext_idx_{table_name}"),
                                field_name=sql.Identifier(f),
                            )
                            cur.execute(mysql_fts_sql)
                            logger.info(f"Created MySQL fulltext index for {f}")
                        except Exception as e:
                            logger.warning(
                                f"MySQL fulltext index failed for {f} (non-fatal): {e}"
                            )
                            self.conn.rollback()

            self.conn.commit()

        logger.info(
            f"Created Vastbase table: {table_name} "
            f"(vector_size={vector_size}, fields={len(mapping)})"
        )

    def insert_batch(self, table_name: str, rows: list[dict[str, Any]]) -> int:
        """
        Insert a batch of rows into Vastbase.

        Uses batch DELETE then individual parameterized INSERTs per call,
        with an explicit commit after each call. This avoids long-running
        transactions and the dead-tuple bloat that kills performance in
        large migrations.

        Individual INSERTs (not execute_values) are used because Vastbase B
        mode can misinterpret long text containing '<', '*', etc. when they
        are packed into a multi-row VALUES clause by execute_values.
        """
        # Clear any aborted transaction from previous errors
        try:
            self.conn.rollback()
        except Exception:
            pass

        if not rows:
            return 0

        # Collect all columns across all rows
        all_columns = list(dict.fromkeys(k for row in rows for k in row.keys()))
        # Ensure id is first
        if "id" in all_columns:
            all_columns.remove("id")
        all_columns.insert(0, "id")

        col_identifiers = sql.SQL(", ").join(
            sql.Identifier(c) for c in all_columns
        )
        placeholders = sql.SQL(", ").join([sql.Placeholder()] * len(all_columns))
        insert_sql = sql.SQL(
            "INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        ).format(
            table=sql.Identifier(table_name),
            columns=col_identifiers,
            placeholders=placeholders,
        )

        # Batch DELETE: remove all target IDs in one statement
        doc_ids = [row.get("id") for row in rows if row.get("id")]
        if doc_ids:
            with self.conn.cursor() as cur:
                placeholders = sql.SQL(", ").join(sql.Placeholder() * len(doc_ids))
                delete_sql = sql.SQL(
                    "DELETE FROM {table} WHERE id IN ({placeholders})"
                ).format(
                    table=sql.Identifier(table_name),
                    placeholders=placeholders,
                )
                cur.execute(delete_sql, doc_ids)

        # Individual INSERTs using parameterized queries (%s).
        # execute_values can cause syntax errors in Vastbase B mode when
        # text fields contain '<', '*', etc., so we use separate INSERTs.
        with self.conn.cursor() as cur:
            for row in rows:
                values = tuple(row.get(c) for c in all_columns)
                try:
                    cur.execute(insert_sql, values)
                except Exception as e:
                    logger.warning(
                        f"  Row insert failed for id={row.get('id')}: {e}"
                    )
                    self.conn.rollback()
                    # Return count of successful inserts so far
                    return len([r for r in rows if r.get("id") in doc_ids[:doc_ids.index(row.get("id"))]]) if row.get("id") in doc_ids else 0

        # Commit immediately — one commit per batch prevents dead-tuple bloat
        self.conn.commit()

        logger.debug(f"Inserted {len(rows)} rows into {table_name}")
        return len(rows)

    def count_rows(self, table_name: str, kb_id: str | None = None) -> int:
        """Count rows in a table."""
        with self.conn.cursor() as cur:
            if kb_id:
                cur.execute(
                    sql.SQL("SELECT COUNT(*) FROM {table} WHERE kb_id = %s").format(
                        table=sql.Identifier(table_name)
                    ),
                    (kb_id,),
                )
            else:
                cur.execute(
                    sql.SQL("SELECT COUNT(*) FROM {table}").format(
                        table=sql.Identifier(table_name)
                    )
                )
            return cur.fetchone()[0]

    def close(self):
        self.conn.close()
        logger.info("Vastbase connection closed")