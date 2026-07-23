"""
Vastbase writer for RAGFlow data migration.

Handles table creation (matching Vastbase's schema) and batch INSERT.
Uses pure psycopg2 — no RAGFlow code dependency.
"""

import json
import logging
import os
import re
import time
from typing import Any

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

# Default Vastbase mapping (fields and their types/defaults).
# Mirrors conf/vastbase_mapping.json but embedded here for independence.
VASTBASE_MAPPING = {
    "id": {"type": "varchar(256)", "default": ""},
    "create_time": {"type": "varchar(32)", "default": ""},
    "create_timestamp_flt": {"type": "double precision", "default": 0.0},
    "doc_id": {"type": "varchar(256)", "default": ""},
    "docnm_kwd": {"type": "text", "default": ""},
    "doc_type_kwd": {"type": "varchar(256)", "default": ""},
    "img_id": {"type": "varchar(128)", "default": ""},
    "important_kwd": {"type": "text", "default": ""},
    "important_tks": {"type": "text", "default": ""},
    "kb_id": {"type": "varchar(256)", "default": ""},
    "page_num_int": {"type": "integer[]", "default": None},
    "position_int": {"type": "integer[]", "default": None},
    "source_id": {"type": "text", "default": ""},
    "title_sm_tks": {"type": "text", "default": ""},
    "title_tks": {"type": "text", "default": ""},
    "top_int": {"type": "integer[]", "default": None},
    "content_with_weight": {"type": "text", "default": ""},
    "content_ltks": {"type": "text", "default": ""},
    "content_sm_ltks": {"type": "text", "default": ""},
    "mom_id": {"type": "varchar(128)", "default": ""},
    "mom_with_weight": {"type": "text", "default": ""},
    "pagerank_fea": {"type": "integer", "default": 0},
    "tag_feas": {"type": "text", "default": ""},
    "toc_kwd": {"type": "varchar(32)", "default": ""},
    "raptor_kwd": {"type": "varchar(32)", "default": ""},
    "raptor_layer_int": {"type": "integer", "default": 0},
    "question_kwd": {"type": "text", "default": ""},
    "question_tks": {"type": "text", "default": ""},
    "chunk_order_int": {"type": "integer", "default": 0},
    "available_int": {"type": "integer", "default": 1},
    "tag_kwd": {"type": "text", "default": ""},
    "knowledge_graph_kwd": {"type": "varchar(256)", "default": ""},
    "entity_kwd": {"type": "varchar(256)", "default": ""},
    "entity_type_kwd": {"type": "varchar(256)", "default": ""},
    "from_entity_kwd": {"type": "varchar(256)", "default": ""},
    "to_entity_kwd": {"type": "varchar(256)", "default": ""},
    "removed_kwd": {"type": "varchar(256)", "default": "N"},
    "weight_int": {"type": "integer", "default": 0},
    "weight_flt": {"type": "double precision", "default": 0.0},
    "entities_kwd": {"type": "text", "default": ""},
    "rank_flt": {"type": "double precision", "default": 0.0},
    "metadata": {"type": "text", "default": ""},
    "extra": {"type": "text", "default": ""},
}

# Doc metadata table mapping (mirrors conf/doc_meta_vastbase_mapping.json)
DOC_META_MAPPING = {
    "id": {"type": "varchar(256)", "default": ""},
    "kb_id": {"type": "varchar(256)", "default": ""},
    "doc_id": {"type": "varchar(256)", "default": ""},
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
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._connect()

    MAX_CONNECT_RETRIES = 24
    CONNECT_RETRY_DELAY = 5  # seconds

    def _connect(self):
        """(Re)connect to Vastbase, retrying up to MAX_CONNECT_RETRIES times.

        If the server is down or restarting, keeps waiting so the migration
        does not abort prematurely.
        """
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
                # Ensure standard_conforming_strings is ON to avoid backslash escaping issues
                with self.conn.cursor() as cur:
                    cur.execute("SET standard_conforming_strings = on")
                    cur.execute("SET backslash_quote = off")
                    cur.execute("SET escape_string_warning = off")
                    cur.execute("SET client_encoding = 'UTF8'")
                    self.conn.commit()
                logger.info(
                    f"Connected to Vastbase at {self._host}:{self._port}, "
                    f"database: {self._database}"
                )
                return
            except Exception as e:
                last_error = e
                logger.warning(
                    f"Vastbase connection attempt {attempt}/{self.MAX_CONNECT_RETRIES} "
                    f"failed: {e}. Retrying in {self.CONNECT_RETRY_DELAY}s..."
                )
                time.sleep(self.CONNECT_RETRY_DELAY)

        raise last_error

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
        """Check if a table exists."""
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

    def create_table(self, table_name: str, vector_size: int, mapping: dict | None = None):
        """
        Create a Vastbase table matching RAGFlow's schema.
        """
        self._ensure_connection()
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

            self.conn.commit()

        logger.info(
            f"Created Vastbase table: {table_name} "
            f"(vector_size={vector_size}, fields={len(mapping)})"
        )

    def create_indexes(self, table_name: str, vector_size: int):
        """
        Create vector and fulltext indexes for a table.

        Features:
          - statement_timeout safety net prevents infinite hangs
            (default: 60min; override via VB_STATEMENT_TIMEOUT env var).
          - Uses CREATE INDEX CONCURRENTLY for PG-mode fulltext index
            so it does not block concurrent writes.
          - Retries on failure (default: 3 attempts; override via
            VB_INDEX_RETRIES env var), with 持续等待 (keep waiting)
            until success or max retries exhausted.
          - Logs start/end with elapsed time for observability.

        Must be called AFTER data migration to avoid per-row index
        maintenance overhead during bulk INSERT.
        """
        import time

        self._ensure_connection()
        try:
            self.conn.rollback()
        except Exception:
            pass

        STATEMENT_TIMEOUT = os.environ.get("VB_STATEMENT_TIMEOUT", "10min")
        MAX_RETRIES = int(os.environ.get("VB_INDEX_RETRIES", "3"))

        # Set a session-level safety timeout so a hung index build
        # does not block the migration process indefinitely.
        with self.conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = '{STATEMENT_TIMEOUT}'")
        self.conn.commit()
        logger.info(
            f"[{table_name}] statement_timeout={STATEMENT_TIMEOUT}, "
            f"max_retries={MAX_RETRIES}"
        )

        with self.conn.cursor() as cur:
            # ---- Vector index (graph_index) ----
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
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        logger.info(
                            f"[{table_name}] Creating vector index "
                            f"(attempt {attempt}/{MAX_RETRIES})..."
                        )
                        t0 = time.time()
                        cur.execute(vec_idx_sql)
                        elapsed = time.time() - t0
                        logger.info(
                            f"[{table_name}] Vector index created "
                            f"({elapsed:.1f}s)"
                        )
                        break
                    except Exception as e:
                        self.conn.rollback()
                        if attempt < MAX_RETRIES:
                            logger.warning(
                                f"[{table_name}] Vector index attempt "
                                f"{attempt} failed: {e}. "
                                f"Retrying in 2s..."
                            )
                            time.sleep(2)
                        else:
                            logger.warning(
                                f"[{table_name}] Vector index failed "
                                f"after {MAX_RETRIES} attempts "
                                f"(non-fatal): {e}"
                            )

                self.conn.commit()

            # ---- Fulltext indexes ----
            if vector_size > 0:
                db_compatibility = os.environ.get(
                    "VB_DBCOMPATIBILITY", "B"
                ).upper()
                text_idx_fields = [
                    "title_tks", "title_sm_tks",
                    "important_kwd", "important_tks",
                    "question_tks", "content_ltks", "content_sm_ltks",
                ]

                if db_compatibility == "PG":
                    # PG-compatible: single GIN index with CONCURRENTLY
                    # so it does not block writes during the build.
                    for attempt in range(1, MAX_RETRIES + 1):
                        try:
                            field_list = sql.SQL(", ").join(
                                sql.SQL(
                                    "to_tsvector('cn_tokenizer', {})"
                                ).format(sql.Identifier(f))
                                for f in text_idx_fields
                            )
                            pg_fts_sql = sql.SQL("""
                                CREATE INDEX CONCURRENTLY
                                IF NOT EXISTS {index_name}
                                ON {table_name} USING gin({field_list})
                            """).format(
                                index_name=sql.Identifier(
                                    f"text_gin_idx_{table_name}"
                                ),
                                table_name=sql.Identifier(table_name),
                                field_list=field_list,
                            )
                            logger.info(
                                f"[{table_name}] Creating PG fulltext index "
                                f"(attempt {attempt}/{MAX_RETRIES})..."
                            )
                            t0 = time.time()
                            cur.execute(pg_fts_sql)
                            elapsed = time.time() - t0
                            logger.info(
                                f"[{table_name}] PG fulltext index created "
                                f"({elapsed:.1f}s)"
                            )
                            break
                        except Exception as e:
                            self.conn.rollback()
                            if attempt < MAX_RETRIES:
                                logger.warning(
                                    f"[{table_name}] PG fulltext index "
                                    f"attempt {attempt} failed: {e}. "
                                    f"Retrying in 5s..."
                                )
                                time.sleep(5)
                            else:
                                logger.warning(
                                    f"[{table_name}] PG fulltext index "
                                    f"failed after {MAX_RETRIES} attempts "
                                    f"(non-fatal): {e}"
                                )

                elif db_compatibility == "B":
                    # MySQL-compatible: ALTER TABLE per field
                    # (no CONCURRENTLY equivalent for this syntax).
                    for f in text_idx_fields:
                        for attempt in range(1, MAX_RETRIES + 1):
                            try:
                                mysql_fts_sql = sql.SQL("""
                                    ALTER TABLE {table_name}
                                    ADD INDEX {index_name}
                                    USING "fulltext" ({field_name})
                                """).format(
                                    table_name=sql.Identifier(table_name),
                                    index_name=sql.Identifier(
                                        f"{f}_fulltext_idx_{table_name}"
                                    ),
                                    field_name=sql.Identifier(f),
                                )
                                logger.info(
                                    f"[{table_name}] Creating fulltext "
                                    f"index for {f} "
                                    f"(attempt {attempt}/{MAX_RETRIES})..."
                                )
                                t0 = time.time()
                                cur.execute(mysql_fts_sql)
                                elapsed = time.time() - t0
                                logger.info(
                                    f"[{table_name}] Fulltext index "
                                    f"for {f} created ({elapsed:.1f}s)"
                                )
                                break
                            except Exception as e:
                                if "already exists" in str(e):
                                    logger.info(
                                        f"[{table_name}] Fulltext index "
                                        f"for {f} already exists, skipping"
                                    )
                                    break
                                self.conn.rollback()
                                if attempt < MAX_RETRIES:
                                    logger.warning(
                                        f"[{table_name}] Fulltext index "
                                        f"for {f} attempt {attempt} "
                                        f"failed: {e}. Retrying in 2s..."
                                    )
                                    time.sleep(2)
                                else:
                                    logger.warning(
                                        f"[{table_name}] Fulltext index "
                                        f"for {f} failed after "
                                        f"{MAX_RETRIES} attempts "
                                        f"(non-fatal): {e}"
                                    )

                self.conn.commit()
                logger.info(f"[{table_name}] Index creation complete")

    COLUMNS_TO_TEXT = {
        "docnm_kwd", "title_kwd",
        "title_sm_tks", "title_tks",
    }

    def widen_columns(self, table_name: str):
        """
        Alter existing tables: change varchar(256) columns to text.

        Only alters columns that actually exist in the table, so it is
        safe to call on both chunk tables and doc_meta tables without
        triggering "column does not exist" errors and cascading
        transaction aborts.
        """
        self._ensure_connection()
        try:
            self.conn.rollback()
        except Exception:
            pass

        # Query existing columns so we only ALTER what actually exists
        existing_columns = set()
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = %s",
                (table_name,),
            )
            existing_columns = {r[0] for r in cur.fetchall()}
        self.conn.commit()

        with self.conn.cursor() as cur:
            for col in self.COLUMNS_TO_TEXT:
                if col not in existing_columns:
                    logger.debug(
                        f"  Column {col} does not exist in {table_name}, "
                        f"skipping widen"
                    )
                    continue
                try:
                    cur.execute(
                        sql.SQL("ALTER TABLE {table} ALTER COLUMN {column} TYPE text")
                        .format(table=sql.Identifier(table_name), column=sql.Identifier(col))
                    )
                except Exception:
                    pass
            self.conn.commit()

    def insert_batch(self, table_name: str, rows: list[dict[str, Any]],
                     skip_delete: bool = False) -> int:
        """
        Insert a batch of rows into Vastbase.

        Uses execute_values (multi-row VALUES) for bulk INSERT, which reduces
        round trips from N per row to roughly 1 per page_size (default 500).

        An explicit DELETE-by-id runs first if skip_delete is False — this
        handles the resume/retry case where some rows may already exist.
        Set skip_delete=True on the first pass of a fresh migration to save
        one round trip.

        Retries transient Vastbase buffer errors (bad buffer ID) up to 3
        times, since those are intermittent storage-engine glitches that
        succeed on retry.
        """
        max_retries = 3
        for attempt in range(max_retries):
            result = self._insert_batch_attempt(table_name, rows, skip_delete)
            if result >= 0:
                return result
            # Negative return means transient failure — retry
            logger.warning(
                f"  Transient error on attempt {attempt + 1}/{max_retries}, retrying..."
            )
            time.sleep(0.5)
        logger.error(f"  All {max_retries} attempts failed for batch into {table_name}")
        return 0

    def _insert_batch_attempt(self, table_name: str, rows: list[dict[str, Any]],
                              skip_delete: bool = False) -> int:
        """
        Single attempt at inserting a batch of rows.

        Returns number of rows inserted, or -1 if a transient error occurred.
        """
        self._ensure_connection()
        if not rows:
            return 0

        all_columns = list(dict.fromkeys(k for row in rows for k in row.keys()))
        if "id" in all_columns:
            all_columns.remove("id")
        all_columns.insert(0, "id")

        col_identifiers = sql.SQL(", ").join(
            sql.Identifier(c) for c in all_columns
        )

        # DELETE existing rows by ID (skip for fresh-migration first pass)
        if not skip_delete:
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

        # Bulk INSERT using multi-row VALUES
        # Use a moderate page_size — one giant VALUES clause with 500 rows
        # produces a ~10 MB SQL string that is slow to parse server-side.
        with self.conn.cursor() as cur:
            try:
                values = [
                    tuple(row.get(c) for c in all_columns)
                    for row in rows
                ]
                execute_values(
                    cur,
                    sql.SQL("INSERT INTO {table} ({columns}) VALUES %s").format(
                        table=sql.Identifier(table_name),
                        columns=col_identifiers,
                    ),
                    values,
                    page_size=100,
                )
            except Exception as e:
                logger.warning(
                    f"  Batch insert failed for {table_name}: {str(e)[:200]}"
                )
                self.conn.rollback()
                return -1

        self.conn.commit()
        logger.debug(f"Inserted {len(rows)} rows into {table_name}")
        return len(rows)

    def count_rows(self, table_name: str, kb_id: str | None = None) -> int:
        """Count rows in a table."""
        self._ensure_connection()
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
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("Vastbase connection closed")

    def _ensure_connection(self):
        """Reconnect if the connection is closed or stale.

        Checks both client-side conn.closed and a lightweight SELECT 1
        probe to catch server-side disconnections that the client hasn't
        noticed yet (e.g. idle timeout, restart).
        """
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