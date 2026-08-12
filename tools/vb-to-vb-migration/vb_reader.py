"""
Vastbase chunk reader for VB → VB migration.

Reads chunk/doc_meta rows from a *source* Vastbase instance using a server-side
(named) cursor so large tables stream in batches without loading fully into
memory.

Pure psycopg2 — no RAGFlow code dependency. Mirrors the connection/retry shape
of vb_writer.py and the iterator interface of ESReader (scroll_documents →
scroll_rows).

Why identity (no converter): rows read from a Vastbase table are *already* in
Vastbase's internal storage format (###-joined _kwd strings, flat integer[]
arrays, JSON-serialized metadata). Applying the ES→VB converter again would
double-convert and corrupt them. See identity.py for the column-intersection +
vector-format handling.
"""

import logging
import re
import time
from typing import Iterator

import psycopg2
from psycopg2 import sql

logger = logging.getLogger(__name__)

# tenant_id / kb_id are 32-char hex (CharField(max_length=32) in db_models.py),
# so they never contain "_" — safe to anchor the regex tightly and reject
# malformed names instead of using a permissive [^_]+.
CHUNK_RE = re.compile(r"^ragflow_([0-9a-f]{32})_([0-9a-f]{32})$")
DOC_META_RE = re.compile(r"^ragflow_doc_meta_([0-9a-f]{32})$")

VECTOR_PATTERN = re.compile(r"^q_(\d+)_vec$")


class VBChunkReader:
    """Read RAGFlow chunk/doc_meta data from a source Vastbase instance."""

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
        self.conn = None
        self._connect()

    MAX_CONNECT_RETRIES = 24
    CONNECT_RETRY_DELAY = 5  # seconds

    def _connect(self):
        """(Re)connect, retrying so a temporarily-down source doesn't abort."""
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
                )
                # Server-side cursors REQUIRE a transaction (autocommit=False).
                self.conn.autocommit = False
                logger.info(
                    f"Connected to source Vastbase at {self._host}:{self._port}, "
                    f"database: {self._database}"
                )
                return
            except Exception as e:
                last_error = e
                logger.warning(
                    f"Source Vastbase connection attempt {attempt}/{self.MAX_CONNECT_RETRIES} "
                    f"failed: {e}. Retrying in {self.CONNECT_RETRY_DELAY}s..."
                )
                time.sleep(self.CONNECT_RETRY_DELAY)
        raise last_error

    def _ensure_connection(self):
        """Reconnect if closed or stale (server-side idle kill, restart, ...)."""
        if self.conn is None or self.conn.closed:
            logger.warning("Source Vastbase connection closed, reconnecting...")
            self._connect()
            return
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1")
        except Exception:
            logger.warning("Source Vastbase connection is stale, reconnecting...")
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
            logger.error(f"Source Vastbase health check failed: {e}")
            return False

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("Source Vastbase connection closed")

    # ── Table discovery (information_schema only — no metadata DB) ────────

    def _list_tables(self, like_pattern: str, exclude_pattern: str | None) -> list[str]:
        self._ensure_connection()
        try:
            self.conn.rollback()
        except Exception:
            pass
        with self.conn.cursor() as cur:
            if exclude_pattern:
                cur.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'public' "
                    "  AND table_name LIKE %s "
                    "  AND table_name NOT LIKE %s "
                    "ORDER BY table_name",
                    (like_pattern, exclude_pattern),
                )
            else:
                cur.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_name LIKE %s "
                    "ORDER BY table_name",
                    (like_pattern,),
                )
            return [r[0] for r in cur.fetchall()]

    def list_chunk_tables(self, tenant_id: str | None = None) -> list[str]:
        """List ragflow_{tenant}_{kb} tables. Filter by tenant in Python for
        exactness (LIKE treats '_' as a wildcard, which is unsafe here)."""
        tables = self._list_tables("ragflow_%", "ragflow_doc_meta_%")
        if tenant_id is None:
            return tables
        result = []
        for t in tables:
            parsed = self.parse_table_name(t)
            if parsed and parsed[0] == "chunk" and parsed[1] == tenant_id:
                result.append(t)
        return result

    def list_doc_meta_tables(self, tenant_id: str | None = None) -> list[str]:
        """List ragflow_doc_meta_{tenant} tables."""
        tables = self._list_tables("ragflow_doc_meta_%", None)
        if tenant_id is None:
            return tables
        result = []
        for t in tables:
            parsed = self.parse_table_name(t)
            if parsed and parsed[0] == "meta" and parsed[1] == tenant_id:
                result.append(t)
        return result

    @staticmethod
    def parse_table_name(table_name: str) -> tuple[str, str, str | None] | None:
        """Parse a RAGFlow table name.

        Returns (kind, tenant_id, kb_id) where kind is "chunk" or "meta"
        (kb_id is None for meta tables), or None if the name is not a
        well-formed RAGFlow table.
        """
        if table_name.startswith("ragflow_doc_meta_"):
            m = DOC_META_RE.match(table_name)
            return ("meta", m.group(1), None) if m else None
        m = CHUNK_RE.match(table_name)
        return ("chunk", m.group(1), m.group(2)) if m else None

    # ── Introspection ─────────────────────────────────────────────────────

    def table_exists(self, table_name: str) -> bool:
        self._ensure_connection()
        try:
            self.conn.rollback()
        except Exception:
            pass
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = %s)",
                (table_name,),
            )
            return cur.fetchone()[0]

    def get_columns(self, table_name: str) -> list[str]:
        """Ordered column names of a table."""
        self._ensure_connection()
        try:
            self.conn.rollback()
        except Exception:
            pass
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = %s "
                    "ORDER BY ordinal_position",
                    (table_name,),
                )
                return [r[0] for r in cur.fetchall()]
        except Exception as e:
            logger.warning(f"Cannot query columns for {table_name}: {e}")
            return []

    def get_vector_columns(self, table_name: str) -> list[dict]:
        """Return list of {name, dim} for floatvector columns."""
        self._ensure_connection()
        try:
            self.conn.rollback()
        except Exception:
            pass
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'public' "
                    "  AND table_name = %s AND column_name LIKE 'q_%%_vec'",
                    (table_name,),
                )
                rows = cur.fetchall()
            results = []
            for r in rows:
                name = r[0]
                m = VECTOR_PATTERN.match(name)
                dim = int(m.group(1)) if m else 0
                results.append({"name": name, "dim": dim})
            return results
        except Exception as e:
            logger.warning(f"Cannot query vector columns for {table_name}: {e}")
            return []

    def get_vector_dim(self, table_name: str) -> int:
        """Dimension of the first vector column, or 0 if none (doc_meta tables)."""
        cols = self.get_vector_columns(table_name)
        return cols[0]["dim"] if cols else 0

    def count_rows(self, table_name: str) -> int:
        self._ensure_connection()
        try:
            self.conn.rollback()
        except Exception:
            pass
        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT COUNT(*) FROM {table}").format(
                    table=sql.Identifier(table_name)
                )
            )
            return cur.fetchone()[0]

    # ── Bulk read (server-side cursor) ────────────────────────────────────

    def scroll_rows(
        self, table_name: str, batch_size: int = 1000
    ) -> Iterator[list[dict]]:
        """Stream all rows of a table in batches via a server-side cursor.

        Vector columns (q_*_vec) are cast to ::text on read so psycopg2 returns
        a deterministic "[v1,v2,...]" string that the writer can re-insert via
        a parameterized query. Non-vector columns are NOT cast — integer[]
        columns (position_int etc.) must stay native Python lists so psycopg2
        adapts them back to PG arrays on insert.

        Server-side cursors live inside a transaction: the read connection is
        autocommit=False, and we only rollback() (which closes the cursor)
        after the fetchmany loop completes. The writer is a separate
        connection, so its commits don't affect this read transaction.
        """
        self._ensure_connection()
        try:
            self.conn.rollback()  # clear any aborted tx before opening cursor
        except Exception:
            pass

        vec_cols = {v["name"] for v in self.get_vector_columns(table_name)}
        all_cols = self.get_columns(table_name)
        if not all_cols:
            logger.warning(f"No columns found for {table_name}, nothing to read")
            return

        select_terms = [
            sql.SQL("{}::text").format(sql.Identifier(c)) if c in vec_cols
            else sql.Identifier(c)
            for c in all_cols
        ]
        stmt = sql.SQL("SELECT {cols} FROM {tbl}").format(
            cols=sql.SQL(", ").join(select_terms),
            tbl=sql.Identifier(table_name),
        )

        # Cursor name must be unique-ish; deterministic from table name.
        cur_name = f"vb_read_{abs(hash(table_name)) & 0xffffffff:x}"
        try:
            with self.conn.cursor(name=cur_name) as cur:
                cur.itersize = batch_size  # network prefetch aligned to batch
                cur.execute(stmt)
                while True:
                    chunk = cur.fetchmany(batch_size)
                    if not chunk:
                        break
                    yield [dict(zip(all_cols, row)) for row in chunk]
        finally:
            # Closing/releasing the server-side cursor: rollback ends the tx
            # that holds it. Safe even if the generator was exhausted.
            try:
                self.conn.rollback()
            except Exception:
                pass
