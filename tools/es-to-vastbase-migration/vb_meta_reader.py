"""
Vastbase metadata reader for RAGFlow data migration.

Reads the RAGFlow metadata database (knowledgebase / document tables) from
Vastbase instead of MySQL. Shares the same schema as the MySQL rag_flow
database, so the SQL queries are identical — only the connection library
differs (psycopg2 vs pymysql).
"""

import logging
from typing import Any

import psycopg2

logger = logging.getLogger(__name__)


class VBMetaReader:
    """Read RAGFlow metadata from Vastbase (PG-compatible interface).

    Uses the same table schema as MySQL's rag_flow database, so the
    SQL queries are identical to MySQLReader.
    """

    source_label = "Vastbase"

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
        self.conn = self._connect()

    def _connect(self):
        """Create a fresh connection with keepalives."""
        conn = psycopg2.connect(
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
        conn.autocommit = True
        logger.info(
            f"Connected to Vastbase (metadata) at "
            f"{self._host}:{self._port}, database: {self._database}"
        )
        return conn

    def _ensure_connection(self):
        """Reconnect if the connection was closed by the server."""
        if self.conn.closed:
            logger.warning("Vastbase metadata connection was closed, reconnecting...")
            self.conn = self._connect()

    def health_check(self) -> bool:
        try:
            self._ensure_connection()
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1 AS ok")
                row = cur.fetchone()
            return row[0] == 1
        except Exception as e:
            logger.error(f"Vastbase metadata health check failed: {e}")
            return False

    def list_knowledge_bases(self, tenant_id: str) -> list[dict[str, Any]]:
        """
        List all KBs for a tenant, with document count from the `document` table.

        Returns list of {"kb_id": str, "doc_count": int}.
        """
        self._ensure_connection()
        sql = """
            SELECT d.kb_id, COUNT(*) AS doc_count
            FROM document d
            JOIN knowledgebase k ON d.kb_id = k.id
            WHERE k.tenant_id = %s
              AND d.status = '1'
            GROUP BY d.kb_id
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (tenant_id,))
            rows = cur.fetchall()
        return [{"kb_id": r[0], "doc_count": r[1]} for r in rows]

    def list_tenants(self) -> list[str]:
        """List all tenant IDs that have active knowledge bases with documents."""
        self._ensure_connection()
        sql = """
            SELECT DISTINCT k.tenant_id
            FROM knowledgebase k
            JOIN document d ON d.kb_id = k.id
            WHERE d.status = '1'
        """
        with self.conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        return [r[0] for r in rows]

    def get_doc_ids_by_kb(self, kb_id: str) -> list[str]:
        """
        Get all document IDs for a given knowledge base (all statuses).

        These are the doc_id values used to match chunks in ES.
        """
        self._ensure_connection()
        sql = """
            SELECT id FROM document
            WHERE kb_id = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (kb_id,))
            rows = cur.fetchall()
        return [r[0] for r in rows]

    def list_all_knowledge_bases(self) -> list[dict[str, Any]]:
        """
        List ALL KBs across all tenants with document counts.
        Useful when tenant_id is not known in advance.
        """
        self._ensure_connection()
        sql = """
            SELECT d.kb_id, k.tenant_id, COUNT(*) AS doc_count
            FROM document d
            JOIN knowledgebase k ON d.kb_id = k.id
            GROUP BY d.kb_id, k.tenant_id
        """
        with self.conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        return [
            {"kb_id": r[0], "tenant_id": r[1], "doc_count": r[2]}
            for r in rows
        ]

    def get_doc_count_by_kb(self, kb_id: str) -> int:
        """Count valid documents in a knowledge base."""
        self._ensure_connection()
        sql = """
            SELECT COUNT(*) AS cnt FROM document
            WHERE kb_id = %s AND status = '1'
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (kb_id,))
            row = cur.fetchone()
        return row[0] if row else 0

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("Vastbase metadata connection closed")
