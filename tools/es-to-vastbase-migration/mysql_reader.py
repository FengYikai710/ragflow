"""
MySQL reader for RAGFlow data migration.

Queries the RAGFlow MySQL metadata database to get the authoritative list
of documents (and their KBs) that should be migrated. Only documents whose
id exists in the `document` table are valid — orphaned ES data is skipped.
"""

import logging
from typing import Any

import pymysql

logger = logging.getLogger(__name__)


class MySQLReader:
    """Read RAGFlow metadata from MySQL."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 3306,
        user: str = "root",
        password: str = "",
        database: str = "rag_flow",
    ):
        self.conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
        )
        logger.info(f"Connected to MySQL at {host}:{port}, database: {database}")

    def health_check(self) -> bool:
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1 AS ok")
                row = cur.fetchone()
            return row["ok"] == 1
        except Exception as e:
            logger.error(f"MySQL health check failed: {e}")
            return False

    def list_knowledge_bases(self, tenant_id: str) -> list[dict[str, Any]]:
        """
        List all KBs for a tenant, with document count from the `document` table.

        Returns list of {"kb_id": str, "doc_count": int}.
        """
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
        return [{"kb_id": r["kb_id"], "doc_count": r["doc_count"]} for r in rows]

    def list_tenants(self) -> list[str]:
        """List all tenant IDs that have active knowledge bases with documents."""
        sql = """
            SELECT DISTINCT k.tenant_id
            FROM knowledgebase k
            JOIN document d ON d.kb_id = k.id
            WHERE d.status = '1'
        """
        with self.conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        return [r["tenant_id"] for r in rows]

    def get_doc_ids_by_kb(self, kb_id: str) -> list[str]:
        """
        Get all document IDs for a given knowledge base (all statuses).

        These are the doc_id values used to match chunks in ES.
        """
        sql = """
            SELECT id FROM document
            WHERE kb_id = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (kb_id,))
            rows = cur.fetchall()
        return [r["id"] for r in rows]

    def list_all_knowledge_bases(self) -> list[dict[str, Any]]:
        """
        List ALL KBs across all tenants with document counts.
        Useful when tenant_id is not known in advance.
        """
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
            {"kb_id": r["kb_id"], "tenant_id": r["tenant_id"], "doc_count": r["doc_count"]}
            for r in rows
        ]

    def get_doc_count_by_kb(self, kb_id: str) -> int:
        """Count valid documents in a knowledge base."""
        sql = """
            SELECT COUNT(*) AS cnt FROM document
            WHERE kb_id = %s AND status = '1'
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (kb_id,))
            row = cur.fetchone()
        return row["cnt"] if row else 0

    def close(self):
        self.conn.close()
        logger.info("MySQL connection closed")