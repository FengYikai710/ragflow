"""
Elasticsearch reader for RAGFlow data migration.

Reads documents from ES indices using search_after (ES 8+ compatible).
"""

import logging
from typing import Any, Iterator

from elasticsearch import Elasticsearch

logger = logging.getLogger(__name__)


class ESReader:
    """Read RAGFlow documents from Elasticsearch."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 9200,
        username: str | None = None,
        password: str | None = None,
        api_key: str | None = None,
        use_ssl: bool = False,
        verify_certs: bool = True,
    ):
        scheme = "https" if use_ssl else "http"
        url = f"{scheme}://{host}:{port}"

        conn_args: dict[str, Any] = {
            "hosts": [url],
            "verify_certs": verify_certs,
            "headers": {"accept": "application/vnd.elasticsearch+json; compatible-with=7"},
        }
        if api_key:
            conn_args["api_key"] = api_key
        elif username and password:
            conn_args["basic_auth"] = (username, password)

        self.client = Elasticsearch(**conn_args)
        logger.info(f"Connected to Elasticsearch at {url}")

    def health_check(self) -> dict[str, Any]:
        return self.client.cluster.health().body

    def count_documents(self, index_name: str, query: dict | None = None) -> int:
        if query is None:
            query = {"match_all": {}}
        response = self.client.count(index=index_name, query=query)
        return response["count"]

    def list_ragflow_indices(self) -> list[str]:
        """List all ragflow_* indices."""
        try:
            response = self.client.indices.get(index="ragflow_*")
            return sorted(response.keys())
        except Exception:
            return []

    def list_knowledge_bases(self, index_name: str) -> list[dict[str, Any]]:
        """Aggregate all unique kb_id values from an ES index."""
        response = self.client.search(
            index=index_name,
            size=0,
            aggs={
                "kb_ids": {
                    "terms": {
                        "field": "kb_id",
                        "size": 10000,
                    }
                }
            },
        )
        buckets = response["aggregations"]["kb_ids"].get("buckets", [])
        return [{"kb_id": b["key"], "doc_count": b["doc_count"]} for b in buckets]

    def scroll_documents(
        self,
        index_name: str,
        batch_size: int = 1000,
        query: dict | None = None,
        total_hint: int | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        """
        Read documents using search_after.
        Each yielded doc has _id + _source merged into one dict.
        If total_hint is provided, logs batch progress as percentage.
        """
        search_body: dict[str, Any] = {
            "size": batch_size,
            "sort": [{"_doc": "asc"}],
            "query": query if query else {"match_all": {}},
            "track_total_hits": True,
        }

        response = self.client.search(index=index_name, body=search_body)
        total = total_hint or (response["hits"]["total"]["value"] if isinstance(response["hits"].get("total"), dict) else len(response["hits"]["hits"]))
        hits = response["hits"]["hits"]
        accumulated = 0

        logger.info(f"Scroll started: index={index_name}, total_docs={total}, batch_size={batch_size}")

        while hits:
            documents = []
            for hit in hits:
                doc = hit["_source"].copy()
                doc["_id"] = hit["_id"]
                documents.append(doc)

            accumulated += len(documents)
            pct = accumulated * 100 / total if total else 0
            print(
                f"\r  Scrolled {accumulated}/{total} docs ({pct:.1f}%), "
                f"this_batch={len(documents)}, took_ms={response.get('took', 0)}",
                end="", flush=True,
            )
            yield documents

            if len(hits) < batch_size:
                print()
                logger.info(f"Scroll finished: {accumulated} docs total")
                break

            search_after = hits[-1]["sort"]
            search_body["search_after"] = search_after
            response = self.client.search(index=index_name, body=search_body)
            hits = response["hits"]["hits"]

    def close(self):
        self.client.close()