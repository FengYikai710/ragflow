"""TAPD Bug connector for syncing bugs from TAPD workspace."""
import logging
from datetime import datetime, timezone
from typing import Any, Generator

import requests

from common.data_source.config import (
    DocumentSource,
    INDEX_BATCH_SIZE,
)
from common.data_source.exceptions import (
    ConnectorMissingCredentialError,
    ConnectorValidationError,
)
from common.data_source.interfaces import LoadConnector, PollConnector, SlimConnectorWithPermSync
from common.data_source.models import Document, SlimDocument

_TAPD_API_BASE = "https://api.tapd.cn"


class TapdBugConnector(LoadConnector, PollConnector, SlimConnectorWithPermSync):
    """Connector for syncing bugs from TAPD workspace."""

    def __init__(
        self,
        username: str = "",
        password: str = "",
        workspace_id: str = "",
        batch_size: int = INDEX_BATCH_SIZE,
    ) -> None:
        self.username = username
        self.password = password
        self.workspace_id = workspace_id
        self.batch_size = batch_size

    def load_credentials(self, credentials: dict[str, Any]) -> dict[str, Any] | None:
        if not self.username:
            self.username = credentials.get("username", "")
        if not self.password:
            self.password = credentials.get("password", "")
        if not self.username or not self.password:
            raise ConnectorMissingCredentialError("TAPD Bug requires 'username' and 'password'")
        return None

    def _fetch_bug_count(self) -> int:
        """Get total bug count for workspace validation."""
        resp = requests.get(
            f"{_TAPD_API_BASE}/bugs/count",
            params={"workspace_id": self.workspace_id},
            auth=(self.username, self.password),
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") != 1:
            raise ConnectorValidationError(f"TAPD API error: {data.get('info', 'unknown')}")

        return data.get("data", {}).get("count", 0)

    def _fetch_bugs_page(self, page: int, limit: int = 200) -> list[dict]:
        """Fetch a single page of bugs."""
        resp = requests.get(
            f"{_TAPD_API_BASE}/bugs",
            params={
                "workspace_id": self.workspace_id,
                "page": page,
                "limit": limit,
            },
            auth=(self.username, self.password),
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") != 1:
            raise ConnectorValidationError(f"TAPD API error: {data.get('info', 'unknown')}")

        return data.get("data", [])

    def _parse_datetime(self, date_str: str | None) -> datetime:
        """Parse TAPD datetime string to timezone-aware UTC datetime."""
        if not date_str:
            return datetime.now(timezone.utc)

        # Try parsing "YYYY-MM-DD HH:MM:SS" format
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass

        # Try ISO format with space
        try:
            dt = datetime.fromisoformat(date_str.replace(" ", "T"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            pass

        # Try Unix timestamp
        try:
            return datetime.fromtimestamp(int(date_str), tz=timezone.utc)
        except ValueError:
            pass

        return datetime.now(timezone.utc)

    def _bug_to_document(self, bug: dict) -> Document | None:
        """Convert a TAPD bug entry to a Document."""
        bug_data = bug.get("Bug", {})
        if not bug_data:
            return None

        bug_id = bug_data.get("id", "")
        if not bug_id:
            return None

        title = bug_data.get("title", "")
        description = bug_data.get("description", "") or bug_data.get("content", "")
        status = bug_data.get("status", "")
        priority = bug_data.get("priority", "")
        created = bug_data.get("created", "")
        modified = bug_data.get("modified", "")

        created_dt = self._parse_datetime(created)
        modified_dt = self._parse_datetime(modified)

        description_blob = description.encode("utf-8") if description else b""

        return Document(
            id=f"tapd_bug:{self.workspace_id}:{bug_id}",
            source=DocumentSource.TAPD_BUG,
            semantic_identifier=title or f"Bug #{bug_id}",
            extension=".txt",
            blob=description_blob,
            doc_updated_at=created_dt,
            size_bytes=len(description_blob),
            metadata={
                "bug_id": bug_id,
                "workspace_id": self.workspace_id,
                "status": status,
                "priority": priority,
                "created": created,
                "modified": modified,
            },
        )

    def _yield_documents(
        self, start: float | None = None, end: float | None = None
    ) -> Generator[list[Document], None, None]:
        """Yield batches of documents from TAPD."""
        page = 1
        limit = 200
        batch: list[Document] = []

        while True:
            bugs = self._fetch_bugs_page(page, limit)

            if not bugs:
                break

            for bug in bugs:
                doc = self._bug_to_document(bug)
                if doc is None:
                    continue

                if start is not None or end is not None:
                    modified_dt = self._parse_datetime(modified_str)
                    modified_ts = modified_dt.timestamp()
                    if start is not None and modified_ts < start:
                        continue
                    if end is not None and modified_ts > end:
                        continue

                batch.append(doc)
                if len(batch) == self.batch_size:
                    yield batch
                    batch = []

            if len(bugs) < limit:
                break

            page += 1

        if batch:
            yield batch

    def load_from_state(self) -> Generator[list[Document], None, None]:
        logging.info("Loading all bugs from TAPD workspace %s", self.workspace_id)
        yield from self._yield_documents()

    def poll_source(
        self, start: float, end: float
    ) -> Generator[list[Document], None, None]:
        logging.info(
            "Polling TAPD workspace %s for changes between %s and %s",
            self.workspace_id,
            datetime.fromtimestamp(start, tz=timezone.utc).isoformat(),
            datetime.fromtimestamp(end, tz=timezone.utc).isoformat(),
        )
        yield from self._yield_documents(start=start, end=end)

    def validate_connector_settings(self) -> None:
        if not self.username or not self.password:
            raise ConnectorMissingCredentialError("TAPD Bug requires 'username' and 'password'")
        if not self.workspace_id:
            raise ConnectorValidationError("TAPD Bug requires 'workspace_id'")

        try:
            count = self._fetch_bug_count()
            logging.info("TAPD workspace %s has %d bugs", self.workspace_id, count)
        except ConnectorValidationError:
            raise
        except Exception as e:
            raise ConnectorValidationError(f"TAPD Bug validation failed: {e}")

    def retrieve_all_slim_docs_perm_sync(
        self, callback: Any = None
    ) -> Generator[list[SlimDocument], None, None]:
        """Retrieve all document IDs for deletion synchronization."""
        slim_batch: list[SlimDocument] = []
        page = 1
        limit = 200

        while True:
            bugs = self._fetch_bugs_page(page, limit)

            if not bugs:
                break

            for bug in bugs:
                bug_data = bug.get("Bug", {})
                bug_id = bug_data.get("id", "")
                if not bug_id:
                    continue

                doc_id = f"tapd_bug:{self.workspace_id}:{bug_id}"
                slim_batch.append(SlimDocument(id=doc_id))

                if len(slim_batch) >= 100:
                    yield slim_batch
                    slim_batch = []
                    if callback:
                        callback.progress("tapd_bug_slim_document", 1)

            if len(bugs) < limit:
                break

            page += 1

        if slim_batch:
            yield slim_batch