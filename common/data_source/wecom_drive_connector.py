"""WeCom (企业微信) WeDrive connector for syncing files."""
import logging
import time
from datetime import datetime, timezone
from typing import Any, Generator

import requests

from common.data_source.config import (
    BLOB_STORAGE_SIZE_THRESHOLD,
    DocumentSource,
    INDEX_BATCH_SIZE,
)
from common.data_source.exceptions import (
    ConnectorMissingCredentialError,
    ConnectorValidationError,
    CredentialExpiredError,
    InsufficientPermissionsError,
)
from common.data_source.interfaces import LoadConnector, OnyxExtensionType, PollConnector
from common.data_source.models import Document, SecondsSinceUnixEpoch
from common.data_source.utils import get_file_ext, is_accepted_file_ext

_WECOM_API_BASE = "https://qyapi.weixin.qq.com"


class WeComDriveConnector(LoadConnector, PollConnector):
    """Connector for syncing files from WeCom (企业微信) WeDrive."""

    def __init__(
        self,
        corp_id: str = "",
        corp_secret: str = "",
        space_id: str = "",
        folder_id: str = "",
        batch_size: int = INDEX_BATCH_SIZE,
    ) -> None:
        self.corp_id = corp_id
        self.corp_secret = corp_secret
        self.space_id = space_id
        self.folder_id = folder_id
        self.batch_size = batch_size
        self._access_token: str | None = None
        self._token_expires: float = 0
        self.size_threshold: int | None = BLOB_STORAGE_SIZE_THRESHOLD
        self._allow_images: bool = False

    def _build_extension_type(self) -> OnyxExtensionType:
        ext_type = OnyxExtensionType.Plain | OnyxExtensionType.Document
        if self._allow_images:
            ext_type |= OnyxExtensionType.Multimedia
        return ext_type

    def _is_supported_file(self, file_name: str) -> bool:
        return is_accepted_file_ext(get_file_ext(file_name), self._build_extension_type())

    def set_allow_images(self, allow_images: bool) -> None:
        self._allow_images = allow_images

    def _get_access_token(self) -> str:
        if self._access_token and time.time() < self._token_expires:
            return self._access_token

        resp = requests.get(
            f"{_WECOM_API_BASE}/cgi-bin/gettoken",
            params={"corpid": self.corp_id, "corpsecret": self.corp_secret},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("errcode") != 0:
            raise CredentialExpiredError(
                f"WeCom access token error: {data.get('errmsg', 'unknown')}"
            )

        self._access_token = data["access_token"]
        self._token_expires = time.time() + data.get("expires_in", 7200) - 300
        logging.info("WeCom access token refreshed, expires in %ds", data.get("expires_in", 7200))
        return self._access_token

    def _api_post(self, path: str, body: dict | None = None) -> dict:
        token = self._get_access_token()
        url = f"{_WECOM_API_BASE}{path}?access_token={token}"
        resp = requests.post(url, json=body or {}, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        if data.get("errcode") in (40014, 42001):
            self._access_token = None
            token = self._get_access_token()
            url = f"{_WECOM_API_BASE}{path}?access_token={token}"
            resp = requests.post(url, json=body or {}, timeout=60)
            resp.raise_for_status()
            data = resp.json()

        if data.get("errcode") != 0:
            raise RuntimeError(
                f"WeCom API error (errcode={data.get('errcode')}): {data.get('errmsg', 'unknown')}"
            )
        return data

    def load_credentials(self, credentials: dict[str, Any]) -> dict[str, Any] | None:
        if not self.corp_id:
            self.corp_id = credentials.get("corp_id", "")
        if not self.corp_secret:
            self.corp_secret = credentials.get("corp_secret", "")
        if not self.corp_id or not self.corp_secret:
            raise ConnectorMissingCredentialError("WeCom Drive requires 'corp_id' and 'corp_secret'")
        return None

    def _list_files(
        self, father_id: str, path_prefix: str = ""
    ) -> Generator[tuple[dict, str], None, None]:
        """Recursively list files in a WeDrive directory."""
        start = 0
        while True:
            data = self._api_post(
                "/cgi-bin/wedrive/file_list",
                {"spaceid": self.space_id, "fatherid": father_id, "start": start, "size": 1000},
            )
            file_list = data.get("file_list", [])
            for item in file_list:
                file_name = item.get("file_name", "unknown")
                current_path = f"{path_prefix}/{file_name}" if path_prefix else file_name
                if item.get("file_type") == 2:
                    yield from self._list_files(item["fileid"], current_path)
                else:
                    yield item, current_path

            total = data.get("total", 0)
            start += len(file_list)
            if start >= total or not file_list:
                break

    def _download_file(self, file_id: str) -> bytes:
        """Download file content via WeCom WeDrive download URL."""
        data = self._api_post(
            "/cgi-bin/wedrive/file_download",
            {"spaceid": self.space_id, "fileid": file_id},
        )
        download_url = data.get("download_url")
        if not download_url:
            raise RuntimeError(f"No download URL returned for file {file_id}")

        cookie_key = data.get("cookie_key", "")
        cookie_value = data.get("cookie_value", "")
        headers = {}
        if cookie_key and cookie_value:
            headers["Cookie"] = f"{cookie_key}={cookie_value}"

        resp = requests.get(download_url, headers=headers, timeout=120)
        resp.raise_for_status()
        return resp.content

    def _file_to_document(self, file_info: dict, file_path: str) -> Document | None:
        """Convert a WeDrive file entry to a Document."""
        file_name = file_info.get("file_name", "unknown")
        if not self._is_supported_file(file_name):
            logging.debug("Skipping unsupported file: %s", file_path)
            return None

        size_bytes = file_info.get("size", 0)
        if (
            self.size_threshold is not None
            and isinstance(size_bytes, int)
            and size_bytes > self.size_threshold
        ):
            logging.warning(
                "File %s exceeds size threshold (%d > %d), skipping",
                file_path, size_bytes, self.size_threshold,
            )
            return None

        file_id = file_info.get("fileid", "")
        mod_time = file_info.get("mod_time", 0)
        updated_at = datetime.fromtimestamp(mod_time, tz=timezone.utc) if mod_time else datetime.now(timezone.utc)

        return Document(
            id=f"wecom_drive:{self.space_id}:{file_id}",
            source=DocumentSource.WECOMDRIVE,
            semantic_identifier=file_path,
            extension=get_file_ext(file_name),
            blob=b"",
            doc_updated_at=updated_at,
            size_bytes=size_bytes or 0,
            metadata={
                "wecom_file_id": file_id,
                "wecom_space_id": self.space_id,
            },
        )

    def _download_and_fill(self, doc: Document) -> Document | None:
        """Download file content and return updated Document."""
        file_id = doc.metadata.get("wecom_file_id", "")
        try:
            blob = self._download_file(file_id)
            if not blob:
                logging.warning("Empty download for file %s", doc.semantic_identifier)
                return None
            return Document(
                id=doc.id,
                source=doc.source,
                semantic_identifier=doc.semantic_identifier,
                extension=doc.extension,
                blob=blob,
                doc_updated_at=doc.doc_updated_at,
                size_bytes=len(blob),
                metadata=doc.metadata,
            )
        except Exception as e:
            logging.exception("Failed to download WeDrive file %s: %s", doc.semantic_identifier, e)
            return None

    def _yield_documents(
        self, start: float | None = None, end: float | None = None
    ) -> Generator[list[Document], None, None]:
        """Yield batches of documents from WeDrive."""
        batch: list[Document] = []
        for file_info, file_path in self._list_files(self.folder_id):
            mod_time = file_info.get("mod_time", 0)
            if start is not None and mod_time and mod_time <= start:
                continue
            if end is not None and mod_time and mod_time > end:
                continue

            doc = self._file_to_document(file_info, file_path)
            if doc is None:
                continue

            filled = self._download_and_fill(doc)
            if filled is None:
                continue

            batch.append(filled)
            if len(batch) == self.batch_size:
                yield batch
                batch = []

        if batch:
            yield batch

    def load_from_state(self) -> Generator[list[Document], None, None]:
        logging.info("Loading all documents from WeCom Drive space %s folder %s", self.space_id, self.folder_id)
        yield from self._yield_documents()

    def poll_source(
        self, start: SecondsSinceUnixEpoch, end: SecondsSinceUnixEpoch
    ) -> Generator[list[Document], None, None]:
        logging.info(
            "Polling WeCom Drive space %s folder %s for changes since %s",
            self.space_id,
            self.folder_id,
            datetime.fromtimestamp(start, tz=timezone.utc).isoformat(),
        )
        yield from self._yield_documents(start=start, end=end)

    def validate_connector_settings(self) -> None:
        if not self.corp_id or not self.corp_secret:
            raise ConnectorMissingCredentialError("WeCom Drive requires 'corp_id' and 'corp_secret'")
        if not self.space_id:
            raise ConnectorValidationError("WeCom Drive requires 'space_id'")
        if not self.folder_id:
            raise ConnectorValidationError("WeCom Drive requires 'folder_id'")

        try:
            self._api_post(
                "/cgi-bin/wedrive/file_list",
                {"spaceid": self.space_id, "fatherid": self.folder_id, "start": 0, "size": 1},
            )
        except CredentialExpiredError:
            raise
        except InsufficientPermissionsError:
            raise
        except Exception as e:
            err_str = str(e)
            if "600001" in err_str or "no privilege" in err_str.lower():
                raise InsufficientPermissionsError(
                    f"No access to WeDrive space '{self.space_id}': {e}"
                )
            if "600003" in err_str or "not exist" in err_str.lower():
                raise ConnectorValidationError(
                    f"WeDrive space '{self.space_id}' does not exist: {e}"
                )
            raise ConnectorValidationError(f"WeCom Drive validation failed: {e}")