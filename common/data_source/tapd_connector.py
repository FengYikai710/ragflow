"""TAPD connector for syncing bugs or stories from TAPD workspace."""
import logging
import re
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
_TAPD_IMAGE_API = "https://api.tapd.cn/files/get_image"
_DEFAULT_PICGO_SERVER_URL = "http://172.16.105.105:36677"

# Entry type constants
ENTRY_TYPE_BUG = "bug"
ENTRY_TYPE_STORY = "story"


def _get_image_download_url(workspace_id: str, image_path: str, auth: tuple) -> str | None:
    """Get image download URL from TAPD."""
    params = {
        "workspace_id": workspace_id,
        "image_path": image_path
    }
    try:
        response = requests.get(_TAPD_IMAGE_API, auth=auth, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        if data.get("status") == 1:
            return data["data"]["Attachment"]["download_url"]
    except Exception:
        return None
    return None


def _download_image(url: str) -> bytes | None:
    """Download image content."""
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        return response.content
    except Exception:
        return None


def _upload_to_picgo(image_data: bytes, filename: str, picgo_server_url: str = _DEFAULT_PICGO_SERVER_URL) -> str | None:
    """Upload image to picgo and return URL."""
    try:
        files = {'files': (filename, image_data, 'image/*')}
        response = requests.post(
            f"{picgo_server_url}/upload",
            files=files,
            timeout=30
        )
        result = response.json()
        if result.get("success") and result.get("result"):
            return result["result"][0]
    except Exception:
        return None
    return None


def _download_and_upload_image(workspace_id: str, image_path: str, auth: tuple, picgo_server_url: str = _DEFAULT_PICGO_SERVER_URL) -> str | None:
    """Download TAPD image and upload to picgo, return new URL."""
    download_url = _get_image_download_url(workspace_id, image_path, auth)
    if not download_url:
        return None

    image_data = _download_image(download_url)
    if not image_data:
        return None

    filename = image_path.split("/")[-1]
    return _upload_to_picgo(image_data, filename, picgo_server_url)


def _html_to_md(html: str, workspace_id: str = "", auth: tuple = None, picgo_server_url: str = _DEFAULT_PICGO_SERVER_URL) -> str:
    """Convert HTML to Markdown."""
    if not html:
        return ""

    md = html

    # Remove data-* attributes
    md = re.sub(r'\s+data-[a-z-]+="[^"]*"', '', md)

    # Remove empty class attributes
    md = re.sub(r'\s+class=""', '', md)

    # Handle headers (h1-h6)
    md = re.sub(r'<h([1-6])([^>]*)>(.*?)</h\1>', r'\n### \3\n', md, flags=re.DOTALL)

    # Handle images with /tfl prefix: download from TAPD and upload to picgo
    def convert_img(match):
        attrs = match.group(0)
        alt_match = re.search(r'alt="([^"]*)"', attrs)
        src_match = re.search(r'src="([^"]*)"', attrs)
        alt = alt_match.group(1) if alt_match else ''
        src = src_match.group(1) if src_match else ''
        if src:
            # If /tfl prefixed and we have auth, download and re-upload
            if src.startswith('/tfl') and workspace_id and auth:
                new_url = _download_and_upload_image(workspace_id, src, auth, picgo_server_url)
                if new_url:
                    return f'![{alt}]({new_url})'
            return f'![{alt}]({src})'
        return ''

    md = re.sub(r'<img\s+[^>]+>', convert_img, md)

    # Handle paragraphs
    md = re.sub(r'<p([^>]*)>(.*?)</p>', r'\2\n', md, flags=re.DOTALL | re.IGNORECASE)

    # Handle line breaks
    md = re.sub(r'<br\s*/?\s*>', '\n', md, flags=re.IGNORECASE)

    # Handle bold
    md = re.sub(r'<strong>(.*?)</strong>', r'**\1**', md, flags=re.DOTALL)
    md = re.sub(r'<b>(.*?)</b>', r'**\1**', md, flags=re.DOTALL)

    # Handle italic
    md = re.sub(r'<em>(.*?)</em>', r'*\1*', md, flags=re.DOTALL)
    md = re.sub(r'<i>(.*?)</i>', r'*\1*', md, flags=re.DOTALL)

    # Handle code blocks
    def convert_code_block(match):
        lang = match.group(1) or ''
        code = match.group(2)
        code = re.sub(r'<[^>]+>', '', code)
        return f'\n```{lang}\n{code.strip()}\n```\n'

    md = re.sub(
        r'<div[^>]*data-type="codeBlock"[^>]*>.*?<pre[^>]*class="language-(\w+)"[^>]*>(.*?)</pre>.*?</div>',
        convert_code_block, md, flags=re.DOTALL
    )

    # Handle blockquote
    md = re.sub(r'<blockquote[^>]*>(.*?)</blockquote>', r'\n> \1\n', md, flags=re.DOTALL)

    # Handle lists
    md = re.sub(r'<li[^>]*>(.*?)</li>', r'\n- \1', md, flags=re.DOTALL)
    md = re.sub(r'<ol[^>]*start="\d+"[^>]*>', '\n', md)
    md = re.sub(r'<ul[^>]*>', '\n', md)
    md = re.sub(r'</(ul|ol)>', '', md)

    # Handle span and remaining tags
    md = re.sub(r'<span[^>]*>(.*?)</span>', r'\1', md, flags=re.DOTALL)

    # Remove empty divs
    md = re.sub(r'<div[^>]*>\s*</div>', '', md)

    # Remove all remaining HTML tags
    md = re.sub(r'<[^>]+>', '', md)

    # Clean up extra newlines
    md = re.sub(r'\n{3,}', '\n\n', md)

    # Clean up HTML entities
    md = md.replace('&nbsp;', ' ')
    md = md.replace('&lt;', '<')
    md = md.replace('&gt;', '>')
    md = md.replace('&amp;', '&')

    return md.strip()


def _entry_to_markdown(entry: dict, comments: list[dict] | None = None, workspace_id: str = "", auth: tuple = None, picgo_server_url: str = _DEFAULT_PICGO_SERVER_URL, entry_type: str = ENTRY_TYPE_BUG) -> str:
    """Convert a TAPD bug or story dict to Markdown."""
    entry_id = entry.get('id', '')
    title = entry.get('title') or entry.get('name', '无标题')
    status = entry.get('status', '')
    priority = entry.get('priority', '')
    reporter = entry.get('reporter', '')
    created = entry.get('created', '')
    modified = entry.get('modified', '')
    module = entry.get('module', '')
    description = entry.get('description', '')

    type_label = '需求ID' if entry_type == ENTRY_TYPE_STORY else '缺陷ID'
    doc_title = f'# {title}'

    md = f"""{doc_title}

| 字段 | 值 |
|------|-----|
| {type_label} | {entry_id} |
| 状态 | {status} |
| 优先级 | {priority} |
| 模块 | {module} |
| 报告人 | {reporter} |
| 创建时间 | {created} |
| 最后修改 | {modified} |

## 描述

{_html_to_md(description, workspace_id, auth, picgo_server_url)}
"""

    if comments:
        md += _comments_to_md(comments, workspace_id, auth, picgo_server_url)

    return md


def _comments_to_md(comments: list[dict], workspace_id: str = "", auth: tuple = None, picgo_server_url: str = _DEFAULT_PICGO_SERVER_URL) -> str:
    """Convert a list of comments to Markdown."""
    if not comments:
        return ""

    lines = ["\n## 评论\n"]
    for c in comments:
        author = c.get('author', '未知')
        created = c.get('created', '')
        title = c.get('title', '')
        desc = _html_to_md(c.get('description', ''), workspace_id, auth, picgo_server_url)
        if title:
            lines.append(f"- **{author}** · {created} · {title}")
        else:
            lines.append(f"- **{author}** · {created}")
        if desc:
            lines.append(f"  > {desc}")

    return '\n'.join(lines)


class TapdConnector(LoadConnector, PollConnector, SlimConnectorWithPermSync):
    """Connector for syncing bugs or stories from TAPD workspace."""

    def __init__(
        self,
        username: str = "",
        password: str = "",
        workspace_id: str = "",
        picgo_server_url: str = "",
        entry_type: str = ENTRY_TYPE_BUG,
        batch_size: int = INDEX_BATCH_SIZE,
    ) -> None:
        self.username = username
        self.password = password
        self.workspace_id = workspace_id
        self.picgo_server_url = picgo_server_url or _DEFAULT_PICGO_SERVER_URL
        self.entry_type = entry_type or ENTRY_TYPE_BUG
        self.batch_size = batch_size

    @property
    def _api_endpoint(self) -> str:
        return "stories" if self.entry_type == ENTRY_TYPE_STORY else "bugs"

    @property
    def _entry_key(self) -> str:
        return "Story" if self.entry_type == ENTRY_TYPE_STORY else "Bug"

    @property
    def _doc_id_prefix(self) -> str:
        return "tapd_story" if self.entry_type == ENTRY_TYPE_STORY else "tapd_bug"

    @property
    def _comment_entry_type(self) -> str:
        if self.entry_type == ENTRY_TYPE_STORY:
            return "story|story_remark"
        return "bug|bug_remark"

    def load_credentials(self, credentials: dict[str, Any]) -> dict[str, Any] | None:
        if not self.username:
            self.username = credentials.get("username", "")
        if not self.password:
            self.password = credentials.get("password", "")
        if not self.username or not self.password:
            raise ConnectorMissingCredentialError("TAPD Bug requires 'username' and 'password'")
        return None

    def _fetch_entry_count(self) -> int:
        """Get total entry count for workspace validation."""
        resp = requests.get(
            f"{_TAPD_API_BASE}/{self._api_endpoint}/count",
            params={"workspace_id": self.workspace_id},
            auth=(self.username, self.password),
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") != 1:
            raise ConnectorValidationError(f"TAPD API error: {data.get('info', 'unknown')}")

        return data.get("data", {}).get("count", 0)

    def _fetch_entries_page(self, page: int, limit: int = 200) -> list[dict]:
        """Fetch a single page of entries."""
        resp = requests.get(
            f"{_TAPD_API_BASE}/{self._api_endpoint}",
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

    def _fetch_comments_page(self, entry_id: str, page: int, limit: int = 200) -> list[dict]:
        """Fetch a single page of comments for an entry."""
        resp = requests.get(
            f"{_TAPD_API_BASE}/comments",
            params={
                "workspace_id": self.workspace_id,
                "entry_id": entry_id,
                "entry_type": self._comment_entry_type,
                "page": page,
                "limit": limit,
            },
            auth=(self.username, self.password),
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") != 1:
            return []

        return data.get("data", [])

    def _fetch_all_comments(self, entry_id: str) -> list[dict]:
        """Fetch all comments for an entry."""
        all_comments = []
        page = 1
        limit = 200

        while True:
            comments = self._fetch_comments_page(entry_id, page, limit)
            if not comments:
                break

            for item in comments:
                comment_data = item.get("Comment", {})
                if comment_data:
                    all_comments.append(comment_data)

            if len(comments) < limit:
                break

            page += 1

        return all_comments

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

    def _entry_to_document(self, entry: dict) -> Document | None:
        """Convert a TAPD entry to a Document."""
        entry_data = entry.get(self._entry_key, {})
        if not entry_data:
            return None

        entry_id = entry_data.get("id", "")
        if not entry_id:
            return None

        title = entry_data.get("title") or entry_data.get("name", "")
        created = entry_data.get("created", "")
        modified = entry_data.get("modified", "") or created

        # Fetch comments for this entry
        comments = self._fetch_all_comments(entry_id)

        created_dt = self._parse_datetime(created)
        modified_dt = self._parse_datetime(modified)
        auth = (self.username, self.password)
        markdown_blob = _entry_to_markdown(entry_data, comments, self.workspace_id, auth, self.picgo_server_url, self.entry_type)
        blob_bytes = markdown_blob.encode("utf-8") if markdown_blob else b""

        return Document(
            id=f"{self._doc_id_prefix}:{self.workspace_id}:{entry_id}",
            source=DocumentSource.TAPD,
            semantic_identifier=title or f"{self.entry_type.capitalize()} #{entry_id}",
            extension=".md",
            blob=blob_bytes,
            doc_updated_at=modified_dt,
            size_bytes=len(blob_bytes),
            metadata={
                "entry_id": entry_id,
                "entry_type": self.entry_type,
                "workspace_id": self.workspace_id,
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
            entries = self._fetch_entries_page(page, limit)

            if not entries:
                break

            for entry in entries:
                entry_data = entry.get(self._entry_key, {})
                modified_str = entry_data.get("modified", "")
                created_str = entry_data.get("created", "")

                if start is not None or end is not None:
                    modified_dt = self._parse_datetime(modified_str if modified_str else created_str if created_str else None)
                    modified_ts = modified_dt.timestamp()
                    if start is not None and modified_ts < start:
                        continue
                    if end is not None and modified_ts > end:
                        continue

                doc = self._entry_to_document(entry)
                if doc is None:
                    continue

                batch.append(doc)
                if len(batch) == self.batch_size:
                    yield batch
                    batch = []

            if len(entries) < limit:
                break

            page += 1

        if batch:
            yield batch

    def load_from_state(self) -> Generator[list[Document], None, None]:
        logging.info("Loading all %ss from TAPD workspace %s", self.entry_type, self.workspace_id)
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
            raise ConnectorMissingCredentialError("TAPD requires 'username' and 'password'")
        if not self.workspace_id:
            raise ConnectorValidationError("TAPD requires 'workspace_id'")

        try:
            count = self._fetch_entry_count()
            logging.info("TAPD workspace %s has %d %ss", self.workspace_id, count, self.entry_type)
        except ConnectorValidationError:
            raise
        except Exception as e:
            raise ConnectorValidationError(f"TAPD validation failed: {e}")

    def retrieve_all_slim_docs_perm_sync(
        self, callback: Any = None
    ) -> Generator[list[SlimDocument], None, None]:
        """Retrieve all document IDs for deletion synchronization."""
        slim_batch: list[SlimDocument] = []
        page = 1
        limit = 200

        while True:
            entries = self._fetch_entries_page(page, limit)

            if not entries:
                break

            for entry in entries:
                entry_data = entry.get(self._entry_key, {})
                entry_id = entry_data.get("id", "")
                if not entry_id:
                    continue

                doc_id = f"{self._doc_id_prefix}:{self.workspace_id}:{entry_id}"
                slim_batch.append(SlimDocument(id=doc_id))

                if len(slim_batch) >= 100:
                    yield slim_batch
                    slim_batch = []
                    if callback:
                        callback.progress("tapd_slim_document", 1)

            if len(entries) < limit:
                break

            page += 1

        if slim_batch:
            yield slim_batch