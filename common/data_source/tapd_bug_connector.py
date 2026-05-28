"""TAPD Bug connector for syncing bugs from TAPD workspace."""
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
_PICGO_SERVER_URL = "http://172.16.105.105:36677"


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


def _upload_to_picgo(image_data: bytes, filename: str) -> str | None:
    """Upload image to picgo and return URL."""
    try:
        files = {'files': (filename, image_data, 'image/*')}
        response = requests.post(
            f"{_PICGO_SERVER_URL}/upload",
            files=files,
            timeout=30
        )
        result = response.json()
        if result.get("success") and result.get("result"):
            return result["result"][0]
    except Exception:
        return None
    return None


def _download_and_upload_image(workspace_id: str, image_path: str, auth: tuple) -> str | None:
    """Download TAPD image and upload to picgo, return new URL."""
    download_url = _get_image_download_url(workspace_id, image_path, auth)
    if not download_url:
        return None

    image_data = _download_image(download_url)
    if not image_data:
        return None

    filename = image_path.split("/")[-1]
    return _upload_to_picgo(image_data, filename)


def _html_to_md(html: str, workspace_id: str = "", auth: tuple = None) -> str:
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
                new_url = _download_and_upload_image(workspace_id, src, auth)
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


def _bug_to_markdown(bug: dict, comments: list[dict] | None = None, workspace_id: str = "", auth: tuple = None) -> str:
    """Convert a TAPD bug dict to Markdown."""
    bug_id = bug.get('id', '')
    title = bug.get('title', '无标题')
    status = bug.get('status', '')
    priority = bug.get('priority', '')
    severity = bug.get('severity', '')
    reporter = bug.get('reporter', '')
    created = bug.get('created', '')
    modified = bug.get('modified', '')
    module = bug.get('module', '')
    description = bug.get('description', '')

    md = f"""# {title}

| 字段 | 值 |
|------|-----|
| 缺陷ID | {bug_id} |
| 状态 | {status} |
| 优先级 | {priority} |
| 严重程度 | {severity} |
| 模块 | {module} |
| 报告人 | {reporter} |
| 创建时间 | {created} |
| 最后修改 | {modified} |

## 描述

{_html_to_md(description, workspace_id, auth)}
"""

    if comments:
        md += _comments_to_md(comments, workspace_id, auth)

    return md


def _comments_to_md(comments: list[dict], workspace_id: str = "", auth: tuple = None) -> str:
    """Convert a list of comments to Markdown."""
    if not comments:
        return ""

    lines = ["\n## 评论\n"]
    for c in comments:
        author = c.get('author', '未知')
        created = c.get('created', '')
        title = c.get('title', '')
        desc = _html_to_md(c.get('description', ''), workspace_id, auth)
        if title:
            lines.append(f"- **{author}** · {created} · {title}")
        else:
            lines.append(f"- **{author}** · {created}")
        if desc:
            lines.append(f"  > {desc}")

    return '\n'.join(lines)


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

    def _fetch_comments_page(self, bug_id: str, page: int, limit: int = 200) -> list[dict]:
        """Fetch a single page of comments for a bug."""
        resp = requests.get(
            f"{_TAPD_API_BASE}/comments",
            params={
                "workspace_id": self.workspace_id,
                "entry_id": bug_id,
                "entry_type": "bug|bug_remark",
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

    def _fetch_all_comments(self, bug_id: str) -> list[dict]:
        """Fetch all comments for a bug."""
        all_comments = []
        page = 1
        limit = 200

        while True:
            comments = self._fetch_comments_page(bug_id, page, limit)
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

    def _bug_to_document(self, bug: dict) -> Document | None:
        """Convert a TAPD bug entry to a Document."""
        bug_data = bug.get("Bug", {})
        if not bug_data:
            return None

        bug_id = bug_data.get("id", "")
        if not bug_id:
            return None

        title = bug_data.get("title", "")
        created = bug_data.get("created", "")

        # Fetch comments for this bug
        comments = self._fetch_all_comments(bug_id)

        created_dt = self._parse_datetime(created)
        auth = (self.username, self.password)
        markdown_blob = _bug_to_markdown(bug_data, comments, self.workspace_id, auth)
        blob_bytes = markdown_blob.encode("utf-8") if markdown_blob else b""

        return Document(
            id=f"tapd_bug:{self.workspace_id}:{bug_id}",
            source=DocumentSource.TAPD_BUG,
            semantic_identifier=title or f"Bug #{bug_id}",
            extension=".md",
            blob=blob_bytes,
            doc_updated_at=created_dt,
            size_bytes=len(blob_bytes),
            metadata={
                "bug_id": bug_id,
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
            bugs = self._fetch_bugs_page(page, limit)

            if not bugs:
                break

            for bug in bugs:
                bug_data = bug.get("Bug", {})
                modified_str = bug_data.get("modified", "")
                created_str = bug_data.get("created", "")

                if start is not None or end is not None:
                    modified_dt = self._parse_datetime(modified_str if modified_str else created_str if created_str else None)
                    modified_ts = modified_dt.timestamp()
                    if start is not None and modified_ts < start:
                        continue
                    if end is not None and modified_ts > end:
                        continue

                doc = self._bug_to_document(bug)
                if doc is None:
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