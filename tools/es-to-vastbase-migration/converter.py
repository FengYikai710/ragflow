"""
Field conversion: Elasticsearch native format → Vastbase encoded format.

RAGFlow's ES stores data in native types (lists, dicts, ints), while
Vastbase encodes certain fields as hex strings or ###-joined strings.
"""

import json
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# Fields that end with _kwd (except docnm_kwd and knowledge_graph_kwd)
# are stored as ###-joined strings in Vastbase.
KEYWORD_FIELDS = {
    "important_kwd", "tag_kwd", "question_kwd", "title_kwd",
    "doc_type_kwd", "toc_kwd", "raptor_kwd", "name_kwd",
    "entities_kwd", "entity_kwd", "entity_type_kwd",
    "from_entity_kwd", "to_entity_kwd", "removed_kwd",
    "knowledge_graph_kwd", "source_id",
}

# Fields that need hex encoding
POSITION_FIELDS = {"position_int", "page_num_int", "top_int"}

# Fields stored as JSON strings
JSON_FIELDS = {"tag_feas"}

# Vector field pattern
VECTOR_PATTERN = re.compile(r"^q_(\d+)_vec$")


def is_vector_field(name: str) -> bool:
    return bool(VECTOR_PATTERN.match(name))


def detect_vector_size(doc: dict[str, Any]) -> int:
    """Extract vector dimension from a document's field names."""
    for k in doc.keys():
        m = VECTOR_PATTERN.match(k)
        if m:
            return int(m.group(1))
    return 0


def convert_field(name: str, value: Any) -> Any:
    """
    Convert a single field from ES native format to Vastbase format.

    Returns the converted value, or None to skip the field.
    """
    if value is None:
        return None

    # Keyword fields: list → ###-joined string
    if name in KEYWORD_FIELDS:
        if isinstance(value, list):
            return "###".join(str(v) for v in value if v is not None)
        return str(value)

    # Position fields: list → hex-encoded underscore-joined string
    if name == "position_int":
        # ES stores [[x1,y1,z1,w1,h1], [x2,y2,z2,w2,h2], ...]
        if isinstance(value, list):
            arr = []
            for row in value:
                if isinstance(row, (list, tuple)):
                    arr.extend(int(v) for v in row)
                else:
                    arr.append(int(row))
            return "_".join(f"{num:08x}" for num in arr)
        return str(value)

    if name in ("page_num_int", "top_int"):
        # ES stores [1, 2, 3, ...]
        if isinstance(value, list):
            arr = [int(v) for v in value]
            return "_".join(f"{num:08x}" for num in arr)
        return str(value)

    # JSON fields: dict → JSON string
    if name in JSON_FIELDS:
        if isinstance(value, dict):
            return json.dumps(value, ensure_ascii=False)
        return str(value)

    # content_with_weight may be a dict → serialize
    if name == "content_with_weight" and isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)

    # kb_id: if it's a list (ES bug), take first element
    if name == "kb_id" and isinstance(value, list):
        return str(value[0]) if value else ""

    # Vector fields: keep as list (psycopg2 handles floatvector via execute_values)
    if is_vector_field(name):
        if isinstance(value, list):
            return value  # keep as list
        return value

    # Default: return as-is
    return value


def convert_document(es_doc: dict[str, Any]) -> dict[str, Any]:
    """
    Convert an ES document (with _id) to a Vastbase row dict.

    Args:
        es_doc: Document from ES, with _id and _source fields merged.

    Returns:
        Dict ready for Vastbase INSERT.
    """
    row: dict[str, Any] = {}

    # Set id from _id
    row["id"] = str(es_doc.get("_id", ""))

    for name, value in es_doc.items():
        if name == "_id":
            continue
        if name == "_score":
            continue

        converted = convert_field(name, value)
        if converted is not None:
            row[name] = converted

    return row


def convert_batch(es_docs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert a batch of ES documents to Vastbase row dicts."""
    return [convert_document(doc) for doc in es_docs]