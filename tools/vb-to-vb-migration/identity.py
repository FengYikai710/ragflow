"""
Identity row processing for VB → VB migration.

Replaces the ES→VB converter (tools/es-to-vastbase-migration/converter.py).
Rows read from a source Vastbase table are ALREADY in Vastbase's internal
storage format, so they must NOT be re-converted — doing so would corrupt
them (e.g. position_int would be flattened twice, _kwd strings re-stringified).

The only transformations needed are:
  1. Column-intersection: drop source columns the target table doesn't have
     (so INSERT never references a non-existent column).
  2. Vector-format guarantee: ensure q_*_vec is a parseable "[v1,v2,...]"
     string for parameterized insert. We read vectors via ::text so they're
     already strings, but handle list defensively in case of adapter changes.

Type safety (verified against vastbase_mapping.json):
  - varchar/text    → str   (passthrough)
  - double precision→ float (passthrough)
  - integer         → int   (passthrough)
  - integer[]       → Python list (psycopg2 adapts list → PG array; insert_batch
                       relies on this — must NOT ::text cast, which yields "{1,2,3}")
  - floatvector     → "[..]" string via ::text (vastbase parses it on insert,
                       per converter.py:114-121 contract)
"""

import re
from typing import Any

VECTOR_RE = re.compile(r"^q_\d+_vec$")


def is_vector_field(name: str) -> bool:
    return bool(VECTOR_RE.match(name))


def ensure_vector_str(value: Any) -> Any:
    """Ensure a floatvector value is a parseable '[v1,v2,...]' string.

    None stays None (lets the column default apply). A list is serialized;
    a string (the ::text read path) is returned as-is.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(str(float(x)) for x in value) + "]"
    # Fallback: unknown scalar type — wrap as single-element vector string.
    return "[" + str(float(value)) + "]"


def identity_row(
    row: dict[str, Any],
    target_columns: set[str],
    vector_fields: set[str],
) -> dict[str, Any]:
    """Keep only columns in target_columns; ensure vector fields are strings.

    No ES→VB conversion is applied — source values are already VB-native.
    """
    out: dict[str, Any] = {}
    for k, v in row.items():
        if k not in target_columns:
            continue
        out[k] = ensure_vector_str(v) if k in vector_fields else v
    return out


def identity_batch(
    rows: list[dict[str, Any]],
    target_columns: set[str],
    vector_fields: set[str],
) -> list[dict[str, Any]]:
    return [identity_row(r, target_columns, vector_fields) for r in rows]
