"""Logic tests for the VB->VB metadata migration (no DB required).

Tests:
  - topo_sort: parent-before-child, determinism, cycle breaking, filters.
  - upsert SQL composition: ON CONFLICT ... DO UPDATE ... EXCLUDED, and the
    no-PK (plain INSERT) fallback.
  - offline package round-trip: dump_meta -> load_meta -> verify_package
    against fake reader/writer, including FK order, datetime/None/list
    encoding, no-PK tables, include filters and incomplete packages.
Run: python test_meta_logic.py
"""
import sys

# import from this dir
from meta_migrator import topo_sort


def test_topo_basic_parent_first():
    # document -> knowledgebase -> tenant : parents must come first
    deps = {
        "document": {"knowledgebase"},
        "knowledgebase": {"tenant"},
    }
    tables = ["tenant", "knowledgebase", "document"]
    out = topo_sort(tables, deps)
    assert out.index("tenant") < out.index("knowledgebase") < out.index("document"), out
    print("  ok parent-first chain:", out)


def test_topo_deterministic_name_order():
    # siblings with no dependency -> name order
    deps = {}
    tables = ["zebra", "apple", "mango"]
    out = topo_sort(tables, deps)
    assert out == ["apple", "mango", "zebra"], out
    print("  ok sibling name order:", out)


def test_topo_cycle_break():
    # a<->b cycle plus an independent c
    deps = {"a": {"b"}, "b": {"a"}}
    tables = ["a", "b", "c"]
    out = topo_sort(tables, deps)
    assert "c" in out and len(out) == 3, out
    # c has no dependency so it comes first
    assert out[0] == "c", out
    print("  ok cycle broken, independents first:", out)


def test_topo_ignores_out_of_set_edges():
    # edge to a table NOT in the migration set must be ignored
    deps = {"document": {"knowledgebase"}, "knowledgebase": {"tenant"}}
    tables = ["document", "knowledgebase"]  # tenant excluded
    out = topo_sort(tables, deps)
    assert out.index("knowledgebase") < out.index("document"), out
    assert "tenant" not in out, out
    print("  ok out-of-set edge ignored:", out)


def test_topo_self_ref_ignored():
    deps = {"a": {"a"}}
    out = topo_sort(["a", "b"], deps)
    assert sorted(out) == ["a", "b"], out
    print("  ok self-ref ignored:", out)


def test_topo_multi_parent():
    # child has two parents; both must precede it
    deps = {"c": {"a", "b"}}
    out = topo_sort(["a", "b", "c"], deps)
    assert out.index("a") < out.index("c") and out.index("b") < out.index("c"), out
    print("  ok multi-parent:", out)


# ── upsert SQL composition ──────────────────────────────────────────────
# Mirror what VBWriter._upsert_batch_attempt builds. We can't render the
# statement without a live psycopg2 connection (as_string requires a real
# connection/cursor via isinstance), so we assert on repr(): a Composed's
# repr embeds the literal SQL fragments and Identifier names, which is enough
# to verify the ON CONFLICT / DO UPDATE / DO NOTHING / plain-INSERT structure.

from psycopg2 import sql


def _build_upsert_stmt(table, all_columns, pk_cols):
    """Exact reproduction of the clause-building in vb_writer._upsert_batch_attempt."""
    col_identifiers = sql.SQL(", ").join(sql.Identifier(c) for c in all_columns)
    if pk_cols:
        pk_set = set(pk_cols)
        conflict_target = sql.SQL(", ").join(sql.Identifier(c) for c in pk_cols)
        non_pk = [c for c in all_columns if c not in pk_set]
        if non_pk:
            set_clause = sql.SQL(", ").join(
                sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(c))
                for c in non_pk
            )
            conflict_action = sql.SQL("DO UPDATE SET {set_clause}").format(set_clause=set_clause)
        else:
            conflict_action = sql.SQL("DO NOTHING")
        conflict_clause = sql.SQL(" ON CONFLICT ({conflict}) {action}").format(
            conflict=conflict_target, action=conflict_action
        )
    else:
        conflict_clause = sql.SQL("")
    return sql.SQL("INSERT INTO {table} ({columns}) VALUES %s{conflict}").format(
        table=sql.Identifier(table),
        columns=col_identifiers,
        conflict=conflict_clause,
    )


def test_upsert_sql_with_pk():
    s = repr(_build_upsert_stmt("document", ["id", "name", "kb_id"], ["id"]))
    assert "INSERT INTO" in s, s
    assert "'document'" in s, s
    assert "ON CONFLICT" in s, s
    assert "DO UPDATE SET" in s, s
    assert "EXCLUDED" in s, s
    assert "VALUES %s" in s, s
    # no RETURNING (B mode constraint)
    assert "RETURNING" not in s, s
    print("  ok upsert w/ PK:", s)


def test_upsert_sql_composite_pk():
    s = repr(_build_upsert_stmt("assoc", ["a_id", "b_id", "val"], ["a_id", "b_id"]))
    assert "ON CONFLICT" in s, s
    assert "'a_id'" in s and "'b_id'" in s, s
    # 'val' (non-PK) updated from EXCLUDED; PK cols (a_id,b_id) are the target only
    assert "'val'" in s and "EXCLUDED" in s, s
    print("  ok composite PK:", s)


def test_upsert_sql_all_pk_do_nothing():
    # row has only PK columns -> DO NOTHING (no SET)
    s = repr(_build_upsert_stmt("t", ["id"], ["id"]))
    assert "DO NOTHING" in s, s
    assert "DO UPDATE" not in s, s
    print("  ok all-PK -> DO NOTHING:", s)


def test_upsert_sql_no_pk_plain_insert():
    s = repr(_build_upsert_stmt("log", ["id", "msg", "ts"], []))
    assert "INSERT INTO" in s, s
    assert "ON CONFLICT" not in s, s
    assert "VALUES %s" in s, s
    print("  ok no-PK plain insert:", s)


# ── offline package round-trip ─────────────────────────────────────────
# dump_meta -> load_meta -> verify_package against in-memory fakes.

import json
import os
import shutil
import tempfile
from datetime import datetime

import meta_package


class FakeReader:
    """Just the surface MetaMigrator/dump_meta touch on the source side."""

    def __init__(self, schema, data, deps=None):
        self.schema = schema  # {table: {"pk": [...], "columns": [...]}}
        self.data = data      # {table: [row dicts]}
        self.deps = deps or {}

    def list_all_tables(self):
        return list(self.schema)

    def get_foreign_key_deps(self):
        return self.deps

    def get_primary_keys(self, t):
        return self.schema[t]["pk"]

    def get_columns(self, t):
        return self.schema[t]["columns"]

    def count_rows(self, t):
        return len(self.data[t])

    def scroll_rows(self, t, batch_size=1000):
        rows = self.data[t]
        for i in range(0, len(rows), batch_size):
            yield rows[i:i + batch_size]


class FakeWriter:
    """Just the surface load_meta/verify_package touch on the target side."""

    def __init__(self, schema):
        self.schema = schema
        self.rows = {t: {} for t in schema}  # keyed by pk tuple
        self.truncated = []
        self.call_order = []

    def table_exists(self, t):
        return t in self.schema

    def get_columns(self, t):
        return self.schema[t]["columns"]

    def truncate(self, t):
        self.truncated.append(t)
        self.rows[t] = {}

    def count_rows(self, t):
        return len(self.rows[t])

    def upsert_batch(self, t, rows, pk_cols):
        for r in rows:
            key = tuple(r[c] for c in pk_cols) if pk_cols else (len(self.rows[t]),)
            self.rows[t][key] = r
        self.call_order.append(t)
        return len(rows)


def _pkg_fixture():
    schema = {
        "tenant": {"pk": ["id"], "columns": ["id", "name", "created_at"]},
        "knowledgebase": {"pk": ["id"], "columns": ["id", "tenant_id", "position_int"]},
    }
    data = {
        "tenant": [
            {"id": "t1", "name": "acme", "created_at": datetime(2026, 1, 2, 3, 4, 5)},
            {"id": "t2", "name": None, "created_at": None},
        ],
        "knowledgebase": [
            {"id": "k1", "tenant_id": "t1", "position_int": [1, 2, 3]},
        ],
    }
    deps = {"knowledgebase": {"tenant"}}
    return schema, data, deps


def test_package_roundtrip_preserves_rows_and_fk_order():
    schema, data, deps = _pkg_fixture()
    tmp = tempfile.mkdtemp()
    # keep the load progress file out of the CWD
    meta_package.LOAD_PROGRESS_FILE = os.path.join(tmp, "progress.json")
    try:
        reader = FakeReader(schema, data, deps)
        manifest = meta_package.dump_meta(reader, tmp)
        assert os.path.exists(os.path.join(tmp, "manifest.json"))
        assert [e["table"] for e in manifest["tables"]] == ["tenant", "knowledgebase"]

        writer = FakeWriter(schema)
        total = meta_package.load_meta(writer, tmp)
        assert total["rows"] == 3 and total["failed"] == 0, total
        # parents loaded before children (manifest order preserved)
        assert writer.call_order == ["tenant", "knowledgebase"], writer.call_order
        # datetime -> str() (space-separated, valid PG timestamp literal),
        # None stays None, list stays a list
        assert writer.rows["tenant"][("t1",)]["created_at"] == "2026-01-02 03:04:05"
        assert writer.rows["tenant"][("t2",)]["name"] is None
        assert writer.rows["knowledgebase"][("k1",)]["position_int"] == [1, 2, 3]

        v = meta_package.verify_package(writer, tmp)
        assert v == {"matches": 2, "mismatches": 0}, v
    finally:
        shutil.rmtree(tmp)
    print("  ok roundtrip: rows, FK order, types, verify")


def test_package_no_pk_table_truncate_insert():
    schema, data, deps = _pkg_fixture()
    schema["sync_logs"] = {"pk": [], "columns": ["msg"]}
    data["sync_logs"] = [{"msg": "x"}, {"msg": "y"}]
    tmp = tempfile.mkdtemp()
    meta_package.LOAD_PROGRESS_FILE = os.path.join(tmp, "progress.json")
    try:
        meta_package.dump_meta(FakeReader(schema, data, deps), tmp)
        writer = FakeWriter(schema)
        writer.rows["sync_logs"] = {(0,): {"msg": "stale"}}  # pre-existing junk
        meta_package.load_meta(writer, tmp)
        assert "sync_logs" in writer.truncated, writer.truncated
        assert writer.count_rows("sync_logs") == 2
    finally:
        shutil.rmtree(tmp)
    print("  ok no-PK table: truncated then plain-inserted")


def test_package_incomplete_refused():
    tmp = tempfile.mkdtemp()
    try:
        with open(os.path.join(tmp, "tenant.jsonl"), "w") as f:
            f.write('{"id": "t1"}\n')
        try:
            meta_package.load_meta(FakeWriter({}), tmp)
            raise AssertionError("expected FileNotFoundError")
        except FileNotFoundError:
            pass
    finally:
        shutil.rmtree(tmp)
    print("  ok missing manifest refused")


def test_package_load_filters():
    schema, data, deps = _pkg_fixture()
    tmp = tempfile.mkdtemp()
    meta_package.LOAD_PROGRESS_FILE = os.path.join(tmp, "progress.json")
    try:
        meta_package.dump_meta(FakeReader(schema, data, deps), tmp)
        writer = FakeWriter(schema)
        meta_package.load_meta(writer, tmp, include=["tenant"])
        assert writer.call_order == ["tenant"], writer.call_order
        assert writer.count_rows("knowledgebase") == 0
    finally:
        shutil.rmtree(tmp)
    print("  ok load-side include filter")


if __name__ == "__main__":
    tests = [
        test_topo_basic_parent_first,
        test_topo_deterministic_name_order,
        test_topo_cycle_break,
        test_topo_ignores_out_of_set_edges,
        test_topo_self_ref_ignored,
        test_topo_multi_parent,
        test_upsert_sql_with_pk,
        test_upsert_sql_composite_pk,
        test_upsert_sql_all_pk_do_nothing,
        test_upsert_sql_no_pk_plain_insert,
        test_package_roundtrip_preserves_rows_and_fk_order,
        test_package_no_pk_table_truncate_insert,
        test_package_incomplete_refused,
        test_package_load_filters,
    ]
    failed = 0
    for t in tests:
        print(f"[*] {t.__name__}")
        try:
            t()
        except Exception as e:
            failed += 1
            print(f"    FAIL: {type(e).__name__}: {e}")
    print(f"\n{'='*50}")
    print(f"{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
