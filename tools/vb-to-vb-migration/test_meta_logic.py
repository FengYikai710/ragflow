"""Logic tests for the VB->VB metadata migration (no DB required).

Tests:
  - topo_sort: parent-before-child, determinism, cycle breaking, filters.
  - upsert SQL composition: ON CONFLICT ... DO UPDATE ... EXCLUDED, and the
    no-PK (plain INSERT) fallback.
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
