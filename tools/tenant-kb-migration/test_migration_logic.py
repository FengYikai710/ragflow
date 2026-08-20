#!/usr/bin/env python3
"""
无数据库的逻辑自测：用 SQLite 内存库模拟 Vastbase(PG 协议) 的两套库，
跑通 dry-run -> execute -> verify 全流程，并断言最终状态。

SQLite 足以承接本工具发出的 SQL（双引号标识符、IN (%s,...)、
ALTER TABLE ... RENAME TO、INSERT...SELECT），information_schema 用
ATTACH 出来的同名 schema + 视图顶替。占位符 %s 由游标翻译成 ?。

    python test_migration_logic.py
"""

import os
import re
import sqlite3
import sys
import tempfile
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import migrate  # noqa: E402

B = uuid.uuid4().hex  # 租户 B（源）
A = uuid.uuid4().hex  # 租户 A（目标）
KB1 = uuid.uuid4().hex  # 有 chunk + doc_meta
KB2 = uuid.uuid4().hex  # 无 chunk（未解析过）
KB_A_SAME_NAME = uuid.uuid4().hex  # 租户 A 已有的同名库（冲突用例）


class TranslatingCursor(sqlite3.Cursor):
    def execute(self, sql, params=()):
        return super().execute(re.sub(r"%s", "?", sql), tuple(params or ()))


class SqliteDb(migrate.Db):
    """接口与 migrate.Db 一致的 SQLite 替身。"""

    def __init__(self, flavor, host, port, user, password, db):
        self.flavor = flavor
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = None
        self.conn.isolation_level = None  # 手动事务
        # information_schema 顶替（table_exists / table_columns 用）
        cur = self.conn.cursor(TranslatingCursor)
        cur.execute("ATTACH ':memory:' AS information_schema")
        cur.execute("CREATE TABLE information_schema.tables (table_name TEXT)")
        cur.execute("CREATE TABLE information_schema.columns "
                    "(table_name TEXT, column_name TEXT, data_type TEXT, "
                    "column_default TEXT, ordinal_position INT)")
        self.conn.commit()

    def q(self, sql, params=None):
        cur = self.conn.cursor(TranslatingCursor)
        cur.execute(sql, params)
        if cur.description is None:
            cur.close()
            return []
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close()
        return rows

    def x(self, sql, params=None) -> int:
        cur = self.conn.cursor(TranslatingCursor)
        cur.execute(sql, params)
        n = cur.rowcount
        cur.close()
        return n if n and n > 0 else 0

    def _sync_schema_views(self):
        cur = self.conn.cursor(TranslatingCursor)
        cur.execute("DELETE FROM information_schema.tables")
        cur.execute("INSERT INTO information_schema.tables (table_name) "
                    "SELECT name FROM sqlite_master WHERE type IN ('table','view') "
                    "AND name NOT LIKE 'information_schema%'")
        cur.close()

    def table_exists(self, table):
        self._sync_schema_views()
        cur = self.conn.cursor(TranslatingCursor)
        cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", (table,))
        r = cur.fetchone()[0]
        cur.close()
        return bool(r)

    def table_columns(self, table):
        # 直接用 SQLite 真实元数据（information_schema.columns 只对 seed 的表登记过，
        # 运行中新建的表拿不到）
        cur = self.conn.cursor(TranslatingCursor)
        cur.execute("SELECT name, type, dflt_value FROM pragma_table_info(?)", (table,))
        cols = [{"column_name": r[0], "data_type": r[1], "column_default": r[2]} for r in cur.fetchall()]
        cur.close()
        return cols

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        self.conn.close()


def seed(meta: SqliteDb, vb: SqliteDb):
    cur = meta.conn.cursor(TranslatingCursor)

    def dml(sql, params=()):
        cur.execute(sql, params)

    # ---- meta: 租户 / 模型 / 知识库 / 文档 / 文件 ----
    # peewee 的 BaseModel 会给每张表补 create_time/create_date/update_time/update_date，
    # 这里同样带上（migrate.py 的 UPDATE 会写这些列）
    TS = ", create_time INT, create_date TEXT, update_time INT, update_date TEXT"
    dml("CREATE TABLE tenant (id TEXT PRIMARY KEY, name TEXT, embd_id TEXT" + TS + ")")
    dml("INSERT INTO tenant (id, name, embd_id) VALUES (?,?,?)", (B, "租户B", "bge@BAAI"))
    dml("INSERT INTO tenant (id, name, embd_id) VALUES (?,?,?)", (A, "租户A", "bge@BAAI"))
    dml("CREATE TABLE knowledgebase (id TEXT PRIMARY KEY, name TEXT, embd_id TEXT, "
        "tenant_embd_id INT, doc_num INT, chunk_num INT, status TEXT, tenant_id TEXT, created_by TEXT" + TS + ")")
    dml("INSERT INTO knowledgebase (id, name, embd_id, tenant_embd_id, doc_num, chunk_num, status, tenant_id, created_by) "
        "VALUES (?,?,?,?,?,?,?,?,?)", (KB1, "测试库1", "bge@BAAI", 11, 2, 42, "1", B, B))
    dml("INSERT INTO knowledgebase (id, name, embd_id, tenant_embd_id, doc_num, chunk_num, status, tenant_id, created_by) "
        "VALUES (?,?,?,?,?,?,?,?,?)", (KB2, "未解析库", "other-embd@XF", None, 1, 0, "1", B, B))
    dml("CREATE TABLE document (id TEXT PRIMARY KEY, kb_id TEXT, created_by TEXT, run TEXT" + TS + ")")
    dml("INSERT INTO document (id, kb_id, created_by, run) VALUES (?,?,?,?)", ("doc1", KB1, B, "0"))
    dml("INSERT INTO document (id, kb_id, created_by, run) VALUES (?,?,?,?)", ("doc2", KB1, B, "0"))
    dml("INSERT INTO document (id, kb_id, created_by, run) VALUES (?,?,?,?)", ("doc3", KB2, B, "0"))
    dml("CREATE TABLE tenant_llm (id INTEGER PRIMARY KEY, tenant_id TEXT, llm_factory TEXT, "
        "llm_name TEXT, model_type TEXT)")
    dml("INSERT INTO tenant_llm VALUES (?,?,?,?,?)", (77, A, "BAAI", "bge", "Text Embedding"))
    dml("CREATE TABLE file (id TEXT PRIMARY KEY, parent_id TEXT, tenant_id TEXT, created_by TEXT, "
        "name TEXT, type TEXT, size INT, location TEXT, source_type TEXT" + TS + ")")
    dml("INSERT INTO file (id, parent_id, tenant_id, created_by, name, type, size, location, source_type) "
        "VALUES (?,?,?,?,?,?,?,?,?)", ("frootB", "frootB", B, B, "/", "folder", 0, "", ""))
    dml("INSERT INTO file (id, parent_id, tenant_id, created_by, name, type, size, location, source_type) "
        "VALUES (?,?,?,?,?,?,?,?,?)", ("fkbB", "frootB", B, B, ".knowledgebase", "folder", 0, "", ""))
    dml("INSERT INTO file (id, parent_id, tenant_id, created_by, name, type, size, location, source_type) "
        "VALUES (?,?,?,?,?,?,?,?,?)", ("fdir1", "fkbB", B, B, "测试库1", "folder", 0, "", "knowledgebase"))
    dml("INSERT INTO file (id, parent_id, tenant_id, created_by, name, type, size, location, source_type) "
        "VALUES (?,?,?,?,?,?,?,?,?)", ("file1", "fdir1", B, B, "a.pdf", "pdf", 10, "a.pdf", "knowledgebase"))
    dml("CREATE TABLE file2document (id TEXT PRIMARY KEY, file_id TEXT, document_id TEXT)")
    dml("INSERT INTO file2document VALUES (?,?,?)", ("f2d1", "file1", "doc1"))
    dml("CREATE TABLE dialog (id TEXT PRIMARY KEY, tenant_id TEXT, name TEXT, kb_ids TEXT)")
    dml("INSERT INTO dialog VALUES (?,?,?,?)", ("dlg1", B, "B的助手", f'["{KB1}"]'))
    dml("CREATE TABLE task (id TEXT PRIMARY KEY, doc_id TEXT)")
    meta.commit()

    # ---- vb: 向量表 + doc_meta ----
    dml_vb = vb.conn.cursor(TranslatingCursor)
    for tbl in (f"ragflow_{B}_{KB1}", f"ragflow_doc_meta_{B}"):
        dml_vb.execute(f'CREATE TABLE "{tbl}" (id TEXT, kb_id TEXT, content TEXT, q_1024_vec TEXT)')
    for i in range(5):
        dml_vb.execute(f'INSERT INTO "ragflow_{B}_{KB1}" VALUES (?,?,?,?)',
                       (f"ck{i}", KB1, f"chunk {i}", "[0.1,0.2]"))
    dml_vb.execute(f'INSERT INTO "ragflow_doc_meta_{B}" VALUES (?,?,?,?)',
                   ("m1", KB1, "doc1", "{}"))
    vb.commit()


def build_migrator(meta: SqliteDb, vb: SqliteDb):
    """构造真实的 TenantKbMigrator（连接参数会被 SqliteDb 忽略），再换成已灌数据的库。"""
    orig_db = migrate.Db
    migrate.Db = SqliteDb
    try:
        args = migrate.build_parser().parse_args([
            "--from-tenant", B, "--to-tenant", A, "--vb-password", "x"])
        mig = migrate.TenantKbMigrator(args)
    finally:
        migrate.Db = orig_db
    mig.vb.close()
    mig.meta.close()
    mig.vb, mig.meta = vb, meta
    return mig


def main():
    failures = []

    def check(label, cond, detail=""):
        print(f"  [{'OK' if cond else 'FAIL':4}] {label}" + (f"  {detail}" if detail else ""))
        if not cond:
            failures.append(label)

    meta, vb = SqliteDb(None, None, None, None, None, None), SqliteDb(None, None, None, None, None, None)
    seed(meta, vb)

    # ---------- 冲突路径：A 有同名库 ----------
    mig = build_migrator(meta, vb)
    # 临时给 A 塞一个与 KB1 同名的库，验证报错；随后删掉走正常流程
    c = mig.meta.conn.cursor(TranslatingCursor)
    c.execute("INSERT INTO knowledgebase (id, name, embd_id, tenant_embd_id, doc_num, chunk_num, status, tenant_id, created_by) "
              "VALUES (?,?,?,?,?,?,?,?,?)",
              (KB_A_SAME_NAME, "测试库1", "bge@BAAI", None, 0, 0, "1", A, A))
    mig.meta.commit()
    plan = mig.load_plan()
    check("同名冲突被识别为错误", any("同名" in e for e in plan["errors"]), str(plan["errors"]))
    c.execute("DELETE FROM knowledgebase WHERE id = ?", (KB_A_SAME_NAME,))
    mig.meta.commit()
    plan = mig.load_plan()
    mig.print_plan(plan)
    check("dry-run 无错误", not plan["errors"])
    check("计划包含 2 个库", len(plan["kbs"]) == 2)
    check("识别到引用库的助手", len(plan["affected"]["dialog"]) == 1)
    check("识别出 A 缺 other-embd 模型",
          any("other-embd" in w for w in plan["warnings"]))
    rows = mig.meta.scalar('SELECT COUNT(*) FROM "knowledgebase" WHERE tenant_id = ?', (B,))
    check("dry-run 未改动数据", rows == 2, f"B 名下仍有 {rows} 库")

    # ---------- execute ----------
    mig.execute(plan)
    rows = mig.meta.q('SELECT tenant_id, created_by FROM "knowledgebase" WHERE id = ?', (KB1,))
    check("KB1 归属 A", rows[0]["tenant_id"] == A and rows[0]["created_by"] == A)
    rows = mig.meta.q('SELECT COUNT(*) AS n FROM "document" WHERE created_by = ?', (A,))
    check("文档 created_by = A", rows[0]["n"] == 3)
    rows = mig.meta.q('SELECT tenant_embd_id FROM "knowledgebase" WHERE id = ?', (KB1,))
    check("tenant_embd_id 重映射到 A 的 tenant_llm(77)", rows[0]["tenant_embd_id"] == 77)
    rows = mig.meta.q('SELECT tenant_embd_id FROM "knowledgebase" WHERE id = ?', (KB2,))
    check("A 缺模型时 tenant_embd_id 置 NULL", rows[0]["tenant_embd_id"] is None)
    check("向量表已改名", mig.vb.table_exists(f"ragflow_{A}_{KB1}") and not mig.vb.table_exists(f"ragflow_{B}_{KB1}"))
    n = mig.vb.scalar(f'SELECT COUNT(*) FROM "ragflow_{A}_{KB1}"')
    check("chunk 行数保留", n == 5, f"{n} 行")
    left = mig.vb.scalar(f'SELECT COUNT(*) FROM "ragflow_doc_meta_{B}"')
    check("doc_meta 源表已清", left == 0)
    n = mig.vb.scalar(f'SELECT COUNT(*) FROM "ragflow_doc_meta_{A}"')
    check("doc_meta 目标表就位", n == 1)
    rows = mig.meta.q('SELECT tenant_id, parent_id FROM "file" WHERE id = ?', ("file1",))
    check("file1 归属 A", rows[0]["tenant_id"] == A)
    parent = rows[0]["parent_id"]
    folder = mig.meta.q('SELECT name FROM "file" WHERE id = ?', (parent,))
    check("file1 挂到 A 的 .knowledgebase/测试库1 下", folder and folder[0]["name"] == "测试库1")
    chain = mig.meta.q('SELECT name, tenant_id FROM "file" WHERE id = ?', (parent,))
    fparent = mig.meta.q('SELECT parent_id FROM "file" WHERE id = ?', (parent,))[0]["parent_id"]
    kbroot = mig.meta.q('SELECT name FROM "file" WHERE id = ?', (fparent,))
    check("父级是 A 的 .knowledgebase", kbroot and kbroot[0]["name"] == ".knowledgebase")
    check("B 侧旧的 kb 空文件夹被清掉", not mig.meta.q('SELECT 1 FROM "file" WHERE id = ?', ("fdir1",)))

    # ---------- verify ----------
    state = migrate.load_state()
    ok = mig.verify(state)
    check("--verify 通过", ok)
    mig.vb.close()
    mig.meta.close()

    print("\n" + ("全部通过。" if not failures else f"失败 {len(failures)} 项: {failures}"))
    return 0 if not failures else 1


if __name__ == "__main__":
    os.chdir(tempfile.mkdtemp())  # STATE_FILE 写到临时目录
    sys.exit(main())
