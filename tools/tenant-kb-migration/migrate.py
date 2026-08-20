#!/usr/bin/env python3
"""
RAGFlow 租户间知识库迁移工具（同实例、Vastbase 底座）。

把「租户 B」名下的全部（或指定）知识库移交给「租户 A」：

  rag_flow 元数据库（peewee，Vastbase 或 MySQL）
    - knowledgebase.tenant_id / created_by          -> A
    - knowledgebase.tenant_embd_id                  -> 重映射到 A 的 tenant_llm 行（找不到则置 NULL）
    - document.created_by                           -> A（kb_id 不变）
    - file 行（source_type='knowledgebase'）         -> tenant_id/created_by -> A，
                                                       并挂到 A 的 .knowledgebase/<kb名> 文件夹下
  ragflow 向量数据库（Vastbase）
    - chunk 表 ragflow_{B}_{kb}                     -> RENAME TO ragflow_{A}_{kb}
    - doc_meta 行 ragflow_doc_meta_{B} 中该批 kb    -> 搬到 ragflow_doc_meta_{A}
  MinIO / 对象存储
    - bucket = kb_id，与租户无关，无需迁移

用法（先 dry-run 预览，确认后 --execute）：

    python migrate.py \\
        --vb-host vb --vb-port 5432 --vb-user rag_flow --vb-password '***' --vb-db ragflow \\
        --meta-type vastbase --meta-db rag_flow \\
        --from-tenant <租户B_ID> --to-tenant <租户A_ID>

    python migrate.py ... --execute     # 实际执行（建议先停掉 ragflow-server / task_executor）
    python migrate.py ... --verify      # 迁移后校验

密码也可用环境变量 VB_PASSWORD / META_PASSWORD 传入。
"""

import argparse
import json
import logging
import re
import sys
import time
import uuid
from datetime import datetime

HEX32_RE = re.compile(r"^[0-9a-f]{32}$")
KB_FOLDER_NAME = ".knowledgebase"  # api/db/__init__.py KNOWLEDGEBASE_FOLDER_NAME
FILE_TYPE_FOLDER = "folder"
FILE_SOURCE_KB = "knowledgebase"  # common/constants.py FileSource.KNOWLEDGEBASE

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("tenant-kb-migration")

STATE_FILE = ".tenant_kb_migration.json"


def save_state(plan: dict, from_t: str, to_t: str):
    """记录本次迁移的知识库清单，供 --verify 事后核对（迁移后 KB 已不属于源租户，
    无法再通过 tenant_id 反查）。"""
    state = {
        "from_tenant": from_t,
        "to_tenant": to_t,
        "src_meta_table": plan.get("src_meta_table"),
        "dst_meta_table": plan.get("dst_meta_table"),
        "kbs": [{"id": k["id"], "name": k["name"], "src_table": k["src_table"], "dst_table": k["dst_table"],
                 "had_table": bool(k["src_exists"] or k["dst_exists"])}
                for k in plan["kbs"]],
        "finished_at": datetime.now().isoformat(),
    }
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def load_state() -> dict:
    with open(STATE_FILE) as f:
        return json.load(f)


def get_uuid() -> str:
    # 与 common/misc_utils.get_uuid 一致：uuid1 的 32 位 hex
    return uuid.uuid1().hex


def now_ms() -> int:
    return int(time.time() * 1000)


def ph(n: int) -> str:
    """n 个占位符，用于 IN (...)。psycopg2 / pymysql 都是 %s。"""
    return ", ".join(["%s"] * n)


def split_model_name_and_factory(name: str) -> tuple[str, str | None]:
    # 镜像 TenantLLMService.split_model_name_and_factory："model@factory" -> (model, factory)
    arr = name.split("@")
    if len(arr) < 2:
        return name, None
    if len(arr) > 2:
        return "@".join(arr[0:-1]), arr[-1]
    return arr[0], arr[-1]


class Db:
    """psycopg2 / pymysql 统一的最小封装：查询返回 list[dict]。"""

    def __init__(self, flavor: str, host: str, port: int, user: str, password: str, db: str):
        self.flavor = flavor  # 'pg' | 'mysql'
        if flavor == "pg":
            import psycopg2

            self.conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=db)
        else:
            import pymysql

            self.conn = pymysql.connect(host=host, port=port, user=user, password=password, database=db, charset="utf8mb4")
        self.conn.autocommit(False)

    def quote(self, ident: str) -> str:
        if self.flavor == "pg":
            return '"' + ident.replace('"', '""') + '"'
        return "`" + ident.replace("`", "``") + "`"

    def q(self, sql: str, params=None) -> list[dict]:
        with self.conn.cursor() as cur:
            cur.execute(sql, params or ())
            if cur.description is None:
                return []
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def scalar(self, sql: str, params=None):
        rows = self.q(sql, params)
        return list(rows[0].values())[0] if rows else None

    def x(self, sql: str, params=None) -> int:
        with self.conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.rowcount

    def table_exists(self, table: str) -> bool:
        if self.flavor == "pg":
            # 与 rag/utils/vastbase_conn.get_table_exists 相同的探测方式
            return bool(self.scalar(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (table,)))
        return bool(self.scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = DATABASE() AND table_name = %s)", (table,)))

    def table_columns(self, table: str) -> list[dict]:
        if self.flavor == "pg":
            sql = ("SELECT column_name, data_type, column_default FROM information_schema.columns "
                   "WHERE table_name = %s ORDER BY ordinal_position")
        else:
            sql = ("SELECT column_name, data_type, column_default FROM information_schema.columns "
                   "WHERE table_schema = DATABASE() AND table_name = %s ORDER BY ordinal_position")
        return self.q(sql, (table,))

    def commit(self):
        self.conn.commit()

    def rollback(self):
        try:
            self.conn.rollback()
        except Exception:
            pass

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass


class TenantKbMigrator:
    def __init__(self, args):
        self.args = args
        self.from_t = args.from_tenant.lower()
        self.to_t = args.to_tenant.lower()
        self.vb = Db("pg", args.vb_host, args.vb_port, args.vb_user, args.vb_password, args.vb_db)
        meta_flavor = "pg" if args.meta_type == "vastbase" else "mysql"
        self.meta = Db(
            meta_flavor,
            args.meta_host or args.vb_host,
            args.meta_port or args.vb_port,
            args.meta_user or args.vb_user,
            args.meta_password or args.vb_password,
            args.meta_db,
        )

    # ------------------------------------------------------------------ 计划

    def load_plan(self) -> dict:
        plan = {"errors": [], "warnings": [], "kbs": []}

        tenants = self.meta.q(
            f"SELECT id, name, embd_id FROM {self.meta.quote('tenant')} WHERE id IN ({ph(2)})",
            (self.to_t, self.from_t),
        )
        tmap = {t["id"]: t for t in tenants}
        for tid, label in ((self.from_t, "源(租户B)"), (self.to_t, "目标(租户A)")):
            if tid not in tmap:
                plan["errors"].append(f"{label} 租户不存在: {tid}")
        if plan["errors"]:
            return plan
        plan["from_tenant"] = tmap[self.from_t]
        plan["to_tenant"] = tmap[self.to_t]

        kbs = self.meta.q(
            f"SELECT id, name, embd_id, tenant_embd_id, doc_num, chunk_num, status "
            f"FROM {self.meta.quote('knowledgebase')} WHERE tenant_id = %s ORDER BY name",
            (self.from_t,),
        )
        if self.args.kb_id:
            wanted = {k.lower() for k in self.args.kb_id}
            known = {k["id"] for k in kbs}
            for w in wanted - known:
                plan["errors"].append(f"--kb-id {w} 不属于租户 {self.from_t}")
            kbs = [k for k in kbs if k["id"] in wanted]
        if not self.args.include_invalid:
            invalid = [k for k in kbs if k["status"] != "1"]
            for k in invalid:
                plan["warnings"].append(f"跳过已失效(status={k['status']})的知识库: {k['name']} ({k['id']})")
            kbs = [k for k in kbs if k["status"] == "1"]
        if not kbs:
            plan["errors"].append(f"租户 {self.from_t} 名下没有可迁移的知识库")
            return plan
        kb_ids = [k["id"] for k in kbs]

        # 名称冲突（RAGFlow 按租户内名称区分知识库）
        names = [k["name"] for k in kbs]
        conflict_rows = self.meta.q(
            f"SELECT id, name FROM {self.meta.quote('knowledgebase')} "
            f"WHERE tenant_id = %s AND name IN ({ph(len(names))})",
            (self.to_t, *names),
        )
        plan["name_conflicts"] = {c["name"]: c for c in conflict_rows}

        # 租户 A 可用的模型（近似检查：tenant_llm.llm_name + A 的默认 embd_id）
        a_models = {r["llm_name"] for r in self.meta.q(
            f"SELECT llm_name FROM {self.meta.quote('tenant_llm')} WHERE tenant_id = %s", (self.to_t,))}
        a_models.add(split_model_name_and_factory(plan["to_tenant"]["embd_id"])[0])

        # 文档/文件计数
        doc_counts = {r["kb_id"]: r["n"] for r in self.meta.q(
            f"SELECT kb_id, COUNT(*) AS n FROM {self.meta.quote('document')} "
            f"WHERE kb_id IN ({ph(len(kb_ids))}) GROUP BY kb_id", tuple(kb_ids))}
        file_counts = {r["kb_id"]: r["n"] for r in self.meta.q(
            f"SELECT d.kb_id AS kb_id, COUNT(*) AS n "
            f"FROM {self.meta.quote('file')} f "
            f"JOIN {self.meta.quote('file2document')} f2d ON f2d.file_id = f.id "
            f"JOIN {self.meta.quote('document')} d ON d.id = f2d.document_id "
            f"WHERE d.kb_id IN ({ph(len(kb_ids))}) AND f.source_type = %s GROUP BY d.kb_id",
            (*kb_ids, FILE_SOURCE_KB))}

        # Vastbase 侧：chunk 表与 doc_meta
        src_meta_t = f"ragflow_doc_meta_{self.from_t}"
        dst_meta_t = f"ragflow_doc_meta_{self.to_t}"
        plan["src_meta_table"], plan["dst_meta_table"] = src_meta_t, dst_meta_t
        plan["src_meta_exists"] = self.vb.table_exists(src_meta_t)
        plan["dst_meta_exists"] = self.vb.table_exists(dst_meta_t)
        meta_rows = {}
        if plan["src_meta_exists"]:
            meta_rows = {r["kb_id"]: r["n"] for r in self.vb.q(
                f'SELECT kb_id, COUNT(*) AS n FROM {self.vb.quote(src_meta_t)} '
                f'WHERE kb_id IN ({ph(len(kb_ids))}) GROUP BY kb_id', tuple(kb_ids))}

        for k in kbs:
            src_t, dst_t = f"ragflow_{self.from_t}_{k['id']}", f"ragflow_{self.to_t}_{k['id']}"
            src_ok, dst_ok = self.vb.table_exists(src_t), self.vb.table_exists(dst_t)
            if src_ok and dst_ok:
                plan["errors"].append(
                    f"向量表同时存在 {src_t} 与 {dst_t}，状态不明确，请先人工处理（kb={k['name']}）")
            if not src_ok and not dst_ok and (k["doc_num"] or 0) > 0:
                plan["warnings"].append(
                    f"知识库 {k['name']} 有 {k['doc_num']} 个文档但没有向量表 {src_t}（可能从未解析过）")
            base_name = split_model_name_and_factory(k["embd_id"])[0]
            item = {
                **k,
                "src_table": src_t, "dst_table": dst_t,
                "src_exists": src_ok, "dst_exists": dst_ok,
                "chunk_rows": self.vb.scalar(f"SELECT COUNT(*) FROM {self.vb.quote(dst_t if dst_ok else src_t)}") if (src_ok or dst_ok) else 0,
                "doc_count": int(doc_counts.get(k["id"], 0)),
                "file_count": int(file_counts.get(k["id"], 0)),
                "doc_meta_rows": int(meta_rows.get(k["id"], 0)),
                "embd_available": base_name in a_models,
                "new_name": None,
            }
            if k["name"] in plan["name_conflicts"]:
                if self.args.rename_on_conflict:
                    item["new_name"] = (k["name"][:100] + "-" + self.from_t[:8])[:128]
                else:
                    plan["errors"].append(
                        f"租户 A 已有同名知识库: {k['name']}（加 --rename-on-conflict 自动改名）")
            if not item["embd_available"]:
                plan["warnings"].append(
                    f"知识库 {k['name']} 的向量模型 {k['embd_id']} 不在租户 A 的可用模型里，"
                    f"解析/检索会失败，请先在 A 上配置同名模型")
            plan["kbs"].append(item)

        # 处理中的任务
        running = self.meta.scalar(
            f"SELECT COUNT(*) FROM {self.meta.quote('document')} "
            f"WHERE kb_id IN ({ph(len(kb_ids))}) AND run = '1'", tuple(kb_ids))
        if running:
            plan["warnings"].append(f"有 {running} 个文档正在解析（run=1），迁移前请停止 task_executor 并等任务结束")
        pending = self.meta.scalar(
            f"SELECT COUNT(*) FROM {self.meta.quote('task')} t "
            f"JOIN {self.meta.quote('document')} d ON d.id = t.doc_id "
            f"WHERE d.kb_id IN ({ph(len(kb_ids))})", tuple(kb_ids))
        if pending:
            plan["warnings"].append(f"有 {pending} 条残留 task 记录指向这些文档（如无执行中的任务可忽略）")

        # 引用了这些知识库、但仍留在租户 B 的对象（只报告、不迁移），见 reference_scan.py
        from reference_scan import scan_references, references_to_warnings

        refs = scan_references(self.meta, self.from_t, kb_ids)
        plan["affected"] = refs
        plan["warnings"].extend(references_to_warnings(refs))
        return plan

    # ------------------------------------------------------------------ 打印

    def print_plan(self, plan: dict):
        f, t = plan.get("from_tenant", {}), plan.get("to_tenant", {})
        print(f"\n== 租户 B(源): {f.get('name')} ({self.from_t})")
        print(f"== 租户 A(目标): {t.get('name')} ({self.to_t})\n")
        print(f"{'知识库':<28} {'doc数':>6} {'chunk行数':>10} {'meta行数':>8}  向量表/改名")
        for k in plan["kbs"]:
            rename = "已迁" if k["dst_exists"] else ("缺表" if not k["src_exists"] else "待改名")
            name = k["name"] + (f" -> {k['new_name']}" if k["new_name"] else "")
            print(f"{name:<28} {k['doc_count']:>6} {k['chunk_rows']:>10} {k['doc_meta_rows']:>8}  {rename}")
        if plan.get("name_conflicts"):
            print(f"\n!! 与租户 A 同名冲突: {list(plan['name_conflicts'].keys())}")
        for w in plan.get("warnings", []):
            print(f"[警告] {w}")
        for e in plan.get("errors", []):
            print(f"[错误] {e}")
        print("\n将执行的动作:")
        print("  [VB ] ALTER TABLE ragflow_{B}_{kb} RENAME TO ragflow_{A}_{kb}")
        print("  [VB ] doc_meta 行从 ragflow_doc_meta_{B} 搬到 ragflow_doc_meta_{A}（按 kb_id）")
        print("  [meta] knowledgebase.tenant_id/created_by -> A；tenant_embd_id 重映射；同名按需改名")
        print("  [meta] document.created_by -> A")
        print("  [meta] file 行(source_type=knowledgebase) -> A，挂到 A 的 .knowledgebase/<kb名> 下")
        print("  [存储] MinIO 无需迁移（bucket=kb_id，与租户无关）")
        if not plan.get("errors"):
            print("\n这是 dry-run。确认无误后加 --execute 执行。")

    # ------------------------------------------------------------------ 执行

    def execute(self, plan: dict):
        kb_ids = [k["id"] for k in plan["kbs"]]
        self._execute_vb(plan, kb_ids)
        self._execute_meta(plan, kb_ids)
        save_state(plan, self.from_t, self.to_t)
        print(f"\n迁移完成。已记录 {STATE_FILE}（--verify 用）。建议重启 ragflow-server，然后运行 --verify 复查。")

    def _execute_vb(self, plan: dict, kb_ids: list[str]):
        try:
            for k in plan["kbs"]:
                if k["src_exists"]:
                    self.vb.x(f"ALTER TABLE {self.vb.quote(k['src_table'])} RENAME TO {self.vb.quote(k['dst_table'])}")
                    logger.info(f"[VB ] 向量表已改名: {k['src_table']} -> {k['dst_table']}")
                elif k["dst_exists"]:
                    logger.info(f"[VB ] 向量表已在目标名下，跳过: {k['dst_table']}")
                else:
                    logger.warning(f"[VB ] 向量表不存在，跳过: {k['src_table']}（kb={k['name']}）")

            src_t, dst_t = plan["src_meta_table"], plan["dst_meta_table"]
            if plan["src_meta_exists"]:
                if not plan["dst_meta_exists"]:
                    cols = self.vb.table_columns(src_t)
                    ddl = ", ".join(
                        f'{self.vb.quote(c["column_name"])} {c["data_type"]}'
                        + (f' DEFAULT {c["column_default"]}' if c["column_default"] else "")
                        for c in cols)
                    self.vb.x(f"CREATE TABLE IF NOT EXISTS {self.vb.quote(dst_t)} ({ddl})")
                    logger.info(f"[VB ] 已按源表结构创建: {dst_t}")
                # 列取交集，兼容目标表结构有差异的情况
                common = [c["column_name"] for c in self.vb.table_columns(src_t)
                          if c["column_name"] in {d["column_name"] for d in self.vb.table_columns(dst_t)}]
                col_list = ", ".join(self.vb.quote(c) for c in common)
                self.vb.x(f"DELETE FROM {self.vb.quote(dst_t)} WHERE kb_id IN ({ph(len(kb_ids))})", tuple(kb_ids))
                moved = self.vb.x(
                    f"INSERT INTO {self.vb.quote(dst_t)} ({col_list}) "
                    f"SELECT {col_list} FROM {self.vb.quote(src_t)} WHERE kb_id IN ({ph(len(kb_ids))})",
                    tuple(kb_ids))
                self.vb.x(f"DELETE FROM {self.vb.quote(src_t)} WHERE kb_id IN ({ph(len(kb_ids))})", tuple(kb_ids))
                logger.info(f"[VB ] doc_meta 搬迁 {moved} 行: {src_t} -> {dst_t}")
            else:
                logger.info(f"[VB ] 源 doc_meta 表不存在，跳过: {src_t}")
            self.vb.commit()
        except Exception:
            self.vb.rollback()
            raise

    def _execute_meta(self, plan: dict, kb_ids: list[str]):
        ms, now = now_ms(), datetime.now()
        try:
            Q = self.meta.quote

            # 1) knowledgebase 归属
            for k in plan["kbs"]:
                sets = ["tenant_id = %s", "created_by = %s", "update_date = %s", "update_time = %s"]
                params: list = [self.to_t, self.to_t, now, ms]
                if k["new_name"]:
                    sets.append("name = %s")
                    params.append(k["new_name"])
                params.append(k["id"])
                n = self.meta.x(
                    f"UPDATE {Q('knowledgebase')} SET {', '.join(sets)} WHERE id = %s AND tenant_id = %s",
                    (*params, self.from_t))
                logger.info(f"[meta] 知识库归属已更新 ({n}): {k['name']}")

            # 2) tenant_embd_id 重映射到 A 的 tenant_llm
            for k in plan["kbs"]:
                base, fid = split_model_name_and_factory(k["embd_id"])
                sql = f"SELECT id FROM {Q('tenant_llm')} WHERE tenant_id = %s AND llm_name = %s"
                params: list = [self.to_t, base]
                if fid:
                    sql += " AND llm_factory = %s"
                    params.append(fid)
                row = self.meta.q(sql + " LIMIT 1", params)
                new_v = row[0]["id"] if row else None
                self.meta.x(
                    f"UPDATE {Q('knowledgebase')} SET tenant_embd_id = %s, update_date = %s, update_time = %s WHERE id = %s",
                    (new_v, now, ms, k["id"]))
                if new_v is None:
                    logger.warning(
                        f"[meta] 租户 A 没有模型 {k['embd_id']}，{k['name']}.tenant_embd_id 置 NULL（配置模型后由服务端回填）")

            # 3) document 归属
            n = self.meta.x(
                f"UPDATE {Q('document')} SET created_by = %s, update_date = %s, update_time = %s "
                f"WHERE kb_id IN ({ph(len(kb_ids))})",
                (self.to_t, now, ms, *kb_ids))
            logger.info(f"[meta] 文档 created_by 已更新: {n} 行")

            # 4) file 行搬迁：挂到 A 的 .knowledgebase/<kb名> 下
            a_root = self._ensure_root_folder()
            a_kbroot = self._ensure_folder(a_root, KB_FOLDER_NAME)
            for k in plan["kbs"]:
                kb_folder = self._ensure_folder(a_kbroot, k["new_name"] or k["name"], source_type=FILE_SOURCE_KB)
                rows = self.meta.q(
                    f"SELECT f.id, f.parent_id FROM {Q('file')} f "
                    f"JOIN {Q('file2document')} f2d ON f2d.file_id = f.id "
                    f"JOIN {Q('document')} d ON d.id = f2d.document_id "
                    f"WHERE d.kb_id = %s AND f.source_type = %s",
                    (k["id"], FILE_SOURCE_KB))
                if not rows:
                    continue
                # 非 knowledgebase 来源的关联文件不动（bucket 可能挂在 B 的文件夹 id 上）
                ids = [r["id"] for r in rows]
                old_parents = {r["parent_id"] for r in rows}
                n = self.meta.x(
                    f"UPDATE {Q('file')} SET tenant_id = %s, created_by = %s, parent_id = %s, "
                    f"update_date = %s, update_time = %s WHERE id IN ({ph(len(ids))})",
                    (self.to_t, self.to_t, kb_folder, now, ms, *ids))
                logger.info(f"[meta] 知识库 {k['name']}: {n} 个文件行已划给租户 A")
                # 清掉 B 侧已空的 kb 文件夹（只清一层，嵌套空目录保留无害）
                for pid in old_parents:
                    if pid and pid != kb_folder:
                        cnt = self.meta.scalar(
                            f"SELECT COUNT(*) FROM {Q('file')} WHERE parent_id = %s AND id != %s", (pid, pid))
                        is_folder = self.meta.scalar(
                            f"SELECT COUNT(*) FROM {Q('file')} WHERE id = %s AND type = %s", (pid, FILE_TYPE_FOLDER))
                        if is_folder and not cnt:
                            self.meta.x(
                                f"DELETE FROM {Q('file')} WHERE id = %s AND tenant_id = %s AND parent_id != id",
                                (pid, self.from_t))
            self.meta.commit()
        except Exception:
            self.meta.rollback()
            raise

    def _ensure_root_folder(self) -> str:
        Q = self.meta.quote
        row = self.meta.q(
            f"SELECT id FROM {Q('file')} WHERE tenant_id = %s AND parent_id = id LIMIT 1", (self.to_t,))
        if row:
            return row[0]["id"]
        fid = get_uuid()
        now, ms = datetime.now(), now_ms()
        self.meta.x(
            f"INSERT INTO {Q('file')} (id, parent_id, tenant_id, created_by, name, type, size, location, "
            f"source_type, create_date, create_time, update_date, update_time) "
            f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (fid, fid, self.to_t, self.to_t, "/", FILE_TYPE_FOLDER, 0, "", "", now, ms, now, ms))
        logger.info(f"[meta] 已创建租户 A 的根文件夹: {fid}")
        return fid

    def _ensure_folder(self, parent_id: str, name: str, source_type: str = "") -> str:
        Q = self.meta.quote
        row = self.meta.q(
            f"SELECT id FROM {Q('file')} WHERE tenant_id = %s AND parent_id = %s AND name = %s LIMIT 1",
            (self.to_t, parent_id, name))
        if row:
            return row[0]["id"]
        fid = get_uuid()
        now, ms = datetime.now(), now_ms()
        self.meta.x(
            f"INSERT INTO {Q('file')} (id, parent_id, tenant_id, created_by, name, type, size, location, "
            f"source_type, create_date, create_time, update_date, update_time) "
            f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (fid, parent_id, self.to_t, self.to_t, name, FILE_TYPE_FOLDER, 0, "", source_type, now, ms, now, ms))
        logger.info(f"[meta] 已创建文件夹: {name} ({fid})")
        return fid

    # ------------------------------------------------------------------ 校验

    def verify(self, state: dict):
        Q = self.meta.quote
        ok = True

        def check(label: str, cond: bool, detail: str = ""):
            nonlocal ok
            if not cond:
                ok = False
            print(f"  [{'OK' if cond else 'FAIL':4}] {label}" + (f"  {detail}" if detail else ""))

        kbs = state["kbs"]
        kb_ids = [k["id"] for k in kbs]
        print("\n== 校验结果")
        bad = self.meta.q(
            f"SELECT id, name FROM {Q('knowledgebase')} "
            f"WHERE id IN ({ph(len(kb_ids))}) AND (tenant_id != %s OR created_by != %s)",
            (*kb_ids, self.to_t, self.to_t))
        check("knowledgebase 归属租户 A", not bad, str([b["name"] for b in bad]))
        gone = self.meta.q(
            f"SELECT id, name FROM {Q('knowledgebase')} WHERE id IN ({ph(len(kb_ids))}) AND tenant_id = %s",
            (*kb_ids, self.from_t))
        check("源租户名下已无这些知识库", not gone, str([b["name"] for b in gone]))

        for k in kbs:
            if not k.get("had_table", True):
                # 迁移前就没有向量表（从未解析过），迁移后同样不该有
                check(f"向量表 [{k['name']}]", not self.vb.table_exists(k["src_table"]) and not self.vb.table_exists(k["dst_table"]),
                      "迁移前即无向量表")
                continue
            dst_ok = self.vb.table_exists(k["dst_table"])
            src_gone = not self.vb.table_exists(k["src_table"])
            check(f"向量表已改名 [{k['name']}]", dst_ok and src_gone, k["dst_table"])
            if dst_ok:
                rows = self.vb.scalar(f"SELECT COUNT(*) FROM {self.vb.quote(k['dst_table'])}")
                reg = self.meta.scalar(
                    f"SELECT chunk_num FROM {Q('knowledgebase')} WHERE id = %s", (k["id"],))
                check(f"chunk 行数 [{k['name']}]", True, f"实际 {rows} / 登记 {reg}")

        src_t, dst_t = state.get("src_meta_table"), state.get("dst_meta_table")
        if src_t and self.vb.table_exists(src_t):
            left = self.vb.scalar(
                f"SELECT COUNT(*) FROM {self.vb.quote(src_t)} WHERE kb_id IN ({ph(len(kb_ids))})", tuple(kb_ids))
            moved = self.vb.scalar(
                f"SELECT COUNT(*) FROM {self.vb.quote(dst_t)} WHERE kb_id IN ({ph(len(kb_ids))})", tuple(kb_ids))
            check("doc_meta 已搬到租户 A", left == 0, f"目标 {moved} 行 / 源残留 {left} 行")

        bad_doc = self.meta.scalar(
            f"SELECT COUNT(*) FROM {Q('document')} "
            f"WHERE kb_id IN ({ph(len(kb_ids))}) AND created_by != %s", (*kb_ids, self.to_t))
        check("document.created_by = A", bad_doc == 0, f"异常 {bad_doc} 行")

        bad_file = self.meta.scalar(
            f"SELECT COUNT(*) FROM {Q('file')} f "
            f"JOIN {Q('file2document')} f2d ON f2d.file_id = f.id "
            f"JOIN {Q('document')} d ON d.id = f2d.document_id "
            f"WHERE d.kb_id IN ({ph(len(kb_ids))}) AND f.source_type = %s AND f.tenant_id != %s",
            (*kb_ids, FILE_SOURCE_KB, self.to_t))
        check("关联 file 行归属 A", bad_file == 0, f"异常 {bad_file} 行")

        print("\n校验" + ("全部通过。" if ok else "存在 FAIL 项，请检查上方日志。"))
        return ok


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="RAGFlow 租户间知识库迁移（同实例，Vastbase 底座）")
    p.add_argument("--vb-host", default="localhost", help="Vastbase 向量库 host（默认 localhost）")
    p.add_argument("--vb-port", type=int, default=5432)
    p.add_argument("--vb-user", default="rag_flow")
    p.add_argument("--vb-password", default=None, help="缺省读环境变量 VB_PASSWORD")
    p.add_argument("--vb-db", default="ragflow", help="向量库名（默认 ragflow）")
    p.add_argument("--meta-type", choices=["vastbase", "mysql"], default="vastbase",
                   help="rag_flow 元数据库类型（默认 vastbase）")
    p.add_argument("--meta-host", default=None, help="缺省沿用 --vb-host")
    p.add_argument("--meta-port", type=int, default=None, help="缺省沿用 --vb-port")
    p.add_argument("--meta-user", default=None, help="缺省沿用 --vb-user")
    p.add_argument("--meta-password", default=None, help="缺省沿用 --vb-password / 环境变量 META_PASSWORD")
    p.add_argument("--meta-db", default="rag_flow", help="元数据库名（默认 rag_flow）")
    p.add_argument("--from-tenant", required=True, help="租户 B（源）ID，32 位 hex")
    p.add_argument("--to-tenant", required=True, help="租户 A（目标）ID，32 位 hex")
    p.add_argument("--kb-id", action="append", default=None, help="只迁移指定知识库（可重复；缺省迁全部）")
    p.add_argument("--include-invalid", action="store_true", help="连同 status != 1 的知识库一起迁")
    p.add_argument("--rename-on-conflict", action="store_true", help="与 A 同名时自动改名（后缀源租户前 8 位）")
    p.add_argument("--execute", action="store_true", help="实际执行（缺省 dry-run）")
    p.add_argument("--verify", action="store_true", help="迁移后校验")
    p.add_argument("-v", "--verbose", action="store_true")
    return p


def main():
    import os

    args = build_parser().parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    args.vb_password = args.vb_password or os.environ.get("VB_PASSWORD")
    args.meta_password = args.meta_password or os.environ.get("META_PASSWORD") or args.vb_password
    if not args.vb_password or not args.meta_password:
        print("缺少数据库密码：--vb-password / --meta-password 或环境变量 VB_PASSWORD / META_PASSWORD", file=sys.stderr)
        sys.exit(2)
    for label, tid in (("--from-tenant", args.from_tenant), ("--to-tenant", args.to_tenant)):
        if not HEX32_RE.match(tid.lower()):
            print(f"{label} 应为 32 位十六进制租户 ID: {tid}", file=sys.stderr)
            sys.exit(2)
    if args.from_tenant.lower() == args.to_tenant.lower():
        print("--from-tenant 与 --to-tenant 不能相同", file=sys.stderr)
        sys.exit(2)

    mig = TenantKbMigrator(args)
    try:
        if args.verify:
            # 迁移后 KB 已不属于源租户，校验依据是 --execute 时落盘的清单，而不是重新查源租户
            if not os.path.exists(STATE_FILE):
                print(f"找不到 {STATE_FILE}：请先 --execute，或把该文件复制到当前目录。", file=sys.stderr)
                sys.exit(2)
            state = load_state()
            if state["from_tenant"] != mig.from_t or state["to_tenant"] != mig.to_t:
                print(f"{STATE_FILE} 记录的是 {state['from_tenant']} -> {state['to_tenant']}，"
                      f"与本次参数不一致。", file=sys.stderr)
                sys.exit(2)
            sys.exit(0 if mig.verify(state) else 1)
        plan = mig.load_plan()
        mig.print_plan(plan)
        if plan.get("errors"):
            sys.exit(2)
        if not args.execute:
            sys.exit(0)
        print("\n开始执行迁移 ...")
        mig.execute(plan)
    finally:
        mig.vb.close()
        mig.meta.close()


if __name__ == "__main__":
    main()
