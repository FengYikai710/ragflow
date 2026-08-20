#!/usr/bin/env python3
"""
扫描「引用了指定知识库、但仍留在源租户」的对象。

库迁走后，源租户 B 名下引用这些 kb_id 的对象会失去检索能力。本模块只报告、不改动：
  - dialog        助手（kb_ids JSON 列表）
  - user_canvas   Agent（kb_id 藏在 dsl JSON 里；表没有 tenant_id，只有 user_id，
                  按属主匹配，B 团队成员自己的 canvas 不在扫描范围）
  - search        搜索（kb_ids 藏在 search_config JSON 里）

判断方式统一为子串匹配 JSON 文本：kb_id 是 32 位 hex，误报率可忽略。

可被 migrate.py 引用（scan_references），也可单独运行：

    python reference_scan.py \\
        --meta-type vastbase --meta-host vb --meta-user rag_flow --meta-password '***' \\
        --meta-db rag_flow --tenant <租户B_ID> --kb-id <kb1> --kb-id <kb2>
"""

import argparse
import os
import sys

# (表名, 属主列, 显示名列, 含 kb_id 的 JSON 列)
SCAN_TARGETS = [
    ("dialog", "tenant_id", "name", "kb_ids"),
    ("user_canvas", "user_id", "title", "dsl"),
    ("search", "tenant_id", "name", "search_config"),
]


def scan_references(meta_db, from_tenant: str, kb_ids: list[str]) -> dict[str, list[dict]]:
    """在元数据库里扫描引用了 kb_ids 且属于源租户的对象。

    meta_db: 需提供 q(sql, params) / table_exists(table) / quote(ident)，
             与 migrate.py 里的 Db 封装一致。
    返回 {表名: [{id, label}, ...]}。
    """
    result: dict[str, list[dict]] = {}
    for table, owner_col, label_col, blob_col in SCAN_TARGETS:
        result[table] = []
        if not meta_db.table_exists(table):
            continue
        rows = meta_db.q(
            f"SELECT id, {meta_db.quote(label_col)} AS label, {meta_db.quote(blob_col)} AS raw "
            f"FROM {meta_db.quote(table)} WHERE {meta_db.quote(owner_col)} = %s",
            (from_tenant,),
        )
        for r in rows:
            raw = str(r.pop("raw") or "")
            if any(k in raw for k in kb_ids):
                result[table].append(r)
    return result


def references_to_warnings(refs: dict[str, list[dict]]) -> list[str]:
    """把扫描结果转成给人看的警告行。"""
    what = {"dialog": "助手", "user_canvas": "Agent", "search": "搜索"}
    warnings = []
    for table, rows in refs.items():
        for r in rows:
            warnings.append(
                f"租户的{what.get(table, table)} {r.get('label')} ({r['id']}) 引用了被迁走的知识库，之后将无法检索")
    return warnings


def main():
    p = argparse.ArgumentParser(description="扫描源租户下引用指定知识库的 dialog / canvas / search")
    p.add_argument("--meta-type", choices=["vastbase", "mysql"], default="vastbase")
    p.add_argument("--meta-host", default="localhost")
    p.add_argument("--meta-port", type=int, default=5432)
    p.add_argument("--meta-user", default="rag_flow")
    p.add_argument("--meta-password", default=None, help="缺省读环境变量 META_PASSWORD")
    p.add_argument("--meta-db", default="rag_flow")
    p.add_argument("--tenant", required=True, help="源租户 ID（32 位 hex）")
    p.add_argument("--kb-id", action="append", required=True, help="知识库 ID（可重复）")
    args = p.parse_args()
    args.meta_password = args.meta_password or os.environ.get("META_PASSWORD")
    if not args.meta_password:
        print("缺少数据库密码：--meta-password 或环境变量 META_PASSWORD", file=sys.stderr)
        sys.exit(2)

    from migrate import Db  # 复用同一个连接封装

    meta = Db("pg" if args.meta_type == "vastbase" else "mysql",
              args.meta_host, args.meta_port, args.meta_user, args.meta_password, args.meta_db)
    try:
        refs = scan_references(meta, args.tenant.lower(), [k.lower() for k in args.kb_id])
        for table, rows in refs.items():
            for r in rows:
                print(f"[{table}] {r.get('label')} ({r['id']})")
        if not any(refs.values()):
            print("没有对象引用这些知识库。")
    finally:
        meta.close()


if __name__ == "__main__":
    main()
