# Vastbase → Vastbase 向量迁移工具

把 RAGFlow 的向量数据（chunk 向量表 + 文档元数据表）从一个 Vastbase 实例迁移到另一个 Vastbase 实例。

适用场景：换库 / 换服务器 / 集群间搬迁 / 数据同步。两端都是 vastbase 的 `ragflow` 向量库。

## 与 ES→VB 工具的区别

`tools/es-to-vastbase-migration/` 的源端是 Elasticsearch；本工具源端是 Vastbase。从源 VB 表读出的数据**已经是 VB 内部存储格式**，因此**不复用 ES→VB 的 converter**（会双重转换出错），改用 identity 行处理（列交集对齐 + 向量格式保证）。

## 数据组织

| 数据类型 | 表名 | 说明 |
|---|---|---|
| Chunk 向量 | `ragflow_{tenant_id}_{kb_id}` | 每个知识库一张表 |
| 文档元数据 | `ragflow_doc_meta_{tenant_id}` | 每租户一张表，无 kb 后缀 |

`tenant_id` / `kb_id` 均为 32 位十六进制字符串。

## 安装

```bash
cd tools/vb-to-vb-migration
pip install -r requirements.txt
```

## 用法

### 1. 列出源实例的租户

```bash
python migrate.py \
    --src-host vb-src --src-port 5432 \
    --src-user rag_flow --src-password '***' --src-db ragflow \
    --list-tenants
```

### 2. 迁移指定租户、排除若干 dataset（先 dry-run）

```bash
python migrate.py \
    --src-host vb-src --src-port 5432 \
    --src-user rag_flow --src-password '***' --src-db ragflow \
    --dst-host vb-dst --dst-port 5432 \
    --dst-user rag_flow --dst-password '***' --dst-db ragflow \
    --tenant d253f468394111f1b41e53bb8d88db1c \
    --exclude-kb-id <要排除的kb1> \
    --exclude-kb-id <要排除的kb2> \
    --dry-run
```

确认计划无误后去掉 `--dry-run` 正式迁移。

### 3. 正式迁移（带断点续传）

```bash
python migrate.py ... --tenant <t> --exclude-kb-id <kb> --resume
```

### 4. 迁移后校验（源行数 vs 目标行数）

```bash
python migrate.py ... --tenant <t> --exclude-kb-id <kb> --verify
```

## 参数

```
源 Vastbase（读）         目标 Vastbase（写）
  --src-host                --dst-host
  --src-port                --dst-port
  --src-user                --dst-user
  --src-password            --dst-password
  --src-db                  --dst-db

范围
  --tenant TENANT_ID        只迁移该租户（不指定则全部）
  --kb-id KB_ID             只迁移单个 kb（可与 --tenant 叠加）
  --exclude-kb-id KB_ID     排除 dataset（可重复，核心功能）
  --table TABLE_NAME        直接指定单表（绕过 tenant/kb 过滤）
  --no-meta                 跳过 doc_meta 表迁移

执行
  --batch-size N            每批行数（默认 1000）
  --resume                  跳过已完成的表（进度文件 .vb_to_vb_progress.json）
  --dry-run                 仅预览，不写数据
  --verify                  对比源/目标行数
  --no-index                跳过目标向量/全文索引创建

内省（仅需 --src-*）
  --list-tenants            列出源租户及表数
  --list-tables             列出源表及行数
  -v / --verbose            详细日志
```

## 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `VB_DBCOMPATIBILITY` | `B` | 目标库兼容模式 `PG`/`B`，决定全文索引语法 |
| `VB_STATEMENT_TIMEOUT` | `10min` | 建索引的安全超时 |
| `VB_INDEX_RETRIES` | `3` | 建索引重试次数 |

## 工作机制

1. **源端枚举**：从 `information_schema.tables` 查 `ragflow_*` 表，用正则解析 tenant/kb（不依赖 MySQL 元数据库）。
2. **批量读取**：服务端命名游标（`cursor(name=...)` + `fetchmany`）分批拉取，大表不占满内存。
3. **向量读取**：`q_*_vec` 列用 `::text` cast 成 `"[v1,v2,...]"` 字符串；非向量列（含 `integer[]`）保持原生类型不 cast。
4. **列交集**：只写源/目标都有的列，避免引用目标表不存在的列。
5. **幂等写入**：`insert_batch` 先 `DELETE ... WHERE id IN (...)` 再批量 `INSERT`，重跑安全覆盖。
6. **索引后建**：数据全部写入后再建 graph_index 向量索引 + 全文索引，避免逐行索引维护开销。
7. **断点续传**：进度记录在 `.vb_to_vb_progress.json`，`--resume` 跳过已完成表。

## 文件结构

```
migrate.py     CLI + 编排（仿 es-to-vastbase-migration/migrate.py）
vb_reader.py   VBChunkReader：源端服务端游标读取 + 表名解析
vb_writer.py   VBWriter：目标端建表/索引/批量插入（复制自 es-to-vastbase，纯 psycopg2）
identity.py    列交集 + 向量格式化（替代 converter）
requirements.txt
README.md
```

## 端到端验证步骤

1. `--list-tenants` / `--list-tables`：确认租户/kb 解析、chunk 与 doc_meta 正确分离。
2. `--tenant <t> --dry-run`：确认计划表/维度/行数正确，不写数据。
3. `--tenant <t> --kb-id <small_kb>`：小批量实迁，确认 "Indexes created"。
4. `--verify`：所有表应 `match`（源行数 == 目标行数）。
5. 中途 Ctrl-C 后 `--resume`：已完成表跳过，部分表幂等完成。
6. 确认 `ragflow_doc_meta_<tenant>` 以 dim=0 迁移、不建向量索引。
