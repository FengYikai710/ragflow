# Vastbase → Vastbase 迁移工具

把 RAGFlow 的数据从一个 Vastbase 实例迁移到另一个 Vastbase 实例，覆盖**两个库**：

| 库 | 内容 | 模式 |
|---|---|---|
| `ragflow` | 向量数据（chunk 向量表 + 文档元数据表） | `--tenant` / `--kb-id` 按租户/知识库 |
| `rag_flow` | 业务元数据（用户/知识库/文档/对话/LLM/Canvas…，~30 表） | `--migrate-meta` 整库 |

适用场景：换库 / 换服务器 / 集群间搬迁 / 数据同步。

**完整迁移需要两步**：先迁 `rag_flow` 元数据（`--migrate-meta`），再迁 `ragflow` 向量（`--tenant`）。目标实例须已部署 RAGFlow 初始化好两边的表结构。

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

## 元数据库迁移（rag_flow）

迁移 RAGFlow 的业务元数据（知识库 / 文档 / 用户 / 对话 / LLM / Canvas…），整库复制到目标 `rag_flow`。

### 设计要点

- **整库复制，不按租户**：~30 张表外键级联（user→knowledgebase→document→dialog→…），按租户切片极易破坏完整性。整库搬，迁移后如需按租户裁剪再删多余数据即可。
- **目标表需已存在**：表结构由 peewee 在 RAGFlow 初始化时创建。源端有但目标端缺的表会跳过并提示（先在目标部署 RAGFlow）。
- **幂等 upsert**：`INSERT ... ON CONFLICT (pk) DO UPDATE`（B 模式原生支持）。新空库 / 已有数据都安全，重跑不冲突。
- **外键拓扑排序**：按 `pg_constraint` 查出的父子依赖排序，父表先插（禁用外键的方案被弃用，避免依赖 superuser 权限）；查不到外键信息则回退表名序。
- **断点续传**：进度记录在 `.vb_to_vb_meta_progress.json`，`--resume` 跳过已完成表。无主键的表用 `TRUNCATE + INSERT`（不支持表内续传）。

### 用法

```bash
# 1) 预览源端 rag_flow 的表 + 行数 + 外键顺序（只需源连接）
python migrate.py \
    --src-host vb-src --src-user rag_flow --src-password '***' \
    --src-meta-db rag_flow \
    --list-meta-tables

# 2) dry-run：列出将迁移的表，不写
python migrate.py \
    --src-host vb-src --src-user rag_flow --src-password '***' --src-meta-db rag_flow \
    --dst-host vb-dst --dst-user rag_flow --dst-password '***' --dst-meta-db rag_flow \
    --migrate-meta --dry-run

# 3) 正式迁移（带断点续传）
python migrate.py ... --migrate-meta --resume

# 4) 干净镜像：迁移前先清空目标表（反外键序 TRUNCATE）
python migrate.py ... --migrate-meta --clear-meta

# 5) 校验
python migrate.py ... --migrate-meta --verify

# 可选：只迁/排除某些表（逗号分隔）
python migrate.py ... --migrate-meta \
    --meta-include-tables user,tenant,knowledgebase,document
```

> 注：`--migrate-meta` 模式下忽略 `--tenant/--kb-id/--exclude-kb-id`（整库）。

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

元数据库 rag_flow（整库迁移）
  --migrate-meta            迁移 rag_flow 元数据库（整库，忽略 tenant/kb）
  --src-meta-db NAME        源元数据库名（默认 rag_flow）
  --dst-meta-db NAME        目标元数据库名（默认 rag_flow）
  --clear-meta              迁移前清空目标表（干净镜像）
  --meta-include-tables A,B 只迁这些表（逗号分隔）
  --meta-exclude-tables A,B 排除这些表（逗号分隔）
  --list-meta-tables        列出 rag_flow 表 + 行数 + 外键顺序（仅需源）
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
migrate.py      CLI + 编排（向量迁移 + --migrate-meta 元数据入口）
vb_reader.py    VBChunkReader：源端服务端游标读取 + 表名解析 + PK/FK 内省
vb_writer.py    VBWriter：目标端建表/索引/批量插入 + upsert_batch（纯 psycopg2）
identity.py     列交集 + 向量格式化（替代 converter）
meta_migrator.py  MetaMigrator：rag_flow 整库复制（FK 拓扑 + upsert + 进度）
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

### 元数据库（rag_flow）

7. `--list-meta-tables`：确认表清单、行数、外键拓扑顺序合理（父表在前）。
8. `--migrate-meta --dry-run`：预览将迁的表与行数，不写。
9. `--migrate-meta`：实迁，确认无外键违反（拓扑序插入）、无 PK 冲突。
10. 重跑 `--migrate-meta`：行数不变（upsert 幂等）。
11. `--migrate-meta --verify`：所有表 `match`。
12. 联调：`--migrate-meta`（元数据）+ `--tenant <t>`（向量）后，目标实例可正常登录、看到知识库与文档。
