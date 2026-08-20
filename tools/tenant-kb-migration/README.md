# 租户间知识库迁移工具（同实例 / Vastbase 底座）

把**租户 B** 名下的全部（或指定）知识库移交给**租户 A**，适用于账号合并、组织调整等场景。
底层是 Vastbase 时，RAGFlow 的数据分布如下，迁移就是按下表逐项处理：

| 层 | 租户 B 的数据 | 处理 |
|---|---|---|
| `rag_flow` 元数据库 | `knowledgebase.tenant_id/created_by` | UPDATE → A |
| | `knowledgebase.tenant_embd_id`（指向 B 的 tenant_llm 行） | 重映射到 A 的 tenant_llm，找不到则置 NULL |
| | `document.created_by`（kb_id 不变） | UPDATE → A |
| | `file` 行（source_type='knowledgebase'，经 file2document 关联） | 归属 → A，挂到 A 的 `.knowledgebase/<kb名>` 下 |
| `ragflow` 向量库 | chunk 表 `ragflow_{B}_{kb}` | `ALTER TABLE ... RENAME TO ragflow_{A}_{kb}` |
| | doc_meta 行（`ragflow_doc_meta_{B}` 中该批 kb） | 搬到 `ragflow_doc_meta_{A}` |
| MinIO / 对象存储 | bucket = kb_id（与租户无关） | **无需迁移** |

> kb_id、document.id、chunk id 全部不变，所以对象存储和文档内容零搬动；
> 向量表只是改名（索引随表走，不需要重建）。

## 安装

```bash
cd tools/tenant-kb-migration
pip install -r requirements.txt
```

## 用法

### 0. 前置

1. **停掉 ragflow-server 和 task executor**（迁移窗口内避免对同一批表/行的并发读写）。
2. 确认租户 A 上已配置迁移库所用的向量模型（dry-run 会提示缺失项），否则迁过去后无法解析/检索。
3. 拿到两个租户的 ID（32 位 hex，即 `tenant`/`user` 表主键）。

### 1. dry-run 预览（默认行为，不写任何数据）

```bash
python migrate.py \
    --vb-host vb --vb-port 5432 \
    --vb-user rag_flow --vb-password '***' --vb-db ragflow \
    --meta-type vastbase --meta-db rag_flow \
    --from-tenant <租户B_ID> --to-tenant <租户A_ID>
```

输出：每个库的文档数/chunk 行数/doc_meta 行数、向量表改名计划、以及全部警告
（同名冲突、A 缺向量模型、有文档在解析、B 的助手/Agent/搜索引用了这些库等）。

### 2. 正式迁移

```bash
python migrate.py ... --execute
```

元数据库在 MySQL 时：`--meta-type mysql --meta-host ... --meta-db rag_flow`。

常用参数：

| 参数 | 说明 |
|---|---|
| `--kb-id` | 只迁指定知识库，可重复；缺省迁全部 |
| `--rename-on-conflict` | 与 A 同名时自动改名（后缀源租户前 8 位）；缺省报错退出 |
| `--include-invalid` | 连同 status != 1 的库一起迁（默认跳过） |
| `--execute` | 实际执行；缺省 dry-run |
| `--verify` | 迁移后校验（依据执行时落盘的 `.tenant_kb_migration.json`） |

密码也可用环境变量 `VB_PASSWORD` / `META_PASSWORD` 传入。

### 3. 校验 + 恢复服务

```bash
python migrate.py ... --verify
```

检查：KB/文档/文件归属、向量表已改名、doc_meta 无残留。通过后重启 ragflow-server。

### 单独扫描引用（可选）

库迁走后，**留在租户 B** 的助手（dialog）、Agent（user_canvas）、搜索（search）会失去检索能力。
dry-run 已含此扫描；如需单独运行：

```bash
python reference_scan.py \
    --meta-type vastbase --meta-host vb --meta-user rag_flow --meta-password '***' \
    --meta-db rag_flow --tenant <租户B_ID> --kb-id <kb1> --kb-id <kb2>
```

> 注：user_canvas 表没有 tenant_id 只有 user_id，扫描按属主匹配，B 团队成员自己的 canvas 不在范围内。

## 幂等与断点

- 向量表改名：源表不在即跳过；doc_meta：目标侧先 DELETE 再 INSERT，源侧 DELETE。
- 元数据 UPDATE 均带 `tenant_id = B` / `kb_id IN (...)` 条件，重跑不产生二次影响。
- 中断后直接重跑 `--execute` 即可（两库各在一个事务里，最多退回单个库的步骤边界）。

## 已知边界

- 只迁 `source_type='knowledgebase'` 的 file 行。KB 文档的存储桶取自 `document.kb_id`，
  与 file.parent_id 无关，所以这些行怎么挂都不影响读写；其他来源（file manager 上传等）的
  关联文件行不动——当前代码路径里 KB 文档不会以那种方式关联。
- 文件树是"拍平"迁移：B 侧按上传子目录建的嵌套文件夹不会在 A 侧重建（不影响功能，仅文件树展示）。
- B 名下引用这些库的 dialog/canvas/search **不迁移**（它们是 B 的资产），迁移后无法检索，
  dry-run 会列出来供人工决策。
- 迁移不含 tenant_llm（模型配置）本身：A 需要自行配置同名模型，`tenant_embd_id` 才能重映射上。

## 文件结构

```
migrate.py                CLI + 计划/执行/校验（向量库 + 元数据库）
reference_scan.py         扫描源租户下引用指定知识库的 dialog/canvas/search（可单独运行）
test_migration_logic.py   无数据库自测：SQLite 内存库顶替 Vastbase，跑通 dry-run -> execute -> verify
requirements.txt
README.md
```

自测（不需要任何真实数据库）：

```bash
python test_migration_logic.py
```

