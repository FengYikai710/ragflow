# Vastbase 提交 RAGFlow 社区：完整步骤

> 本文档为内部工作文档，存放在 `.claude/` 下（已在"永不提交"清单中），不会进入 PR。
> 分支：`vastbase-doc-engine`（当前 0 commit，改动散在暂存区/工作区/未跟踪目录）。

## 全景

```
阶段一 社区预热        开 issue 征求意见（文案见附录 A）
阶段二 代码准备   步骤 0-4：备份 → 裁剪 → 修问题 → rebase → 本地真库验证
阶段三 测试补齐   步骤 5：mock 单测（不需要任何镜像）
阶段四 正式提交   步骤 6：整理 commit → fork → PR（closes issue）
阶段五 合入维护   步骤 7：CI 绿 + 响应 review → 后续 PR 系列
```

核心叙事（issue 已定稿）：

- Vastbase 用户要用**最新版 RAGFlow + 官方社区镜像**部署；
- `DOC_ENGINE=vastbase` + `DB_TYPE=vastbase` 两个环境变量切换；
- 整层数据（chunk / 向量 / 消息 / 元数据）都在 Vastbase 里；
- 分两步走：先 doc engine 替换 ES，再元数据库替换 MySQL；
- 零新增 Python 依赖（复用 psycopg2），官方镜像无需重新构建。

表述纪律：**不提 OceanBase，不提信创**，全部中性技术表述。

---

## 阶段一：开 issue 预热（先于一切代码动作）

- 英文 issue，标题 `[Feature] Proposal: add Vastbase G100 as document engine and metadata database`；
- 内容：动机（最新版 + 官方镜像 + 数据面合并）、两步 scope、mock 测试策略、license 约束坦诚说明、三个问题征询 maintainers（完整文案见附录 A）；
- **为什么先开 issue**：给社区一个讨论"要不要"的入口，避免直接甩 2k 行 diff；后续 PR 描述里写 `Closes #N`。

---

## 步骤 0：钉住现状（5 分钟，不可跳过）

当前分支 **0 个 commit**，改动散在暂存区/工作区/未跟踪目录。先本地提交一个 WIP commit，或 `git branch backup/vastbase` 存档。

- **风险**：不备份就裁剪/rebase 容易丢工作；但这个 WIP **绝不能推到 fork**——含 `Vastdata@123` 等真实口令，推上公开仓库就进了 git 历史，事后必须 force-push 重写。

## 步骤 1：裁剪 PR 边界

| 处置 | 内容 |
|---|---|
| **保留** | `rag/utils/vastbase_conn.py`、`memory/utils/vastbase_conn.py`、两个 mapping JSON（`conf/vastbase_mapping.json`、`conf/doc_meta_vastbase_mapping.json`）、`common/settings.py` 接线、`api/db/db_models.py` 的 Vastbase 枚举、opt-in compose 服务与配置模板 |
| **拆后续 PR** | 三个 `tools/*-migration/`（去密码、英文化后单独提）、元数据库 peewee 支持（Step 2 PR）、`rag/svr/task_executor.py` 的 `chunk_order_int`（若是通用修复单独提） |
| **永不提交** | `.claude/`、`docker/prompts/`、`docker/migration_vb.sh`、`docker/vastbase/init/` 外的重复脚本（`01_init.sh`/`02_bcompat.sh` 两层字节级相同，只留 `init/`）、compose 的 MCP command / 宿主机 bind-mount 调试改动 |

- **风险**：`git add -A` 一把梭把 `.claude/` 和带密码的 tools 带进去——最常见翻车点。用显式路径添加，提完 `git show --stat HEAD` 复核文件清单。

## 步骤 2：修 P0/P1 问题

**还原默认值（P0，reviewer 第一眼就看）：**

- `docker/.env`：`DOC_ENGINE` 默认还原 `elasticsearch`、去掉 `DB_TYPE:-vastbase` 默认；
- `docker/docker-compose.yml`：`depends_on` 还原 `mysql`（vastbase 只在 profile 下启动，其他引擎用户 compose 直接起不来）；
- `Dockerfile`：`NEED_MIRROR` 还原 `0`。

**去凭据（P0）：**

- `conf/service_conf.yaml` 的 `Vastdata@123` 改占位符；
- 统一三处不一致的默认密码（`conf`、`rag/utils`、`memory/utils`）。

**镜像变量化（P0）：**

- compose 默认镜像 tag（商业离线包，无人能 pull）改为 `${VASTBASE_IMAGE:-...}` 变量；
- `docker/README.md` 写清"需自备镜像 + license"。

**收窄共享代码改动（P1）：**

- `api/db/db_models.py` `JSONField.python_value` 吞异常 → 还原或仅 vastbase 降级；
- `RetryingPooledPostgresqlDatabase.begin()` 与 `alter_db_add_column` 的宽 catch → 收窄；
- `rag/graphrag/search.py` 社区排序改动（去掉 DB 端 ORDER BY、无序 LIMIT 后 Python 排序，会截断候选集）→ 用 `settings.DOC_ENGINE_VASTBASE` 门控或还原；
- `rag/nlp/search.py` 的 psycopg2 字符串 list 解析 → 下沉到 `vastbase_conn.get_fields`；
- `RetryingPooledVastbaseBDatabase`（约 60 行复制粘贴）→ 改为继承复用（这也是 issue 里的承诺）。

**清死代码（P1）：**

- `rag/utils/vastbase_conn.py` 约 120 行注释调试代码及其孤儿 helper、INFO 级搜索日志、`logger.setLevel(INFO)`、未用 import、license 头缺词（"governing"）；
- `memory/utils/vastbase_conn.py` 的 f-string SQL 改 `psycopg2.sql` 组合。

- **风险**：每条未修的都对应明确拒绝理由；门控别写反、收窄别改坏自己功能——改完必须本地真库回归（步骤 4）。

## 步骤 3：rebase 到最新 main

```bash
git fetch origin && git rebase main
```

已知冲突点（当前 HEAD 落后 main 约 4 个月）：

- `rag/nlp/search.py`：main 改了 `get_vector` 签名（`top_k`/`num_candidates`）；
- `rag/graphrag/search.py`：main 已异步化，暂存的 hunk 直接丢弃用 main 版；
- `docker/docker-compose-base.yml`：main 有新改动。

- **风险**：不 rebase 必拒；解冲突时误吞 main 新逻辑——逐文件过，完了跑测试。

## 步骤 4：本地真库回归（CI 替代不了）

- `uv run --with pytest pytest test/unit_test/...` + `uvx ruff@0.11.6 check` / `format`（venv 没装 pytest/ruff，用 uv 直接带）；
- 走全链路：上传 → 解析 → 检索 → graphrag。**真库实例不要求是本地容器/镜像**——任何能连上的 vastbase 都行：开发调试用的实例、公司内网测试服务器、或本地容器，把 `docker/.env` / `conf/service_conf.yaml` 指过去即可；
- 若确需本地起容器又拿不到官方离线镜像：Docker Hub 第三方镜像（`thankwhite/vastbase_g100` 等）**只够开发冒烟**——版本与目标商业版可能不一致，B/PG 模式、`@~@` 语法行为未必相同，PR 里的验证结论以真实目标版本实例为准。

- **风险**：CI 只有 mock，SQL 兼容性（B/PG 模式、`@~@` 全文语法）在 CI 完全暴露不出——这是唯一能发现真问题的地方，也是 reviewer 问"怎么验证"时要出示的记录。

## 步骤 5：补 mock 单测（不需要任何镜像）

新建 `test/unit_test/rag/utils/test_vastbase_conn.py`，两种写法：

```python
# ① 纯函数直测：把 mapping 加载、过滤表达式拼接、值转换抽成模块级函数
from rag.utils.vastbase_conn import <纯函数>

def test_xxx():
    assert ...

# ② mock 连接：SQL 构造完就断在逻辑分支，从不真连库
from unittest.mock import Mock, patch

@patch('rag.utils.vastbase_conn.VBConnection')
def test_yyy(mock_conn):
    mock_conn.return_value.health.return_value = {"status": "healthy"}
    ...
```

- 无 Docker、无镜像、无 skipif；唯一前提是 psycopg2 装得上（RAGFlow 既有依赖，零新增）；
- 参考上游同类单测的既有风格（纯函数 + `@patch` + `@patch.dict(os.environ, ...)`）。

- **风险**：极低；唯一禁忌是写需要活 vastbase 的测试——CI 没镜像必挂。

## 步骤 6：整理提交物并开 PR

- squash 成少量逻辑 commit，英文提交信息；
- fork 后推分支，PR 提到 `infiniflow/ragflow` 的 **main**，描述里 `Closes #<issue编号>`；
- 按 PR 模板填：What problem（动机）+ Type of change = New Feature；
- 可选加分项：`workflow_dispatch` 手动触发的 workflow 跑真库集成测试。

- **风险**：
  - 体量 ~2.3k 行——靠边界干净 + issue 预热对冲；
  - **无公开镜像 + 商业 license**——最大结构性风险；预案：opt-in + README 说明 + 自己的验证记录；镜像差距在**部署文档层**消化，不影响 CI、不影响单测、不构成合入障碍；
  - Go 引擎路径（main 已有 `internal/engine/`）——答"后续 PR"；
  - issue 里承诺了"零新增依赖"——若最终加了驱动，issue 与 PR 表述要同步改。

## 步骤 7：合入前维护

- CI 必须绿：ruff + 在 ES/Infinity 上跑的 testcase（共享代码改动不能影响这两个引擎）；
- 2-3 轮 review 修改是常态。

- **风险**：**响应慢**。main 每天在动，拖两三周就要二次 rebase；长期无响应的 PR 会被 close。

---

## 后续 PR 系列（首发 PR 合入后）

1. **元数据库 peewee 支持**（`DB_TYPE=vastbase`）——兑现 issue 承诺：复用 PostgreSQL pooled/retry 类，不复制粘贴；
2. **ES→Vastbase 迁移工具**——去密码、README/注释英文化后单独提；
3. 视社区需要：Go 引擎路径。

---

## 关键事实速查

| 问题 | 答案 |
|---|---|
| CI 需要 vastbase 镜像吗 | **不需要**。CI 只在 ES/Infinity 上跑 testcase；新引擎单测全是纯函数 + mock，零镜像、零服务、零 skipif |
| 前提条件 | import 链上 psycopg2 装得上（既有依赖）；ruff 过；testcase 默认路径不依赖 vastbase 活服务 |
| Vastbase 有公开官方镜像吗 | 没有。官方渠道：联系海量数据 / 离线包；Docker Hub 仅有第三方镜像（`thankwhite/vastbase_g100` 等） |
| 上游接收过类似的新引擎吗 | 有（多种引擎、多次先例），模式固定：连接器 + settings 接线 + opt-in compose + 配置模板 + mock 单测；真库验证用 `workflow_dispatch` 手动 workflow |
| 最大的坎 | P0 默认值/凭据/不可拉取镜像 + 共享代码越权改动 + rebase |

**风险密度最高的三步**：步骤 1（误提交密钥，不可逆）、步骤 2（P0 默认值与共享代码）、步骤 4（唯一能发现真兼容性问题的地方）。

---

## 附录 A：Issue 文案（定稿）

**Title:**

```
[Feature] Proposal: add Vastbase G100 as document engine and metadata database
```

**Body:**

````markdown
## Summary

We would like to contribute support for **Vastbase G100** as an alternative storage
backend for RAGFlow, in two steps: first as a `DOC_ENGINE` backend (chunk store +
message store, replacing Elasticsearch), then as the metadata/business database
(replacing MySQL). The goal is that Vastbase users can run the latest RAGFlow releases
with the official community image, with the entire data layer on Vastbase. Before
opening the PR (~2k lines), we'd like to check whether the community is interested in
accepting and maintaining this backend.

## What is Vastbase

[Vastbase G100](https://docs.vastdata.com.cn) is a commercial, PostgreSQL-compatible
database from Vastdata, widely deployed in enterprise environments. Capabilities
relevant to RAGFlow:

- PostgreSQL wire protocol and SQL dialect (B / PG compatibility modes)
- `floatvector` type with vector similarity search
- ParadeDB-style full-text search (`@~@` operator, `bm25_score()`), so BM25 + vector
  hybrid scoring can run inside the database
- Graph index support (relevant to GraphRAG)
- Full transactional OLTP, suitable for the metadata/business layer as well

## Motivation

A growing number of Vastbase users want RAGFlow with Vastbase as the document
engine — typically organizations that have standardized on Vastbase as their database
platform. What they expect is the standard community experience, not a patched
distribution:

- Run the **latest RAGFlow release**, not a fork frozen at an old version;
- Deploy with the **official `infiniflow/ragflow` image** — set `DOC_ENGINE=vastbase`
  and `DB_TYPE=vastbase`, point it at their Vastbase server, done;
- Put the **entire data layer on one database**: chunks, vectors, messages and
  metadata — one system to deploy, back up, monitor and secure, instead of operating
  Elasticsearch and MySQL alongside RAGFlow;
- Have engine compatibility maintained **inside the community release cycle**, so each
  RAGFlow upgrade keeps working instead of silently breaking the backend.

Today the only way to offer that is a private fork: constant rebasing, custom image
builds and distribution, and drift behind upstream — bad for users, and it forfeits
community testing of the shared retrieval code paths. Upstreaming the connector makes
"RAGFlow + Vastbase" just another supported combination of the official image:
no fork, no custom build, two environment variables.

## Proposed scope

**Step 1 — document engine (initial PR, self-contained):**

- `rag/utils/vastbase_conn.py` — `VBConnection(DocStoreConnection)` for the chunk store
- `memory/utils/vastbase_conn.py` — message store
- Registration in `common/settings.py` behind `DOC_ENGINE=vastbase`
- Opt-in compose service under `profiles: [vastbase]` (activated via the existing
  `COMPOSE_PROFILES=${DOC_ENGINE}` mechanism — zero impact on default deployments)
- Config templates and a section in `docker/README.md`

**Step 2 — metadata/business database (follow-up PR):**

- A `DB_TYPE=vastbase` peewee backend reusing the existing PostgreSQL pooled/retrying
  database classes, replacing MySQL as the metadata store
- Corresponding health/status reporting

Both steps use psycopg2, already a dependency — **no new Python dependencies**, which
also means the existing official image works with Vastbase as-is, with no build
changes. An ES→Vastbase migration tool is planned as a further follow-up.

## Testing & CI

CI does **not** need a Vastbase image: unit tests are pure-function and mock-based
(no live instance, no skip markers), and we'll make sure none of the existing test
paths change behavior. Real-database verification results (doc engine and metadata
schema) will be documented in the PRs, and we can add a `workflow_dispatch`-only
workflow for integration tests against a self-provided image, so reviewers can
reproduce them on demand.

## Known constraint: image & license

Vastbase G100 has no official public Docker Hub image — the image is obtained from
Vastdata (vendor contact / offline package) and requires a commercial license. Note
this only concerns the **database side**: the RAGFlow side remains the standard
community image. The compose service would therefore be strictly opt-in, reference the
image via a `${VASTBASE_IMAGE}` variable, and the docs would explain how to supply it.
Community members without a license won't be able to spin up a full Vastbase
deployment as easily as with engines that ship public community images — we want to
be upfront about that and hear your thoughts.

## Questions for maintainers

1. Is a Vastbase backend something you're willing to accept and maintain upstream?
2. Is the two-step sequencing (doc engine first, metadata DB second) acceptable, or
   would you rather review them together / differently scoped?
3. Any concerns about the non-public database image / licensing situation that we
   should address in the PR?

We are committed to maintaining this integration long-term, including follow-ups
(migration tooling and a Go engine path if desired). Happy to adjust the design based
on feedback before submitting. Thanks!
````
