# Vastbase G100 配置指南(doc engine)

本指南说明如何在 Docker Compose 部署下,把 RAGFlow 的**文档引擎**切换为 Vastbase G100。涉及三个文件:

| 文件 | 作用 |
|---|---|
| [`docker/.env`](../.env) | 用户-facing 配置,`VB_*` 变量块 |
| [`docker/docker-compose-base.yml`](../docker-compose-base.yml) | `vastbase` 服务定义(profile 隔离) |
| [`docker/service_conf.yaml.template`](../service_conf.yaml.template) | `vastbase:` 段,由 `VB_*` 变量渲染成 `service_conf.yaml`,Go 引擎(`internal/engine/vastbase`)读取 |

先明确边界:

- Vastbase 在 RAGFlow 中只作为 `DOC_ENGINE`(向量检索 + 全文检索),走 PostgreSQL 线协议,Go 服务用 lib/pq 直连 `5432`。
- **业务元数据库不受影响**,仍是 MySQL(`DB_TYPE=mysql`)。`DB_TYPE` 不支持 `vastbase`,不要设置。
- Vastbase G100 镜像只有 linux-x86_64;Go 部署入口本身也面向 Linux x86-64 主机。

## 1. 前置条件

### 1.1 获取镜像

Compose 默认使用本地 tag:

```
VB_IMAGE=vastbase-g100-single-v1.4-3.0.9.31338-linux-x86_64:20260513112809
```

该 tag 无 registry 前缀,需要先从厂商获取离线镜像并加载:

```bash
docker load -i vastbase-g100-single-v1.4-*.tar
docker images | grep vastbase   # 确认本地 tag 与默认值一致
```

若你的镜像来自私有仓库或 tag 不同,在 `.env` 中显式设置 `VB_IMAGE`(完整地址)。

### 1.2 放置 License

Vastbase G100 需要商业 license。把厂商签发的 license 文件放到 [`docker/vastbase/license/`](./license/) 目录(该目录已提交 `.gitkeep`,license 文件本身**严禁提交**,见 [license/README.md](./license/README.md))。Compose 会把它挂载到容器内 `/home/vastbase/vastbase/lic`。

### 1.3 资源

`vastbase` 容器受 `MEM_LIMIT` 约束(默认约 8G,与其他限容服务共享该值)。生产环境按实际内存调整 `.env` 中的 `MEM_LIMIT`。

## 2. 配置 `.env`

最小改动只需一行——把 `DOC_ENGINE` 的默认值改为 `vastbase`:

```bash
DOC_ENGINE=vastbase
```

`.env` 中 `COMPOSE_PROFILES=${DOC_ENGINE},${DEVICE},metadata-${METADATA_DB_PROFILE}` 会随之拉起 `vastbase`、`cpu`、`metadata-mysql` 三个 profile,即:Vastbase 容器 + MySQL/MinIO/Kvrocks/NATS/ClickHouse 等依赖 + RAGFlow 服务。

其余 `VB_*` 变量按需调整:

| 变量 | 默认值 | 说明 |
|---|---|---|
| `VB_HOST` | `vastbase` | 引擎连接地址(RAGFlow 容器视角)。用外部 Vastbase 时改这里 |
| `VB_PORT` | `5432` | 引擎连接端口。用外部 Vastbase 时改这里 |
| `EXPOSE_VB_PORT` | `5432` | 宿主机发布端口,仅影响宿主机访问,容器内部始终走 `vastbase:5432` |
| `VB_BIND_ADDR` | `127.0.0.1`(注释态) | 发布端口的绑定地址。见 §5 安全 |
| `VB_USERNAME` | `ragflow` | 业务用户,由 init 脚本在首次初始化时创建 |
| `VB_PASSWORD` | `Infini_Rag@123` | 密码,**必须**满足 Vastbase 口令策略:大小写字母 + 数字 + 特殊字符,长度 ≥ 8。生产必改 |
| `VB_VEC_DBNAME` | `ragflow` | 文档引擎使用的数据库 |
| `VB_DBCOMPATIBILITY` | `B` | 兼容模式,`B` 或 `PG`,见 §4。**首次 initdb 时固定**,之后改环境变量无效 |
| `VB_SSL_MODE` | `disable` | lib/pq sslmode,见 §5 TLS |

一份典型配置(改动处加粗注释):

```bash
DOC_ENGINE=vastbase          # 唯一必改项
VB_HOST=vastbase
VB_PORT=5432
EXPOSE_VB_PORT=5432
# VB_BIND_ADDR=127.0.0.1     # 默认仅绑定回环,需要外部访问再打开
VB_USERNAME=ragflow
VB_PASSWORD='Your_Str0ng@Pass'   # 满足口令策略
VB_VEC_DBNAME=ragflow
VB_DBCOMPATIBILITY=B
VB_SSL_MODE=disable          # 容器内网内明文;外部实例见 §5
```

启动:

```bash
cd docker
docker compose -f docker-compose.yml up -d
```

## 3. 首次启动发生了什么

1. **initdb**:镜像入口脚本以 `VB_DBCOMPATIBILITY` 调用 `vb_initdb`,实例级兼容模式就此固定。
2. **init 脚本**(仅数据卷为空的首次初始化执行,遵循 `/docker-entrypoint-initdb.d` 约定):
   - [`01_init.sh`](./init/01_init.sh):创建角色 `VB_USERNAME`、数据库 `VB_VEC_DBNAME`,并授予库/schema/默认表权限;
   - [`02_bcompat.sh`](./init/02_bcompat.sh):仅 B 模式执行,通过 `gs_guc reload` 设置
     `b_format_behavior_compat_options = 'set_keyword_as_colname, show_attalias_as_colname, pg_todate_format'`;PG 模式直接跳过。
3. **Go 引擎连接**:`entrypoint.sh` 用 `VB_*` 渲染出 `service_conf.yaml` 的 `vastbase:` 段;Go 引擎重试连接最长 2 分钟(等待容器完成初始化),随后执行启动校验(见 §4),日志出现
   `Vastbase engine initialized` + `dbcompatibility=B` 即就绪。

> 初始化结果落在 `vastbase_data` 卷。**之后修改 `VB_PASSWORD`/`VB_USERNAME`/`VB_VEC_DBNAME` 不会重跑 init 脚本**——要么手动 `ALTER ROLE`,要么删卷重建(见 §7)。

## 4. B 模式与 PG 模式

`VB_DBCOMPATIBILITY` 决定实例的 SQL 兼容行为,引擎侧对应两套完全不同的 SQL 方言:

| | `B`(默认) | `PG` |
|---|---|---|
| 向量距离算子 | `<+>` | `<=>`(pgvector 风格) |
| 全文索引 | 列级 `ADD INDEX USING "fulltext"` | GIN + `to_tsvector` |
| 行为兼容 GUC | `02_bcompat.sh` 设置 | 不设置 |
| 适合场景 | MySQL 语法习惯、信创迁移 | PostgreSQL/pgvector 生态习惯 |

**启动强校验**:Go 引擎启动时读取实例的 `pg_database.datcompatibility`,与配置的模式不符会**拒绝启动**,报错形如:

```
vastbase: dbcompatibility mismatch: instance reports "B" but the engine is configured for "PG";
set dbcompatibility to the database's mode (VB_DBCOMPATIBILITY in docker deployments) or recreate the database, ...
```

这是因为距离算子和全文索引在两种模式下互不兼容,与其在查询期失败,不如启动即失败。

**切换模式 = 重建数据卷**(元数据库 `mysql_data` 不受影响,只丢文档引擎数据):

```bash
cd docker
docker compose -f docker-compose.yml stop vastbase
docker compose -f docker-compose.yml rm -f vastbase
docker volume rm docker_vastbase_data      # compose 项目名默认为目录名 docker,-p 部署时以实际项目名为准
# 修改 .env 中 VB_DBCOMPATIBILITY 后重新 up -d
```

## 5. 安全

### 端口暴露

默认只绑定宿主机回环地址(`127.0.0.1:5432`):数据库自带默认口令,而 RAGFlow 走容器内网 `ragflow` 访问它,宿主机端口仅用于调试。需要从其他机器访问时,在 `.env` 显式设置 `VB_BIND_ADDR=0.0.0.0`(或指定网卡 IP)——这是一个刻意的开放动作,同时务必修改 `VB_PASSWORD`。

### TLS(`VB_SSL_MODE`)

策略由引擎实现(`internal/engine/vastbase/client.go` 的 `buildDSN`):

- `.env` 显式设置时按设置值传给 lib/pq。当前默认 `disable` 是对**容器私有网络**的明文授信——内置容器不提供 TLS,保持 `disable` 即可。
- 留空时引擎按目标地址决定:回环/unix socket 用 `disable`,**其余 TCP 主机一律 `verify-full`**,不会静默降级为明文。
- 指向外部 Vastbase 且链路不可信时,设置 `require`/`verify-ca`/`verify-full`。注意 `VB_SSL_MODE` 只传 `sslmode` 一个参数;`verify-ca`/`verify-full` 需要 RAGFlow 容器信任对应 CA(系统信任库),或在 `VASTBASE_DSN`(见 §6)里附带 `sslrootcert=...`。

## 6. 进阶:连接池与 DSN 覆盖

以下变量不在 `.env` 的 VB 块中,但同样可以通过 `.env`(`env_file` 会传给 RAGFlow 容器)或容器环境设置:

| 变量 | 默认 | 说明 |
|---|---|---|
| `VB_POOL_SIZE` | `50` | 连接池常驻大小 |
| `VB_MAX_OVERFLOW` | `100` | 允许的溢出连接数(`MaxOpenConns = 两者之和`);连接生命周期固定 30 分钟 |
| `VASTBASE_DSN` | 空 | **完整 lib/pq DSN 覆盖**,设置后忽略 host/port/user/password/dbname/sslmode 全部配置。适合传入 `sslrootcert`、`connect_timeout` 等额外参数;密码会在日志中脱敏 |

另:Go 进程也支持 `RAGFLOW_VASTBASE_HOST` 等带 `RAGFLOW_` 前缀的环境变量覆盖 `service_conf.yaml`(viper AutomaticEnv),优先级高于 YAML。

## 7. 使用外部 Vastbase 实例

不复用内置容器、直连已有 Vastbase 时:

1. `.env` 保留 `DOC_ENGINE=vastbase`(决定 RAGFlow 用哪个引擎),但**手写 `COMPOSE_PROFILES` 去掉 vastbase profile**,避免再起一个内置容器:

   ```bash
   DOC_ENGINE=vastbase
   COMPOSE_PROFILES=cpu,metadata-mysql
   ```

2. 连接信息指向外部实例:

   ```bash
   VB_HOST=10.x.x.x          # 外部实例地址
   VB_PORT=5432
   VB_USERNAME=ragflow       # 需在该实例上预先建好用户和库,并具备建表/建索引权限
   VB_PASSWORD='...'
   VB_VEC_DBNAME=ragflow
   VB_DBCOMPATIBILITY=B      # 必须与该库实际的 datcompatibility 一致,否则启动校验失败
   VB_SSL_MODE=require       # 非授信网络不要用 disable
   ```

3. 外部实例需满足:数据库已创建且归属用户;兼容模式与 `VB_DBCOMPATIBILITY` 一致;`02_bcompat.sh` 中的 B 模式 GUC(若用 B 模式)需 DBA 用 `gs_guc` 等价设置。

## 8. 验证部署

```bash
# 容器与健康检查(compose 用 vsql -c 'SELECT 1' 探活)
docker compose -f docker-compose.yml ps vastbase

# 首次初始化日志(init 脚本输出、license 报错都在这里)
docker compose -f docker-compose.yml logs vastbase

# RAGFlow 侧:等待 "Vastbase engine initialized"、"dbcompatibility=B"
docker logs --tail 100 ragflow-cpu

# 服务健康
curl -f http://localhost/api/v1/system/healthz

# 直连验证(宿主机回环)
docker compose -f docker-compose.yml exec vastbase \
  vsql -U ragflow -d ragflow -c 'SELECT vb_version();'
```

跑仓库自带的集成测试(需要已就绪的实例,`build.sh` 会接好 CGO/原生库):

```bash
RAGFLOW_TEST_VASTBASE_HOST=127.0.0.1 \
RAGFLOW_TEST_VASTBASE_PORT=5432 \
RAGFLOW_TEST_VASTBASE_USER=ragflow \
RAGFLOW_TEST_VASTBASE_PASSWORD='Infini_Rag@123' \
RAGFLOW_TEST_VASTBASE_DBNAME=ragflow \
RAGFLOW_TEST_VASTBASE_DBCOMPATIBILITY=B \
RAGFLOW_TEST_VASTBASE_SSL_MODE=disable \
bash build.sh --test-integration ./internal/engine/vastbase/
```

## 9. 常见问题

| 现象 | 原因与处理 |
|---|---|
| 启动报 `dbcompatibility mismatch` | `VB_DBCOMPATIBILITY` 与实例实际模式不符。改配置对齐,或按 §4 删卷重建 |
| 容器反复重启/初始化失败 | 优先看 license:是否放入 `docker/vastbase/license/`、是否过期。其次看 `VB_PASSWORD` 是否违反口令策略(大小写+数字+特殊字符,≥8 位) |
| 改了 `VB_PASSWORD` 但连不上 | init 脚本只在空卷时执行,旧密码仍生效。`vsql -c "ALTER ROLE ragflow PASSWORD '...';"` 或删卷重建 |
| 宿主机 5432 被占用 | 本机已有 PostgreSQL 时改 `EXPOSE_VB_PORT`;RAGFlow 容器间通信不受影响 |
| 拉不到镜像 | 默认 tag 无 registry 前缀,需 `docker load` 或 `VB_IMAGE` 指向私有仓库(§1.1) |
| 改 `VB_DBCOMPATIBILITY` 无效 | 该值只在首次 initdb 时生效,需按 §4 重建 `vastbase_data` 卷 |
| ARM 机器无法运行 | 镜像仅 x86_64;Go 部署入口也仅支持 Linux x86-64 |
