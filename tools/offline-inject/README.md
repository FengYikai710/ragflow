# RAGFlow 离线文档注入工具（pack / apply）

把一个交付目录打成离线包，在目标环境直接写入 MinIO + 元数据库（`rag_flow`），
**不触发解析**——文档不建 `ingestion_task` 行（派生状态 UNSTART），后续在 UI 里点解析，或用
同目录的 `parse_docs.py` 自动触发（见下），或直接调
`POST /api/v1/datasets/<kb_id>/documents/parse`（body `{"document_ids":[...]}`）。

复刻的是上传 API 的写入路径（`internal/service/document/document_upload.go` 的
`UploadLocalDocuments`），只是去掉 HTTP 与 ingestion 任务。

## 用法

```bash
# 1a. 打包——单库模式：整个 --src-dir 进一个 KB
python inject.py pack --src-dir ./交付文件 --out ./init_pkg

# 1b. 打包——按目录分库：--src-dir 下每个一级子目录 = 一个知识库（以目录名命名），
#     只收指定扩展名（如 md），可只挑部分目录先做实验；
#     --template-kb 把克隆模板 KB id 记进 manifest，apply 免传；
#     加 --src-meta-password 时顺带从源元数据库读模板 KB 配置烤进 manifest
#     （template_kb_config），目标缺模板时 apply 会自行导入建出
python inject.py pack --src-dir ~/Repository/dev420/001export --out ./init_pkg \
    --by-dir --ext md --only VDS_v3.9.2 --template-kb <kb_id> \
    --src-meta-host <src-db> --src-meta-password '***' \
    --tenant-email admin@ragflow.io
#     --tenant-email 把交付绑定到该邮箱用户的租户：记进 manifest（tenant_email），
#     apply 在目标库解析邮箱 -> 租户，新建 KB 一律落在这个租户下（同名查找也
#     限定在该租户内）；apply --tenant-email 可覆盖

# 2a. 注入单库包：目标库已建好 KB，--kb-id 指定
python inject.py apply --pkg ./init_pkg --kb-id <kb_id> \
    --dst-host vb-dst --dst-user ragflow --dst-password '***' \
    --minio-host minio:9000 --minio-user rag_flow --minio-password '***' \
    [--minio-bucket rag_flow] [--dry-run] [--resume]

# 2b. 注入 --by-dir 包：KB 按 (tenant, name) 复用；不存在则从模板 KB 克隆一个
#     （embd/parser/tenant 配置来源）。模板优先级：apply --template-kb >
#     pack 时记进 manifest 的 template_kb；目标缺模板行时，若包里烤了
#     template_kb_config（pack --src-meta-*）则先导入建出该模板 KB 再克隆
python inject.py apply --pkg ./init_pkg \
    --dst-host vb-dst --dst-user ragflow --dst-password '***' \
    --minio-host minio:9000 --minio-user rag_flow --minio-password '***'

# MinIO 参数也可以从 service_conf.yaml 读（不要传未替换变量的模板）：
python inject.py apply --pkg ./init_pkg --kb-id <kb_id> \
    --dst-host vb-dst --dst-password '***' \
    --service-conf /path/to/service_conf.yaml
```

## 包结构

```
init_pkg/
├── manifest.json     # 文件清单：name / parent_path / type / size / xxh128
└── objects/          # 文件本体，相对路径 = <kb 目录>/<文档名>
```

manifest 最后原子写入：目录里没有 manifest.json 就是不完整的包，apply 会拒绝。

## 文档命名

**文档名 = 原始相对目录路径 + 文件名**（中文原样保留），例如
`版本发布说明/VDS V3.9.md`。`name` 与 `location`（MinIO 对象键）相同；
MinIO 对象键是 UTF-8，中文路径段原样往返。这是对上传 API 的刻意偏离——
上传 API 的 `sanitize_path` 只保留 ASCII，中文目录名会折叠。

路径+文件名在文件系统内天然唯一，折叠时代的同名撞车（`安装部署.md` vs
`安装部署/安装部署.md`）不复存在。

## 行为细节

- **对象键布局**与 `rag/utils/minio_conn.py` 完全一致：配了默认 bucket 时
  物理键为 `<bucket>/[<prefix_path>/]<kb_id>/<location>`；没配则每个 KB 一个
  bucket（键 = `<location>`），bucket 按需创建。
- **幂等**：按 content_hash（xxh128）判重——同内容已在目标 KB（无论叫什么名）
  就跳过，缺 file/file2document 行会补齐；重复 apply 安全。
- **吞吐**：`--jobs N`（默认 8）并发做哈希查重与 MinIO 上传，行写入按
  `--flush-every`（默认 500）个文件合并成**一个事务**（3 个批量 INSERT、
  每条 200 行 + 1 次 COMMIT）——逐文件提交时每次 COMMIT 都是一次 WAL
  fsync，是此前的瓶颈。查重不走数据库：每个 KB 开始时一次预加载
  `KBIndex`（现有文档的 hash/name/location/关联关系全在内存），
  worker 全程零 DB 往返，包内同内容文件也能互相判重。flush 失败丢一整批
  （重跑自动补），批越大波及面越大。
- **doc_num 对账**：apply 末尾把 `knowledgebase.doc_num` 重算为实际 document
  数（直插 SQL 绕过了上传 API 的计数器维护）。
- **不写**：task 行、Redis 消息、thumbnail（UI 显示占位图，不影响解析）。
- 名字超过 255 字符（document.name/location 列宽）的文件会在 pack 阶段报错剔除。

## 前置条件

- 目标元数据库表已存在（目标部署过 RAGFlow，或 `--load-meta` 装好了元数据）
- 单库包：`--kb-id` 指定的知识库已存在；by-dir 包：传 `--template-kb`
- 运行环境：python3 + psycopg2 / minio / xxhash / PyYAML（repo `.venv` 均有）。
  `inject.py` 是独立脚本，无其他文件依赖（最小 Vastbase writer 已内嵌，
  精简自 `../vb-to-vb-migration/vb_writer.py`）

## 触发解析（parse_docs.py）

注入的文档是 UNSTART（无 `ingestion_task` 行），用 `parse_docs.py` 批量启动（走 UI 同款
`POST /api/v1/datasets/<kb_id>/documents/parse`，任务经正常 ingestion executor 执行；
脚本自动在 `api_token` 表种一个幂等 token——main 起必须带 `user_id`，脚本会从
`user_tenant` 的 owner 行解析——无需去 UI 生成 API Key）：

```bash
# 预览各 KB 有多少待解析文档
python parse_docs.py --dst-host 172.16.105.105 --dst-password '***' --dry-run

# 全部启动并等待完成，最后报告失败明细
python parse_docs.py --dst-host 172.16.105.105 --dst-password '***' \
    --api-url http://172.16.105.105:9380 --tenant-email admin@vastdata.com.cn --wait

# 只解析部分库（支持 % 通配）
python parse_docs.py ... --kb-name VDS_v3.9.2 --kb-name 'G100_%'
```

依赖：psycopg2 + requests；`--api-url` 指向 ragflow server（容器内默认
`http://127.0.0.1:9380`）。重复执行安全：只挑无 `ingestion_task` 行（UNSTART）的文档。
每 5 个文档一次 API 调用（`--batch` 可调）——小批量让服务端每次请求的
同步处理更短，对 UI 响应更友好；代价是提交阶段 HTTP 次数变多。
