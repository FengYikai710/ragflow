# RAGFlow 离线文档注入工具（pack / apply）

把一个交付目录打成离线包，在目标环境直接写入 MinIO + 元数据库（`rag_flow`），
**不触发解析**——文档落在 UNSTART 状态（`run="0"`），后续在 UI 里点解析，或调
`POST /api/v1/datasets/<kb_id>/documents/parse` 批量启动。

复刻的是上传 API 的写入路径（`api/db/services/file_service.py` 的
`upload_document`），只是去掉 HTTP、task、Redis。

## 用法

```bash
# 1a. 打包——单库模式：整个 --src-dir 进一个 KB
python inject.py pack --src-dir ./交付文件 --out ./init_pkg

# 1b. 打包——按目录分库：--src-dir 下每个一级子目录 = 一个知识库（以目录名命名），
#     只收指定扩展名（如 md），可只挑部分目录先做实验；
#     --template-kb 把克隆模板 KB id 记进 manifest，apply 免传
python inject.py pack --src-dir ~/Repository/dev420/001export --out ./init_pkg \
    --by-dir --ext md --only VDS_v3.9.2 --template-kb <kb_id>

# 2a. 注入单库包：目标库已建好 KB，--kb-id 指定
python inject.py apply --pkg ./init_pkg --kb-id <kb_id> \
    --dst-host vb-dst --dst-user ragflow --dst-password '***' \
    --minio-host minio:9000 --minio-user rag_flow --minio-password '***' \
    [--minio-bucket rag_flow] [--dry-run] [--resume]

# 2b. 注入 --by-dir 包：KB 按 (tenant, name) 复用；不存在则从模板 KB 克隆一个
#     （embd/parser/tenant 配置来源）。模板优先级：apply --template-kb >
#     pack 时记进 manifest 的 template_kb
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
- **doc_num 对账**：apply 末尾把 `knowledgebase.doc_num` 重算为实际 document
  数（直插 SQL 绕过了上传 API 的计数器维护）。
- **不写**：task 行、Redis 消息、thumbnail（UI 显示占位图，不影响解析）。
- 名字超过 255 字符（document.name/location 列宽）的文件会在 pack 阶段报错剔除。

## 前置条件

- 目标元数据库表已存在（目标部署过 RAGFlow，或 `--load-meta` 装好了元数据）
- 单库包：`--kb-id` 指定的知识库已存在；by-dir 包：传 `--template-kb`
- 运行环境：repo `.venv`（psycopg2 / minio / xxhash / PyYAML），并复用
  `../vb-to-vb-migration` 的 `VBWriter` 与 `identity_batch`
