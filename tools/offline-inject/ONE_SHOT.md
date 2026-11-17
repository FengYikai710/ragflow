# one_shot.sh 使用手册

一条命令跑完整个离线注入流水线：**解压 → 图片上图床 → pack 打包 → apply 写库**
（可选第 5 步：`PARSE=1` 自动批量启动解析）。
面向在目标机（如 105）上操作的人员。

## 0. 目录要求与前置条件

脚本目录（如 `/root/export`）需要这几个文件：

```
inject.py                    # 核心：pack / apply（one_shot.sh 必须与它同目录）
one_shot.sh                  # 本脚本
markdown_image_uploader.py   # 图片上图床工具（默认从当前目录找）
vastdata_docs.tar.gz         # 交付压缩包（顶层目录必须是 vastdata_docs/）
```

前置条件：

> **建议直接在 ragflow 的 docker 容器中执行**（`docker exec -it docker-ragflow-cpu-1 bash`）：
> 容器自带 python3 及 psycopg2 / minio / xxhash / PyYAML 等全部依赖，无需 pip 安装。
> 先把脚本和压缩包复制进去：`docker cp /root/export docker-ragflow-cpu-1:/export/`。
> 只有在宿主机上执行时才需要装依赖：

```bash
pip3 install psycopg2-binary minio xxhash pyyaml requests   # 仅宿主机执行需要
chmod +x one_shot.sh
# 目标库已部署过 RAGFlow（元数据库表已存在）
```

## 1. 快速开始

```bash
# 默认使用脚本同目录的 vastdata_docs.tar.gz
./one_shot.sh

# 或指定压缩包路径
./one_shot.sh /path/to/其他包.tar.gz
```

配置有两种方式，任选其一：

- **临时**：命令前加环境变量 `DST_HOST=x.x.x.x ./one_shot.sh`
- **永久**：直接编辑脚本顶部 `========== Config ==========` 区块

环境变量优先于脚本里的默认值。

## 2. 配置项一览

| 变量 | 默认值 | 说明 |
|---|---|---|
| `DST_HOST` | `vastbase` | Vastbase 元数据库地址。**容器里跑用默认；宿主机跑要改成 `172.16.105.105`**（容器名只在 docker 网络内可解析） |
| `DST_PORT` | `5432` | Vastbase 端口，非 5432 时必改 |
| `DST_USER` | `ragflow` | 数据库用户 |
| `DST_PASSWORD` | `Ragflow@123` | 数据库密码 |
| `DST_DB` | `rag_flow` | 元数据库库名（对应 apply 的 `--dst-meta-db`） |
| `S3_HOST` | `minio` | MinIO 地址。容器里跑用默认；宿主机跑改成 `172.16.105.105`（容器名只在 docker 网络内可解析） |
| `S3_PORT` | `9000` | MinIO 端口。脚本自动拼成 `host:port` 传给 apply；若 `S3_HOST` 里已带端口则原样使用 |
| `S3_AK` | `rag_flow` | MinIO 用户 |
| `S3_SK` | `infini_rag_flow` | MinIO 密码 |
| `JOBS` | `8` | 并发数。机器没问题可提到 `16` 提速 |
| `TENANT_EMAIL` | 空 | 把交付包绑定到该邮箱用户的租户（105 上用 `admin@vastdata.com.cn`）。**在 pack 步骤生效**——包已打好时改它无效，需 `FORCE=1` 重打或用 `APPLY_EXTRA_ARGS` 覆盖（见 §4） |
| `PICGO_URL` | `http://172.16.105.105:36677` | PicGo 图床服务地址 |
| `UPLOADER` | 脚本同目录的 `markdown_image_uploader.py` | 上传脚本路径，默认锚定在**脚本所在目录**（与执行时的当前目录无关，`bash /xxx/one_shot.sh` 在哪跑都不会错）；找不到时会去解压目录里搜 |
| `WORK_ROOT` | `./` | 工作目录根。产物都落在这下面（见 §5） |
| `APPLY_EXTRA_ARGS` | 空 | 追加给 apply 的参数，如 `"--dry-run"`、`"--resume"`、`"--flush-every 200"` |
| `SKIP_UPLOAD_CHECK` | 未设 | 设为 `1` 时图片上传失败不中止（默认有失败就停） |
| `PARSE` | 未设 | 设为 `1` 时注入完成后自动执行第 5 步：批量启动解析（`parse_docs.py`） |
| `PARSE_WAIT` | 未设 | 配合 `PARSE=1`：设为 `1` 时等解析全部完成再退出（默认排队完即退出） |
| `API_URL` | `http://127.0.0.1:9380` | ragflow server 地址（第 5 步用）。容器内默认即可；宿主机跑用 `http://172.16.105.105:9380` |

## 3. 105 环境推荐命令

```bash
# 宿主机（/root/export 下）全量跑：
DST_HOST=172.16.105.105 \
S3_HOST=172.16.105.105 \
TENANT_EMAIL=admin@vastdata.com.cn \
JOBS=16 \
./one_shot.sh

# 在 ragflow 容器里跑（推荐——自带 python3 和依赖，DST_HOST/S3_HOST 用默认值即可）：
# docker exec -it docker-ragflow-cpu-1 bash
# 其余同上
```

## 4. 断点续跑与强制重跑

每步完成都会留标记，中断后**直接重跑同一命令**即从断点继续：

| 步骤 | 标记 | 跳过条件 |
|---|---|---|
| 解压 | `<工作目录>/.extract_done` | 标记存在 |
| 图片上传 | `<工作目录>/.upload_done` | 标记存在 |
| 打包 | `<包目录>/manifest.json` | 包完整（manifest 是最后原子写入的） |
| 注入 | （apply 自身按内容哈希幂等） | 已注入的文件自动 skip |

```bash
FORCE=1 ./one_shot.sh        # 全部重来：重新解压、重新上图床、重新打包
                             # 注意：手改过解压目录里的文件也要 FORCE=1 才生效

# 包已打好、只改租户绑定，不想重打：
APPLY_EXTRA_ARGS="--tenant-email admin@vastdata.com.cn" ./one_shot.sh
```

## 5. 产物与日志

```
$WORK_ROOT/
├── vastdata_docs_extracted/     # 解压 + 图片替换后的源文件
├── inject_vastdata_docs/        # 离线包（manifest.json + objects/）
└── offline_inject_logs/         # 各步日志：upload_*.log / pack_*.log / apply_*.log
```

出问题先看 `offline_inject_logs/` 里最新的 `apply_*.log`；apply 结尾的
`INJECT SUMMARY: injected=… skipped=… repaired=… failed=…` 是总账，
`failed=0` 即全部成功。

## 6. 常见问题

| 现象 | 原因与处理 |
|---|---|
| ~~MinIO 报连接 80 端口失败~~ | 旧版忘带 `:9000` 导致；现在端口由 `S3_PORT` 自动拼接，不会再出现 |
| 数据库/MinIO 连不上 | 在宿主机上用了容器名（`vastbase`/`minio`）；改成 `172.16.105.105` |
| `Image upload reported failures … aborting` | 图床有失败项，看 `upload_*.log`；确认可忽略后 `SKIP_UPLOAD_CHECK=1 ./one_shot.sh` |
| 改了源文件重跑没生效 | 断点标记认为输入没变；`FORCE=1` 重跑 |
| `No .md files found after extraction` | 压缩包顶层目录不是 `vastdata_docs/`，重新打包 |
| 想先演练不写库 | `APPLY_EXTRA_ARGS="--dry-run" ./one_shot.sh` |

## 7. 注入完成后：批量启动解析

一体化方式——one_shot.sh 加 `PARSE=1`，注入完自动进入第 5 步排队解析
（`PARSE_WAIT=1` 再等全部完成）：

```bash
# 宿主机全流程 + 自动解析：
DST_HOST=172.16.105.105 S3_HOST=172.16.105.105 \
TENANT_EMAIL=admin@vastdata.com.cn JOBS=16 \
API_URL=http://172.16.105.105:9380 PARSE=1 \
./one_shot.sh
```

也可单独手动跑 `parse_docs.py`（第 5 步内部就是它），先预览再执行：

```bash
# 先预览各 KB 待解析数量
python3 parse_docs.py --dst-host 172.16.105.105 --dst-password 'Ragflow@123' --dry-run

# 全部启动（每 5 个文档一批）；--wait 等跑完并报告失败明细
python3 parse_docs.py --dst-host 172.16.105.105 --dst-password 'Ragflow@123' \
    --api-url http://127.0.0.1:9380 \
    --tenant-email admin@vastdata.com.cn --wait
```

在容器里跑时 `--dst-host vastbase`、`--api-url` 用默认即可；重复执行安全
（只挑 `run='0'` 的文档，已在解析/完成的不会重复提交）。日志：`parse_*.log`。
