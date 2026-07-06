# ES → Vastbase Migration Tool

将 RAGFlow 在 Elasticsearch 中的文档数据迁移到 Vastbase。

## 原理

RAGFlow 在 ES 中使用单个索引（如 `ragflow_{tenant}`），所有知识库数据混在一起，通过 `kb_id` 字段区分。在 Vastbase 中则是**按知识库分表**，表名格式 `{index_name}_{kb_id}`。

迁移工具会：
1. 从 ES 中聚合出所有 `kb_id`
2. 对每个 `kb_id` 创建 Vastbase 表（含向量索引）
3. 分批读取 ES 数据，转换字段格式，批量写入 Vastbase
4. 支持断点续传

## 安装

```bash
pip install -r requirements.txt
```

## 使用

### 1. 查看 ES 中的索引

```bash
python migrate.py --es-host localhost --es-port 9200 --list-indices
```

### 2. 执行迁移

```bash
# 迁移所有索引
python migrate.py \
    --es-host localhost --es-port 9200 \
    --vb-host localhost --vb-port 5432 \
    --vb-user vastbase --vb-password 'Vastdata@123' \
    --vb-db vastbase

# 迁移指定索引
python migrate.py \
    --es-host localhost --es-port 9200 \
    --vb-host localhost --vb-port 5432 \
    --vb-user vastbase --vb-password 'Vastdata@123' \
    --vb-db vastbase \
    --index ragflow_xxx

# 迁移指定知识库
python migrate.py ... --index ragflow_xxx --kb-id <uuid>
```

### 3. 断点续传

```bash
python migrate.py ... --resume
```

### 4. 先预览（不实际写入）

```bash
python migrate.py ... --dry-run
```

### 5. 验证数据一致性

```bash
python migrate.py ... --verify
```

### 6. 环境变量

| 变量 | 说明 | 默认值 |
|---|---|---|
| `VB_DBCOMPATIBILITY` | Vastbase 兼容模式（`PG` 或 `B`） | `PG` |

## 字段转换说明

| ES 格式 | Vastbase 格式 |
|---|---|
| `position_int: [[1,2,3,4,5]]` | `"00000001_00000002_00000003_00000004_00000005"` |
| `page_num_int: [1, 2]` | `"00000001_00000002"` |
| `top_int: [10, 20]` | `"0000000a_00000014"` |
| `important_kwd: ["a", "b"]` | `"a###b"` |
| `tag_feas: {"key": "val"}` | `'{"key": "val"}'` |
| `q_768_vec: [0.1, 0.2, ...]` | `floatvector` 数组 |
