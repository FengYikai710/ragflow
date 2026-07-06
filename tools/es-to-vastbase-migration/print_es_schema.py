"""
Print the structure of one document from ES to understand the JSON schema.
"""

from es_reader import ESReader
import json

es = ESReader(host="localhost", port=1200, username="elastic", password="infini_rag_flow")

# List indices first
indices = es.list_ragflow_indices()
print(f"Available indices: {indices}\n")

# Pick the first (or specify your own)
index_name = indices[0] if indices else "ragflow_your_index_name"
print(f"Reading from index: {index_name}\n")

# Get just 1 document
for batch in es.scroll_documents(index_name, batch_size=1):
    doc = batch[0]
    print(f"_id: {doc.get('_id', 'N/A')}\n")
    print("=== Fields ===")
    for key, value in sorted(doc.items()):
        if key == "_id":
            continue
        val_type = type(value).__name__
        val_preview = json.dumps(value, ensure_ascii=False, default=str)
        if len(val_preview) > 120:
            val_preview = val_preview[:120] + "..."
        print(f"  {key:30s}  ({val_type:10s})  {val_preview}")
    break

print("\n=== Vector fields ===")
for key in doc:
    if key.startswith("q_") and key.endswith("_vec"):
        vec = doc[key]
        print(f"  {key}: length={len(vec)}, first 5={vec[:5]}")

es.close()