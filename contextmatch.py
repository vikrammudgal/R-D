import pandas as pd
from sentence_transformers import SentenceTransformer
import faiss
import numpy as np
from rapidfuzz import fuzz, process

# ====== 1. Prepare Data ======
# Example master data
ledger_list = [
    "3M Electro and Communication India Pvt. Ltd.-P",
    "3M India Limited-P",
    "3M India Limited-S",
    "Accord Software and Systems Pvt.Ltd",
    "Accounting & Compliance Fees",
]

cost_centre_list = [
    "0603-F/B-220E",
    "3M 1205 Polymide Film tape",
    "3040 Electronics Coolant",
]

group_list = [
    "Sales Interstate",
    "Sundry Creditors",
    "Advance to Suppliers",
]

cost_category_list = [
    "Sales - Blg",
    "Administartion",
    "Primary Cost Category",
]

# Build unified DataFrame
entities = []
for name in ledger_list:
    entities.append({"id": f"L-{len(entities)+1}", "name": name, "type": "Ledger"})
for name in cost_centre_list:
    entities.append({"id": f"CC-{len(entities)+1}", "name": name, "type": "Cost Centre"})
for name in group_list:
    entities.append({"id": f"G-{len(entities)+1}", "name": name, "type": "Group"})
for name in cost_category_list:
    entities.append({"id": f"CAT-{len(entities)+1}", "name": name, "type": "Cost Category"})

df_entities = pd.DataFrame(entities)

# ====== 2. Build Vector Index ======
model = SentenceTransformer('all-MiniLM-L6-v2')
embeddings = model.encode(df_entities['name'].tolist(), convert_to_numpy=True)

dim = embeddings.shape[1]
index = faiss.IndexFlatL2(dim)
index.add(embeddings)
metadata_list = df_entities.to_dict(orient="records")

# ====== 3. Search Function ======
def search_entities(query, top_k=12):
    # Vector search
    q_vec = model.encode([query], convert_to_numpy=True)
    distances, idxs = index.search(q_vec, top_k*3)  # get more for later filtering
    vector_results = [(metadata_list[i], float(distances[0][pos])) for pos, i in enumerate(idxs[0])]

    # Fuzzy search
    fuzzy_results = process.extract(query, df_entities['name'], scorer=fuzz.partial_ratio, limit=top_k*3)
    fuzzy_results = [(metadata_list[idx], 100 - score) for name, score, idx in fuzzy_results]

    # Merge results (simple approach: concat and deduplicate by ID)
    merged = {}
    for m, score in vector_results + fuzzy_results:
        if m['id'] not in merged or score < merged[m['id']]['score']:
            merged[m['id']] = {"data": m, "score": score}

    # Sort by score
    ranked = sorted(merged.values(), key=lambda x: x['score'])
    return [r['data'] for r in ranked[:top_k]]

# ====== 4. Build Context Slice for LLM ======
def build_context_slice(query, top_k=12):
    candidates = search_entities(query, top_k)
    lines = []
    for c in candidates:
        lines.append(f"{c['id']}|{c['name']}|{c['type']}| | |{{}}")
    return "\n".join(lines)

# ====== 5. Example Usage ======
user_query = "Did we cross 10 lakhs in sales for Accord Soft?"
context_slice = build_context_slice(user_query)
print("=== Known Entities Slice for LLM ===")
print(context_slice)
