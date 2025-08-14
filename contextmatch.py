import json
import re
from sentence_transformers import SentenceTransformer, util
from rapidfuzz import fuzz
import numpy as np

# -----------------------------
# 1. Load your entity data
# -----------------------------
# Example: each entity is {"name": "Ledger Name", "type": "Ledger"}
# You can load from your Excel's "Master data" sheet instead.
entities = [
    {"name": "3M India Limited-P", "type": "Ledger"},
    {"name": "Accord Software and Systems Pvt.Ltd", "type": "Ledger"},
    {"name": "Sales Interstate", "type": "Group"},
    {"name": "GST @ 18%Freight/courier", "type": "Ledger"},
    {"name": "Primary Cost Category", "type": "Cost Category"},
    # ... load the rest from your master data
]

# -----------------------------
# 2. Define boost map
# -----------------------------
boost_map = {
    "Ledger": [
        "sales", "turnover", "revenue", "income", "expenses", "purchase", "payable",
        "receivable", "debtor", "creditor", "interest", "commission", "discount",
        "royalty", "fees", "charges", "tds", "gst", "tax", "igst", "cgst", "sgst",
        "vat", "service tax", "excise", "duty", "cess"
    ],
    "Stock Item": [
        "inventory", "stock", "product", "item", "goods", "material", "spare",
        "component", "consumable", "raw material", "finished goods", "wip",
        "warehouse", "batch", "lot"
    ],
    "Group": [
        "account group", "ledger group", "category", "division", "segment",
        "department"
    ],
    "Cost Centre": [
        "department", "project", "branch", "unit", "section", "division",
        "office", "team", "zone", "region"
    ],
    "Cost Category": [
        "allocation", "cost pool", "expense type", "overhead", "category"
    ],
    "Taxation & Compliance": [
        "tds", "gst", "tax", "igst", "cgst", "sgst", "vat", "service tax", "excise",
        "duty", "cess", "professional tax", "income tax", "withholding tax",
        "pf", "provident fund", "esi", "employee state insurance", "pt",
        "labour welfare fund", "compliance", "statutory", "epf", "epfo"
    ]
}

# -----------------------------
# 3. Create embeddings
# -----------------------------
model = SentenceTransformer('all-MiniLM-L6-v2')
entity_names = [e["name"] for e in entities]
entity_embeddings = model.encode(entity_names, convert_to_tensor=True)

# -----------------------------
# 4. Boost calculation
# -----------------------------
def calculate_boost(query, entity_type):
    boost = 0.0
    q_lower = query.lower()
    if entity_type in boost_map:
        for kw in boost_map[entity_type]:
            if kw in q_lower:
                boost += 0.1  # boost factor per keyword match
    return boost

# -----------------------------
# 5. Search function
# -----------------------------
def search_entities(query, top_k=5):
    query_embedding = model.encode(query, convert_to_tensor=True)
    
    # Semantic similarity
    cos_scores = util.cos_sim(query_embedding, entity_embeddings)[0].cpu().numpy()
    
    results = []
    for idx, score in enumerate(cos_scores):
        entity = entities[idx]
        boost = calculate_boost(query, entity["type"])
        final_score = score + boost
        
        results.append({
            "name": entity["name"],
            "type": entity["type"],
            "score": final_score
        })
    
    # Sort by score
    results.sort(key=lambda x: x["score"], reverse=True)
    
    # Fuzzy matching fallback
    fuzz_results = []
    for e in entities:
        fuzz_score = fuzz.token_sort_ratio(query.lower(), e["name"].lower()) / 100
        fuzz_results.append({
            "name": e["name"],
            "type": e["type"],
            "score": fuzz_score
        })
    fuzz_results.sort(key=lambda x: x["score"], reverse=True)
    
    # Merge (vector first, then fuzzy if missing)
    seen = set()
    final = []
    for r in results:
        if r["name"] not in seen:
            final.append(r)
            seen.add(r["name"])
    for r in fuzz_results:
        if r["name"] not in seen:
            final.append(r)
            seen.add(r["name"])
    
    return final[:top_k]

# -----------------------------
# 6. Prompt builder for LLM
# -----------------------------
def build_context_prompt(query):
    matched_entities = search_entities(query, top_k=10)
    context_lines = [f"{e['name']} ({e['type']})" for e in matched_entities]
    return (
        f"Known Entities in this company:\n" +
        "\n".join(context_lines) +
        f"\n\nUser question: {query}\n" +
        "Answer based only on above entities and known financial context."
    )

# -----------------------------
# 7. Example
# -----------------------------
if __name__ == "__main__":
    user_query = "Did we cross 10 lakhs in sales for Accord Soft?"

    # Example queries
    queries = [
        'How much did we sell to Aero Mfg last quarter?',
        'What’s the monthly sale trend for Areo Manufac?',
        'to Abhinav Sys in July?',
        'Did we cross 10 lakhs in sales for Accord Soft?',
        'Show sales summary for Power One in Q1.',
        'Compare sales of ITW and 3M India last year.',
        'Which items did we sell most to Accrd Global?',
        'How much GST collected from 3M Ltd sales?',
        'Give me invoice-wise sale to EMSD in April.',
        'Did we sell 0603-F/B to any customer last week?',
        'Show total revenue from EMS Dvn.',
        'What are the top 5 customers by sales?',
        'Generate sales breakdown for item group Cable.',
        'Which cities did we ship Scotchcast to?',
        'Is there a dip in sales to IAT Division?',
        'Show product-wise sales for Sirshila.',
        'Sales by cost centre Administ.',
        'Sales to 3S Infra in this FY?',
        'How much did we bill Arvind Asso?',
        'Show total number of sales invoices raised.',
        'What were our purchases from 3M Elect last month?',
        'How much did we spend with Aereo Mfg this quarter?',
        'Purchase trend for ABHINAV over 6 months?',
        'Item-wise purchase from EMS Divn?',
        'Which vendors did we buy 1170 (31mm) from?',
        'Generate purchase report for Power1.',
        'Which stock categories had max purchases?',
        'Purchase from Accord Soft last 3 months?',
        'How much did we spend on item 3M 1099?',
        'Top 5 vendors by purchase value?',
        'Total units bought of 120/9 3M GEL?',
        'Which vendor supplied the most items?',
        'Monthly purchase from 3S Infra?',
        'Did we buy anything from Bharath R this month?',
        'Show purchase ledger for 3M Pvt.',
        'PO-wise purchase detail for IATD?',
        'What was the largest purchase invoice?',
        'Vendor-wise purchase for Aug?',
        'Cost centre-wise purchase breakup?',
        'Purchases from Direct Exp group?',
        'What do we owe to Aero Mfg as of today?',
        'Show pending bills to Anuradha.',
        'How much are we to pay to Accord Soft?',
        'Payables aging report for 3M Elect Comm?',
        'Who are our top 5 creditors?',
        'Total amount due to EMS Div.',
        'Which vendor has overdue invoices?',
        'How much is due to Abhinav Sys this month?',
        'Are there any payables due from Admin Cost Centre?',
        'Outstanding to Arvind Assoc as on 31st March?',
        'Vendor-wise payable summary.',
        'Payables grouped by Ledger Grp?',
        'Are we carrying any payables for 3S Infra?',
        'Longest unpaid bill for ITW?',
        'Show bills payable by cost centre Sirshila.',
        'Total due to Current Liab group?',
        'Next 7-day payables forecast.',
        'How much TDS is pending to be paid?',
        'Give me unpaid GRNs or POs.',
        'Which vendor has credit days exceeding 90?',
        'How much is receivable from Aero Ltd?',
        'Outstanding from Accord Globl?',
        'Receivables grouped by ledger grp?',
        'Debtors with balances over 1 lakh?',
        'Is any payment pending from Abhi Systch?',
        'Show aging of receivables for Power One.',
        'What are we expecting from Anisha?',
        'Which customers are overdue?',
        'Sales ledger outstanding for 3M India Ltd?',
        'Customer-wise receivables snapshot.',
        'What’s the oldest unpaid invoice?',
        'Receivables due from Direct Incomes?',
        'Amount due from EMSD region?',
        'Invoice-wise pending for Bharath R?',
        'Group-wise receivable balances?',
        'Which cost centre has most receivables?',
        'Receivables from Sundry Debtors?',
        'Who paid partially but not fully?',
        'Are there bounced cheques pending?',
        'Receivables pending from 3M ELCTRO?',
        'What’s the current stock of 3M 1170?',
        'Stock level of Scotchcast Resin?',
        'How many 3040 Coolant left in inventory?',
        'Item-wise closing stock for July?',
        'Top 5 items by stock value?',
        'Stock items below reorder level?',
        'Did we receive 0603-F/B in last week?',
        'What’s the average cost of 3M GEL?',
        'Value of 1170 (31mm) in stock?',
        'Stock movement for Cable items?',
        'Show item group wise stock position.',
        'Inventory balance by stock category?',
        'Slow-moving stock from last year?',
        'Stock ageing for ITW items?',
        'Show Godown-wise stock for Powerone?',
        'Batch-wise quantity for 3M 1099?',
        'Consumption report for IAT items?',
        'Stock valuation by cost centre?',
        'Which items were received in April?',
        'Any negative stock items?',
        'What’s the valuation of 3M stock?',
        'GST paid on purchases from Areo?',
        'HSN summary for Scotchcast items?',
        'Tax liability for July?',
        'GST input from EMS Vendors?',
        'Show output tax collected from ITW group?',
        'Tax ledgers used in last 10 vouchers?',
        'IGST amount on 3M Elect purchase?',
        'Valuation method used for 1170 item?',
        'Stock value by FIFO for 3M 1099?',
        'What was total COGS for July?',
        'GST reconciliation summary?',
        'GST payable to govt for Aug?',
        'Any difference in GST return vs books?',
        'Tax mismatch report by cost centre?',
        'Tax paid grouped by item category?',
        'TDS entries for Arvind Assoc?',
        'Deferred tax entries if any?',
        'GST rate used for IATD sales?',
        'Monthly tax payment schedule?',
        'How much cash inflow did we have last month?',
        'Cash outflow to Abhinav Pvt Ltd?',
        'Net cash from operating activities?',
        'Cash vs Bank ledger for July?',
        'Did Sirshila receive any advance?',
        'Daily cash balance trend?',
        'Which cost centre spent the most cash?',
        'Petty cash voucher details?',
        'Cash movement for Admin division?',
        'Total cash receipts from 3M Elect?',
        'Show all cash ledger entries.',
        'Reconcile cash with bank statement.',
        'Withdrawals from Axis account?',
        'Cash balance as on 31st Mar?',
        'Compare cash flow from April vs May?',
        'Inflows from EMSD region?',
        'Who was paid in cash?',
        'Total UPI receipts?',
        'NEFT vs Cash ratio last quarter?',
        'Show liquidity summary.',
        'Sales by cost centre Sales - Blg?',
        'Expenses under Indirect Exp group?',
        'Show balance of 3M ledger group?',
        'Ledgers under Loans Liability?',
        'Cost centre-wise ledger summary?',
        'Income from Direct Incomes?',
        'Show ledger trail for Bharath Rao?',
        'Ledger hierarchy of EMS Div?',
        'Group-wise P&L contribution?',
        'Movement in Branch / Division ledgers?',
        'Which ledgers were added recently?',
        'Any ledgers without entries?',
        'Balance sheet group breakup?',
        'Top 5 ledgers by transaction count?',
        'How many groups under Current Assets?',
        'Cash flow by group category?',
        'Cost centre hierarchy report?',
        'Which cost category has highest spend?',
        'How many ledgers belong to Anuradha group?',
        'Assign a ledger to Fixed Asset group?',
    ]

    print('checking queries')
    for q in queries:
        print(f"\nQuery: {q}")
        print(build_context_prompt(q))

    
