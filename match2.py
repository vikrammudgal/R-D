from pkgutil import get_data
import re
import ahocorasick
from collections import defaultdict
from rapidfuzz import fuzz, process
import pymysql


class HybridMasterMatcher:
    def __init__(self, threshold=80, top_n=5):
        """
        threshold: fuzzy match minimum score (0-100)
        top_n: max matches per category
        """
        self.threshold = threshold
        self.top_n = top_n
        self.masters_by_category = {}
        self.masters_lower = {}
        self.automaton = ahocorasick.Automaton()
        self.master_index = []  # [(category, master_id, master_name_lower)]
    
    def get_db_data(self):
        """fetch the data from databse or any other source"""
        dict = {}
        conn = pymysql.connect( host='3.108.40.112',user='talkingtotals',password='T@lkingTotals!@#$', database='tt_customer_db_1')
        cursor = conn.cursor()
        queries = [
            "SELECT id, name FROM ledger_groups WHERE company_id = '1'",
            "SELECT id, name FROM ledgers WHERE company_id = '1'",
            "SELECT id, name FROM stock_items WHERE company_id = '1'",
            "SELECT id, name FROM stock_item_categories WHERE company_id = '1'",
            "SELECT id, name FROM stock_item_groups WHERE company_id = '1'",
            "SELECT id, name FROM cost_centres WHERE company_id = '1'",
        ]
        keyword = ['Groups', 'Ledgers', 'Stock', 'Stock_Categories', 'Stock_Groups', 'Cost_Center']
        index = 0
        for query in queries:
            cursor.execute(query)
            rows = cursor.fetchall()
            print(f"Fetched {len(rows)} rows for {keyword[index]}")
            for row in rows:
                id, name = row 
                # print(row)
                dict.setdefault(keyword[index], []).append((id, name))   
            index += 1
        cursor.close()
        conn.close()
        for item in dict:
            print(f"{item} : {len(dict[item])}")
        return dict

    def generate_ngrams(self, tokens, max_n=4):
        """Generate n-grams from a list of tokens"""
        ngrams = []
        for n in range(1, max_n+1):
            for i in range(len(tokens)-n+1):
                ngram = " ".join(tokens[i:i+n])
                ngrams.append(ngram)
        return ngrams
    
    def preprocess(self, text: str) -> str:
        """Normalize query for matching"""
        text = text.lower()
        text = re.sub(r'[^a-z0-9\s]', '', text)
        return text
    
    def load_masters(self, masters_dict):
        """
        masters_dict: {
            "Ledger": [(id, name), ...],
            "Group": [(id, name), ...],
            ...
        }
        """
        self.masters_by_category = masters_dict
        self.masters_lower = {
            cat: [(mid, name.lower()) for mid, name in items]
            for cat, items in masters_dict.items()
        }

        # Build Aho-Corasick automaton for fast exact/substring detection
        idx = 0
        for cat, items in self.masters_lower.items():
            for mid, name_lower in items:
                self.automaton.add_word(name_lower, (cat, mid, name_lower))
                self.master_index.append((cat, mid, name_lower))
                idx += 1

        self.automaton.make_automaton()

    def match_query_with_phrases(self,query, masters_dict, threshold=70):
        # Preprocess query
        query_norm = re.sub(r'[^a-z0-9\s]', '', query.lower())
        tokens = query_norm.split()

        # Generate n-grams (prioritize longer first)
        ngrams = self.generate_ngrams(tokens, max_n=5)
        ngrams.sort(key=lambda x: len(x.split()), reverse=True)

        results = []
        for category, items in masters_dict.items():
            names_lower = [name.lower() for _, name in items]
            # print(names_lower)
            # Check each n-gram against master list
            for phrase in ngrams:
                match, score, idx = process.extractOne(phrase, names_lower, scorer=fuzz.ratio)
                if score >= threshold:
                    mid, original_name = items[idx]
                    results.append({
                        "query": query,
                        "category": category,
                        "master_id": mid,
                        "master_name": original_name,
                        "score": round(score, 2),
                        "matched_phrase": phrase
                    })

        # Keep best match per master_id
        unique = {}
        for r in results:
            key = (r["category"], r["master_id"])
            if key not in unique or r["score"] > unique[key]["score"]:
                unique[key] = r

        return list(unique.values())

    def match_query(self, query: str):
        """Return matches for a query"""
        query_norm = self.preprocess(query)
        matches = defaultdict(list)
        scored_results = []

        # Step 1: Exact / substring matches via Aho-Corasick
        for end_idx, (cat, mid, name_lower) in self.automaton.iter(query_norm):
            original_name = dict(self.masters_by_category[cat])[mid]
            scored_results.append({
                "query": query,
                "category": cat,
                "master_id": mid,
                "master_name": original_name,
                "score": 100.0
            })

        # Step 2: Fuzzy matching on tokens to catch typos
        tokens = query_norm.split()
        for token in tokens:
            # Search top_n fuzzy matches for each token across all masters
            for cat, items in self.masters_lower.items():
                names_lower = [name for _, name in items]
                best_matches = process.extract(token, names_lower, scorer=fuzz.ratio, limit=self.top_n)
                for name_lower, score, idx in best_matches:
                    if score >= self.threshold:
                        mid, _ = self.masters_lower[cat][idx]
                        original_name = dict(self.masters_by_category[cat])[mid]
                        scored_results.append({
                            "query": query,
                            "category": cat,
                            "master_id": mid,
                            "master_name": original_name,
                            "score": round(score, 2)
                        })

        # Step 3: Deduplicate results by (category, master_id)
        unique_results = {}
        for r in scored_results:
            key = (r["category"], r["master_id"])
            if key not in unique_results or r["score"] > unique_results[key]["score"]:
                unique_results[key] = r

        return list(unique_results.values())

    

# --------------------------
# Example usage
# --------------------------
if __name__ == "__main__":
    matcher = HybridMasterMatcher(threshold=80, top_n=5)
    masters_dict = matcher.get_db_data()
    matcher.load_masters(masters_dict)

    queries = [
        # 'How much 3M Tape 79 did we sell this month',
        # 'What is my total GST payable for this quarter?',
        # 'What is my total GST obligation for this quarter?',
        # 'Show me the balance of ledger account Caleb Bhasin (Vendor)',
        # 'What is the total credit balance for all customers?',
        'Show me the debit balance of ledger account Amphenol Interconct',
        # 'List all transactions for ledger account Caleb Bhasin (Vendor)',
        # 'Show the opening balance for ledger account Courier Charges',
        # 'What is the total balance for the group "Sundry Debtors"?',
        # 'Show the total credit balance for all groups.',
        # 'What is the balance for the "Purchase" group?',
        # 'Show the recent transactions in the "Bank" ledger.',
        # 'What is the total debit balance for the "Expenses" group?',
        # 'Show the list of ledgers in the "Sundry Creditors" group.',
        # 'What is the total balance of the "Assets" group?',
        # 'Show me the opening balance for ledger account "Cash in Hand".',
        # 'What is the total balance for all income groups combined?',
        # 'Show the closing balance for the "Sales" ledger account.',
        # 'List all transactions for the ledger account "Expenses" in the last month.',
        # 'Show the balance for the "Liabilities" group for this quarter.',
        # 'What is the total balance for all liability groups combined?',
        # 'Show me the transactions for "Accounts Payable" ledger account.',
        # 'Show all ledgers in the "Income" group.',
        # 'Show all journals posted to the "Bank" ledger in the last week.',
        # 'What is the credit balance of the "Sundry Creditors" group?',
        # 'Show the balance in the "Sales" ledger account as of today.',
        # 'Show the total debit balance for "Fixed Assets" group.',
        # 'Show the transactions for "Accounts Receivable" ledger for this month.',
        # 'Show the balances of the "Other Liabilities" group.',
        # 'List all transactions for "Sundry Creditors" ledger account.',
        # 'What is the total debit balance of the "Revenue" group?',
        # 'Show all receipts made to "Cash" ledger in the last 15 days.',
        # 'What is the total balance for all expense groups?',
        # 'Show all credit entries for "Sundry Debtors" ledger account.',
        # 'Show all ledgers under the "Capital" group.',
        # 'Show the balance for "Accounts Receivable" ledger account.',
        # 'What is the total balance for the "Equity" group?',
        # 'Show all debit entries for the "Cash in Bank" ledger.',
        # 'List all groups under "Liabilities" category.',
        # 'Show all journals posted to the "Sales" ledger in the last quarter.',
        # 'What is the total credit balance for the "Bank" group?',
        # 'Show the transactions for "Loan Payable" ledger for the last 30 days.',
        # 'Show the balance of the "Miscellaneous Expenses" group.',
        # 'Show me the closing balance for the "Accounts Payable" ledger account.',
        # 'Show the balance for the "Short-Term Liabilities" group.',
        # 'Show the balance for the "Cash" ledger account.',
        # 'List all ledgers in the "Current Assets" group.',
        # 'Show the credit balance for "Sundry Creditors" ledger account.',
        # 'What is the total balance of all equity groups?',
        # 'Show all transactions posted to the "Revenue" ledger account.',
        # 'Show the balance for the "Long-Term Liabilities" group.',
        # 'What is the total credit balance in the "Sundry Debtors" ledger?',
        # 'List all the groups in the "Assets" category.',
        # 'Show the credit balance for "Bank" ledger account.',
        # 'What is the balance for the "Other Current Liabilities" group?',
        # 'Show the balance for the "Inventory" ledger account.',
        # 'Show the total credit balance for the "Fixed Assets" group.',
        # 'Show me all transactions for "Accounts Receivable" ledger account.',
        # 'What is the total balance for the "Revenue" group this quarter?',
        # 'Show the balance for "Purchases" ledger account as of last month.',
        # 'Show the balance for the "Sundry Creditors" group for last year.',
        # 'What is the total debit balance for "Cash in Bank" ledger account?',
        # 'List all ledgers in the "Liabilities" group.',
        # 'Show the balance for "Loans" ledger account.',
        # 'Show the balance for the "Current Liabilities" group.',
        # 'List all transactions in the "Capital" ledger account.',
        # 'What is the total credit balance for the "Fixed Assets" group this year?',
        # 'Show all transactions for the "Loan Receivable" ledger account.',
        # 'Show the balance for the "Fixed Assets" group for this financial year.',
        # 'Show me the balance for the "Accounts Receivable" ledger account for this quarter.',
        # 'Show the balance for the "Income" group this year.',
        # 'Show the total debit balance in the "Sundry Debtors" ledger account.',
        # 'What is the total balance for the "Other Liabilities" group for the past year?',
        # 'Show the balance for the "Rent Payable" ledger account.',
        # 'What is the balance for the "Non-Current Liabilities" group?',
        # 'Show all transactions for the "Revenue" ledger account in the past 6 months.',
        # 'What is the total balance for the "Other Current Assets" group?',
        # 'Show the debit balance for "Purchases" ledger account.',
        # 'What is the total debit balance for the "Equity" group?',
        # 'Show me all transactions for the "Bank" ledger in the last quarter.',
        # 'Show the total balance for the "Fixed Assets" group as of today.',
        # 'Show all transactions for the "Accounts Receivable" ledger in the last 30 days.',
        # 'Show the balance for "Sundry Debtors" group for the current year.',
        # 'Show the debit balance for the "Cash" ledger account.',
        # 'Show me the credit balance for the "Sundry Creditors" group.',
        # 'Show the transactions for the "Income" ledger account.',
        # 'What is the total balance for the "Capital" group this quarter?',
        # 'Show the debit balance for "Accounts Receivable" ledger account.',
        # 'What is the total balance for the "Assets" group for the current quarter?',
        # 'Show the balance for the "Cash in Hand" ledger account.',
        # 'Show the balance for the "Short-Term Liabilities" group for this quarter.',
        # 'Show the balance for the "Loan Receivable" ledger account as of last quarter.',
        # 'Show the balance for the "Expenses" group for last year.',
        # 'What is the total debit balance in the "Accounts Payable" ledger?',
        # 'Show the total credit balance in the "Loans" group.',
        # 'List all debit transactions for the "Purchases" ledger.',
        # 'What is the balance for the "Other Assets" group?',
        # 'Show the total credit balance for "Accounts Payable" ledger account.',
        # 'Show the balance for the "Current Liabilities" group for this quarter.',
        # 'Show me the debit entries for the "Revenue" ledger account.',
        # 'Show the balance for the "Fixed Assets" group for last month.',
        # 'Show all transactions in the "Capital" ledger account for the current year.',
        # 'Show the balance for the "Sundry Debtors" group as of last quarter.',
        # 'Show me the balance of ledger account Caleb Bhasin (Vendor)',
        # 'What is the total credit balance for all customers?',
        # 'Show me the debit balance of ledger account Zansi Bhandari.',
        # 'List all transactions for ledger account Caleb Bhasin (Vendor)',
        # 'Show the opening balance for ledger account Oviya Gole',
        # 'What is the total balance for the group "Sundry Debtors"?',
        # 'Show the total credit balance for all groups.',
        # 'What is the balance for the "Purchase" group?',
        # 'Show the recent transactions in the "Bank" ledger.',
        # 'What is the total debit balance for the "Expenses" group?',
        # 'Show the list of ledgers in the "Sundry Creditors" group.',
        # 'What is the total balance of the "Assets" group?',
        # 'Show me the opening balance for ledger account "Cash in Hand".',
        # 'What is the total balance for all income groups combined?',
        # 'Show the closing balance for the "Sales" ledger account.',
        # 'List all transactions for the ledger account "Expenses" in the last month.',
        # 'Show the balance for the "Liabilities" group for this quarter.',
    ]

    for q in queries:
        # results = matcher.match_query(q)
        results = matcher.match_query_with_phrases(q, masters_dict)
        print(f"\nQuery: {q}")
        for r in results:
            print(r)
