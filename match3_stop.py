import re
import pymysql
import pandas as pd
from rapidfuzz import fuzz, process

# ---------------------------------------
# 1. DB connection and master data fetch
# ---------------------------------------
def get_master_data(db_config):
    conn = pymysql.connect(**db_config)
    query = """
        SELECT name , 'category'  as category, 'ledger' as master_name
        FROM ledgers where company_id = 1
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# ---------------------------------------
# 2. Normalization helper
# ---------------------------------------
def normalize(text):
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)  # keep alphanumeric
    text = re.sub(r"\s+", " ", text).strip()
    return text

# ---------------------------------------
# 3. Build dynamic stop words
# ---------------------------------------
BASE_STOP_WORDS = {
    "what", "is", "are", "the", "for", "a", "an", "of", "and", "in", "on", "at",
    "from", "to", "by", "this", "that", "did", "we", "do", "amount", "total",
    "how", "much", "please", "give", "me", "show","what's", "How's", "Does" , "compare", "list",
}

GENERIC_FINANCE_WORDS = {"trend", "sales", "sale", "monthly", "quarterly", "yearly", "report"}


def build_master_vocab(master_df):
    vocab = set()
    for name in master_df["name"]:
        vocab.update(name.split())
    return vocab

def build_dynamic_stop_words(master_df):
    vocab = set()
    for name in master_df['master_name']:
        tokens = normalize(name).split()
        vocab.update(tokens)
    stop_words = BASE_STOP_WORDS | GENERIC_FINANCE_WORDS
    return {w for w in stop_words if w not in vocab}

# ---------------------------------------
# 4. Remove stopwords
# ---------------------------------------
def remove_stopwords(text, stop_words, master_vocab):
    tokens = normalize(text).split()
    # Remove tokens that are stopwords or are short noise words (< 2 chars) not in master vocab
    clean_tokens = [t for t in tokens if (t not in stop_words) and (len(t) > 1 or t in master_vocab)]
    return " ".join(clean_tokens)


# ---------------------------------------
# 5. Create n-grams
# ---------------------------------------
def generate_ngrams(tokens, n):
    return [" ".join(tokens[i:i+n]) for i in range(len(tokens)-n+1)]

def generate_all_ngrams(tokens, max_n=5):
    all_ngrams = []
    for n in range(min(len(tokens), max_n), 0, -1):  # longest first
        all_ngrams.extend(generate_ngrams(tokens, n))
    return all_ngrams

def match_query(query, master_df, stop_words, master_vocab, threshold=80):
    clean_query = remove_stopwords(query, stop_words, master_vocab)
    tokens = clean_query.split()
    ngrams = generate_all_ngrams(tokens)

    matches = []
    for ng in ngrams:
        results = process.extract(ng, master_df["name"], scorer=fuzz.token_sort_ratio, limit=3)
        for match_name, score, idx in results:
            print(f'match name is {match_name}')
            print(f'score is {score}')
            print(f'idx is {idx}')
            if score >= threshold:
                matches.append({
                    "query": query,
                    "ngram": ng,
                    "category": master_df.iloc[idx]["category"],
                    # "master_id": master_df.iloc[idx]["master_id"],
                    "name": master_df.iloc[idx]["name"],
                    "score": score
                })
    # Remove duplicates by master_id keeping best score
    unique_matches = {}
    for m in matches:
        mid = m["master_id"]
        if mid not in unique_matches or m["score"] > unique_matches[mid]["score"]:
            unique_matches[mid] = m

    return list(unique_matches.values())



# ---------------------------------------
# 6. Match query to master data
# ---------------------------------------
# def match_query_to_master(query, master_df, stop_words, min_score=80):
#     cleaned = remove_stopwords(query, stop_words)
#     tokens = cleaned.split()

#     results = []

#     # Generate n-grams up to length of query
#     all_ngrams = []
#     for n in range(1, min(len(tokens), 5) + 1):  # up to 5-word entities
#         all_ngrams.extend(generate_ngrams(tokens, n))

#     print(f'generated ngrams are {all_ngrams}')
#     for category in master_df['category'].unique():
#         df_cat = master_df[master_df['category'] == category]

#         # Use fuzzy matching for each n-gram against master names in this category
#         for ng in all_ngrams:
#             print(f'checking for {ng}')
#             match, score, idx = process.extractOne(
#                 ng,
#                 df_cat['master_name'].apply(normalize).tolist(),
#                 scorer=fuzz.token_sort_ratio
#             )
#             print(f'score {score}, match is {match}')
#             if score >= min_score:
#                 matched_row = df_cat.iloc[idx]
#                 results.append({
#                     "query": query,
#                     "category": matched_row['category'],
#                     "master_id": matched_row['master_id'],
#                     "master_name": matched_row['master_name'],
#                     "score": score
#                 })

#     # Sort by score descending
#     results = sorted(results, key=lambda x: x['score'], reverse=True)
#     return results

# ---------------------------------------
# 7. Example usage
# ---------------------------------------
if __name__ == "__main__":
    # Replace with your DB credentials
    db_config = {
        "host": "3.108.40.112",
        "user": "talkingtotals",
        "password": "T@lkingTotals!@#$",
        "database": "tt_customer_db_1",
        "charset": "utf8mb4"
    }

    # Fetch master data
    # print(f"fetching master")
    master_df = get_master_data(db_config)
    # print(f'maser df is {master_df}')

    master_vocab = build_master_vocab(master_df)

    # Build dynamic stop words
    print(f'buidling stop words')
    stop_words = build_dynamic_stop_words(master_df)

    print(f'Stop words are {stop_words}')

    # Example queries
    queries = [
        "What’s the monthly sale trend for Areo Manufac?",
        "How much 3M Tape 79 did we sell this month?"
    ]

    print('checking queries')
    for q in queries:
        print(f"\nQuery: {q}")
        # matches = match_query_to_master(q, master_df, stop_words)
        matches = match_query(q, master_df, stop_words, master_vocab, threshold=70)
        for m in matches:
            print(m)
