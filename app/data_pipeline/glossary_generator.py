from app.core.config import settings
from sklearn.cluster import KMeans
from sklearn.metrics import pairwise_distances_argmin_min
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.feature_extraction.text import TfidfVectorizer
from tqdm import tqdm

EMBED_MODEL = settings.embed_model_path
embedder = SentenceTransformer(EMBED_MODEL)

def extract_keywords_tfidf(docs, topk=5):
    texts = [d["text"] for d in docs]
    vec = TfidfVectorizer(max_features=5000, ngram_range=(1,2))
    X = vec.fit_transform(texts)
    features = np.array(vec.get_feature_names_out())
    keywords = []
    for i in range(X.shape[0]):
        row = X[i].toarray().ravel()
        top_idx = row.argsort()[-topk:][::-1]
        keywords.append([features[j] for j in top_idx if row[j] > 0])
    return keywords

def generate_glossary_from_docs(docs, n_terms=100):
    texts = [d["text"] for d in docs]
    embs = embedder.encode(texts, show_progress_bar=True)
    k = max(5, min(200, len(docs)//10))
    kmeans = KMeans(n_clusters=k, random_state=42)
    labels = kmeans.fit_predict(embs)
    centers = kmeans.cluster_centers_
    closest, _ = pairwise_distances_argmin_min(centers, embs)

    # keywords by TFIDF
    doc_keywords = extract_keywords_tfidf(docs, topk=8)

    glossary = []
    for i, center_idx in enumerate(closest):
        sample_doc = docs[center_idx]
        candidate_terms = doc_keywords[center_idx][:3]
        term = candidate_terms[0] if candidate_terms else sample_doc["table"]
        definition = "Term auto-generated: " + " / ".join(doc_keywords[center_idx][:5])
        glossary.append({
            "term": term,
            "definition": definition,
            "representative_table": sample_doc["id"],
            "count": int(sum(1 for l in labels if l==i))
        })
    # sort by cluster size and limit
    glossary = sorted(glossary, key=lambda x: x["count"], reverse=True)[:n_terms]
    return glossary