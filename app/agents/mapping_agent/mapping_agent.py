from .retriever import retrieve_candidates

def map_tables(question, top_k=5):
 return retrieve_candidates(question, top_k=top_k)
