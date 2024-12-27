from functools import lru_cache
from typing import Dict, Set
import json

@lru_cache(maxsize=1000)
def get_inverted_batch(batch_key: str, doc_type: str, index_dir: str) -> Dict[str, list]:
    """Cache inverted index batches"""
    try:
        with open(f"{index_dir}/{doc_type}/inverted_index_{batch_key}.json", "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

@lru_cache(maxsize=100)
def get_doc_ids_for_token(token_id: int, doc_type: str, index_dir: str) -> Set[str]:
    """Get document IDs for a token with caching"""
    batch_start = (token_id // 50000) * 50000
    batch_end = batch_start + 49999
    batch_key = f"{batch_start}-{batch_end}"
    
    batch = get_inverted_batch(batch_key, doc_type, index_dir)
    return set(batch.get(str(token_id), []))
