import json
import pandas as pd
from functools import lru_cache
from typing import Dict, Any

@lru_cache(maxsize=1000)
def read_json(file_path: str) -> Dict[str, Any]:
    """Cached JSON file reading"""
    try:
        with open(file_path, "r", encoding='utf-8-sig') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def write_json(file_path: str, data: Dict[str, Any]):
    """Write JSON and clear cache for that file"""
    with open(file_path, "w", encoding='utf-8-sig') as f:
        json.dump(data, f)
    read_json.cache_clear()  # Clear cache when file is updated

@lru_cache(maxsize=100)
def read_csv(file_path: str) -> pd.DataFrame:
    """Cached CSV file reading"""
    try:
        return pd.read_csv(file_path)
    except FileNotFoundError:
        return pd.DataFrame()

def write_csv(file_path: str, df: pd.DataFrame):
    """Write CSV and clear cache for that file"""
    df.to_csv(file_path, index=False)
    read_csv.cache_clear()  # Clear cache when file is updated

def clear_cache():
    """Clear all file operation caches"""
    read_json.cache_clear()
    read_csv.cache_clear()

def search_tag(json_dict, tag):
    value = json_dict.get(tag)
    if value is not None:
        return value
    else:
        return f"Tag '{tag}' not found in the JSON."
