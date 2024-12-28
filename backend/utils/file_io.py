import json
import pandas as pd
from functools import lru_cache
from typing import Dict, Any
import logging
import os

@lru_cache(maxsize=1000)
def read_json(file_path: str) -> Dict[str, Any]:
    """Cached JSON file reading"""
    if not os.path.isfile(file_path):
        return {}
    try:
        with open(file_path, "r", encoding='utf-8-sig') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logging.warning(f"Warning: Corrupted JSON in {file_path}, error: {e}")
        return {}
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

def search_value(json_dict, value):
    tags = [tag for tag, val in json_dict.items() if val == value]
    if tags:
        return tags
    else:
        return f"Value '{value}' not found in the JSON."

def read_json(filepath: str) -> dict:
    try:
        if not os.path.exists(filepath):
            # print(f"File not found: {filepath}")
            return {}
        with open(filepath, 'r', encoding='utf-8-sig') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error reading JSON file {filepath}: {str(e)}")
        return {}

def write_json(filepath: str, data: dict):
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w', encoding='utf-8-sig') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        print(f"Error writing JSON file {filepath}: {str(e)}")

def read_csv(filepath: str) -> pd.DataFrame:
    try:
        if not os.path.exists(filepath):
            print(f"File not found: {filepath}")
            return pd.DataFrame()
        return pd.read_csv(filepath, encoding='utf-8-sig')
    except Exception as e:
        print(f"Error reading CSV file {filepath}: {str(e)}")
        return pd.DataFrame()

def write_csv(filepath: str, df: pd.DataFrame):
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
    except Exception as e:
        print(f"Error writing CSV file {filepath}: {str(e)}")