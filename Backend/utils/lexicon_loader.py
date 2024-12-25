import json
from functools import lru_cache
import time


@lru_cache()
def load_lexicon(lexicon_path):
    """Load lexicon with caching for subsequent calls."""
    print("Loading lexicon from disk...")
    with open(lexicon_path, "r", encoding="utf-8-sig") as f:
        lexicon = json.load(f)
    return lexicon


def measure_time(func, *args):
    start_time = time.time()
    result = func(*args)
    end_time = time.time()
    return result, end_time - start_time


if __name__ == "__main__":
    lexicon_path = "../index data/lexicon/final/combined_lexicon.json"
    lexicon, load_time = measure_time(load_lexicon, lexicon_path)
    print(f"Lexicon loaded in {load_time:.8f} seconds")
    print(f"Loaded {len(lexicon)} words from lexicon")
    print(f"Word ID for 'apple': {lexicon.get('apple')}")
    print(f"Word ID for 'banana': {lexicon.get('banana')}")

    new_lexicon, load_time = measure_time(load_lexicon, lexicon_path)
    print(f"Lexicon loaded in {load_time:.8f} seconds")
    print(f"Loaded {len(new_lexicon)} words from lexicon")
    print(f"Word ID for 'apple': {new_lexicon.get('apple')}")
    print(f"Word ID for 'banana': {new_lexicon.get('banana')}")

    new_1lexicon, load_time = measure_time(load_lexicon, lexicon_path)
    print(f"Lexicon loaded in {load_time:.8f} seconds")
    print(f"Loaded {len(new_1lexicon)} words from lexicon")
    print(f"Word ID for 'apple': {new_1lexicon.get('apple')}")
    print(f"Word ID for 'banana': {new_1lexicon.get('banana')}")
