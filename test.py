import time
from functools import lru_cache
import json
from backend.utils.file_io import search_tag, search_value

def measure_time(func, *args):
    start_time = time.time()
    result = func(*args)
    end_time = time.time()
    return result, end_time - start_time


@lru_cache(maxsize=None)
def load_json(file_path):
    try:
        with open(file_path, "r", encoding='utf-8-sig') as f:
            return json.load(f)
    except json.JSONDecodeError:
        print(f"Error: The file {file_path} is empty or not a valid JSON.")
        return None
    except FileNotFoundError:
        print(f"Error: The file {file_path} was not found.")
        return None


if __name__ == "__main__":
    file_path = "C:\\Users\\PC\\VSCODE\\Search-Engine\\backend\\index data\\lexicon\\lexicon.json"

    # First call
    result, duration = measure_time(load_json, file_path)
    print(f"First call - Time to open and load JSON: {duration} seconds")

    # # Second call
    # result, duration = measure_time(load_json, file_path)
    # print(f"Second call - Time to open and load JSON: {duration} seconds")

    # # Second call
    # result, duration = measure_time(load_json, file_path)
    # print(f"Third call - Time to open and load JSON: {duration} seconds")
    # # print(result)
    tag = "good"
    val = 347107
    print(f"{tag}: {search_tag(result, tag)}")
    print(f"{val}: {search_value(result, val)}")
    # print(load_json(file_path))