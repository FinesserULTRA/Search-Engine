import json

def read_json(file_path):
    """Reads JSON data from a file."""
    try:
        with open(file_path, "r", encoding='utf-8-sig') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def write_json(file_path, data):
    """Writes JSON data to a file."""
    with open(file_path, "w", encoding='utf-8-sig') as f:
        json.dump(data, f)

def search_tag(json_dict, tag):
    value = json_dict.get(tag)
    if value is not None:
        return value
    else:
        return f"Tag '{tag}' not found in the JSON."
