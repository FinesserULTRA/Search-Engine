import json

def read_json(file_path):
    """Reads JSON data from a file."""
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def write_json(file_path, data):
    """Writes JSON data to a file."""
    with open(file_path, "w") as f:
        json.dump(data, f)
