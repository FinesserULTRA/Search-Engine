import json
import os
from collections import defaultdict
import multiprocessing as mp
import traceback

BATCH_SIZE = 20000

def load_forward_index_file(file_path):
    """Safely load a forward index JSON or return an empty dict."""
    try:
        if not os.path.exists(file_path):
            return {}
        with open(file_path, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error loading {file_path}: {str(e)}")
        print(f"Error location: line {e.lineno}, column {e.colno}")
        return {}
    except Exception as e:
        print(f"Unexpected error loading {file_path}: {str(e)}")
        return {}

def safe_json_load(file_path):
    """Safely load any JSON file with proper error handling."""
    try:
        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8-sig") as f:
                content = f.read()
                try:
                    return json.loads(content)
                except json.JSONDecodeError as e:
                    print(f"Invalid JSON in {file_path}")
                    print(f"Error: {str(e)}")
                    print(f"Error location: line {e.lineno}, column {e.colno}")
                    # Try to recover by removing problematic characters
                    content = content.replace("\x00", "")
                    try:
                        return json.loads(content)
                    except:
                        return {}
        return {}
    except Exception as e:
        print(f"Error reading {file_path}: {str(e)}")
        return {}

def map_forward_to_partial_inverted(forward_index_file):
    """
    'Map' step: 
    Read one forward_index_*.json, build and return a partial inverted index dict in memory.
    
    partial_inverted = {
      word_id(str): {
        "docs": [
          {"id": doc_id, "freq": freq, "positions": [...], "fields": [...]},
          ...
        ]
      },
      ...
    }
    """
    print(f"[Map] Processing {forward_index_file} in worker {os.getpid()}")
    partial_inverted = {}

    forward_data = load_forward_index_file(forward_index_file)
    if not forward_data:
        return partial_inverted  # empty

    for doc_id, doc_info in forward_data.items():
        word_positions = doc_info["word_positions"]   # e.g. {"1234": [0,5], ...}
        field_matches = doc_info["field_matches"]     # e.g. {"name": ["1234","1567"], ...}

        for word_id_str, positions in word_positions.items():
            word_id = int(word_id_str)
            freq = len(positions)

            # find which fields had this word_id
            fields_used = []
            for field, wlist in field_matches.items():
                if word_id_str in wlist:
                    fields_used.append(field)

            # Insert into partial_inverted
            if word_id_str not in partial_inverted:
                partial_inverted[word_id_str] = {"docs": []}
            partial_inverted[word_id_str]["docs"].append({
                "id": doc_id,
                "freq": freq,
                "positions": positions,
                "fields": fields_used
            })

    return partial_inverted

def reduce_partials_and_write(all_partials, output_dir):
    """
    'Reduce' step:
    Merge all partial_inverted dicts from the workers, 
    then write final inverted_index_{start}-{end}.json files.
    """

    # 1) Merge them in memory
    # We'll do a single big dict: merged_inverted[word_id_str]["docs"] => extended list
    print("[Reduce] Merging partial dicts in main process...")
    merged_inverted = {}

    for partial_dict in all_partials:
        for word_id_str, data in partial_dict.items():
            if word_id_str not in merged_inverted:
                merged_inverted[word_id_str] = {"docs": []}
            merged_inverted[word_id_str]["docs"].extend(data["docs"])

    # 2) Now we write them out by word_id range
    #    e.g. word_id => find batch_start, batch_end => open that file, merge, etc.
    #    But simpler is to build an in-memory dict-of-dicts, then dump them.
    #    We'll do a smaller step approach: for each word_id_str, figure out the file name,
    #    load existing on disk if any, unify, then write.

    print("[Reduce] Writing final inverted index JSON files...")
    os.makedirs(output_dir, exist_ok=True)

    for word_id_str, data in merged_inverted.items():
        word_id = int(word_id_str)
        batch_start = (word_id // BATCH_SIZE)*BATCH_SIZE
        batch_end = batch_start + BATCH_SIZE -1
        inv_file = os.path.join(output_dir, f"inverted_index_{batch_start}-{batch_end}.json")

        # Use safe_json_load instead of direct json.load
        existing_data = safe_json_load(inv_file)

        if word_id_str not in existing_data:
            existing_data[word_id_str] = {"docs":[]}

        postings = existing_data[word_id_str]["docs"]

        # Now we unify the docs from 'data["docs"]' into 'postings',
        # summing freq if doc_id matches, etc.
        for new_doc in data["docs"]:
            found = False
            for p in postings:
                if p["id"] == new_doc["id"]:
                    p["freq"] += new_doc["freq"]
                    p["positions"].extend(new_doc["positions"])
                    # unify fields
                    for ff in new_doc["fields"]:
                        if ff not in p["fields"]:
                            p["fields"].append(ff)
                    found = True
                    break
            if not found:
                postings.append(new_doc)

        # finally, write back
        try:
            with open(inv_file, "w", encoding="utf-8-sig") as f:
                json.dump(existing_data, f, indent=4, ensure_ascii=False)
        except Exception as e:
            print(f"Error writing {inv_file}: {str(e)}")
            print(traceback.format_exc())

    print("[Reduce] Done writing all inverted index files!")


def create_or_update_inverted_index_parallel(forward_index_dir, inverted_index_dir, num_workers=None):
    """
    1) Gather forward_index_*.json files
    2) Use multiprocessing Pool to map each file -> partial_inverted dict
    3) Merge them all in main process
    4) Write them out
    """
    forward_files = [
        os.path.join(forward_index_dir, f) 
        for f in os.listdir(forward_index_dir)
        if f.startswith("forward_index_") and f.endswith(".json")
    ]
    forward_files.sort()

    if not forward_files:
        print("No forward_index_*.json files found!")
        return

    if num_workers is None:
        num_workers = min(mp.cpu_count(), len(forward_files))

    print(f"Using {num_workers} processes to map {len(forward_files)} forward_index files...")

    # -- MAP phase --
    with mp.Pool(processes=num_workers) as pool:
        partial_dicts = pool.map(map_forward_to_partial_inverted, forward_files)

    # partial_dicts is a list of partial_inverted dicts from each file

    # -- REDUCE phase --
    reduce_partials_and_write(partial_dicts, inverted_index_dir)

    print("Inverted index creation/update complete (parallel map-reduce)!")


if __name__ == "__main__":
    create_or_update_inverted_index_parallel(
        "../index data/forward_index/hotels", 
        "../index data/inverted_index/hotels",
    )
    create_or_update_inverted_index_parallel(
        "../index data/forward_index/reviews", 
        "../index data/inverted_index/reviews",
    )
