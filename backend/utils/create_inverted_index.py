import json
import os
from collections import defaultdict

BATCH_SIZE = 20000

def load_inverted_index_file(file_path):
    """Safely load an existing inverted index JSON or return an empty structure."""
    if not os.path.exists(file_path):
        return {}
    with open(file_path, "r", encoding="utf-8-sig") as f:
        return json.load(f)

def save_inverted_index_file(inverted_data, file_path):
    """Write the updated inverted data to disk safely."""
    with open(file_path, "w", encoding="utf-8-sig") as f:
        json.dump(inverted_data, f, indent=4)

def update_inverted_index_from_forward(forward_file, inverted_dir):
    """Read a forward_index_X-Y.json, then update the relevant inverted_index files."""
    if not os.path.exists(forward_file):
        print(f"Forward file not found: {forward_file}")
        return

    with open(forward_file, "r", encoding="utf-8-sig") as f:
        forward_data = json.load(f)

    for doc_id, doc_info in forward_data.items():
        word_positions = doc_info["word_positions"]
        # doc_info["word_counts"]
        field_matches = doc_info["field_matches"]

        for word_id_str, positions in word_positions.items():
            word_id = int(word_id_str)
            batch_start = (word_id // BATCH_SIZE)*BATCH_SIZE
            batch_end = batch_start + BATCH_SIZE -1
            inv_file = os.path.join(
                inverted_dir, f"inverted_index_{batch_start}-{batch_end}.json"
            )

            # load or init
            inv_data = load_inverted_index_file(inv_file)
            if str(word_id) not in inv_data:
                inv_data[str(word_id)] = {"docs":[]}

            # find if doc_id is already present
            postings = inv_data[str(word_id)]["docs"]
            freq = len(positions)
            fields_used = []
            # find which fields had this word_id
            for field, wlist in field_matches.items():
                if word_id in wlist:
                    fields_used.append(field)

            found = False
            for posting in postings:
                if posting["id"] == doc_id:
                    # update freq, positions, fields, etc.
                    posting["freq"] += freq
                    posting["positions"].extend(positions)
                    for ff in fields_used:
                        if ff not in posting["fields"]:
                            posting["fields"].append(ff)
                    found = True
                    break

            if not found:
                postings.append({
                    "id": doc_id,
                    "freq": freq,
                    "positions": positions,
                    "fields": fields_used
                })

            # save updated
            save_inverted_index_file(inv_data, inv_file)

def create_or_update_inverted_index(forward_index_dir, inverted_index_dir):
    """For each forward_index_*.json, update the relevant inverted_index_*.json files incrementally."""
    os.makedirs(inverted_index_dir, exist_ok=True)

    forward_files = [
        f for f in os.listdir(forward_index_dir)
        if f.startswith("forward_index_") and f.endswith(".json")
    ]
    forward_files.sort()  # optional, if you want sorted processing

    for ffile in forward_files:
        full_path = os.path.join(forward_index_dir, ffile)
        print(f"Updating from {full_path}")
        update_inverted_index_from_forward(full_path, inverted_index_dir)

    print("Inverted index creation/update complete!")


if __name__ == "__main__":
    # Example usage
    lexicon_path = "../index data/lexicon/lexicon.json"
    create_or_update_inverted_index(
        "../index data/forward_index/hotels", 
        "../index data/inverted_index/hotels"
    )
    create_or_update_inverted_index(
        "../index data/forward_index/reviews", 
        "../index data/inverted_index/reviews"
    )
