import json
import os
from collections import defaultdict
from lexicon_loader import load_lexicon
from functools import lru_cache

BATCH_SIZE = 50000


@lru_cache(maxsize=1)
def get_batch_mapping():
    """Cache batch range mappings"""
    return {}


def save_inverted_index(inverted_index, output_dir, start_word_id, end_word_id):
    output_file = os.path.join(
        output_dir, f"inverted_index_{start_word_id}-{end_word_id}.json"
    )
    with open(output_file, "w", encoding="utf-8-sig") as f:
        json.dump(inverted_index, f, indent=4)
    print(f"Saved {output_file} with {len(inverted_index)} word IDs")


def create_inverted_index(forward_index_dir, output_dir):
    forward_index_files = [
        os.path.join(forward_index_dir, f)
        for f in os.listdir(forward_index_dir)
        if f.startswith("forward_index_") and f.endswith(".json")
    ]

    os.makedirs(output_dir, exist_ok=True)

    # First, get the maximum word ID from the lexicon using cached loader
    lexicon_path = "../index data/lexicon/lexicon.json"
    lexicon = load_lexicon(lexicon_path)
    max_word_id = max(int(word_id) for word_id in lexicon.values())

    # Calculate number of files needed
    num_files = (max_word_id // BATCH_SIZE) + 1
    print(f"Max word ID: {max_word_id}")
    print(f"Number of files needed: {num_files}")

    # Process forward index files in chunks
    batch_indices = defaultdict(
        lambda: defaultdict(
            lambda: {
                "docs": [],
                "total_occurrences": 0,
                "field_occurrences": defaultdict(int),
                "avg_position": 0.0,
            }
        )
    )

    for forward_index_file in forward_index_files:
        print(f"Processing {forward_index_file}")
        with open(forward_index_file, "r", encoding="utf-8-sig") as f:
            forward_index = json.load(f)

            for doc_id, doc_info in forward_index.items():
                word_positions = doc_info["word_positions"]
                word_counts = doc_info["word_counts"]

                for word_id, freq in word_counts.items():
                    # Get positions for this word
                    positions = word_positions.get(str(word_id), [])

                    # Calculate batch key
                    start_id = (int(word_id) // BATCH_SIZE) * BATCH_SIZE
                    batch_key = f"{start_id}-{start_id + BATCH_SIZE - 1}"

                    # Get fields where this word appears
                    fields = [
                        field
                        for field, words in doc_info["field_matches"].items()
                        if int(word_id) in words
                    ]

                    # Update inverted index entry
                    batch_indices[batch_key][word_id]["docs"].append(
                        {
                            "id": doc_id,
                            "freq": freq,
                            "positions": positions,
                            "fields": fields,
                        }
                    )

                    # Update overall statistics
                    entry = batch_indices[batch_key][word_id]
                    entry["total_occurrences"] += freq
                    for field in fields:
                        entry["field_occurrences"][field] += 1

                    if positions:
                        # Update running average position
                        n = len(entry["docs"])
                        old_avg = entry["avg_position"]
                        new_pos_avg = sum(positions) / len(positions)
                        entry["avg_position"] = ((old_avg * (n - 1)) + new_pos_avg) / n

            # Save batch when it gets too large
            for batch_key, index_data in batch_indices.items():
                if len(index_data) >= BATCH_SIZE:
                    save_inverted_index(
                        dict(index_data), output_dir, *map(int, batch_key.split("-"))
                    )
                    batch_indices[batch_key].clear()

    # Save remaining batches
    for batch_key, index_data in batch_indices.items():
        if index_data:
            save_inverted_index(
                dict(index_data), output_dir, *map(int, batch_key.split("-"))
            )

    print(f"Inverted index creation complete!")


if __name__ == "__main__":
    # create_inverted_index(
    #     "../index data/forward_index/hotels", "../index data/inverted_index/hotels"
    # )
    create_inverted_index(
        "../index data/forward_index/reviews", "../index data/inverted_index/reviews"
    )
