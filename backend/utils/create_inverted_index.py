import json
import os
from collections import defaultdict
from lexicon_loader import load_lexicon
from functools import lru_cache
import multiprocessing as mp
from itertools import repeat


BATCH_SIZE = 20000
# Global variables to be shared across processes
LEXICON = None
FORWARD_INDEX_CACHE = {}


def save_inverted_index(inverted_index, output_dir, start_word_id, end_word_id):
    output_file = os.path.join(
        output_dir, f"inverted_index_{start_word_id}-{end_word_id}.json"
    )
    with open(output_file, "w", encoding="utf-8-sig") as f:
        json.dump(inverted_index, f, indent=4)
    print(f"Saved {output_file} with {len(inverted_index)} word IDs")


def init_worker(lexicon_path):
    """Initialize global variables for each worker process"""
    global LEXICON
    LEXICON = load_lexicon(lexicon_path)


def load_forward_index(file_path):
    """Cache forward index files to avoid multiple reads"""
    if file_path not in FORWARD_INDEX_CACHE:
        with open(file_path, "r", encoding="utf-8-sig") as f:
            FORWARD_INDEX_CACHE[file_path] = json.load(f)
    return FORWARD_INDEX_CACHE[file_path]


def process_forward_index_file(args):
    forward_index_file, output_dir = args
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

    print(f"Processing {forward_index_file}")
    forward_index = load_forward_index(forward_index_file)

    for doc_id, doc_info in forward_index.items():
        # Rest of the processing logic remains the same
        word_positions = doc_info["word_positions"]
        word_counts = doc_info["word_counts"]

        for word_id, freq in word_counts.items():
            positions = word_positions.get(str(word_id), [])
            start_id = (int(word_id) // BATCH_SIZE) * BATCH_SIZE
            batch_key = f"{start_id}-{start_id + BATCH_SIZE - 1}"

            fields = [
                field
                for field, words in doc_info["field_matches"].items()
                if int(word_id) in words
            ]

            batch_indices[batch_key][word_id]["docs"].append(
                {
                    "id": doc_id,
                    "freq": freq,
                    "positions": positions,
                    "fields": fields,
                }
            )

            entry = batch_indices[batch_key][word_id]
            entry["total_occurrences"] += freq
            for field in fields:
                entry["field_occurrences"][field] += 1

            if positions:
                n = len(entry["docs"])
                old_avg = entry["avg_position"]
                new_pos_avg = sum(positions) / len(positions)
                entry["avg_position"] = ((old_avg * (n - 1)) + new_pos_avg) / n

    # Save all batches for this file
    for batch_key, index_data in batch_indices.items():
        if index_data:
            save_inverted_index(
                dict(index_data), output_dir, *map(int, batch_key.split("-"))
            )


def create_inverted_index(forward_index_dir, output_dir):
    forward_index_files = [
        os.path.join(forward_index_dir, f)
        for f in os.listdir(forward_index_dir)
        if f.startswith("forward_index_") and f.endswith(".json")
    ]

    os.makedirs(output_dir, exist_ok=True)

    lexicon_path = "../index data/lexicon/lexicon.json"

    # Process files in parallel with shared resources
    num_processes = min(mp.cpu_count(), len(forward_index_files))
    print(f"Using {num_processes} processes")

    with mp.Pool(
        processes=num_processes, initializer=init_worker, initargs=(lexicon_path,)
    ) as pool:
        pool.map(
            process_forward_index_file, zip(forward_index_files, repeat(output_dir))
        )

    print("Inverted index creation complete!")


if __name__ == "__main__":
    # create_inverted_index(
    #     "../index data/forward_index/hotels", "../index data/inverted_index/hotels"
    # )
    create_inverted_index(
        "../index data/forward_index/reviews", "../index data/inverted_index/reviews"
    )