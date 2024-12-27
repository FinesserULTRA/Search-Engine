import json
import os
from collections import defaultdict
from lexicon_loader import load_lexicon
from functools import lru_cache

BATCH_SIZE = 50000

@lru_cache(maxsize=1)
def get_batch_mapping():
    """Cache batch range mappings"""
    return {}  # Will be populated during processing

def save_inverted_index(inverted_index, output_dir, start_word_id, end_word_id):
    output_file = os.path.join(
        output_dir, f"inverted_index_{start_word_id}-{end_word_id}.json"
    )
    with open(output_file, "w", encoding="utf-8-sig") as f:
        json.dump(inverted_index, f)
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
    batch_indices = defaultdict(lambda: defaultdict(list))
    
    for forward_index_file in forward_index_files:
        print(f"Processing {forward_index_file}")
        with open(forward_index_file, "r", encoding="utf-8-sig") as f:
            forward_index = json.load(f)
            
            # Group by batch to minimize memory usage
            for doc_id, word_ids in forward_index.items():
                for word_id in word_ids:
                    start_id = (int(word_id) // BATCH_SIZE) * BATCH_SIZE
                    end_id = start_id + BATCH_SIZE - 1
                    batch_key = f"{start_id}-{end_id}"
                    batch_indices[batch_key][str(word_id)].append(doc_id)
                    
            # Save batches as they fill up
            for batch_key, index_data in batch_indices.items():
                if len(index_data) >= BATCH_SIZE:
                    save_inverted_index(dict(index_data), output_dir, 
                                     *map(int, batch_key.split("-")))
                    batch_indices[batch_key].clear()

    # Save remaining batches
    for batch_key, index_data in batch_indices.items():
        if index_data:
            save_inverted_index(dict(index_data), output_dir, 
                             *map(int, batch_key.split("-")))

    print(f"Inverted index creation complete! Total files created: {num_files}")


if __name__ == "__main__":
    create_inverted_index(
        "../index data/forward_index/hotels", "../index data/inverted_index/hotels"
    )

    create_inverted_index(
        "../index data/forward_index/reviews", "../index data/inverted_index/reviews"
    )
