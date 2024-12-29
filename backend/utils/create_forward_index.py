import json
import os
import pandas as pd
import multiprocessing as mp
from functools import partial
from tokenizer import Tokenizer
from file_io import read_json, write_json, read_csv, write_csv

BATCH_SIZE = 20000
global_lexicon = None
global_tokenizer = None

def init_globals(lexicon_file):
    """Initialize the global lexicon + tokenizer in each worker."""
    global global_lexicon, global_tokenizer
    with open(lexicon_file, "r", encoding="utf-8-sig") as f:
        global_lexicon = json.load(f)
    global_tokenizer = Tokenizer()

def process_chunk(chunk_data, id_column, text_columns):
    """Process a DataFrame chunk, building forward_index for each row."""
    global global_lexicon, global_tokenizer
    forward_index = {}

    for _, row in chunk_data.iterrows():
        item_id = str(row[id_column])  # Use string ID as dictionary key
        doc_info = {
            "word_positions": {},
            "word_counts": {},
            "field_matches": {},
        }

        for field in text_columns:
            text = str(row.get(field, ""))
            words = global_tokenizer.tokenize_with_spacy(text)
            doc_info["field_matches"][field] = []

            for pos, word in enumerate(words):
                if word in global_lexicon:
                    word_id = global_lexicon[word]
                    if word_id not in doc_info["word_positions"]:
                        doc_info["word_positions"][word_id] = []
                    doc_info["word_positions"][word_id].append(pos)
                    doc_info["word_counts"][word_id] = (
                        doc_info["word_counts"].get(word_id, 0) + 1
                    )
                    doc_info["field_matches"][field].append(word_id)

        forward_index[item_id] = doc_info
    return forward_index

def create_forward_index(input_csv, lexicon_file, output_dir, id_column, text_columns):
    os.makedirs(output_dir, exist_ok=True)

    # Count total rows for chunked processing
    total_rows = sum(1 for _ in pd.read_csv(input_csv))
    estimated_files = (total_rows // BATCH_SIZE) + 1
    print(f"Estimated total files to be created: {estimated_files}")

    # Use multiprocessing with an initializer
    pool = mp.Pool(initializer=init_globals, initargs=(lexicon_file,))

    process_func = partial(
        process_chunk, id_column=id_column, text_columns=text_columns
    )

    chunk_id = 0
    total_files = 0
    row_start_id = 1  # track the row start for naming files

    for chunk_df in pd.read_csv(input_csv, chunksize=BATCH_SIZE):
        forward_index = pool.apply_async(process_func, (chunk_df,)).get()

        # figure out the naming for this chunk
        num_items = len(forward_index)
        row_end_id = row_start_id + num_items - 1
        output_file = os.path.join(
            output_dir, f"forward_index_{row_start_id}-{row_end_id}.json"
        )

        # write out the forward index
        with open(output_file, "w", encoding="utf-8-sig") as f:
            json.dump(forward_index, f, indent=4)

        print(
            f"Created forward_index_{row_start_id}-{row_end_id}.json with {num_items} items"
        )
        row_start_id += num_items
        chunk_id += 1
        total_files += 1

    pool.close()
    pool.join()
    print(f"Forward index creation complete! Total files created: {total_files}")


if __name__ == "__main__":
    create_forward_index(
        "../data/hotels_cleaned.csv",
        "../index data/lexicon/lexicon.json",
        "../index data/forward_index/hotels",
        "hotel_id",
        ["name", "locality", "street-address", "region"],
    )

    create_forward_index(
        "../data/reviews_cleaned.csv",
        "../index data/lexicon/lexicon.json",
        "../index data/forward_index/reviews",
        "rev_id",
        ["title", "text"],
    )
