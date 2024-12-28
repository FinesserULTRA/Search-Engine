import pandas as pd
import json
import os
from tokenizer import Tokenizer
import multiprocessing as mp
from functools import partial

BATCH_SIZE = 20000
global_lexicon = None
global_tokenizer = None


def init_globals(lexicon_file):
    global global_lexicon, global_tokenizer
    with open(lexicon_file, "r", encoding="utf-8-sig") as f:
        global_lexicon = json.load(f)
    global_tokenizer = Tokenizer()


def process_chunk(chunk_data, id_column, text_columns):
    global global_lexicon, global_tokenizer
    forward_index = {}

    for _, row in chunk_data.iterrows():
        item_id = row[id_column]
        doc_info = {
            "word_positions": {},
            "word_counts": {},
            "field_matches": {},
            "total_words": 0,
        }

        for field in text_columns:
            text = str(row[field])
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

            doc_info["total_words"] += len(words)

        forward_index[item_id] = doc_info

    return forward_index


def create_forward_index(input_csv, lexicon_file, output_dir, id_column, text_columns):
    os.makedirs(output_dir, exist_ok=True)

    # Initialize pool with global variables
    pool = mp.Pool(initializer=init_globals, initargs=(lexicon_file,))

    total_rows = sum(1 for _ in pd.read_csv(input_csv))
    estimated_files = (total_rows // BATCH_SIZE) + 1
    print(f"Estimated total files to be created: {estimated_files}")

    chunk_id = 0
    total_files = 0

    # Create partial function with fixed arguments
    process_func = partial(
        process_chunk, id_column=id_column, text_columns=text_columns
    )

    for chunk in pd.read_csv(input_csv, chunksize=BATCH_SIZE):
        forward_index = pool.apply_async(process_func, (chunk,)).get()

        start_id = (chunk_id * BATCH_SIZE) + 1
        end_id = start_id + len(forward_index) - 1
        output_file = os.path.join(
            output_dir, f"forward_index_{start_id}-{end_id}.json"
        )

        with open(output_file, "w", encoding="utf-8-sig") as f:
            json.dump(forward_index, f, indent=4)

        print(
            f"Created forward_index_{start_id}-{end_id}.json with {len(forward_index)} items"
        )
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
