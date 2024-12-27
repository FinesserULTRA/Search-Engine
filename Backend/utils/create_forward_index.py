import pandas as pd
import json
import os
from tokenizer import Tokenizer  # Change import

BATCH_SIZE = 50000

def create_forward_index(input_csv, lexicon_file, output_dir, id_column, text_columns):
    tokenizer = Tokenizer()  # Use original Tokenizer
    with open(lexicon_file, "r", encoding="utf-8-sig") as f:
        lexicon = json.load(f)

    os.makedirs(output_dir, exist_ok=True)

    total_rows = sum(1 for _ in pd.read_csv(input_csv))
    estimated_files = (total_rows // BATCH_SIZE) + 1
    print(f"Estimated total files to be created: {estimated_files}")

    chunk_id = 0
    total_files = 0
    for chunk in pd.read_csv(input_csv, chunksize=BATCH_SIZE):
        forward_index = {}

        for _, row in chunk.iterrows():
            item_id = row[id_column]
            text = " ".join(str(row[col]) for col in text_columns)

            # Use enhanced tokenizer
            tokens = tokenizer.tokenize_with_spacy(text)
            word_ids = set()

            for word in tokens:
                if word in lexicon:
                    word_ids.add(lexicon[word])

            forward_index[item_id] = list(word_ids)

        start_id = (chunk_id * BATCH_SIZE) + 1
        end_id = start_id + len(forward_index) - 1
        output_file = os.path.join(output_dir, f"forward_index_{start_id}-{end_id}.json")
        with open(output_file, "w", encoding="utf-8-sig") as f:
            json.dump(forward_index, f)

        print(f"Created forward_index_{chunk_id}.json with {len(forward_index)} items")
        chunk_id += 1
        total_files += 1

    print(f"Forward index creation complete! Total files created: {total_files}")

if __name__ == "__main__":
    create_forward_index(
        "../data/hotels_cleaned.csv",
        "../index data/lexicon/lexicon.json",
        "../index data/forward_index/hotels",
        "hotel_id",
        ["name", "locality", "street-address", "region"]
    )
    create_forward_index(
        "../data/reviews_cleaned.csv",
        "../index data/lexicon/lexicon.json",
        "../index data/forward_index/reviews",
        "rev_id",
        ["title", "text"]
    )
