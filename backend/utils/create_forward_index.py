import pandas as pd
import json
import os
from tokenizer import Tokenizer

BATCH_SIZE = 20000


def create_forward_index(
    input_csv, lexicon_file, output_dir, id_column, text_columns, is_review=False
):
    tokenizer = Tokenizer()
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

            # Enhanced document info
            doc_info = {
                "word_positions": {},  # word -> list of positions
                "word_counts": {},  # word -> frequency
                "field_matches": {},  # field -> list of words
                "total_words": 0,
            }

            for field in text_columns:
                text = str(row[field])
                words = tokenizer.tokenize_with_spacy(text)

                # Track words in this field
                doc_info["field_matches"][field] = []

                for pos, word in enumerate(words):
                    if word in lexicon:
                        word_id = lexicon[word]

                        # Update positions
                        if word_id not in doc_info["word_positions"]:
                            doc_info["word_positions"][word_id] = []
                        doc_info["word_positions"][word_id].append(pos)

                        # Update counts
                        doc_info["word_counts"][word_id] = (
                            doc_info["word_counts"].get(word_id, 0) + 1
                        )

                        # Track field matches
                        doc_info["field_matches"][field].append(word_id)

                doc_info["total_words"] += len(words)

            forward_index[item_id] = doc_info

        start_id = (chunk_id * BATCH_SIZE) + 1
        end_id = start_id + len(forward_index) - 1
        output_file = os.path.join(
            output_dir, f"forward_index_{start_id}-{end_id}.json"
        )
        with open(output_file, "w", encoding="utf-8-sig") as f:
            json.dump(forward_index, f, indent=4)

        print(f"Created forward_index_{start_id}-{end_id}.json with {len(forward_index)} items")
        chunk_id += 1
        total_files += 1

    print(f"Forward index creation complete! Total files created: {total_files}")


if __name__ == "__main__":
    create_forward_index(
        "../data/hotels_cleaned.csv",
        "../index data/lexicon/lexicon.json",
        "../index data/forward_index/hotels",
        "hotel_id",
        ["name", "locality", "street-address", "region"],
    )
    # create_forward_index(
    #     "../data/reviews_cleaned.csv",
    #     "../index data/lexicon/lexicon.json",
    #     "../index data/forward_index/reviews",
    #     "rev_id",
    #     ["title", "text"],
    # )
