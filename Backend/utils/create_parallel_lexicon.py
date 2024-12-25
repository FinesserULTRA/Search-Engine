import multiprocessing
import time
import pandas as pd
import json
import os
from tokenizer import Tokenizer

def tokenize_chunk(chunk, tokenizer):
    """Tokenize a list of text rows using the tokenizer."""
    tokens = []
    for text in chunk:
        try:
            tokens.extend(tokenizer.tokenize_with_spacy(text))
        except Exception as e:
            print(f"Error tokenizing text: {text[:100]} - {e}")
    return list(set(tokens))


def process_review_file(review_file, hotels_df, tokenizer):
    try:
        print(f"Processing file: {review_file}")
        reviews_df = pd.read_csv(review_file, encoding="utf-8")
        print(f"Loaded {len(reviews_df)} reviews from {review_file}")

        # Merge with hotels data
        combined_df = pd.merge(
            reviews_df[["hotel_id", "title", "text"]],
            hotels_df[["hotel_id", "name", "region", "street-address", "locality"]],
            on="hotel_id",
            how="right",
        )
        print(f"Merged dataset for {review_file}: {len(combined_df)} rows")

        # Prepare text for tokenization
        columns_to_tokenize = [
            "name",
            "title",
            "text",
            "region",
            "street-address",
            "locality",
        ]
        string_to_tokenize = combined_df[columns_to_tokenize].apply(
            lambda x: " ".join(x.dropna().astype(str)), axis=1
        )

        # Tokenize and deduplicate
        tokens = list(set(tokenize_chunk(string_to_tokenize.tolist(), tokenizer)))
        print(f"Total tokens for {review_file} (after deduplication): {len(tokens)}")

        # Save lexicon
        lexicon = {word: idx for idx, word in enumerate(tokens)}
        output_filename = f"../index data/lexicon/partial/lexicon_{os.path.basename(review_file).replace('.csv', '')}.json"
        os.makedirs(os.path.dirname(output_filename), exist_ok=True)

        with open(output_filename, "w", encoding="utf-8-sig") as json_file:
            json.dump(lexicon, json_file)

        print(f"Saved lexicon to {output_filename}")
    except Exception as e:
        print(f"Error processing file {review_file}: {e}")


def run_process_independently(review_file, hotels_df_path):
    tk = Tokenizer()  # Use original Tokenizer
    hotels_df = pd.read_csv(hotels_df_path, encoding="utf-8")
    process_review_file(review_file, hotels_df, tk)


def merge_lexicons(lexicon_dir):
    """Merge all partial lexicons into a single final lexicon."""
    all_tokens = set()

    # Collect all unique tokens
    for lexicon_file in os.listdir(lexicon_dir):
        if lexicon_file.startswith("lexicon_") and lexicon_file.endswith(".json"):
            with open(
                os.path.join(lexicon_dir, lexicon_file), "r", encoding="utf-8-sig"
            ) as f:
                lexicon = json.load(f)
                all_tokens.update(lexicon.keys())

    # Create final lexicon with new indices
    final_lexicon = {word: idx for idx, word in enumerate(sorted(all_tokens))}

    # Save final lexicon
    output_path = os.path.join(os.path.dirname(lexicon_dir), "lexicon.json")
    with open(output_path, "w", encoding="utf-8-sig") as f:
        json.dump(final_lexicon, f)

    print(f"Created final lexicon with {len(final_lexicon)} tokens at {output_path}")


if __name__ == "__main__":
    reviews_dir = "../reviews"
    hotels_df_path = "../data/hotels_cleaned.csv"
    review_files = [
        os.path.join(reviews_dir, f)
        for f in os.listdir(reviews_dir)
        if f.endswith(".csv")
    ]

    print(f"Found {len(review_files)} review files")

    # Process files in parallel
    t1 = time.time()
    processes = []
    for review_file in review_files:
        p = multiprocessing.Process(
            target=run_process_independently, args=(review_file, hotels_df_path)
        )
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    # Merge partial lexicons
    merge_lexicons("../index data/lexicon/partial")

    t2 = time.time()
    print(f"Total time taken: {round(t2 - t1, 2)} seconds")
