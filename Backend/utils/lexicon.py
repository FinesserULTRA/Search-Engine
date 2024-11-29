import concurrent.futures
import time
import pandas as pd
import json
import os
from tokenizer import Tokenizer


def tokenize_chunk(chunk, tokenizer):
    """
    Tokenize a list of text rows using the tokenizer.
    """
    tokens = []
    for text in chunk:
        try:
            tokens.extend(tokenizer.tokenize_with_spacy(text))
        except Exception as e:
            print(f"Error tokenizing text: {text[:100]} - {e}")
    return tokens


if __name__ == "__main__":
    # Initialize tokenizer
    tk = Tokenizer()

    # Load datasets
    reviews_df = pd.read_csv("../data/reviews_cleaned.csv", encoding="utf-8")
    hotels_df = pd.read_csv("../data/hotels_cleaned.csv", encoding="utf-8")

    print(f"Hotels: {len(hotels_df)}")
    print(f"Reviews: {len(reviews_df)}")

    # Merge datasets
    combined_df = pd.merge(
        reviews_df[["hotel_id", "title", "text"]],
        hotels_df[["hotel_id", "name", "region", "street-address", "locality"]],
        on="hotel_id",
        how="right",
    )

    print(f"Combined records: {len(combined_df)}")

    # Prepare text data for tokenization
    columns_to_tokenize = ["name", "title", "text", "region", "street-address", "locality"]
    string_to_tokenize = combined_df[columns_to_tokenize].apply(
        lambda x: " ".join(x.dropna().astype(str)), axis=1
    )
    text_data = string_to_tokenize.tolist()

    # Split text data into chunks
    chunk_size = 2000
    chunks = [text_data[i : i + chunk_size] for i in range(0, len(text_data), chunk_size)]

    print(f"Processing {len(chunks)} chunks...")

    # Process chunks in parallel
    t1 = time.time()
    all_tokens = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        # Pass the tokenizer explicitly if it holds large state
        results = executor.map(tokenize_chunk, chunks, [tk] * len(chunks))

        # Flatten results during processing to reduce memory usage
        for token_list in results:
            all_tokens.extend(token_list)

    t2 = time.time()

    # Print results
    print(f"Total time taken: {round(t2 - t1, 2)} seconds")
    print(f"Total tokens (before removing duplicates): {len(all_tokens)}")

    # Remove duplicates
    all_tokens = list(set(all_tokens))
    print(f"Total tokens (after removing duplicates): {len(all_tokens)}")
    print(f"Sample tokens: {all_tokens[:100]}")

    # Save the tokens to a JSON file
    lexicon = {word: idx for idx, word in enumerate(all_tokens)}
    with open("../index data/lexicon_tokenize.json", "w", encoding="utf-8-sig") as json_file:
        json.dump(lexicon, json_file, indent=4)
