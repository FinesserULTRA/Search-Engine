import multiprocessing
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

def process_review_file(review_file, hotels_df, tokenizer):
    try:
        print(f"Processing file: {review_file}")
        reviews_df = pd.read_csv(review_file, encoding="utf-8")
        print(f"Loaded {len(reviews_df)} reviews from {review_file}")
        combined_df = pd.merge(
            reviews_df[["hotel_id", "title", "text"]],
            hotels_df[["hotel_id", "name", "region", "street-address", "locality"]],
            on="hotel_id",
            how="right",
        )
        print(f"Merged dataset for {review_file}: {len(combined_df)} rows")
        # Prepare text data for tokenization
        columns_to_tokenize = ["name", "title", "text", "region", "street-address", "locality"]
        string_to_tokenize = combined_df[columns_to_tokenize].apply(
            lambda x: " ".join(x.dropna().astype(str)), axis=1
        )
        text_data = string_to_tokenize.tolist()
        # Tokenize the text data
        tokens = tokenize_chunk(text_data, tokenizer)
        # Remove duplicates from the token list
        tokens = list(set(tokens))
        print(f"Total tokens for {review_file} (after removing duplicates): {len(tokens)}")

        # Save the tokens to a JSON file corresponding to each review file
        lexicon = {word: idx for idx, word in enumerate(tokens)}
        output_filename = f"../index data/lexicon_{os.path.basename(review_file).replace('.csv', '')}.json"
        with open(output_filename, "w", encoding="utf-8-sig") as json_file:
            json.dump(lexicon, json_file, indent=4)

        print(f"Saved lexicon for {review_file} to {output_filename}")
    except Exception as e:
        print(f"Error processing file {review_file}: {e}")

def run_process_independently(review_file, hotels_df_path):
    # Initialize tokenizer
    tk = Tokenizer()
    # Load hotels dataset
    hotels_df = pd.read_csv(hotels_df_path, encoding="utf-8")
    # Process the review file
    process_review_file(review_file, hotels_df, tk)

if __name__ == "__main__":
    # Directory containing review files
    reviews_dir = "../reviews"
    review_files = [os.path.join(reviews_dir, f) for f in os.listdir(reviews_dir) if f.endswith(".csv")]

    # Path to hotels dataset
    hotels_df_path = "../data/hotels_cleaned.csv"
    print(f"Found {len(review_files)} review files.")

    # Process each review file independently using multiprocessing
    t1 = time.time()
    processes = []
    for review_file in review_files:
        p = multiprocessing.Process(target=run_process_independently, args=(review_file, hotels_df_path))
        p.start()
        processes.append(p)

    # Wait for all processes to complete
    for p in processes:
        p.join()

    t2 = time.time()

    # Print total time taken
    print(f"Total time taken: {round(t2 - t1, 2)} seconds")
