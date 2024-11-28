import concurrent.futures
import time
import pandas as pd
import json
from tokenizer import Tokenizer


# Function to tokenize text in parallel
def tokenize_in_process(text_chunk):
    tk = Tokenizer()  # Initialize the Tokenizer
    print("done")
    return tk.tokenize(text_chunk)


if __name__ == "__main__":
    reviews_df = pd.read_csv("test_reviews.csv")
    hotels_df = pd.read_csv("hotels.csv")

    print(f"Hotels: {len(hotels_df)}")
    print(f"Reviews: {len(reviews_df)}")

    combined_df = pd.merge(
        reviews_df[["hotel_id", "title", "text"]],
        hotels_df[["hotel_id", "name", "region", "street-address", "locality"]],
        on="hotel_id",
        how="outer",
    )

    print(len(combined_df))

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

    text_data = string_to_tokenize.tolist()

    # Split the list into chunks for parallel processing
    chunk_size = 100  # Adjust the chunk size as needed
    chunks = [
        " ".join(text_data[i : i + chunk_size])  # Combine 10 rows into 1 string
        for i in range(0, len(text_data), chunk_size)
    ]

    print(chunks[0])

    # Process the tokenization in parallel using ProcessPoolExecutor
    t1 = time.time()
    # all_tokens = []
    # for chunk in chunks:
    #     all_tokens.extend(tokenize_in_process(chunk))
    with concurrent.futures.ProcessPoolExecutor() as executor:
        # Submit tasks to the process pool
        results = [executor.submit(tokenize_in_process, chunk) for chunk in chunks]

        # Collect results as they complete
        all_tokens = []
        for future in concurrent.futures.as_completed(results):
            all_tokens.extend(future.result())

    t2 = time.time()

    # Print the time taken and some sample tokens
    print(f"Total time taken: {round(t2 - t1, 2)} seconds")
    print(f"Total tokens (before removing duplicates): {len(all_tokens)}")
    print(f"Total tokens (after removing duplicates): {len(set(all_tokens))}")
    print(f"Some sample tokens: {all_tokens[:100]}")

    all_tokens = list(set(all_tokens))

    # Save the tokens to a json file
    # Create lexicon with word as key and ID as value
    lexicon = {word: idx for idx, word in enumerate(all_tokens)}

    # Save lexicon to a JSON file
    with open("lexicon_tokenize.json", "w") as json_file:
        json.dump(lexicon, json_file, indent=4)
