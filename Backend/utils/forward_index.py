import pandas as pd
import json
from collections import defaultdict
from pathlib import Path
from tokenizer import Tokenizer
from concurrent.futures import ProcessPoolExecutor, as_completed
import time
import os


class ForwardIndexGenerator:
    def __init__(self, review_files_path: str, index_data_path: str):
        self.review_files_path = Path(review_files_path)
        self.index_data_path = Path(index_data_path)
        self.lexicon = self.load_lexicon()

    def load_lexicon(self):
        # Load the combined lexicon
        with open(
            self.index_data_path / "combined_lexicon.json", "r", encoding="utf-8-sig"
        ) as f:
            lexicon = json.load(f)

        return lexicon

    def process_review_file(self, file_path):
        forward_index = defaultdict(list)
        reviews_df = pd.read_csv(file_path)
        tokenizer = Tokenizer()

        # Extract relevant columns and create the forward index
        for _, row in reviews_df.iterrows():
            doc_id = row["rev_id"]
            title = row["title"]
            text = row["text"]

            text_to_tokenize = f"{title} {text}"
            if pd.isna(text_to_tokenize):
                continue

            # Tokenize and map tokens to IDs
            tokens = tokenizer.tokenize_with_spacy(text_to_tokenize)

            token_ids = [
                self.lexicon[token] for token in tokens if token in self.lexicon
            ]
            forward_index[doc_id] = token_ids

        return forward_index

    def generate_reviews_forward_index(self):
        # List of review CSV files
        review_files = [
            os.path.join(self.review_files_path, f)
            for f in os.listdir(self.review_files_path)
            if f.startswith("reviews_") and f.endswith(".csv")
        ]

        # Combine forward indices from all files
        review_file_paths = [file for file in review_files]
        combined_forward_index = defaultdict(list)

        # Use ProcessPoolExecutor for parallel processing
        with ProcessPoolExecutor() as executor:
            futures = [
                executor.submit(self.process_review_file, file_path)
                for file_path in review_file_paths
            ]

        # Collect results as they complete
        for future in as_completed(futures):
            forward_index = future.result()
            for doc_id, token_ids in forward_index.items():
                combined_forward_index[doc_id].extend(token_ids)

        # Write the combined forward index to a JSON file
        with open(self.index_data_path / "review_forward_index.json", "w") as f:
            json.dump(combined_forward_index, f)

        return "Forward index generation completed successfully!"

    def generate_hotels_forward_index(self):
        tokenizer = Tokenizer()
        hotel_df = pd.read_csv("../data/hotels_cleaned.csv")

        forward_index = defaultdict(list)

        for _, row in hotel_df.iterrows():
            doc_id = row["hotel_id"]
            text = f"{row['name']} {row['locality']} {row['street-address']} {row['region']}"
            if pd.isna(text):
                continue

            # Tokenize and map tokens to IDs
            tokens = tokenizer.tokenize_with_spacy(text)

            token_ids = [
                self.lexicon[token] for token in tokens if token in self.lexicon
            ]

            forward_index[doc_id] = token_ids

        # Write the forward index to a JSON file
        with open(self.index_data_path / "hotel_forward_index.json", "w") as f:
            json.dump(forward_index, f)

        return "Forward index for hotels generated successfully!"


# Generate forward index without FastAPI
if __name__ == "__main__":
    generator = ForwardIndexGenerator(
        review_files_path="../reviews", index_data_path="../index data"
    )
    t1 = time.time()
    result = generator.generate_reviews_forward_index()
    t2 = time.time()
    print(f"Time taken: {t2 - t1:.2f} seconds")
    print(result)
