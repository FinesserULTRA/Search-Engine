import pandas as pd
import json
from collections import defaultdict
from pathlib import Path


class ForwardIndexGenerator:
    def __init__(self, review_files_path: str, index_data_path: str):
        self.review_files_path = Path(review_files_path)
        self.index_data_path = Path(index_data_path)
        self.token_to_id = self.load_lexicon()

    def load_lexicon(self):
        # Load the combined lexicon
        with open(self.index_data_path / "combined_lexicon.json", "r", encoding='utf-8-sig') as f:
            lexicon = json.load(f)
        # Map tokens to unique IDs
        token_to_id = {token: idx for idx, token in enumerate(lexicon.keys())}
        return token_to_id

    def process_review_file(self, file_path):
        forward_index = defaultdict(list)
        reviews_df = pd.read_csv(file_path)

        # Extract relevant columns and create the forward index
        for _, row in reviews_df.iterrows():
            doc_id = row["rev_id"]
            text = row["text"]
            if pd.isna(text):
                continue

            # Tokenize and map tokens to IDs
            tokens = (
                text.split()
            )  # Simple split, assuming tokenizer was used during lexicon creation
            token_ids = [
                self.token_to_id[token] for token in tokens if token in self.token_to_id
            ]
            forward_index[doc_id] = token_ids

        return forward_index

    def generate_forward_index(self):
        # List of review CSV files
        review_files = [
            "reviews_1-1000.csv",
            "reviews_1001-2000.csv",
            "reviews_2001-3000.csv",
            "reviews_3001-4000.csv",
            "reviews_4001-5000.csv",
        ]

        # Combine forward indices from all files
        combined_forward_index = defaultdict(list)
        for review_file in review_files:
            review_file_path = self.review_files_path / review_file
            forward_index = self.process_review_file(review_file_path)
            for doc_id, token_ids in forward_index.items():
                combined_forward_index[doc_id].extend(token_ids)

        # Write the combined forward index to a JSON file
        with open(self.index_data_path / "forward_index.json", "w") as f:
            json.dump(combined_forward_index, f)

        return "Forward index generation completed successfully!"


# Generate forward index without FastAPI
if __name__ == "__main__":
    generator = ForwardIndexGenerator(
        review_files_path="../reviews", index_data_path="../index data"
    )
    result = generator.generate_forward_index()
    print(result)
