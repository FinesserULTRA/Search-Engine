import json
from collections import defaultdict
from pathlib import Path
import time


class InvertedIndexGenerator:
    def __init__(self, review_files_path: str, index_data_path: str):
        self.review_files_path = Path(review_files_path)
        self.index_data_path = Path(index_data_path)

    def generate_reviews_inverted_index(self):
        # Load the forward index for hotels
        with open(self.index_data_path / "review_forward_index.json", "r") as f:
            forward_index = json.load(f)

        print(len(forward_index))

        # Create the inverted index for hotels
        inverted_index = defaultdict(list)
        for doc_id, token_ids in forward_index.items():
            for token_id in token_ids:
                inverted_index[token_id].append(doc_id)

        # Write the inverted index to a JSON file
        with open(self.index_data_path / "review_inverted_index.json", "w") as f:
            json.dump(inverted_index, f)

        return "Inverted index for reviews generated successfully!"

    def generate_hotels_inverted_index(self):
        # Load the forward index for hotels
        with open(self.index_data_path / "hotel_forward_index.json", "r") as f:
            forward_index = json.load(f)

        # Create the inverted index for hotels
        inverted_index = defaultdict(list)
        for doc_id, token_ids in forward_index.items():
            for token_id in token_ids:
                inverted_index[token_id].append(doc_id)

        # Write the inverted index to a JSON file
        with open(self.index_data_path / "hotel_inverted_index.json", "w") as f:
            json.dump(inverted_index, f)

        return "Inverted index for hotels generated successfully!"


# Generate forward index without FastAPI
if __name__ == "__main__":
    generator = InvertedIndexGenerator(
        review_files_path="../reviews", index_data_path="../index data"
    )
    t1 = time.time()
    result = generator.generate_reviews_inverted_index()
    t2 = time.time()
    print(f"Time taken: {t2 - t1:.2f} seconds")
    print(result)
