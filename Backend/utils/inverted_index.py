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

        # Create barrels from the inverted index
        barrels = self.create_barrels(inverted_index)

        # Write the barrels to JSON files
        for i, barrel in enumerate(barrels):
            with open(f"../index data/review_barrels/review_barrel_{i}.json", "w") as f:
                json.dump(barrel, f)

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

        # Create barrels from the inverted index
        barrels = self.create_barrels(inverted_index)

        # Write the barrels to JSON files
        for i, barrel in enumerate(barrels):
            with open(f"../index data/hotel_barrels/hotel_barrel_{i}.json", "w") as f:
                json.dump(barrel, f)

        return "Inverted index for hotels generated successfully!"


    def create_barrels(self, inverted_index):

        barrels = [defaultdict(dict) for _ in range(27)]
        lexicon = json.load(
            open("../index data/combined_lexicon.json", "r", encoding="utf-8-sig")
        )
        reverse_lexicon = {word_id: word for word, word_id in lexicon.items()}
        for token_id, doc_ids in inverted_index.items():
            word = reverse_lexicon[token_id]
            barrel_index = self.get_barrel_index(word)
            barrels[barrel_index][word] = doc_ids

        return barrels

    def get_barrel_index(self, word):
        first_char = word[0].lower()
        if "a" <= first_char <= "z":  # Check if it's a letter
            return ord(first_char) - ord("a")  # Map 'a' to 0, 'b' to 1, ..., 'z' to 25
        return 26  # Map non-letter characters to index 26

# Generate forward index without FastAPI
if __name__ == "__main__":
    generator = InvertedIndexGenerator(
        review_files_path="../reviews", index_data_path="../index data"
    )
    t1 = time.time()
    result = generator.generate_reviews_inverted_index()
    result = generator.generate_hotels_inverted_index()
    t2 = time.time()
    print(f"Time taken: {t2 - t1:.2f} seconds")
    print(result)
