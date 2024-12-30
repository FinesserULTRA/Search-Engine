import json
import os

def create_basic_lexicon():
    basic_words = {
        "best": 1,
        "good": 2,
        "great": 3,
        "excellent": 4,
        "amazing": 5,
        "wonderful": 6,
        "fantastic": 7,
        "better": 8,
        "well": 9,
        "bad": 10,
        "poor": 11,
        "worst": 12,
    }
    
    lexicon_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "index data", "lexicon")
    os.makedirs(lexicon_dir, exist_ok=True)
    
    lexicon_path = os.path.join(lexicon_dir, "lexicon.json")
    with open(lexicon_path, "w", encoding="utf-8-sig") as f:
        json.dump(basic_words, f, indent=4)
    print(f"Created basic lexicon at {lexicon_path}")

if __name__ == "__main__":
    create_basic_lexicon()
