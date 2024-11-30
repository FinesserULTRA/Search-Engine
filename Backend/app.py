from fastapi import FastAPI, UploadFile, HTTPException
import os
import json
import pandas as pd
import re
from utils.tokenizer import Tokenizer

app = FastAPI()

# Constants
REVIEWS_DIR = "./reviews/"
HOTELS_FILE = "./data/hotels.json"
LEXICON_FILE = "./index data/lexicon_tokenize.json"
INVERTED_INDEX_FILE = "./index data/inverted_index.json"
REVIEW_INDEX_FILE = "./index data/review_index.json"


def tokenize(text):
    """Tokenizes input text."""
    tokenizer = Tokenizer()
    return tokenizer.tokenize_with_spacy(text)


def get_batch_file(offering_id):
    """Get the batch file name based on offering_id."""
    batch_size = 1000
    batch_index = (int(offering_id) - 1) // batch_size + 1
    return f"{REVIEWS_DIR}/reviews_batch_{batch_index}.json"


# add lexicon file
def rebuild_hotel_index(hotels):
    """Rebuild the inverted index for hotels."""
    inverted_index = {}
    tokenizer = Tokenizer()
    for offering_id, hotel in hotels.items():
        # Tokenize relevant fields
        tokens = (
            tokenizer.tokenize_with_spacy(hotel["name"])
            + tokenizer.tokenize_with_spacy(hotel["region"])
            + tokenizer.tokenize_with_spacy(hotel.get("locality", ""))
            + tokenizer.tokenize_with_spacy(hotel.get("street-address", ""))
        )

        for token in tokens:
            if token not in inverted_index:
                inverted_index[token] = []
            inverted_index[token].append(str(offering_id))

    # Save the inverted index to a file
    with open(INVERTED_INDEX_FILE, "w") as f:
        json.dump(inverted_index, f)


def rebuild_review_index():
    pass
    # try:
    #     review_index = {}
    #     reviews_dir = "reviews/"

    #     for review_file in os.listdir(reviews_dir):
    #         if review_file.endswith(".json"):
    #             with open(os.path.join(reviews_dir, review_file), "r") as f:
    #                 reviews = json.load(f)
    #                 for i, review in enumerate(reviews):
    #                     tokens = review["text"].lower().split()
    #                     for token in tokens:
    #                         review_id = f"{review['offering_id']}:{i}"
    #                         review_index.setdefault(token, []).append(review_id)

    #     with open(REVIEW_INDEX_FILE, "w") as f:
    #         json.dump(review_index, f)

    #     return {"status": "success", "message": "Review index rebuilt successfully"}
    # except Exception as e:
    #     raise HTTPException(status_code=400, detail=str(e))


@app.post("/upload/hotels")
async def upload_hotels(file: UploadFile):
    """Upload hotel metadata and update inverted index."""
    try:
        df = pd.read_csv(file.file)
        hotels = {}
        required_columns = ["offering_id", "name", "region", "locality"]

        # Load existing hotel data
        if os.path.exists(HOTELS_FILE):
            with open(HOTELS_FILE, "r") as f:
                hotels = json.load(f)

        # Add new hotels
        for _, row in df.iterrows():
            hotel = row.to_dict()
            hotels[hotel["offering_id"]] = hotel

        # Save updated hotel data
        with open(HOTELS_FILE, "w") as f:
            json.dump(hotels, f)

        # Update inverted index
        rebuild_hotel_index(hotels)
        return {"status": "success", "message": "Hotels uploaded and indexed"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/upload/reviews")
async def upload_reviews():
    """Upload reviews and partition them into batches."""
    os.makedirs(REVIEWS_DIR, exist_ok=True)
    try:
        # for chunk in pd.read_csv(file.file, chunksize=1000):
        for chunk in pd.read_csv("./data/cleaned_reviews.csv", chunksize=1000):
            for _, row in chunk.iterrows():
                review = row.to_dict()
                batch_file = get_batch_file(review["offering_id"])

                # Load or create batch file
                try:
                    with open(batch_file, "r") as f:
                        batch = json.load(f)
                except FileNotFoundError:
                    batch = {}

                # Append review to the corresponding hotel
                batch.setdefault(review["offering_id"], []).append(review)

                # Save updated batch file
                with open(batch_file, "w") as f:
                    json.dump(batch, f)

        rebuild_review_index()
        return {"status": "success", "message": "Reviews uploaded and indexed"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/search/hotels")
async def search_hotels(query: str):
    """Search for hotels using the query string."""
    try:
        tokenizer = Tokenizer()

        # Load the inverted index
        if not os.path.exists(INVERTED_INDEX_FILE):
            raise HTTPException(status_code=400, detail="Inverted index not found")

        with open(INVERTED_INDEX_FILE, "r") as f:
            inverted_index = json.load(f)

        tokens = tokenizer.tokenize_with_spacy(query)
        print(f"Tokens from query: {tokens}")

        matched_ids = set()

        for token in tokens:
            if token in inverted_index:
                print(f"Token '{token}' found in index.")
                matched_ids.update(inverted_index[token])
            else:
                print(f"Token '{token}' not found in index.")

        # Load hotels from hotels.json
        if not os.path.exists(HOTELS_FILE):
            raise HTTPException(status_code=400, detail="Hotels data file not found")

        with open(HOTELS_FILE, "r") as f:
            hotels = json.load(f)

        # Filter results
        results = [
            hotels[str(hotel_id)] for hotel_id in matched_ids if str(hotel_id) in hotels
        ]

        return {"status": "success", "count": len(results), "results": results}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/hotel/{offering_id}/reviews")
async def get_hotel_reviews(offering_id: str):
    """Retrieve reviews for a specific hotel."""
    try:
        batch_file = get_batch_file(offering_id)

        if os.path.exists(batch_file):
            with open(batch_file, "r") as f:
                batch = json.load(f)
            reviews = batch.get(offering_id, [])
        else:
            reviews = []

        return {"status": "success", "reviews": reviews}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/search/reviews")
async def search_reviews(query: str):
    """Search reviews based on query."""
    try:
        with open(REVIEW_INDEX_FILE, "r") as f:
            review_index = json.load(f)

        tokens = query.lower().split()
        matched_review_ids = set()

        # Token-to-review mapping
        for token in tokens:
            if token in review_index:
                matched_review_ids.update(review_index[token])

        # Fetch matching reviews
        reviews = []
        for review_id in matched_review_ids:
            offering_id, pos = review_id.split(":")
            batch_file = get_batch_file(offering_id)

            if os.path.exists(batch_file):
                with open(batch_file, "r") as f:
                    batch = json.load(f)
                    reviews.append(batch[offering_id][int(pos)])

        return {"status": "success", "results": reviews}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/hotel/{offering_id}")
async def get_hotel(offering_id: str):
    """Retrieve metadata for a specific hotel."""
    try:
        with open(HOTELS_FILE, "r") as f:
            hotels = json.load(f)

        hotel = hotels.get(offering_id)
        if not hotel:
            raise HTTPException(status_code=404, detail="Hotel not found")

        return {"status": "success", "hotel": hotel}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
