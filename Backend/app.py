from fastapi import FastAPI, UploadFile, HTTPException, Query
import os
import json
import pandas as pd
import re
from utils.tokenizer import Tokenizer
from utils.file_io import search_tag

app = FastAPI()

# Constants
REVIEWS_DIR = "./reviews/"
HOTELS_FILE = "./data/hotels_cleaned.csv"
LEXICON_FILE = "./index data/combined_lexicon.json"
HOTEL_INVERTED_INDEX = ".\\index data\\hotel_inverted_index.json"
REVIEW_INVERTED_INDEX = "./index data/review_inverted_index.json"


def tokenize(text):
    """Tokenizes input text."""
    tokenizer = Tokenizer()
    return tokenizer.tokenize_with_spacy(text)


def get_batch_file(hotel_id: str):
    """Determine the batch file containing reviews for the given hotel_id."""
    # Define ranges and corresponding filenames
    file_ranges = [
        ("1", "1000", "./reviews/reviews_1-1000.csv"),
        ("1001", "2000", "./reviews/reviews_1001-2000.csv"),
        ("2001", "3000", "./reviews/reviews_2001-3000.csv"),
        ("3001", "4000", "./reviews/reviews_3001-4000.csv"),
        ("4001", "5000", "./reviews/reviews_4001-5000.csv"),
    ]

    # Find the file containing the hotel_id
    for start, end, filename in file_ranges:
        if start <= hotel_id <= end:
            return filename

    # If no file is found
    return None


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
    with open(HOTEL_INVERTED_INDEX, "w") as f:
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

    #     with open(REVIEW_INVERTED_INDEX, "w") as f:
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
async def search_hotels(query: str = Query(...)):
    """Search for hotels using the query string."""
    try:
        tokenizer = Tokenizer()
        print(f"Query received: {query}")

        # Load inverted index
        with open(HOTEL_INVERTED_INDEX, "r", encoding="utf-8-sig") as f:
            inverted_index = json.load(f)

        # Load lexicon
        with open(LEXICON_FILE, "r", encoding="utf-8-sig") as f:
            lexicon = json.load(f)

        # Tokenize query
        tokens = tokenizer.tokenize_with_spacy(query)

        matched_ids = set()

        for token in tokens:
            token_id = lexicon.get(token)
            if token_id is not None and str(token_id) in inverted_index:
                matched_ids.update(inverted_index[str(token_id)])
            else:
                print(f"Token '{token}' not found in lexicon or index.")

        # Load hotels from CSV
        if not os.path.exists(HOTELS_FILE):
            raise HTTPException(status_code=400, detail="Hotels data file not found")

        hotels_df = pd.read_csv(HOTELS_FILE)
        hotels_df["hotel_id"] = hotels_df["hotel_id"].astype(
            str
        )  # Ensure hotel_id is a string

        # Filter results
        results = []
        for hotel_id in matched_ids:
            if (
                str(hotel_id) in hotels_df["hotel_id"].values
            ):  # Ensure matched_ids are in values
                hotel_row = (
                    hotels_df[hotels_df["hotel_id"] == str(hotel_id)].iloc[0].to_dict()
                )
                hotel_row["hotel_id"] = str(hotel_id)  # Add hotel_id back to the result
                results.append(hotel_row)

        return {"status": "success", "count": len(results), "results": results}

    except Exception as e:
        print(f"Error occurred: {e}")
        raise HTTPException(
            status_code=500, detail="An error occurred while processing the request."
        )


@app.get("/hotel/{hotel_id}/reviews")
async def get_hotel_reviews(hotel_id: str):
    """Retrieve reviews for a specific hotel."""
    try:
        # Find the appropriate review file for the hotel_id
        review_file = get_batch_file(hotel_id)
        if review_file is None:
            return {"status": "success", "reviews": []}

        # Load the review file
        reviews_df = pd.read_csv(review_file, encoding="utf-8-sig")

        # Filter reviews for the specific hotel_id
        hotel_reviews = reviews_df[reviews_df["hotel_id"] == int(hotel_id)]

        # Handle NaN values before returning the data
        hotel_reviews = hotel_reviews.fillna("").to_dict(orient="records")

        return {
            "status": "success",
            "num_reviews": len(hotel_reviews),
            "reviews": hotel_reviews,
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/search/reviews")
async def search_reviews(query: str):
    """Search reviews based on query."""
    try:
        with open(REVIEW_INVERTED_INDEX, "r") as f:
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


@app.get("/hotel/{hotel_id}")
async def get_hotel(hotel_id: str):
    """
    Retrieve details for a specific hotel by its hotel_id.
    """
    try:
        # Load the CSV file into a DataFrame
        hotels_df = pd.read_csv(HOTELS_FILE)

        # Ensure hotel_id is treated as a string for comparison
        hotels_df["hotel_id"] = hotels_df["hotel_id"].astype(str)

        # Filter the DataFrame for the specific hotel_id
        hotel_details = hotels_df[hotels_df["hotel_id"] == hotel_id]

        # Check if hotel details are found
        if hotel_details.empty:
            raise HTTPException(status_code=404, detail=f"No hotel found with ID {hotel_id}")

        # Convert the result to a dictionary
        hotel_details_dict = hotel_details.iloc[0].to_dict()

        return {"status": "success", "hotel_details": hotel_details_dict}

    except FileNotFoundError:
        raise HTTPException(status_code=500, detail=f"File {HOTELS_FILE} not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
