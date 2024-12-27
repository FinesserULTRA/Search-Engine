from fastapi import FastAPI, UploadFile, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from functools import lru_cache
from utils.tokenizer import Tokenizer
from utils.file_io import read_json, write_json, read_csv, write_csv
from utils.batch_cache import get_doc_ids_for_token
import pandas as pd
import os
from typing import List, Dict
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import json
from typing import Optional

# Initialize FastAPI and configurations
app = FastAPI(title="Hotel Search Engine")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

# Constants
DATA_DIR = "./data"
INDEX_DIR = "./index data"
REVIEWS_DIR = "./reviews"
BATCH_SIZE = 50000
# MAX_RESULTS = 1000

PATHS = {
    "HOTELS": f"{DATA_DIR}/hotels_cleaned.csv",
    "LEXICON": f"{INDEX_DIR}/lexicon/lexicon.json",
    "FORWARD_INDEX": f"{INDEX_DIR}/forward_index",
    "INVERTED_INDEX": f"{INDEX_DIR}/inverted_index",
}

# Initialize directories
for dir_path in [DATA_DIR, REVIEWS_DIR, INDEX_DIR, f"{INDEX_DIR}/lexicon"]:
    os.makedirs(dir_path, exist_ok=True)

# Initialize shared services
tokenizer = Tokenizer()


class ReviewCreate(BaseModel):
    title: str
    text: str
    hotel_id: int
    service: Optional[float] = None
    cleanliness: Optional[float] = None
    overall: Optional[float] = None
    value: Optional[float] = None
    location: Optional[float] = None
    sleep_quality: Optional[float] = None
    rooms: Optional[float] = None


class HotelCreate(BaseModel):
    name: str
    region_id: str
    region: str
    street_address: str = Field(..., alias="street-address")
    locality: str
    hotel_class: float
    service: float
    cleanliness: float
    overall: float
    value: float
    location: float
    sleep_quality: float
    rooms: float
    average_score: float = None


# Cache utilities
@lru_cache(maxsize=1)
def get_hotels_df():
    return read_csv(PATHS["HOTELS"])


@lru_cache(maxsize=1)
def get_lexicon():
    return read_json(PATHS["LEXICON"])


def get_batch_range(word_id: int) -> tuple:
    batch_start = (word_id // BATCH_SIZE) * BATCH_SIZE
    batch_end = batch_start + BATCH_SIZE - 1
    return batch_start, batch_end


def get_review_batch_file(hotel_id: int) -> str:
    batch_start = ((hotel_id - 1) // 1000) * 1000 + 1
    batch_end = batch_start + 999
    return f"{REVIEWS_DIR}/reviews_{batch_start}-{batch_end}.csv"


@lru_cache(maxsize=1000)
def get_reviews_from_batch(batch_file: str, hotel_id: int = None) -> List[Dict]:
    reviews_df = read_csv(batch_file)
    if reviews_df.empty:
        return []
    if hotel_id is not None:
        reviews_df = reviews_df[reviews_df["hotel_id"] == hotel_id]
    return reviews_df.replace([np.inf, -np.inf, np.nan], None).to_dict("records")


def get_index_batch_path(word_id: int, doc_type: str) -> str:
    """Get the correct inverted index batch file path for a word ID"""
    batch_start = (word_id // 50000) * 50000
    batch_end = batch_start + 49999
    return f"{PATHS['INVERTED_INDEX']}/{doc_type}/inverted_index_{batch_start}-{batch_end}.json"


@lru_cache(maxsize=100)
def get_doc_ids_for_word(word: str, doc_type: str) -> set:
    """Get document IDs for a word using batched indices"""
    try:
        # Get word ID from lexicon
        lexicon = get_lexicon()
        word_id = lexicon.get(word)
        if not word_id:
            return set()

        # Get correct batch file
        batch_file = get_index_batch_path(word_id, doc_type)
        if not os.path.exists(batch_file):
            return set()

        # Get document IDs
        with open(batch_file, "r", encoding="utf-8-sig") as f:
            index_batch = json.load(f)
            return set(index_batch.get(str(word_id), []))

    except Exception as e:
        print(f"Error getting docs for word {word}: {e}")
        return set()


def get_doc_ids(word_id: int, doc_type: str) -> set:
    """Get document IDs from the correct inverted index batch"""
    batch_start = (word_id // 50000) * 50000
    batch_end = batch_start + 49999
    batch_file = f"{PATHS['INVERTED_INDEX']}/{doc_type}/inverted_index_{batch_start}-{batch_end}.json"

    try:
        with open(batch_file, "r", encoding="utf-8-sig") as f:
            index_batch = json.load(f)
            return set(index_batch.get(str(word_id), []))
    except FileNotFoundError:
        print(f"Batch file not found: {batch_file}")
        return set()


def calculate_relevance_score(doc_entry, query_tokens, doc_type="hotels"):
    """Calculate relevance score based on multiple factors"""
    score = 0

    # Base frequency score
    score += doc_entry["freq"] * 0.3

    # Field weights
    field_weights = {
        "name": 3.0,
        "title": 2.5,
        "text": 1.0,
        "locality": 1.5,
        "region": 1.0,
        "street-address": 1.0,
    }

    # Add field-based scores
    for field in doc_entry["fields"]:
        score += field_weights.get(field, 1.0)

    # Position bonus (words appearing earlier get higher score)
    avg_pos = doc_entry["positions"][0] if doc_entry["positions"] else 0
    score += max(0, 1 - (avg_pos / 100)) * 0.5

    return score


@app.get("/search")
async def search(query: str = Query(...), type: str = Query("hotels")):
    """Enhanced search with better relevance scoring"""
    try:
        # Get both original and tokenized forms
        original_words = [word.lower() for word in query.split()]
        tokenized_words = tokenizer.tokenize_with_spacy(query.lower())
        tokens = list(set(original_words + tokenized_words))
        if not tokens:
            return {"results": [], "count": 0}

        # Get word IDs from lexicon
        lexicon = get_lexicon()
        matched_docs = {}

        # Process each search token
        for token in tokens:
            if token in lexicon:
                word_id = lexicon[token]
                print(f"Processing token: {token}, word_id: {word_id}")  # Debug print
                batch_file = get_index_batch_path(word_id, type)

                try:
                    with open(batch_file, "r", encoding="utf-8-sig") as f:
                        index_batch = json.load(f)
                        token_info = index_batch.get(str(word_id))

                        if token_info and "docs" in token_info:
                            # Process each matching document
                            for doc in token_info["docs"]:
                                doc_id = doc["id"]
                                score = calculate_relevance_score(
                                    doc, tokens, type
                                )  # Pass tokens instead of token

                                if doc_id not in matched_docs:
                                    matched_docs[doc_id] = {
                                        "score": score,
                                        "matched_tokens": set(
                                            [token]
                                        ),  # Track actual tokens
                                        "fields": doc["fields"],
                                        "matches": {
                                            token: doc
                                        },  # Track matches per token
                                    }
                                else:
                                    matched_docs[doc_id]["score"] += score
                                    matched_docs[doc_id]["matched_tokens"].add(token)
                                    matched_docs[doc_id]["fields"].extend(doc["fields"])
                                    matched_docs[doc_id]["matches"][
                                        token
                                    ] = doc  # Track matches

                except Exception as e:
                    print(f"Error processing batch file {batch_file}: {e}")
                    continue

        # Sort by number of matches first, then by score
        sorted_docs = sorted(
            matched_docs.items(),
            key=lambda x: (len(x[1]["matched_tokens"]), x[1]["score"]),
            reverse=True,
        )

        if type == "hotels":
            hotels_df = get_hotels_df()
            results = []

            # Get hotels with their review counts and handle float values
            for doc_id, info in sorted_docs[:50]:  # Limit to top 50 results
                try:
                    hotel_row = hotels_df[hotels_df["hotel_id"].astype(str) == doc_id]
                    if not hotel_row.empty:
                        hotel_dict = hotel_row.iloc[0].to_dict()
                        hotel_dict = {
                            k: (
                                None
                                if isinstance(v, float) and (np.isinf(v) or np.isnan(v))
                                else str(v) if isinstance(v, float) else v
                            )
                            for k, v in hotel_dict.items()
                        }
                        # Add search relevance info
                        hotel_dict["matched_terms"] = list(info["matched_tokens"])
                        hotel_dict["search_score"] = info["score"]
                        results.append(hotel_dict)
                except Exception as e:
                    print(f"Error processing hotel {doc_id}: {e}")
                    continue

            return {
                "results": results,
                "count": len(results),
                "total_matches": len(sorted_docs),
            }

        else:  # reviews
            results = []
            hotels_df = get_hotels_df()

            # Get all reviews from matched IDs
            for doc_id, _ in sorted_docs:
                try:
                    hotel_id = doc_id.split(":")[0]
                    batch_file = get_review_batch_file(int(hotel_id))

                    if os.path.exists(batch_file):
                        # Get hotel info
                        hotel_info = (
                            hotels_df[hotels_df["hotel_id"].astype(str) == hotel_id]
                            .replace([np.inf, -np.inf, np.nan], None)
                            .iloc[0]
                            .to_dict()
                            if hotel_id in hotels_df["hotel_id"].astype(str).values
                            else None
                        )

                        if hotel_info:
                            reviews_df = read_csv(batch_file)
                            reviews_df = reviews_df[
                                reviews_df["hotel_id"].astype(str) == hotel_id
                            ]

                            for _, review in reviews_df.iterrows():
                                review_dict = review.to_dict()
                                # Replace inf/-inf/nan with None
                                review_dict = {
                                    k: (
                                        None
                                        if isinstance(v, float)
                                        and (np.isinf(v) or np.isnan(v))
                                        else v
                                    )
                                    for k, v in review_dict.items()
                                }

                                # Check if query terms appear in title and/or text
                                title_matches = all(
                                    token in str(review_dict["title"]).lower()
                                    for token in tokens
                                )
                                text_matches = all(
                                    token in str(review_dict["text"]).lower()
                                    for token in tokens
                                )

                                if title_matches or text_matches:
                                    review_dict["hotel"] = hotel_info
                                    review_dict["relevance_score"] = (
                                        2 if title_matches else 0
                                    ) + (1 if text_matches else 0)
                                    results.append(review_dict)

                        if len(results) >= 1000:
                            break
                except Exception as e:
                    print(f"Error processing review for hotel {hotel_id}: {e}")
                    continue

            # Sort results by relevance score
            results.sort(key=lambda x: x["relevance_score"], reverse=True)

            # Clean results before returning
            cleaned_results = []
            for result in results:
                # Remove scoring field
                result.pop("relevance_score", None)
                # Ensure all float values are JSON-serializable
                cleaned_result = {
                    k: (
                        None
                        if isinstance(v, float) and (np.isinf(v) or np.isnan(v))
                        else v
                    )
                    for k, v in result.items()
                }
                cleaned_results.append(cleaned_result)

            return {
                "results": cleaned_results[:1000],
                "count": len(cleaned_results),
                "hotels_with_reviews": len(
                    {r.get("hotel", {}).get("hotel_id") for r in cleaned_results}
                ),
            }

    except Exception as e:
        print(f"Search error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/hotels", status_code=201)
async def create_hotels(file: UploadFile):
    """Add new hotels"""
    try:
        df = pd.read_csv(file.file)
        hotels_df = get_hotels_df()

        # Update forward and inverted indices
        for _, row in df.iterrows():
            hotel_data = row.to_dict()
            text = f"{hotel_data['name']} {hotel_data['locality']} {hotel_data['street-address']} {hotel_data['region']}"

            # Get token IDs
            tokens = tokenizer.tokenize_with_spacy(text)
            lexicon = get_lexicon()
            token_ids = [lexicon[token] for token in tokens if token in lexicon]

            # Update forward index
            forward_file = f"{PATHS['FORWARD_INDEX']}/hotels/forward_index_latest.json"
            forward_index = read_json(forward_file)
            forward_index[str(hotel_data["hotel_id"])] = token_ids
            write_json(forward_file, forward_index)

            # Update inverted indices by batch
            for token_id in token_ids:
                batch_start, batch_end = get_batch_range(token_id)
                batch_file = f"{PATHS['INVERTED_INDEX']}/hotels/inverted_index_{batch_start}-{batch_end}.json"

                inverted_index = read_json(batch_file)
                if str(token_id) not in inverted_index:
                    inverted_index[str(token_id)] = []

                if str(hotel_data["hotel_id"]) not in inverted_index[str(token_id)]:
                    inverted_index[str(token_id)].append(str(hotel_data["hotel_id"]))

                write_json(batch_file, inverted_index)

        # Update hotels file
        updated_df = pd.concat([hotels_df, df], ignore_index=True)
        write_csv(PATHS["HOTELS"], updated_df)
        get_hotels_df.cache_clear()

        return {"status": "success", "message": f"Added {len(df)} hotels"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def process_hotel_data(hotel_data: dict):
    """Process single hotel data and update indices"""
    # Calculate average score if not provided
    if not hotel_data.get("average_score"):
        scores = [
            hotel_data[field]
            for field in [
                "service",
                "cleanliness",
                "overall",
                "value",
                "location",
                "sleep_quality",
                "rooms",
            ]
        ]
        hotel_data["average_score"] = round(sum(scores) / len(scores), 1)

    # Generate hotel_id
    hotels_df = get_hotels_df()
    hotel_data["hotel_id"] = len(hotels_df) + 1

    # Update lexicon
    text = f"{hotel_data['name']} {hotel_data['locality']} {hotel_data['street-address']} {hotel_data['region']}"
    tokens = tokenizer.tokenize_with_spacy(text.lower())

    lexicon = get_lexicon()
    # Handle empty lexicon case
    max_word_id = max(lexicon.values()) if lexicon else 0

    # Add new tokens to lexicon
    for token in tokens:
        if token not in lexicon:
            max_word_id += 1
            lexicon[token] = max_word_id

    # Save updated lexicon
    write_json(PATHS["LEXICON"], lexicon)
    get_lexicon.cache_clear()

    # Get token IDs for forward index
    token_ids = [lexicon[token] for token in tokens]

    # Update forward index
    batch_start = ((hotel_data["hotel_id"] - 1) // BATCH_SIZE) * BATCH_SIZE
    batch_end = batch_start + BATCH_SIZE - 1
    forward_file = (
        f"{PATHS['FORWARD_INDEX']}/hotels/forward_index_{batch_start}-{batch_end}.json"
    )

    forward_index = read_json(forward_file) if os.path.exists(forward_file) else {}
    forward_index[str(hotel_data["hotel_id"])] = token_ids
    write_json(forward_file, forward_index)

    # Update inverted indices
    for token_id in token_ids:
        batch_start, batch_end = get_batch_range(token_id)
        batch_file = f"{PATHS['INVERTED_INDEX']}/hotels/inverted_index_{batch_start}-{batch_end}.json"

        inverted_index = read_json(batch_file) if os.path.exists(batch_file) else {}
        if str(token_id) not in inverted_index:
            inverted_index[str(token_id)] = []

        if str(hotel_data["hotel_id"]) not in inverted_index[str(token_id)]:
            inverted_index[str(token_id)].append(str(hotel_data["hotel_id"]))

        write_json(batch_file, inverted_index)

    return hotel_data


@app.post("/hotels/single", status_code=201)
async def create_hotel(hotel: HotelCreate):
    """Add a single new hotel"""
    try:
        hotel_data = hotel.dict(by_alias=True)
        processed_hotel = process_hotel_data(hotel_data)

        # Update hotels CSV
        hotels_df = get_hotels_df()
        updated_df = pd.concat(
            [hotels_df, pd.DataFrame([processed_hotel])], ignore_index=True
        )
        write_csv(PATHS["HOTELS"], updated_df)
        get_hotels_df.cache_clear()

        return {
            "status": "success",
            "message": "Hotel added",
            "hotel_id": processed_hotel["hotel_id"],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/hotels/upload", status_code=201)
async def upload_hotels(file: UploadFile):
    """Add multiple hotels via CSV upload"""
    try:
        df = pd.read_csv(file.file)
        required_columns = [
            "name",
            "region_id",
            "region",
            "street-address",
            "locality",
            "hotel_class",
            "service",
            "cleanliness",
            "overall",
            "value",
            "location",
            "sleep_quality",
            "rooms",
        ]

        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            raise HTTPException(
                status_code=400,
                detail=f"Missing required columns: {', '.join(missing_cols)}",
            )

        processed_hotels = []
        for _, row in df.iterrows():
            hotel_data = row.to_dict()
            processed_hotel = process_hotel_data(hotel_data)
            processed_hotels.append(processed_hotel)

        # Update hotels CSV
        hotels_df = get_hotels_df()
        updated_df = pd.concat(
            [hotels_df, pd.DataFrame(processed_hotels)], ignore_index=True
        )
        write_csv(PATHS["HOTELS"], updated_df)
        get_hotels_df.cache_clear()

        return {
            "status": "success",
            "message": f"Added {len(processed_hotels)} hotels",
            "hotel_ids": [h["hotel_id"] for h in processed_hotels],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/reviews", status_code=201)
async def create_review(review: ReviewCreate):
    """Add new review"""
    try:
        hotels_df = get_hotels_df()

        # Verify hotel exists
        if str(review.hotel_id) not in hotels_df["hotel_id"].astype(str).values:
            raise HTTPException(status_code=404, detail="Hotel not found")

        review_dict = review.dict()
        # Generate review ID with timestamp
        review_id = f"{review.hotel_id}:{pd.Timestamp.now().timestamp()}"

        # Combine title and text for tokenization
        text = f"{review.title} {review.text}"

        # Get token IDs
        tokens = tokenizer.tokenize_with_spacy(text.lower())
        lexicon = get_lexicon()
        token_ids = [lexicon[token] for token in tokens if token in lexicon]

        # Update forward index
        forward_file = f"{PATHS['FORWARD_INDEX']}/reviews/forward_index_latest.json"
        forward_index = read_json(forward_file) if os.path.exists(forward_file) else {}
        forward_index[review_id] = token_ids
        write_json(forward_file, forward_index)

        # Update inverted indices
        for token_id in token_ids:
            batch_start, batch_end = get_batch_range(token_id)
            batch_file = f"{PATHS['INVERTED_INDEX']}/reviews/inverted_index_{batch_start}-{batch_end}.json"

            inverted_index = read_json(batch_file) if os.path.exists(batch_file) else {}
            if str(token_id) not in inverted_index:
                inverted_index[str(token_id)] = []

            if review_id not in inverted_index[str(token_id)]:
                inverted_index[str(token_id)].append(review_id)

            write_json(batch_file, inverted_index)

        # Save review to batch file
        batch_file = get_review_batch_file(review.hotel_id)
        reviews_df = (
            read_csv(batch_file) if os.path.exists(batch_file) else pd.DataFrame()
        )

        reviews_df = pd.concat(
            [reviews_df, pd.DataFrame([review_dict])], ignore_index=True
        )
        write_csv(batch_file, reviews_df)
        get_reviews_from_batch.cache_clear()

        return {"status": "success", "message": "Review added", "review_id": review_id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/hotels/{hotel_id}")
async def get_hotel(hotel_id: str):
    """Get hotel details"""
    try:
        hotels_df = get_hotels_df()
        hotel_details = hotels_df[hotels_df["hotel_id"].astype(str) == hotel_id]

        if hotel_details.empty:
            raise HTTPException(
                status_code=404, detail=f"No hotel found with ID {hotel_id}"
            )

        return {
            "status": "success",
            "hotel_details": hotel_details.replace([np.inf, -np.inf, np.nan], None)
            .iloc[0]
            .to_dict(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/hotels/{hotel_id}/reviews")
async def get_hotel_reviews(hotel_id: str):
    """Get hotel reviews"""
    try:
        hotel_id_int = int(hotel_id)
        batch_file = get_review_batch_file(hotel_id_int)

        if not os.path.exists(batch_file):
            return {"status": "success", "reviews": [], "num_reviews": 0}

        reviews = get_reviews_from_batch(batch_file, hotel_id_int)
        return {"status": "success", "reviews": reviews, "num_reviews": len(reviews)}
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid hotel ID format")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
