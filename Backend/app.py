from fastapi import FastAPI, UploadFile, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
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
    "LEXICON": f"{INDEX_DIR}/lexicon/final/combined_lexicon.json",
    "FORWARD_INDEX": f"{INDEX_DIR}/forward_index",
    "INVERTED_INDEX": f"{INDEX_DIR}/inverted_index",
}

# Initialize directories
for dir_path in [DATA_DIR, REVIEWS_DIR, INDEX_DIR, f"{INDEX_DIR}/lexicon"]:
    os.makedirs(dir_path, exist_ok=True)

# Initialize shared services
tokenizer = Tokenizer()  # Use original Tokenizer


# Models
class ReviewCreate(BaseModel):
    title: str
    text: str
    rating: float = None


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
        with open(batch_file, 'r', encoding='utf-8-sig') as f:
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
        with open(batch_file, 'r', encoding='utf-8-sig') as f:
            index_batch = json.load(f)
            return set(index_batch.get(str(word_id), []))
    except FileNotFoundError:
        print(f"Batch file not found: {batch_file}")
        return set()

@app.get("/search")
async def search(query: str = Query(...), type: str = Query("hotels")):
    """Simple search using inverted index batches"""
    try:
        # Get token IDs from lexicon
        tokens = tokenizer.tokenize_with_spacy(query.lower())
        if not tokens:
            return {"results": [], "count": 0}
            
        # Get word IDs from lexicon
        lexicon = get_lexicon()
        token_ids = []
        for token in tokens:
            if token in lexicon:
                token_ids.append(lexicon[token])
                
        if not token_ids:
            return {"results": [], "count": 0}
            
        # Get matching documents
        all_doc_ids = [get_doc_ids(token_id, type) for token_id in token_ids]
        if not all_doc_ids:
            return {"results": [], "count": 0}
            
        # Intersect results (AND search)
        matched_ids = set.intersection(*all_doc_ids)
        if not matched_ids:
            return {"results": [], "count": 0}
            
        # Return results based on type
        if type == "hotels":
            # Get hotel details
            hotels_df = get_hotels_df()
            results = (hotels_df[hotels_df['hotel_id'].astype(str).isin(matched_ids)]
                      .replace([np.inf, -np.inf, np.nan], None)
                      .to_dict('records'))
            return {"results": results, "count": len(results)}
            
        else:  # reviews
            results = []
            hotels_df = get_hotels_df()
            
            # Process each matching review
            for review_id in matched_ids:
                try:
                    hotel_id = review_id.split(':')[0]
                    batch_file = get_review_batch_file(int(hotel_id))
                    
                    if os.path.exists(batch_file):
                        # Get hotel info
                        hotel_info = hotels_df[
                            hotels_df['hotel_id'].astype(str) == hotel_id
                        ].iloc[0].to_dict() if hotel_id in hotels_df['hotel_id'].astype(str).values else None
                        
                        # Get reviews
                        if hotel_info:
                            reviews = get_reviews_from_batch(batch_file)
                            for review in reviews:
                                review['hotel'] = hotel_info
                                results.append(review)
                                
                        if len(results) >= 1000:
                            break
                except:
                    continue
            
            return {
                "results": results[:1000],
                "count": len(results),
                "hotels_with_reviews": len({r.get('hotel', {}).get('hotel_id') for r in results})
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


@app.post("/reviews/{hotel_id}", status_code=201)
async def create_review(hotel_id: str, review: ReviewCreate):
    """Add new review"""
    try:
        hotels_df = get_hotels_df()
        hotel_id_int = int(hotel_id)

        if str(hotel_id_int) not in hotels_df["hotel_id"].astype(str).values:
            raise HTTPException(status_code=404, detail="Hotel not found")

        review_dict = review.dict()
        review_id = f"{hotel_id}:{pd.Timestamp.now().timestamp()}"
        text = f"{review.title} {review.text}"

        # Get token IDs
        tokens = tokenizer.tokenize_with_spacy(text)
        lexicon = get_lexicon()
        token_ids = [lexicon[token] for token in tokens if token in lexicon]

        # Update forward index
        forward_file = f"{PATHS['FORWARD_INDEX']}/reviews/forward_index_latest.json"
        forward_index = read_json(forward_file)
        forward_index[review_id] = token_ids
        write_json(forward_file, forward_index)

        # Update inverted indices
        for token_id in token_ids:
            batch_start, batch_end = get_batch_range(token_id)
            batch_file = f"{PATHS['INVERTED_INDEX']}/reviews/inverted_index_{batch_start}-{batch_end}.json"

            inverted_index = read_json(batch_file)
            if str(token_id) not in inverted_index:
                inverted_index[str(token_id)] = []

            if review_id not in inverted_index[str(token_id)]:
                inverted_index[str(token_id)].append(review_id)

            write_json(batch_file, inverted_index)

        # Save review to batch file
        batch_file = get_review_batch_file(hotel_id_int)
        reviews_df = read_csv(batch_file)

        review_dict["hotel_id"] = hotel_id_int
        reviews_df = pd.concat(
            [reviews_df, pd.DataFrame([review_dict])], ignore_index=True
        )
        write_csv(batch_file, reviews_df)
        get_reviews_from_batch.cache_clear()

        return {"status": "success", "message": "Review added"}
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid hotel ID format")
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
