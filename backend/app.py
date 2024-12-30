from fastapi import FastAPI, HTTPException, Query, BackgroundTasks, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.concurrency import run_in_threadpool
import pandas as pd
import json
import os
import io
from typing import List, Dict, Optional
from collections import defaultdict, Counter
from datetime import datetime
import aiofiles
import math
import asyncio
import numpy as np
from pydantic import BaseModel, validator, Field
import logging
import threading
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from utils.tokenizer import Tokenizer
from utils.file_io import read_json, write_json, read_csv, write_csv

# Configure logging
logger = logging.getLogger(__name__)

########################################
# Sentiment Analyzer Initialization
########################################
analyzer = SentimentIntensityAnalyzer()


########################################
# Helper Functions
########################################
def analyze_sentiment(text: str) -> float:
    """
    Analyze the sentiment of the given text and return the compound score.
    Compound score ranges from -1 (most negative) to +1 (most positive).
    """
    sentiment = analyzer.polarity_scores(text)
    return sentiment["compound"]


def clean_float_values(obj):
    if isinstance(obj, dict):
        return {k: clean_float_values(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_float_values(x) for x in obj]
    elif isinstance(obj, float):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return obj
    return obj


########################################
# Configuration
########################################
class Config:
    DATA_DIR = "./data"
    REVIEWS_DIR = f"./reviews"
    INDEX_DIR = f"./index data"
    INVERTED_INDEX_PATH = f"{INDEX_DIR}/inverted_index"
    FORWARD_INDEX_PATH = f"{INDEX_DIR}/forward_index"
    HOTELS_PATH = f"{DATA_DIR}/hotels_cleaned.csv"
    LEXICON_PATH = f"{INDEX_DIR}/lexicon/lexicon.json"
    SENTIMENT_PATH = f"{INDEX_DIR}/doc_sentiment.json"

    INVERTED_BATCH_SIZE = 20000
    FORWARD_BATCH_SIZE = 20000
    REVIEW_BATCH_SIZE = 1000

    MAX_RESULTS = 500
    MAX_DOCS_TO_PROCESS = 1000000

    SCORING_PARAMS = {
        "field_weights": {
            "name": 4.0,
            "region": 2.0,
            "street-address": 3.0,
            "locality": 2.5,
            "title": 3.0,
            "text": 1.5,
        },
        # Additional scoring knobs
        "base_freq_weight": 0.3,  # base weight for freq
        "multi_token_bonus": 0.2,  # small bonus per distinct token matched
        "field_weight_bonus": 1.0,  # how much to multiply field weights
        "length_norm_factor": 0.05,  # small factor to reduce huge freq blowups
        # For sentiment adjustments
        "negative_sentiment_weight": 2.0,  # Weight multiplier if query sentiment is negative
        "positive_sentiment_weight": 1.5,  # Weight multiplier if query sentiment is positive
    }

    @staticmethod
    def initialize():
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        os.makedirs(Config.REVIEWS_DIR, exist_ok=True)
        os.makedirs(Config.INVERTED_INDEX_PATH, exist_ok=True)
        os.makedirs(Config.FORWARD_INDEX_PATH, exist_ok=True)
        os.makedirs(f"{Config.INVERTED_INDEX_PATH}/hotels", exist_ok=True)
        os.makedirs(f"{Config.INVERTED_INDEX_PATH}/reviews", exist_ok=True)
        os.makedirs(f"{Config.FORWARD_INDEX_PATH}/hotels", exist_ok=True)
        os.makedirs(f"{Config.FORWARD_INDEX_PATH}/reviews", exist_ok=True)
        os.makedirs(os.path.dirname(Config.LEXICON_PATH), exist_ok=True)


Config.initialize()


########################################
# Pydantic Models
########################################
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

    @validator(
        "overall",
        "service",
        "cleanliness",
        "value",
        "location",
        "sleep_quality",
        "rooms",
    )
    def validate_ratings(cls, v):
        if v is not None and (v < 0 or v > 5):
            raise ValueError("Rating must be between 0 and 5")
        return v


class HotelCreate(BaseModel):
    name: str
    region_id: str
    region: str
    street_address: str = Field(..., alias="street-address")
    locality: str
    hotel_class: Optional[float] = None
    service: Optional[float] = None
    cleanliness: Optional[float] = None
    overall: Optional[float] = None
    value: Optional[float] = None
    location: Optional[float] = None
    sleep_quality: Optional[float] = None
    rooms: Optional[float] = None
    average_score: Optional[float] = None

    @validator("hotel_class")
    def validate_hotel_class(cls, v):
        if v is not None and (v < 0 or v > 5):
            raise ValueError("Hotel class must be between 0 and 5")
        return v


########################################
# Cache Implementation
########################################
class Cache:
    def __init__(self, ttl_seconds: int = 3600):
        self.cache = {}
        self.ttl = ttl_seconds
        self.timestamps = {}

    async def get(self, key: str):
        if (
            key in self.cache
            and (datetime.now() - self.timestamps[key]).total_seconds() < self.ttl
        ):
            logger.debug(f"Cache hit for key: {key}")
            return self.cache[key]
        logger.debug(f"Cache miss for key: {key}")
        return None

    async def set(self, key: str, value: any):
        self.cache[key] = value
        self.timestamps[key] = datetime.now()
        logger.debug(f"Cache set for key: {key}")

    async def delete(self, key: str):
        if key in self.cache:
            del self.cache[key]
            logger.debug(f"Cache deleted for key: {key}")
        if key in self.timestamps:
            del self.timestamps[key]

    async def clear(self):
        self.cache.clear()
        self.timestamps.clear()
        logger.debug("Cache cleared.")


########################################
# Search Engine Implementation
########################################
class SearchEngine:
    def __init__(self):
        self.tokenizer = Tokenizer()
        self.lexicon = {}
        self.hotels_df = pd.DataFrame()
        self.reviews_df = pd.DataFrame()
        self.document_cache = Cache()
        self.config = Config

        self.current_rev_id = 0
        self.rev_id_lock = threading.Lock()
        self.rev_id_file = os.path.join(Config.DATA_DIR, "current_rev_id.json")

        # rev_id -> hotel_id
        self.rev_to_hotel = {}

        # doc_id -> sentiment score
        self.doc_sentiment = {}

        self._load_data()
        self._initialize_rev_id()
        self._rebuild_rev_to_hotel_from_disk()
        self._load_sentiment_scores()

    def _load_data(self):
        self.lexicon = read_json(Config.LEXICON_PATH)
        self.hotels_df = read_csv(Config.HOTELS_PATH)
        logger.debug(f"Loaded lexicon with {len(self.lexicon)} entries.")
        logger.debug(f"Loaded hotels data with {len(self.hotels_df)} entries.")

    def reload_data(self):
        self.lexicon = read_json(Config.LEXICON_PATH)
        self.hotels_df = read_csv(Config.HOTELS_PATH)
        self.reviews_df = self._load_reviews()
        logger.debug("Reloaded data for search engine.")

    def _initialize_rev_id(self):
        if os.path.exists(self.rev_id_file):
            with open(self.rev_id_file, "r") as f:
                data = json.load(f)
                self.current_rev_id = int(
                    data.get("current_rev_id", self._find_max_rev_id())
                )
                logger.debug(
                    f"Initialized current_rev_id from file: {self.current_rev_id}"
                )
        else:
            self.current_rev_id = self._find_max_rev_id()
            self._persist_rev_id()
            logger.debug(
                f"Initialized current_rev_id by finding max rev_id: {self.current_rev_id}"
            )

    def _find_max_rev_id(self) -> int:
        max_rev_id = 0
        try:
            for file in os.listdir(Config.REVIEWS_DIR):
                if file.startswith("reviews_") and file.endswith(".csv"):
                    path = os.path.join(Config.REVIEWS_DIR, file)
                    df = read_csv(path)
                    if "rev_id" in df.columns and not df.empty:
                        cur = df["rev_id"].max()
                        if cur > max_rev_id:
                            max_rev_id = cur
            logger.debug(f"Found max rev_id: {max_rev_id}")
            return int(max_rev_id)
        except Exception as e:
            logger.error(f"Error finding max rev_id: {e}", exc_info=True)
            return 0

    def _persist_rev_id(self):
        with open(self.rev_id_file, "w") as f:
            json.dump({"current_rev_id": int(self.current_rev_id)}, f)
            logger.debug(f"Persisted current_rev_id: {self.current_rev_id}")

    def _get_next_rev_id(self) -> int:
        with self.rev_id_lock:
            self.current_rev_id += 1
            self._persist_rev_id()
            logger.debug(f"Generated new rev_id: {self.current_rev_id}")
            return self.current_rev_id

    def _rebuild_rev_to_hotel_from_disk(self):
        self.rev_to_hotel.clear()
        for fn in os.listdir(Config.REVIEWS_DIR):
            if fn.startswith("reviews_") and fn.endswith(".csv"):
                df = read_csv(os.path.join(Config.REVIEWS_DIR, fn))
                if "rev_id" in df.columns and "hotel_id" in df.columns:
                    for _, row_data in df.iterrows():
                        self.rev_to_hotel[int(row_data["rev_id"])] = int(
                            row_data["hotel_id"]
                        )
        logger.debug(
            f"Rebuilt rev_to_hotel mapping with {len(self.rev_to_hotel)} entries."
        )

    def _get_review_batch_file(self, hotel_id: int) -> str:
        start = (
            (hotel_id - 1) // self.config.REVIEW_BATCH_SIZE
        ) * self.config.REVIEW_BATCH_SIZE + 1
        end = start + self.config.REVIEW_BATCH_SIZE - 1
        return f"{Config.REVIEWS_DIR}/reviews_{start}-{end}.csv"

    def get_hotels_df(self):
        if self.hotels_df.empty:
            self.hotels_df = read_csv(Config.HOTELS_PATH)
            logger.debug(f"Loaded hotels data with {len(self.hotels_df)} entries.")
        return self.hotels_df

    def _load_reviews(self):
        revs = []
        for f in os.listdir(Config.REVIEWS_DIR):
            if f.endswith(".csv"):
                df = read_csv(os.path.join(Config.REVIEWS_DIR, f))
                if not df.empty:
                    revs.append(df)
        if revs:
            combined = pd.concat(revs, ignore_index=True)
            logger.debug(f"Loaded reviews data with {len(combined)} entries.")
            return combined
        logger.debug("No reviews data found.")
        return pd.DataFrame()

    def get_reviews_df(self):
        if self.reviews_df.empty:
            self.reviews_df = self._load_reviews()
        return self.reviews_df

    def _load_sentiment_scores(self):
        """
        Load sentiment scores from the sentiment JSON file.
        If the file doesn't exist, initialize an empty dictionary.
        """
        if os.path.exists(Config.SENTIMENT_PATH):
            try:
                with open(Config.SENTIMENT_PATH, "r") as f:
                    self.doc_sentiment = json.load(f)
                logger.debug(
                    f"Loaded sentiment scores with {len(self.doc_sentiment)} entries."
                )
            except json.JSONDecodeError as e:
                logger.error(f"Error loading sentiment scores: {e}")
                self.doc_sentiment = {}
        else:
            self.doc_sentiment = {}
            logger.debug("Initialized empty sentiment scores.")

    def _save_sentiment_scores(self):
        """
        Save the sentiment scores to the sentiment JSON file.
        """
        try:
            write_json(Config.SENTIMENT_PATH, self.doc_sentiment)
            logger.debug(f"Saved sentiment scores to {Config.SENTIMENT_PATH}.")
        except Exception as e:
            logger.error(f"Error saving sentiment scores: {e}", exc_info=True)

    ##########################################################
    # Main search with Sentiment and Filter Adjustments
    ##########################################################
    async def search(
        self,
        query: str,
        doc_type: str,
        location: Optional[str] = None,
        hotel_class: Optional[int] = None,
    ) -> Dict:
        logger.info(
            f"search(query='{query}', doc_type='{doc_type}', location='{location}', hotel_class='{hotel_class}') called."
        )
        from fastapi.concurrency import run_in_threadpool

        # 1) Basic tokenization
        original_words = [w for w in query.lower().split() if w]
        spacy_tokens = await run_in_threadpool(
            self.tokenizer.tokenize_with_spacy, query.lower()
        )
        base_tokens = list(set(original_words + spacy_tokens))
        logger.debug(f"Tokenized query: {base_tokens}")
        if not base_tokens:
            logger.debug("No tokens extracted from query.")
            return {"results": [], "count": 0, "total_matches": 0}

        # 2) Compute sentiment of the query
        query_text = " ".join(base_tokens)
        query_sentiment = analyze_sentiment(query_text)  # -1 to +1
        logger.info(f"Query sentiment score: {query_sentiment}")

        # 3) Convert tokens to word IDs
        word_ids = []
        for token in base_tokens:
            if token in self.lexicon:
                word_ids.append(self.lexicon[token])
                logger.debug(f"Token '{token}' mapped to word ID {self.lexicon[token]}")
            else:
                logger.debug(f"Token '{token}' not found in lexicon.")
        if not word_ids:
            logger.debug("No word IDs found for tokens.")
            return {"results": [], "count": 0, "total_matches": 0}

        # 4) Union-based search in hotels and reviews
        matched_hotels = await self._search_union(word_ids, "hotels")
        matched_reviews = await self._search_union(word_ids, "reviews")

        # 5) Apply Filters: Location and Hotel Class
        if location:
            # Normalize location string for case-insensitive matching
            location_normalized = location.strip().lower()
            # Filter matched_hotels with partial matching
            filtered_matched_hotels = {}
            for h_id, info in matched_hotels.items():
                hotel_row = self.hotels_df[self.hotels_df["hotel_id"] == int(h_id)]
                if hotel_row.empty:
                    continue
                hotel_locality = str(hotel_row.iloc[0]["locality"]).strip().lower()
                if location_normalized not in hotel_locality:
                    continue
                if hotel_class is not None:
                    hotel_class_value = hotel_row.iloc[0]["hotel_class"]
                    if hotel_class_value != hotel_class:
                        continue
                filtered_matched_hotels[h_id] = info
            matched_hotels = filtered_matched_hotels
            logger.debug(
                f"After location and hotel_class filtering, matched_hotels count: {len(matched_hotels)}"
            )

            # Filter matched_reviews based on associated hotel's location and hotel_class
            filtered_matched_reviews = {}
            for rev_id_str, info in matched_reviews.items():
                try:
                    rev_id_int = int(rev_id_str)
                except ValueError:
                    logger.debug(f"Invalid review ID: {rev_id_str}")
                    continue
                if rev_id_int not in self.rev_to_hotel:
                    logger.debug(f"Review ID {rev_id_int} not mapped to any hotel.")
                    continue
                h_id = self.rev_to_hotel[rev_id_int]
                hotel_row = self.hotels_df[self.hotels_df["hotel_id"] == h_id]
                if hotel_row.empty:
                    continue
                hotel_locality = str(hotel_row.iloc[0]["locality"]).strip().lower()
                if location_normalized not in hotel_locality:
                    continue
                if hotel_class is not None:
                    hotel_class_value = hotel_row.iloc[0]["hotel_class"]
                    if hotel_class_value != hotel_class:
                        continue
                filtered_matched_reviews[rev_id_str] = info
            matched_reviews = filtered_matched_reviews
            logger.debug(
                f"After location and hotel_class filtering, matched_reviews count: {len(matched_reviews)}"
            )

        else:
            if hotel_class is not None:
                # If only hotel_class is provided without location
                filtered_matched_hotels = {}
                for h_id, info in matched_hotels.items():
                    hotel_row = self.hotels_df[self.hotels_df["hotel_id"] == int(h_id)]
                    if hotel_row.empty:
                        continue
                    hotel_class_value = hotel_row.iloc[0]["hotel_class"]
                    if hotel_class_value != hotel_class:
                        continue
                    filtered_matched_hotels[h_id] = info
                matched_hotels = filtered_matched_hotels
                logger.debug(
                    f"After hotel_class filtering, matched_hotels count: {len(matched_hotels)}"
                )

                # Filter matched_reviews based on associated hotel's hotel_class
                filtered_matched_reviews = {}
                for rev_id_str, info in matched_reviews.items():
                    try:
                        rev_id_int = int(rev_id_str)
                    except ValueError:
                        logger.debug(f"Invalid review ID: {rev_id_str}")
                        continue
                    if rev_id_int not in self.rev_to_hotel:
                        logger.debug(f"Review ID {rev_id_int} not mapped to any hotel.")
                        continue
                    h_id = self.rev_to_hotel[rev_id_int]
                    hotel_row = self.hotels_df[self.hotels_df["hotel_id"] == h_id]
                    if hotel_row.empty:
                        continue
                    hotel_class_value = hotel_row.iloc[0]["hotel_class"]
                    if hotel_class_value != hotel_class:
                        continue
                    filtered_matched_reviews[rev_id_str] = info
                matched_reviews = filtered_matched_reviews
                logger.debug(
                    f"After hotel_class filtering, matched_reviews count: {len(matched_reviews)}"
                )

        if doc_type == "reviews":
            final_list = self._score_and_fetch_reviews(
                matched_reviews, base_tokens, query_sentiment
            )
            total = len(final_list)
            final_list = final_list[: self.config.MAX_RESULTS]
            return {
                "results": final_list,
                "count": len(final_list),
                "total_matches": total,
            }
        else:
            # doc_type=all or doc_type=hotels
            unified_hotels = {}

            for doc_id, info in matched_hotels.items():
                try:
                    h_id = int(doc_id)
                except ValueError:
                    logger.debug(f"Invalid hotel ID: {doc_id}")
                    continue
                if h_id not in unified_hotels:
                    unified_hotels[h_id] = {
                        "freq": info["freq"],
                        "fields": info["fields"][:],
                    }
                else:
                    unified_hotels[h_id]["freq"] += info["freq"]
                    unified_hotels[h_id]["fields"].extend(info["fields"])

            for rev_id_str, info in matched_reviews.items():
                try:
                    rev_id_int = int(rev_id_str)
                except ValueError:
                    logger.debug(f"Invalid review ID: {rev_id_str}")
                    continue
                if rev_id_int not in self.rev_to_hotel:
                    logger.debug(f"Review ID {rev_id_int} not mapped to any hotel.")
                    continue
                h_id = self.rev_to_hotel[rev_id_int]
                if h_id not in unified_hotels:
                    unified_hotels[h_id] = {
                        "freq": info["freq"],
                        "fields": info["fields"][:],
                    }
                else:
                    unified_hotels[h_id]["freq"] += info["freq"]
                    unified_hotels[h_id]["fields"].extend(info["fields"])

            final_list = self._score_hotels(
                unified_hotels, base_tokens, query_sentiment
            )
            total = len(final_list)
            final_list = final_list[: self.config.MAX_RESULTS]
            return {
                "results": final_list,
                "count": len(final_list),
                "total_matches": total,
            }

    async def _search_union(self, word_ids: List[int], doc_type: str) -> Dict:
        """
        For each word_id, gather docs from that doc_type => perform a union
        Summation of freq if doc appears multiple times
        """
        results = {}
        doc_counter = 0

        for w_id in word_ids:
            batch_start = (
                w_id // self.config.INVERTED_BATCH_SIZE
            ) * self.config.INVERTED_BATCH_SIZE
            batch_end = batch_start + self.config.INVERTED_BATCH_SIZE - 1
            inv_file = f"{self.config.INVERTED_INDEX_PATH}/{doc_type}/inverted_index_{batch_start}-{batch_end}.json"

            if not os.path.exists(inv_file):
                logger.debug(f"Inverted index file {inv_file} does not exist.")
                continue

            try:
                async with aiofiles.open(inv_file, "r", encoding="utf-8-sig") as f:
                    content = await f.read()
                inv_data = json.loads(content)
                logger.debug(f"Loaded inverted index from {inv_file}.")
            except Exception as e:
                logger.error(f"Error reading {inv_file}: {e}", exc_info=True)
                continue

            if str(w_id) not in inv_data:
                logger.debug(f"Word ID {w_id} not found in {inv_file}.")
                continue

            postings = inv_data[str(w_id)]["docs"]
            logger.debug(f"Word ID {w_id} found in {len(postings)} documents.")

            for p in postings:
                doc_id = p["id"]
                freq = p["freq"]
                fields = p["fields"]

                if doc_id not in results:
                    results[doc_id] = {
                        "freq": freq,
                        "fields": fields[:],
                    }
                    doc_counter += 1
                    logger.debug(f"Added new document ID {doc_id} with freq {freq}.")
                    if doc_counter >= self.config.MAX_DOCS_TO_PROCESS:
                        logger.info("Reached MAX_DOCS_TO_PROCESS limit.")
                        break
                else:
                    results[doc_id]["freq"] += freq
                    results[doc_id]["fields"].extend(fields)
                    logger.debug(
                        f"Updated document ID {doc_id} with additional freq {freq}."
                    )

            if doc_counter >= self.config.MAX_DOCS_TO_PROCESS:
                break

        logger.debug(f"Total matched documents for '{doc_type}': {len(results)}")
        return results

    def _score_hotels(
        self, matched_docs: Dict, query_tokens: List[str], query_sentiment: float
    ) -> List[Dict]:
        """
        Scoring for hotels with sentiment adjustment.
        """
        df = self.get_hotels_df()
        if df.empty:
            logger.debug("Hotels DataFrame is empty.")
            return []

        # Scoring parameters
        base_freq = self.config.SCORING_PARAMS["base_freq_weight"]
        multi_bonus = self.config.SCORING_PARAMS["multi_token_bonus"]
        length_norm = self.config.SCORING_PARAMS["length_norm_factor"]
        field_importance = self.config.SCORING_PARAMS["field_weights"]

        # Sentiment adjustment parameters
        negative_weight = self.config.SCORING_PARAMS["negative_sentiment_weight"]
        positive_weight = self.config.SCORING_PARAMS["positive_sentiment_weight"]

        # Sentiment scaling factors
        SENTIMENT_BOOST_FACTOR = 0.1
        SENTIMENT_PENALTY_FACTOR = 0.1

        # Determine if the query is negative or positive
        if query_sentiment < -0.05:
            sentiment_type = "negative"
        elif query_sentiment > 0.05:
            sentiment_type = "positive"
        else:
            sentiment_type = "neutral"

        logger.debug(f"Query sentiment type: {sentiment_type}")

        doc_ids = list(matched_docs.keys())[: self.config.MAX_DOCS_TO_PROCESS]
        distinct_query_tokens = set(query_tokens)
        results = []

        logger.debug(f"Number of matched hotels to score: {len(doc_ids)}")

        for doc_id in doc_ids:
            doc_data = matched_docs[doc_id]
            try:
                h_id = int(doc_id)
            except ValueError:
                logger.debug(f"Invalid hotel ID: {doc_id}")
                continue

            row = df[df["hotel_id"] == h_id]
            if row.empty:
                logger.debug(f"No hotel found with ID {h_id}")
                continue

            freq = doc_data["freq"]
            fields = doc_data["fields"]

            # Basic frequency-based scoring
            score = freq * base_freq

            # Field-based weighting
            for f in fields:
                score += field_importance.get(f, 2.0)  # fallback=2.0 for unknown fields

            # Multi-token bonus
            approx_matched = min(len(distinct_query_tokens), freq)
            score += approx_matched * multi_bonus

            # Length normalization
            score /= 1 + length_norm * freq

            # Sentiment adjustment
            doc_sentiment = self.doc_sentiment.get(
                str(h_id), 0.0
            )  # Default to neutral if not found

            if sentiment_type == "negative" and doc_sentiment < 0:
                score += SENTIMENT_BOOST_FACTOR * abs(doc_sentiment)
                logger.debug(
                    f"Hotel ID {h_id} aligned with negative sentiment. Boosted score to {score}"
                )
            elif sentiment_type == "positive" and doc_sentiment > 0:
                score += SENTIMENT_BOOST_FACTOR * doc_sentiment
                logger.debug(
                    f"Hotel ID {h_id} aligned with positive sentiment. Boosted score to {score}"
                )
            elif sentiment_type == "negative" and doc_sentiment > 0:
                score -= SENTIMENT_PENALTY_FACTOR * doc_sentiment
                logger.debug(
                    f"Hotel ID {h_id} misaligned with negative sentiment. Penalized score to {score}"
                )
            elif sentiment_type == "positive" and doc_sentiment < 0:
                score -= SENTIMENT_PENALTY_FACTOR * abs(doc_sentiment)
                logger.debug(
                    f"Hotel ID {h_id} misaligned with positive sentiment. Penalized score to {score}"
                )
            # Neutral sentiment: No adjustment

            # Ensure the score doesn't drop below base_freq
            if score < base_freq:
                score = base_freq
                logger.debug(
                    f"Score for hotel ID {h_id} adjusted to base_freq {base_freq}."
                )

            # Store final result
            info = row.iloc[0].to_dict()
            info["search_score"] = score
            info["matched_fields"] = list(set(fields))
            info["matched_terms"] = list(distinct_query_tokens)
            info["sentiment_score"] = (
                doc_sentiment  # Optional: Include sentiment score in results
            )

            results.append(clean_float_values(info))
            logger.debug(f"Hotel ID {h_id} scored with search_score {score}")

        # Sort by final score in descending order
        results.sort(key=lambda x: x["search_score"], reverse=True)
        logger.debug(f"Top {len(results)} hotels after scoring.")
        return results

    def _score_and_fetch_reviews(
        self, matched_docs: Dict, query_tokens: List[str], query_sentiment: float
    ) -> List[Dict]:
        """
        Scoring for reviews with sentiment adjustment.
        """
        if not matched_docs:
            logger.debug("No matched reviews to score.")
            return []
        from collections import defaultdict, Counter

        df = self.get_reviews_df()
        if df.empty:
            logger.debug("Reviews DataFrame is empty.")
            return []

        # Scoring parameters
        base_freq = self.config.SCORING_PARAMS["base_freq_weight"]
        multi_bonus = self.config.SCORING_PARAMS["multi_token_bonus"]
        length_norm = self.config.SCORING_PARAMS["length_norm_factor"]
        field_importance = self.config.SCORING_PARAMS["field_weights"]

        # Sentiment adjustment parameters
        negative_weight = self.config.SCORING_PARAMS["negative_sentiment_weight"]
        positive_weight = self.config.SCORING_PARAMS["positive_sentiment_weight"]

        # Sentiment scaling factors
        SENTIMENT_BOOST_FACTOR = 0.1
        SENTIMENT_PENALTY_FACTOR = 0.1

        # Determine if the query is negative or positive
        if query_sentiment < -0.05:
            sentiment_type = "negative"
        elif query_sentiment > 0.05:
            sentiment_type = "positive"
        else:
            sentiment_type = "neutral"

        logger.debug(f"Query sentiment type: {sentiment_type}")

        distinct_query_tokens = set(query_tokens)

        # Group rev_ids by hotel
        hotel_map = defaultdict(list)
        for doc_id, doc_info in matched_docs.items():
            try:
                rev_id_int = int(doc_id)
            except ValueError:
                logger.debug(f"Invalid review ID: {doc_id}")
                continue
            if rev_id_int not in self.rev_to_hotel:
                logger.debug(f"Review ID {rev_id_int} not mapped to any hotel.")
                continue
            h_id = self.rev_to_hotel[rev_id_int]
            hotel_map[h_id].append(rev_id_int)

        results = []
        hotel_review_count = Counter()

        logger.debug(f"Number of hotels with matched reviews: {len(hotel_map)}")

        for h_id, rev_list in hotel_map.items():
            batch_file = self._get_review_batch_file(h_id)
            if not os.path.exists(batch_file):
                logger.debug(
                    f"Review batch file {batch_file} does not exist for hotel ID {h_id}."
                )
                continue
            batch_df = read_csv(batch_file)
            if batch_df.empty or "rev_id" not in batch_df.columns:
                logger.debug(
                    f"Review batch file {batch_file} is empty or missing 'rev_id'."
                )
                continue
            subdf = batch_df[
                batch_df["rev_id"].astype(str).isin([str(x) for x in rev_list])
            ]
            if subdf.empty:
                logger.debug(
                    f"No matching reviews found in {batch_file} for hotel ID {h_id}."
                )
                continue

            logger.debug(
                f"Found {len(subdf)} matching reviews in {batch_file} for hotel ID {h_id}."
            )

            for _, row_data in subdf.iterrows():
                rev_str = str(row_data["rev_id"])
                if rev_str not in matched_docs:
                    logger.debug(f"Review ID {rev_str} not in matched_docs.")
                    continue

                doc_info = matched_docs[rev_str]
                freq = doc_info["freq"]
                fields = doc_info["fields"]

                # Basic frequency-based scoring
                score = freq * base_freq

                # Field-based weighting
                for f in fields:
                    score += field_importance.get(
                        f, 1.0
                    )  # fallback=1.0 for unknown fields

                # Multi-token bonus
                approx_matched = min(len(distinct_query_tokens), freq)
                score += approx_matched * multi_bonus

                # Length normalization
                score /= 1 + length_norm * freq

                # Sentiment adjustment
                doc_sentiment = self.doc_sentiment.get(
                    rev_str, 0.0
                )  # Default to neutral if not found

                if sentiment_type == "negative" and doc_sentiment < 0:
                    score += SENTIMENT_BOOST_FACTOR * abs(doc_sentiment)
                    logger.debug(
                        f"Review ID {rev_str} aligned with negative sentiment. Boosted score to {score}"
                    )
                elif sentiment_type == "positive" and doc_sentiment > 0:
                    score += SENTIMENT_BOOST_FACTOR * doc_sentiment
                    logger.debug(
                        f"Review ID {rev_str} aligned with positive sentiment. Boosted score to {score}"
                    )
                elif sentiment_type == "negative" and doc_sentiment > 0:
                    score -= SENTIMENT_PENALTY_FACTOR * doc_sentiment
                    logger.debug(
                        f"Review ID {rev_str} misaligned with negative sentiment. Penalized score to {score}"
                    )
                elif sentiment_type == "positive" and doc_sentiment < 0:
                    score -= SENTIMENT_PENALTY_FACTOR * abs(doc_sentiment)
                    logger.debug(
                        f"Review ID {rev_str} misaligned with positive sentiment. Penalized score to {score}"
                    )
                # Neutral sentiment: No adjustment

                # Ensure the score doesn't drop below base_freq
                if score < base_freq:
                    score = base_freq
                    logger.debug(
                        f"Score for review ID {rev_str} adjusted to base_freq {base_freq}."
                    )

                # Store final result
                row_dict = row_data.to_dict()
                row_dict["search_score"] = score
                row_dict["matched_fields"] = list(set(fields))
                row_dict["matched_terms"] = list(distinct_query_tokens)
                row_dict["sentiment_score"] = (
                    doc_sentiment  # Optional: Include sentiment score in results
                )

                results.append(clean_float_values(row_dict))
                logger.debug(f"Review ID {rev_str} scored with search_score {score}")
                hotel_review_count[row_dict["hotel_id"]] += 1

        # Multi-review bonus
        for r in results:
            extra = hotel_review_count[r["hotel_id"]] - 1
            if extra > 0:
                r["search_score"] += 0.05 * extra
                logger.debug(
                    f"Applied multi-review bonus for hotel ID {r['hotel_id']}. New search_score: {r['search_score']}"
                )

        # Sort by final score in descending order
        results.sort(key=lambda x: x["search_score"], reverse=True)
        logger.debug(f"Top {len(results)} reviews after scoring.")
        return results

    ##########################################################
    # Index Updating with Sentiment Scoring
    ##########################################################
    async def update_indices(self, doc_id: str, text: str, doc_type: str, fields: Dict):
        logger.info(
            f"update_indices(doc_id={doc_id}, doc_type={doc_type}) => fields={fields}"
        )
        try:
            tokens = self.tokenizer.tokenize_with_spacy(text.lower())
            logger.debug(f"update_indices tokens => {tokens}")
            lex = self.lexicon or {}
            max_id = max(lex.values()) if lex else 0
            updated = False
            for t in tokens:
                if t not in lex:
                    max_id += 1
                    lex[t] = max_id
                    updated = True
                    logger.debug(
                        f"Added new token '{t}' with word ID {max_id} to lexicon."
                    )
            if updated:
                logger.info(
                    f"Lexicon updated with new tokens. Saving to {Config.LEXICON_PATH}"
                )
                write_json(Config.LEXICON_PATH, lex)
                self.lexicon = lex

            from collections import defaultdict

            word_counts = defaultdict(int)
            field_matches = defaultdict(list)
            positions = defaultdict(list)
            pos_ctr = 0
            for fkey, fval in fields.items():
                f_toks = self.tokenizer.tokenize_with_spacy(str(fval).lower())
                for ft in f_toks:
                    if ft in lex:
                        w_id = lex[ft]
                        word_counts[w_id] += 1
                        field_matches[fkey].append(w_id)
                        positions[w_id].append(pos_ctr)
                        logger.debug(
                            f"Word '{ft}' (ID {w_id}) in field '{fkey}' at position {pos_ctr}."
                        )
                    pos_ctr += 1

            # Compute sentiment score
            if doc_type == "reviews":
                sentiment_text = f"{fields.get('title', '')} {fields.get('text', '')}"
            elif doc_type == "hotels":
                # If hotels have a description, include it. Otherwise, use aggregate sentiment from reviews.
                # For simplicity, we'll assume hotels don't have additional text fields here.
                sentiment_text = ""
            else:
                sentiment_text = ""

            sentiment_score = (
                analyze_sentiment(sentiment_text) if sentiment_text.strip() else 0.0
            )
            self.doc_sentiment[doc_id] = sentiment_score
            logger.debug(
                f"Computed sentiment score for document ID {doc_id}: {sentiment_score}"
            )
            self._save_sentiment_scores()

            try:
                doc_id_int = int(doc_id)
            except ValueError:
                doc_id_int = 0
                logger.debug(f"Document ID {doc_id} is not an integer.")

            fwd_start = (
                doc_id_int // self.config.FORWARD_BATCH_SIZE
            ) * self.config.FORWARD_BATCH_SIZE
            fwd_end = fwd_start + self.config.FORWARD_BATCH_SIZE - 1
            fwd_dir = f"{self.config.FORWARD_INDEX_PATH}/{doc_type}"
            os.makedirs(fwd_dir, exist_ok=True)
            fwd_file = f"{fwd_dir}/forward_index_{fwd_start}-{fwd_end}.json"

            fwd_idx = read_json(fwd_file) if os.path.exists(fwd_file) else {}
            fwd_idx[doc_id] = {
                "word_counts": {str(k): v for k, v in word_counts.items()},
                "field_matches": {
                    ff: [str(x) for x in wlist] for ff, wlist in field_matches.items()
                },
                "word_positions": {str(k): v for k, v in positions.items()},
                "sentiment": sentiment_score,
            }
            write_json(fwd_file, fwd_idx)
            logger.debug(
                f"Updated forward index for document ID {doc_id} in {fwd_file}."
            )

            # Update inverted index
            for w_id, cnt in word_counts.items():
                inv_start = (
                    w_id // self.config.INVERTED_BATCH_SIZE
                ) * self.config.INVERTED_BATCH_SIZE
                inv_end = inv_start + self.config.INVERTED_BATCH_SIZE - 1
                inv_dir = f"{self.config.INVERTED_INDEX_PATH}/{doc_type}"
                os.makedirs(inv_dir, exist_ok=True)
                inv_file = f"{inv_dir}/inverted_index_{inv_start}-{inv_end}.json"

                inv_idx = read_json(inv_file) if os.path.exists(inv_file) else {}
                if str(w_id) not in inv_idx:
                    inv_idx[str(w_id)] = {"docs": []}

                found = False
                for d in inv_idx[str(w_id)]["docs"]:
                    if d["id"] == doc_id:
                        d["freq"] += cnt
                        for ff in field_matches.keys():
                            if ff not in d["fields"]:
                                d["fields"].append(ff)
                        d["positions"].extend(positions[w_id])
                        found = True
                        logger.debug(
                            f"Updated existing entry for doc_id {doc_id} in inverted index {inv_file}."
                        )
                        break

                if not found:
                    inv_idx[str(w_id)]["docs"].append(
                        {
                            "id": doc_id,
                            "freq": cnt,
                            "fields": list(field_matches.keys()),
                            "positions": positions[w_id],
                        }
                    )
                    logger.debug(
                        f"Added new entry for doc_id {doc_id} in inverted index {inv_file}."
                    )

                write_json(inv_file, inv_idx)
                logger.debug(
                    f"Updated inverted index file {inv_file} with word ID {w_id}."
                )
        except Exception as e:
            logger.error(
                f"Error updating indices for doc_id={doc_id}: {e}", exc_info=True
            )
            raise

# Initialize SearchEngine
search_engine = SearchEngine()

##################################
# FastAPI Endpoints
##################################

app = FastAPI(title="Hotel Search Engine")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

@app.get("/search")
async def search(
    query: str = Query(..., description="Search query terms."),
    doc_type: str = Query("all", description="Document type to search: all, hotels, reviews."),
    location: Optional[str] = Query(None, description="Filter results by locality (e.g., New York City)."),
    hotel_class: Optional[int] = Query(None, description="Filter results by hotel class (e.g., 5 for 5-star hotels).")
):
    """
    Search endpoint that allows filtering by location and hotel_class.
    Location filter takes precedence over other filters.
    Supports partial matching for location (e.g., "New York" matches "New York City").
    """
    try:
        results = await search_engine.search(query, doc_type, location, hotel_class)
        logger.debug(f"Search returned {len(results['results'])} results.")
        return results
    except Exception as e:
        logger.error(f"Search error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/hotels/{hotel_id}")
async def get_hotel(hotel_id: int):
    """
    Return single hotel info plus reviews from the correct chunk.
    """
    try:
        hotels_df = search_engine.get_hotels_df()
        row = hotels_df[hotels_df["hotel_id"] == hotel_id]
        if row.empty:
            raise HTTPException(
                status_code=404, detail=f"No hotel found with ID {hotel_id}"
            )
        hotel_data = row.iloc[0].to_dict()

        cache_key = f"reviews:{hotel_id}"
        cached_reviews = await search_engine.document_cache.get(cache_key)
        if cached_reviews is not None:
            hotel_data["reviews"] = cached_reviews
            logger.debug(f"Returned cached reviews for hotel ID {hotel_id}.")
            return clean_float_values(hotel_data)

        batch_file = search_engine._get_review_batch_file(hotel_id)
        reviews = []
        if os.path.exists(batch_file):
            df = read_csv(batch_file)
            sub = df[df["hotel_id"] == hotel_id]
            if not sub.empty:
                reviews = sub.to_dict("records")
                logger.debug(f"Fetched {len(reviews)} reviews from {batch_file} for hotel ID {hotel_id}.")

        hotel_data["reviews"] = reviews
        await search_engine.document_cache.set(cache_key, reviews)
        logger.debug(f"Cached reviews for hotel ID {hotel_id}.")
        return clean_float_values(hotel_data)

    except Exception as e:
        logger.error(f"Error fetching hotel: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/hotels", status_code=201)
async def create_hotel(hotel: HotelCreate, background_tasks: BackgroundTasks):
    """
    Create a single hotel and index it.
    """
    try:
        logger.info(f"Creating hotel: {hotel}")
        hotels_df = search_engine.get_hotels_df()
        h_data = hotel.dict(by_alias=True)
        h_data["hotel_id"] = len(hotels_df) + 1

        if not h_data.get("average_score"):
            sc = []
            for ff in [
                "service",
                "cleanliness",
                "overall",
                "value",
                "location",
                "sleep_quality",
                "rooms",
            ]:
                if ff in h_data and h_data[ff] is not None:
                    sc.append(h_data[ff])
            if sc:
                h_data["average_score"] = round(sum(sc) / len(sc), 1)
                logger.debug(f"Computed average_score for hotel ID {h_data['hotel_id']}: {h_data['average_score']}")

        newdf = pd.DataFrame([h_data])
        updated_df = pd.concat([hotels_df, newdf], ignore_index=True)
        write_csv(Config.HOTELS_PATH, updated_df)
        search_engine.reload_data()

        text_for_index = (
            f"{h_data['name']} "
            f"{h_data['locality']} "
            f"{h_data['street-address']} "
            f"{h_data['region']}"
        )
        fields_dict = {
            "name": h_data["name"],
            "locality": h_data["locality"],
            "street-address": h_data["street-address"],
            "region": h_data["region"],
        }
        background_tasks.add_task(
            search_engine.update_indices,
            str(h_data["hotel_id"]),
            text_for_index,
            "hotels",
            fields_dict,
        )
        logger.info(f"Hotel created with ID {h_data['hotel_id']} and indexing started.")

        return {
            "status": "success",
            "message": "Hotel created",
            "hotel_id": h_data["hotel_id"],
        }
    except Exception as e:
        logger.error(f"Error creating hotel: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/reviews", status_code=201)
async def create_review(review: ReviewCreate, background_tasks: BackgroundTasks):
    """
    Create a single review and index it.
    """
    try:
        hotels_df = search_engine.get_hotels_df()
        if str(review.hotel_id) not in hotels_df["hotel_id"].astype(str).values:
            logger.debug(f"Hotel ID {review.hotel_id} not found for review creation.")
            raise HTTPException(status_code=404, detail="Hotel not found")

        r_dict = review.dict()
        rev_id = search_engine._get_next_rev_id()
        r_dict["rev_id"] = rev_id

        search_engine.rev_to_hotel[rev_id] = review.hotel_id
        logger.debug(f"Mapping review ID {rev_id} to hotel ID {review.hotel_id}.")

        start = ((review.hotel_id - 1) // Config.REVIEW_BATCH_SIZE) * Config.REVIEW_BATCH_SIZE + 1
        end = start + Config.REVIEW_BATCH_SIZE - 1
        chunk_file = f"{Config.REVIEWS_DIR}/reviews_{start}-{end}.csv"

        existing = read_csv(chunk_file) if os.path.exists(chunk_file) else pd.DataFrame()
        newdf = pd.concat([existing, pd.DataFrame([r_dict])], ignore_index=True)
        write_csv(chunk_file, newdf)
        logger.debug(f"Added review ID {rev_id} to {chunk_file}.")

        text_for_index = f"{review.title} {review.text}"
        fields_dict = {"title": review.title, "text": review.text}
        doc_id = str(rev_id)
        background_tasks.add_task(
            search_engine.update_indices,
            doc_id,
            text_for_index,
            "reviews",
            fields_dict
        )
        logger.info(f"Review created with ID {rev_id} and indexing started.")

        # Invalidate the cached reviews for this hotel
        await search_engine.document_cache.delete(f"reviews:{review.hotel_id}")
        logger.debug(f"Invalidated cache for reviews of hotel ID {review.hotel_id}.")

        return {"status": "success", "message": "Review added", "review_id": rev_id}

    except Exception as e:
        logger.error(f"Error creating review: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/hotels/upload", status_code=201)
async def upload_hotels(
    file: UploadFile = File(...), background_tasks: BackgroundTasks = BackgroundTasks()
):
    """
    Bulk upload hotels and index them.
    """
    try:
        logger.info("Starting bulk upload of hotels.")
        content = await file.read()
        # Run read_csv in threadpool to avoid blocking
        df = await run_in_threadpool(
            pd.read_csv, io.StringIO(content.decode("utf-8-sig"))
        )
        hotels_df = await run_in_threadpool(search_engine.get_hotels_df)
        start_id = len(hotels_df) + 1
        df["hotel_id"] = range(start_id, start_id + len(df))

        # Compute average_score where missing using vectorized operations
        avg_score_cols = [
            "service",
            "cleanliness",
            "overall",
            "value",
            "location",
            "sleep_quality",
            "rooms",
        ]
        # Calculate mean across the specified columns, ignoring NaNs
        df["average_score"] = df.apply(
            lambda row: round(row[avg_score_cols].mean(), 1) if pd.isna(row.get("average_score", np.nan)) else row["average_score"],
            axis=1
        )
        logger.debug("Computed average_score for missing entries.")

        updated_df = pd.concat([hotels_df, df], ignore_index=True)
        await run_in_threadpool(write_csv, Config.HOTELS_PATH, updated_df)
        await run_in_threadpool(search_engine.reload_data)

        # Prepare data for indexing
        hotels_to_index = df.to_dict("records")
        for hotel in hotels_to_index:
            h_id = int(hotel["hotel_id"])
            text_for_index = (
                f"{hotel['name']} "
                f"{hotel['locality']} "
                f"{hotel['street-address']} "
                f"{hotel['region']}"
            )
            fields_dict = {
                "name": hotel["name"],
                "locality": hotel["locality"],
                "street-address": hotel["street-address"],
                "region": hotel["region"],
            }
            background_tasks.add_task(
                search_engine.update_indices,
                str(h_id),
                text_for_index,
                "hotels",
                fields_dict,
            )
            logger.debug(f"Scheduled indexing for hotel ID {h_id}.")

        logger.info(f"Bulk upload of {len(df)} hotels completed and indexing tasks scheduled.")
        return {
            "status": "success",
            "message": f"Added {len(df)} hotels",
            "hotel_ids": [int(x) for x in df["hotel_id"].values],
        }
    except Exception as e:
        logger.error(f"Error uploading hotels: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/reviews/upload", status_code=201)
async def upload_reviews(
    file: UploadFile = File(...), background_tasks: BackgroundTasks = BackgroundTasks()
):
    """
    Bulk upload reviews and index them.
    """
    try:
        logger.info("Starting bulk upload of reviews.")
        content = await file.read()
        # Run read_csv in threadpool to avoid blocking
        df = await run_in_threadpool(
            pd.read_csv, io.StringIO(content.decode("utf-8-sig"))
        )
        hotels_df = await run_in_threadpool(search_engine.get_hotels_df)

        # Check all hotel IDs exist
        hotel_ids = set(hotels_df["hotel_id"].astype(str))
        missing = [h for h in df["hotel_id"].unique() if str(h) not in hotel_ids]
        if missing:
            logger.debug(f"Missing hotel IDs in bulk review upload: {missing}")
            raise HTTPException(
                status_code=404, detail=f"Some hotel_ids do not exist: {missing}"
            )

        updated_hotels = set()

        # Assign rev_id and map to hotel_id using vectorized operations
        rev_ids = await run_in_threadpool(search_engine._get_next_rev_id)
        # To handle multiple rev_ids, it's better to process in batches
        with search_engine.rev_id_lock:
            current_rev_id = search_engine.current_rev_id
            df["rev_id"] = range(current_rev_id + 1, current_rev_id + 1 + len(df))
            search_engine.current_rev_id += len(df)
            search_engine._persist_rev_id()

        # Map rev_id to hotel_id
        rev_to_hotel_map = dict(zip(df["rev_id"], df["hotel_id"]))
        search_engine.rev_to_hotel.update(rev_to_hotel_map)
        updated_hotels.update(df["hotel_id"].unique())
        logger.debug(f"Mapped review IDs to hotel IDs.")

        # Group reviews by hotel_id for batch processing
        grouped = df.groupby("hotel_id")
        for h_id, group in grouped:
            start = (
                (h_id - 1) // Config.REVIEW_BATCH_SIZE
            ) * Config.REVIEW_BATCH_SIZE + 1
            end = start + Config.REVIEW_BATCH_SIZE - 1
            chunk_file = f"{Config.REVIEWS_DIR}/reviews_{start}-{end}.csv"
            existing = await run_in_threadpool(
                read_csv, chunk_file
            ) if os.path.exists(chunk_file) else pd.DataFrame()
            newdf = pd.concat([existing, group], ignore_index=True)
            await run_in_threadpool(write_csv, chunk_file, newdf)
            logger.debug(f"Added {len(group)} reviews to {chunk_file}.")

        # Prepare data for indexing
        reviews_to_index = df.to_dict("records")
        for review in reviews_to_index:
            doc_id = str(review["rev_id"])
            text_for_index = f"{review['title']} {review['text']}"
            fields_dict = {"title": review["title"], "text": review["text"]}
            background_tasks.add_task(
                search_engine.update_indices,
                doc_id,
                text_for_index,
                "reviews",
                fields_dict
            )
            logger.debug(f"Scheduled indexing for review ID {doc_id}.")

        # Invalidate caches for all hotels that got new reviews
        for h_id in updated_hotels:
            await search_engine.document_cache.delete(f"reviews:{h_id}")
            logger.debug(f"Invalidated cache for reviews of hotel ID {h_id}.")

        logger.info(f"Bulk upload of {len(df)} reviews completed and indexing tasks scheduled.")
        return {"status": "success", "message": f"Added {len(df)} reviews"}

    except Exception as e:
        logger.error(f"Error uploading reviews: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
