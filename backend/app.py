from fastapi import FastAPI, HTTPException, Query, BackgroundTasks, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.concurrency import run_in_threadpool
import pandas as pd
import json
import os
import io
from typing import List, Dict, Optional
from collections import defaultdict
from datetime import datetime
import aiofiles
import math
import asyncio
import numpy as np
from pydantic import BaseModel, validator, Field
import logging
import threading

# Your custom utilities
from utils.tokenizer import Tokenizer
from utils.file_io import read_json, write_json, read_csv, write_csv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Config:
    DATA_DIR = "./data"
    REVIEWS_DIR = f"./reviews"
    INDEX_DIR = f"./index data"
    INVERTED_INDEX_PATH = f"{INDEX_DIR}/inverted_index"
    FORWARD_INDEX_PATH = f"{INDEX_DIR}/forward_index"
    HOTELS_PATH = f"{DATA_DIR}/hotels_cleaned.csv"
    LEXICON_PATH = f"{INDEX_DIR}/lexicon/lexicon.json"

    INVERTED_BATCH_SIZE = 20000
    FORWARD_BATCH_SIZE = 20000
    REVIEW_BATCH_SIZE = 1000

    MAX_RESULTS = 500
    MAX_DOCS_TO_PROCESS = 100000

    SCORING_PARAMS = {
        "field_weights": {
            "name": 4.0,
            "region": 2.0,
            "street-address": 3.0,
            "locality": 2.5,
            "title": 3.0,
            "text": 1.5,
        }
    }

    @staticmethod
    def initialize():
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        os.makedirs(Config.REVIEWS_DIR, exist_ok=True)
        os.makedirs(Config.INVERTED_INDEX_PATH, exist_ok=True)
        os.makedirs(Config.FORWARD_INDEX_PATH, exist_ok=True)

Config.initialize()


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
            return self.cache[key]
        return None

    async def set(self, key: str, value: any):
        self.cache[key] = value
        self.timestamps[key] = datetime.now()

    async def delete(self, key: str):
        if key in self.cache:
            del self.cache[key]
        if key in self.timestamps:
            del self.timestamps[key]

    async def clear(self):
        self.cache.clear()
        self.timestamps.clear()


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


class SearchEngine:
    def __init__(self):
        self.tokenizer = Tokenizer()
        self.lexicon = {}
        self.hotels_df = pd.DataFrame()
        self.reviews_df = pd.DataFrame()
        self.document_cache = Cache()

        self.current_rev_id = 0
        self.rev_id_lock = threading.Lock()
        self.rev_id_file = os.path.join(Config.DATA_DIR, "current_rev_id.json")

        # rev_id -> hotel_id
        self.rev_to_hotel = {}

        self._load_data()
        self._initialize_rev_id()
        self._rebuild_rev_to_hotel_from_disk()

    def _load_data(self):
        self.lexicon = read_json(Config.LEXICON_PATH)
        self.hotels_df = read_csv(Config.HOTELS_PATH)

    def reload_data(self):
        self.lexicon = read_json(Config.LEXICON_PATH)
        self.hotels_df = read_csv(Config.HOTELS_PATH)
        self.reviews_df = self._load_reviews()

    def _initialize_rev_id(self):
        if os.path.exists(self.rev_id_file):
            with open(self.rev_id_file, "r") as f:
                data = json.load(f)
                self.current_rev_id = int(
                    data.get("current_rev_id", self._find_max_rev_id())
                )
        else:
            self.current_rev_id = self._find_max_rev_id()
            self._persist_rev_id()

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
            return int(max_rev_id)
        except:
            return 0

    def _persist_rev_id(self):
        with open(self.rev_id_file, "w") as f:
            json.dump({"current_rev_id": int(self.current_rev_id)}, f)

    def _get_next_rev_id(self) -> int:
        with self.rev_id_lock:
            self.current_rev_id += 1
            self._persist_rev_id()
            return self.current_rev_id

    def _rebuild_rev_to_hotel_from_disk(self):
        self.rev_to_hotel.clear()
        for fn in os.listdir(Config.REVIEWS_DIR):
            if fn.startswith("reviews_") and fn.endswith(".csv"):
                df = read_csv(os.path.join(Config.REVIEWS_DIR, fn))
                if "rev_id" in df.columns and "hotel_id" in df.columns:
                    for _, row_data in df.iterrows():
                        self.rev_to_hotel[int(row_data["rev_id"])] = int(row_data["hotel_id"])

    def _get_review_batch_file(self, hotel_id: int)->str:
        start = ((hotel_id-1)//Config.REVIEW_BATCH_SIZE)*Config.REVIEW_BATCH_SIZE +1
        end = start + Config.REVIEW_BATCH_SIZE -1
        return f"{Config.REVIEWS_DIR}/reviews_{start}-{end}.csv"

    def get_hotels_df(self):
        if self.hotels_df.empty:
            self.hotels_df = read_csv(Config.HOTELS_PATH)
        return self.hotels_df

    def _load_reviews(self):
        revs = []
        for f in os.listdir(Config.REVIEWS_DIR):
            if f.endswith(".csv"):
                df = read_csv(os.path.join(Config.REVIEWS_DIR, f))
                if not df.empty:
                    revs.append(df)
        if revs:
            return pd.concat(revs, ignore_index=True)
        return pd.DataFrame()

    def get_reviews_df(self):
        if self.reviews_df.empty:
            self.reviews_df = self._load_reviews()
        return self.reviews_df

    async def search(self, query: str, doc_type: str)->Dict:
        """
        doc_type: "all" => search both hotels+reviews, but return only hotels
                   "reviews" => search both hotels+reviews, but return only reviews
        """
        from fastapi.concurrency import run_in_threadpool
        logger.info(f"search(query='{query}', doc_type='{doc_type}') called.")

        # 1) tokenize
        original_words = [w for w in query.lower().split() if w]
        spacy_tokens = await run_in_threadpool(self.tokenizer.tokenize_with_spacy, query.lower())
        tokens = list(set(original_words + spacy_tokens))

        if not tokens:
            return {"results": [], "count": 0, "total_matches": 0}

        word_ids = [self.lexicon[t] for t in tokens if t in self.lexicon]
        if not word_ids:
            return {"results": [], "count": 0, "total_matches": 0}

        # 2) get matched docs from both indexes
        matched_hotels = await self._search_in_index_multiword(word_ids, "hotels")
        matched_reviews = await self._search_in_index_multiword(word_ids, "reviews")

        # 3) doc_type logic
        if doc_type == "reviews":
            # only return reviews
            final_list = self._score_and_fetch_reviews(matched_reviews, tokens)
        else:
            # doc_type='all' or anything else => only return hotels
            final_list = self._score_hotels(matched_hotels, tokens)

        total = len(final_list)
        final_list = final_list[: Config.MAX_RESULTS]
        return {
            "results": final_list,
            "count": len(final_list),
            "total_matches": total
        }

    async def _search_in_index_multiword(self, word_ids: List[int], doc_type: str)->Dict:
        """
        multiword intersection approach in doc_type=hotels or doc_type=reviews index
        returns {doc_id: {freq, fields, positions, ...}}
        """
        matched_docs = None
        doc_counter = 0

        for i, w_id in enumerate(word_ids):
            batch_start = (w_id//Config.INVERTED_BATCH_SIZE)*Config.INVERTED_BATCH_SIZE
            batch_end = batch_start + Config.INVERTED_BATCH_SIZE -1
            inv_file = f"{Config.INVERTED_INDEX_PATH}/{doc_type}/inverted_index_{batch_start}-{batch_end}.json"

            if not os.path.exists(inv_file):
                # no docs for this token
                matched_docs = {} if i == 0 else {}
                break

            try:
                async with aiofiles.open(inv_file,"r", encoding="utf-8-sig") as f:
                    content = await f.read()
                idx_data = json.loads(content)
            except json.JSONDecodeError as e:
                logger.error(f"Corrupted JSON in {inv_file}: {e}")
                idx_data = {}
            except Exception as e:
                logger.error(f"Error reading {inv_file}: {e}", exc_info=True)
                idx_data = {}

            if str(w_id) not in idx_data:
                # no docs for this word
                matched_docs = {} if i == 0 else {}
                break

            # docs for w_id => new_docs
            new_docs = {}
            for doc in idx_data[str(w_id)]["docs"]:
                doc_id = doc["id"]
                new_docs[doc_id] = {
                    "freq": doc["freq"],
                    "fields": doc["fields"],
                    "positions": doc["positions"],
                }

            if i == 0:
                matched_docs = new_docs
            else:
                # intersection
                intersection = {}
                for d_id, existing_data in matched_docs.items():
                    if d_id in new_docs:
                        # merge freq/fields/positions
                        combined_freq = existing_data["freq"] + new_docs[d_id]["freq"]
                        combined_fields = existing_data["fields"] + new_docs[d_id]["fields"]
                        combined_positions = existing_data["positions"] + new_docs[d_id]["positions"]
                        intersection[d_id] = {
                            "freq": combined_freq,
                            "fields": combined_fields,
                            "positions": combined_positions
                        }
                matched_docs = intersection

            doc_counter = len(matched_docs)
            if doc_counter >= Config.MAX_DOCS_TO_PROCESS:
                logger.info("Reached MAX_DOCS_TO_PROCESS limit.")
                break
            if not matched_docs:
                break

        if not matched_docs:
            return {}
        return matched_docs

    def _score_hotels(self, matched_docs:Dict, tokens:List[str])->List[Dict]:
        """
        matched_docs => { doc_id: {freq, fields, positions} }
        each doc_id => int(hotel_id)
        return a list of hotel dictionaries with scoring
        """
        df = self.get_hotels_df()
        field_weights = Config.SCORING_PARAMS["field_weights"]

        results=[]
        doc_ids = list(matched_docs.keys())[: Config.MAX_DOCS_TO_PROCESS]
        for doc_id in doc_ids:
            data = matched_docs[doc_id]
            try:
                h_id = int(doc_id)
            except:
                continue

            row = df[df["hotel_id"]==h_id]
            if row.empty:
                continue

            freq = data["freq"]
            fields = data["fields"]
            # naive scoring
            score = freq * 0.3
            for f in fields:
                score += field_weights.get(f,1.0)
            matched_tokens = set(tokens)
            score += len(matched_tokens)*0.2

            info = row.iloc[0].to_dict()
            info["search_score"] = score
            info["matched_fields"] = list(set(fields))
            info["matched_terms"] = list(matched_tokens)
            results.append(clean_float_values(info))

        results.sort(key=lambda x: x["search_score"], reverse=True)
        return results

    def _score_and_fetch_reviews(self, matched_docs:Dict, tokens:List[str])->List[Dict]:
        """
        matched_docs => {doc_id => {freq, fields, positions}}
        doc_id => str(rev_id)
        we do rev_to_hotel[rev_id] => hotel, read chunk, etc.
        Return a list of reviews
        """
        from collections import defaultdict, Counter
        if not matched_docs:
            return []
        doc_ids = list(matched_docs.keys())[: Config.MAX_DOCS_TO_PROCESS]

        field_weights = Config.SCORING_PARAMS["field_weights"]
        # group by hotel
        hotel_map = defaultdict(list)
        for doc_id in doc_ids:
            try:
                rev_id_int = int(doc_id)
            except:
                continue
            if rev_id_int not in self.rev_to_hotel:
                continue
            h_id = self.rev_to_hotel[rev_id_int]
            hotel_map[h_id].append(rev_id_int)

        results=[]
        hotel_review_count= Counter()

        for h_id, rev_list in hotel_map.items():
            chunk_file = self._get_review_batch_file(h_id)
            if not os.path.exists(chunk_file):
                continue
            df = read_csv(chunk_file)
            if df.empty or "rev_id" not in df.columns:
                continue
            subdf = df[df["rev_id"].astype(str).isin([str(x) for x in rev_list])]
            if subdf.empty:
                continue

            for _, row_data in subdf.iterrows():
                rev_id_str= str(row_data["rev_id"])
                if rev_id_str not in matched_docs:
                    continue
                freq = matched_docs[rev_id_str]["freq"]
                fields= matched_docs[rev_id_str]["fields"]
                positions = matched_docs[rev_id_str]["positions"]

                # naive scoring
                score = freq * 0.2
                for f in fields:
                    score+= field_weights.get(f,1.0)*0.5
                matched_tokens = set(tokens)
                score+= len(matched_tokens)*0.1

                row_dict= row_data.to_dict()
                row_dict["search_score"] = score
                row_dict["matched_fields"] = list(set(fields))
                row_dict["matched_terms"] = list(matched_tokens)

                results.append(clean_float_values(row_dict))
                hotel_review_count[row_dict["hotel_id"]] += 1

        # multi-review bonus
        for r in results:
            extra = hotel_review_count[r["hotel_id"]] -1
            if extra>0:
                r["search_score"]+= 0.05*extra

        results.sort(key=lambda x: x["search_score"], reverse=True)
        return results


search_engine = SearchEngine()


# ------------------------
# Index Updating
# ------------------------
async def update_indices(doc_id: str, text: str, doc_type: str, fields: Dict):
    logger.info(
        f"update_indices(doc_id={doc_id}, doc_type={doc_type}) => fields={fields}"
    )
    try:
        tokens = search_engine.tokenizer.tokenize_with_spacy(text.lower())
        logger.info(f"update_indices tokens => {tokens}")
        lex = search_engine.lexicon or {}
        max_id = max(lex.values()) if lex else 0
        updated = False
        for t in tokens:
            if t not in lex:
                max_id += 1
                lex[t] = max_id
                updated = True
        if updated:
            logger.info(f"Lexicon updated => {Config.LEXICON_PATH}")
            write_json(Config.LEXICON_PATH, lex)
            search_engine.lexicon = lex

        from collections import defaultdict

        word_counts = defaultdict(int)
        field_matches = defaultdict(list)
        positions = defaultdict(list)
        pos_ctr = 0
        for fkey, fval in fields.items():
            f_toks = search_engine.tokenizer.tokenize_with_spacy(str(fval).lower())
            for ft in f_toks:
                if ft in lex:
                    w_id = lex[ft]
                    word_counts[w_id] += 1
                    field_matches[fkey].append(w_id)
                    positions[w_id].append(pos_ctr)
                pos_ctr += 1

        try:
            doc_id_int = int(doc_id)
        except:
            doc_id_int = 0

        fwd_start = (
            doc_id_int // Config.FORWARD_BATCH_SIZE
        ) * Config.FORWARD_BATCH_SIZE
        fwd_end = fwd_start + Config.FORWARD_BATCH_SIZE - 1
        fwd_dir = f"{Config.FORWARD_INDEX_PATH}/{doc_type}"
        os.makedirs(fwd_dir, exist_ok=True)
        fwd_file = f"{fwd_dir}/forward_index_{fwd_start}-{fwd_end}.json"
        fwd_idx = read_json(fwd_file) if os.path.exists(fwd_file) else {}
        fwd_idx[doc_id] = {
            "word_counts": {str(k): v for k, v in word_counts.items()},
            "field_matches": {
                ff: [str(x) for x in wlist] for ff, wlist in field_matches.items()
            },
            "word_positions": {str(k): v for k, v in positions.items()},
        }
        write_json(fwd_file, fwd_idx)

        for w_id, cnt in word_counts.items():
            inv_start = (
                w_id // Config.INVERTED_BATCH_SIZE
            ) * Config.INVERTED_BATCH_SIZE
            inv_end = inv_start + Config.INVERTED_BATCH_SIZE - 1
            inv_dir = f"{Config.INVERTED_INDEX_PATH}/{doc_type}"
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
            write_json(inv_file, inv_idx)

    except Exception as e:
        logger.error(f"Error updating indices: {e}", exc_info=True)
        raise


# ------------------------
# The App
# ------------------------
app = FastAPI(title="Hotel Search Engine")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


@app.get("/search")
async def search(query: str = Query(...), doc_type: str = Query("all")):
    """
    doc_type='reviews' => search both indexes, return reviews 
    doc_type='all' => search both indexes, return hotels
    """
    if doc_type not in ["all", "reviews"]:
        raise HTTPException(status_code=400, detail="doc_type must be 'all' or 'reviews'")
    try:
        return await search_engine.search(query, doc_type)
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
            return clean_float_values(hotel_data)

        batch_file = search_engine._get_review_batch_file(hotel_id)
        reviews = []
        if os.path.exists(batch_file):
            df = read_csv(batch_file)
            sub = df[df["hotel_id"] == hotel_id]
            if not sub.empty:
                reviews = sub.to_dict("records")

        hotel_data["reviews"] = reviews
        await search_engine.document_cache.set(cache_key, reviews)
        return clean_float_values(hotel_data)

    except Exception as e:
        logger.error(f"Error fetching hotel: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/hotels", status_code=201)
async def create_hotel(hotel: HotelCreate, background_tasks: BackgroundTasks):
    """
    Create single hotel. Index it by doc_id = hotel_id
    """
    try:
        logger.info(hotel)
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
            update_indices,
            str(h_data["hotel_id"]),
            text_for_index,
            "hotels",
            fields_dict,
        )
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
    try:
        hotels_df = search_engine.get_hotels_df()
        if str(review.hotel_id) not in hotels_df["hotel_id"].astype(str).values:
            raise HTTPException(status_code=404, detail="Hotel not found")

        r_dict = review.dict()
        rev_id = search_engine._get_next_rev_id()
        r_dict["rev_id"] = rev_id

        # keep track of rev->hotel
        search_engine.rev_to_hotel[rev_id] = review.hotel_id

        # chunking by hotel_id
        start = ((review.hotel_id - 1) // Config.REVIEW_BATCH_SIZE) * Config.REVIEW_BATCH_SIZE + 1
        end = start + Config.REVIEW_BATCH_SIZE - 1
        chunk_file = f"{Config.REVIEWS_DIR}/reviews_{start}-{end}.csv"

        existing = read_csv(chunk_file) if os.path.exists(chunk_file) else pd.DataFrame()
        newdf = pd.concat([existing, pd.DataFrame([r_dict])], ignore_index=True)
        write_csv(chunk_file, newdf)

        text_for_index = f"{review.title} {review.text}"
        fields_dict = {"title": review.title, "text": review.text}
        doc_id = str(rev_id)
        background_tasks.add_task(
            update_indices, doc_id, text_for_index, "reviews", fields_dict
        )

        # ---------- KEY: invalidate the cached reviews for this hotel ----------
        await search_engine.document_cache.delete(f"reviews:{review.hotel_id}")

        return {"status": "success", "message": "Review added", "review_id": rev_id}

    except Exception as e:
        logger.error(f"Error creating review: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))



@app.post("/hotels/upload", status_code=201)
async def upload_hotels(
    file: UploadFile = File(...), background_tasks: BackgroundTasks = BackgroundTasks()
):
    try:
        content = await file.read()
        df = pd.read_csv(io.StringIO(content.decode("utf-8-sig")))
        hotels_df = search_engine.get_hotels_df()
        start_id = len(hotels_df) + 1
        df["hotel_id"] = range(start_id, start_id + len(df))

        for idx, row_data in df.iterrows():
            if pd.isna(row_data.get("average_score", np.nan)):
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
                    if ff in row_data and not pd.isna(row_data[ff]):
                        sc.append(row_data[ff])
                if sc:
                    df.loc[idx, "average_score"] = round(sum(sc) / len(sc), 1)

        updated_df = pd.concat([hotels_df, df], ignore_index=True)
        write_csv(Config.HOTELS_PATH, updated_df)
        search_engine.reload_data()

        for _, row_data in df.iterrows():
            h_id = row_data["hotel_id"]
            text_for_index = (
                f"{row_data['name']} "
                f"{row_data['locality']} "
                f"{row_data['street-address']} "
                f"{row_data['region']}"
            )
            fields_dict = {
                "name": row_data["name"],
                "locality": row_data["locality"],
                "street-address": row_data["street-address"],
                "region": row_data["region"],
            }
            background_tasks.add_task(
                update_indices,
                str(h_id),
                text_for_index,
                "hotels",
                fields_dict,
            )

        return {
            "status": "success",
            "message": f"Added {len(df)} hotels",
            "hotel_ids": list(df["hotel_id"].values),
        }
    except Exception as e:
        logger.error(f"Error uploading hotels: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/reviews/upload", status_code=201)
async def upload_reviews(file: UploadFile = File(...), background_tasks: BackgroundTasks = BackgroundTasks()):
    try:
        content = await file.read()
        df = pd.read_csv(io.StringIO(content.decode("utf-8-sig")))
        hotels_df = search_engine.get_hotels_df()

        content = await file.read()
        df = pd.read_csv(io.StringIO(content.decode("utf-8-sig")))
        hotels_df = search_engine.get_hotels_df()
        # check all hotel IDs exist
        missing = [
            h
            for h in df["hotel_id"].unique()
            if str(h) not in hotels_df["hotel_id"].astype(str).values
        ]
        if missing:
            raise HTTPException(
                status_code=404, detail=f"Some hotel_ids do not exist: {missing}"
            )

        updated_hotels = set()  # track which hotels got new reviews

        for idx, row_data in df.iterrows():
            rev_id = search_engine._get_next_rev_id()
            df.loc[idx, "rev_id"] = rev_id
            h_id = row_data["hotel_id"]
            search_engine.rev_to_hotel[rev_id] = h_id
            updated_hotels.add(h_id)

        grouped = df.groupby("hotel_id")
        for h_id, group in grouped:
            start = ...
            end = ...
            chunk_file = f"{Config.REVIEWS_DIR}/reviews_{start}-{end}.csv"
            existing = read_csv(chunk_file) if os.path.exists(chunk_file) else pd.DataFrame()
            newdf = pd.concat([existing, group], ignore_index=True)
            write_csv(chunk_file, newdf)

        # indexing
        for _, row_data in df.iterrows():
            doc_id = str(row_data["rev_id"])
            text_for_index = f"{row_data['title']} {row_data['text']}"
            fields_dict = {"title": row_data["title"], "text": row_data["text"]}
            background_tasks.add_task(update_indices, doc_id, text_for_index, "reviews", fields_dict)

        # ---------- KEY: invalidate caches for all hotels that got new reviews ----------
        for h_id in updated_hotels:
            await search_engine.document_cache.delete(f"reviews:{h_id}")

        return {"status": "success", "message": f"Added {len(df)} reviews"}

    except Exception as e:
        logger.error(f"Error uploading reviews: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)