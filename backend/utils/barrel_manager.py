import json
from collections import defaultdict, OrderedDict
from pathlib import Path
from typing import Dict, List, Set
import logging


class BarrelManager:
    def __init__(self, index_data_path: str, barrel_size: int = 1000):
        self.index_data_path = Path(index_data_path)
        if not self.index_data_path.exists():
            self.index_data_path.mkdir(parents=True)
        self.barrel_size = max(1, barrel_size)  # Ensure positive barrel size
        self.barrel_cache = OrderedDict()  # Using OrderedDict for LRU cache
        self.max_cache_size = 5
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def get_barrel_number(self, token_id: str) -> int:
        try:
            return int(float(token_id)) // self.barrel_size
        except (ValueError, TypeError) as e:
            self.logger.error(f"Invalid token_id: {token_id}")
            raise ValueError(f"Invalid token_id format: {token_id}") from e

    def create_barrels(self, inverted_index: dict, index_type: str) -> None:
        """Split inverted index into barrels and save them"""
        barrels = defaultdict(dict)

        # Distribute tokens to barrels
        for token_id, doc_ids in inverted_index.items():
            barrel_num = self.get_barrel_number(token_id)
            barrels[barrel_num][token_id] = doc_ids

        # Save barrels to disk
        for barrel_num, barrel_data in barrels.items():
            self._save_barrel(barrel_data, barrel_num, index_type)

    def _save_barrel(self, barrel_data: dict, barrel_num: int, index_type: str) -> None:
        try:
            barrel_path = (
                self.index_data_path / f"{index_type}_barrel_{barrel_num}.json"
            )
            with open(barrel_path, "w", encoding="utf-8") as f:
                json.dump(barrel_data, f)
        except IOError as e:
            self.logger.error(f"Failed to save barrel {barrel_num}: {e}")
            raise

    def load_barrel(self, barrel_num: int, index_type: str) -> dict:
        cache_key = f"{index_type}_{barrel_num}"

        # Return from cache if available
        if cache_key in self.barrel_cache:
            # Move to end (most recently used)
            self.barrel_cache.move_to_end(cache_key)
            return self.barrel_cache[cache_key]

        try:
            barrel_path = (
                self.index_data_path / f"{index_type}_barrel_{barrel_num}.json"
            )
            if barrel_path.exists():
                with open(barrel_path, "r", encoding="utf-8") as f:
                    barrel_data = json.load(f)

                # Implement LRU cache
                if len(self.barrel_cache) >= self.max_cache_size:
                    self.barrel_cache.popitem(last=False)  # Remove least recently used

                self.barrel_cache[cache_key] = barrel_data
                return barrel_data
            return {}
        except IOError as e:
            self.logger.error(f"Failed to load barrel {barrel_num}: {e}")
            return {}

    def get_posting_lists(
        self, token_ids: List[str], index_type: str
    ) -> Dict[str, List[str]]:
        """Get posting lists for multiple token IDs efficiently"""
        needed_barrels: Set[int] = {self.get_barrel_number(tid) for tid in token_ids}
        result = {}

        for barrel_num in needed_barrels:
            barrel_data = self.load_barrel(barrel_num, index_type)
            for token_id in token_ids:
                if token_id in barrel_data:
                    result[token_id] = barrel_data[token_id]

        return result

    def clear_cache(self) -> None:
        """Clear the barrel cache"""
        self.barrel_cache.clear()
