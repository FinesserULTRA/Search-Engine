import pandas as pd
import json
import os
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import logging
from typing import Dict
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration Paths
DATA_DIR = "../data"
REVIEWS_DIR = f"../reviews"
HOTELS_PATH = f"{DATA_DIR}/hotels_cleaned.csv"
SENTIMENT_PATH = f"../index data/sentiments/doc_sentiment.json"

# Initialize Sentiment Analyzer
analyzer = SentimentIntensityAnalyzer()

def analyze_sentiment(text: str) -> float:
    """
    Analyze the sentiment of the given text and return the compound score.
    """
    sentiment = analyzer.polarity_scores(text)
    return sentiment['compound']

def load_hotels():
    """
    Load hotels from the CSV file.
    """
    if not os.path.exists(HOTELS_PATH):
        logger.error(f"Hotels file not found at {HOTELS_PATH}")
        return pd.DataFrame()
    df = pd.read_csv(HOTELS_PATH)
    logger.info(f"Loaded {len(df)} hotels.")
    return df

def load_reviews():
    """
    Load all reviews from CSV files in the REVIEWS_DIR.
    """
    if not os.path.exists(REVIEWS_DIR):
        logger.error(f"Reviews directory not found at {REVIEWS_DIR}")
        return pd.DataFrame()
    
    review_files = [f for f in os.listdir(REVIEWS_DIR) if f.endswith('.csv') and f.startswith('reviews_')]
    if not review_files:
        logger.warning(f"No review CSV files found in {REVIEWS_DIR}.")
        return pd.DataFrame()
    
    reviews = []
    for file in review_files:
        path = os.path.join(REVIEWS_DIR, file)
        df = pd.read_csv(path)
        if not df.empty:
            reviews.append(df)
            logger.info(f"Loaded {len(df)} reviews from {file}.")
    
    if reviews:
        combined_reviews = pd.concat(reviews, ignore_index=True)
        logger.info(f"Total loaded reviews: {len(combined_reviews)}.")
        return combined_reviews
    else:
        logger.warning("No reviews found after loading all files.")
        return pd.DataFrame()

def compute_sentiment_scores(hotels_df: pd.DataFrame, reviews_df: pd.DataFrame) -> Dict[str, float]:
    """
    Compute sentiment scores for hotels and reviews.
    Returns a dictionary with document IDs as keys and sentiment scores as values.
    """
    doc_sentiment = {}

    # Compute sentiment for hotels (if applicable)
    # Currently, hotels have no descriptive text, so we set sentiment to 0.0
    for _, row in hotels_df.iterrows():
        hotel_id = str(row['hotel_id'])
        doc_sentiment[hotel_id] = 0.0
        logger.debug(f"Set sentiment score for hotel ID {hotel_id} to 0.0.")

    # Compute sentiment for reviews
    for _, row in reviews_df.iterrows():
        rev_id = str(row['rev_id'])
        title = str(row.get('title', ''))
        text = str(row.get('text', ''))
        combined_text = f"{title} {text}".strip()
        sentiment_score = analyze_sentiment(combined_text) if combined_text else 0.0
        doc_sentiment[rev_id] = sentiment_score
        logger.debug(f"Computed sentiment score for review ID {rev_id}: {sentiment_score}")

    return doc_sentiment

def save_sentiment_scores(doc_sentiment: Dict[str, float]):
    """
    Save the sentiment scores to the SENTIMENT_PATH JSON file.
    """
    os.makedirs(os.path.dirname(SENTIMENT_PATH), exist_ok=True)
    try:
        with open(SENTIMENT_PATH, 'w') as f:
            json.dump(doc_sentiment, f, indent=4)
        logger.info(f"Sentiment scores saved to {SENTIMENT_PATH}.")
    except Exception as e:
        logger.error(f"Failed to save sentiment scores: {e}")

def main():
    hotels_df = load_hotels()
    reviews_df = load_reviews()

    if hotels_df.empty and reviews_df.empty:
        logger.error("No data found to compute sentiment scores.")
        return

    doc_sentiment = compute_sentiment_scores(hotels_df, reviews_df)
    save_sentiment_scores(doc_sentiment)

if __name__ == "__main__":
    main()
