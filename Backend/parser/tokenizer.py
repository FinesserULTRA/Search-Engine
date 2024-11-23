import pandas as pd
import nltk
from nltk.tokenize import word_tokenize
import string
import json
from fastapi import FastAPI

# # FastAPI initialization
# app = FastAPI()

# Download necessary resources from NLTK
nltk.download('punkt_tab')
nltk.download('stopwords')

class Tokenizer:
    def __init__(self, offering_path: str, reviews_path: str):
        self.offering_df = pd.read_csv(offering_path)
        self.reviews_df = pd.read_csv(reviews_path)
        self.lexicon = set()

    def tokenize_text(self, text):
        if pd.isna(text):
            return []
        # Tokenize the text
        tokens = word_tokenize(text)
        # Convert to lowercase
        tokens = [token.lower() for token in tokens]
        # Remove punctuation
        tokens = [token for token in tokens if token not in string.punctuation]
        return tokens

    def apply_tokenization(self):
        # Applying the tokenizer to relevant columns if they exist
        if 'name' in self.offering_df.columns:
            self.offering_df['name_tokens'] = self.offering_df['name'].apply(self.tokenize_text)
        if 'location' in self.offering_df.columns:
            self.offering_df['location_tokens'] = self.offering_df['location'].apply(self.tokenize_text)
        if 'title' in self.reviews_df.columns:
            self.reviews_df['title_tokens'] = self.reviews_df['title'].apply(self.tokenize_text)
        if 'text' in self.reviews_df.columns:
            self.reviews_df['text_tokens'] = self.reviews_df['text'].apply(self.tokenize_text)

    def save_tokenized_output(self):
        # Save the tokenized output for lexicon, forward, and inverted indexing
        columns_to_save_offering = [col for col in ['name_tokens', 'location_tokens'] if col in self.offering_df.columns]
        columns_to_save_reviews = [col for col in ['title_tokens', 'text_tokens'] if col in self.reviews_df.columns]

        if columns_to_save_offering:
            offering_tokens = self.offering_df[columns_to_save_offering]
            offering_tokens.to_csv('../dat/offering_tokens.csv', index=False)

        if columns_to_save_reviews:
            reviews_tokens = self.reviews_df[columns_to_save_reviews]
            reviews_tokens.to_csv('../dat/reviews_tokens.csv', index=False)

    def create_lexicons(self):
        offering_lexicon = set()
        reviews_lexicon = set()

        # Add tokens from offering to the offering lexicon
        if 'name_tokens' in self.offering_df.columns:
            self.offering_df['name_tokens'].apply(lambda tokens: offering_lexicon.update(tokens))
        if 'location_tokens' in self.offering_df.columns:
            self.offering_df['location_tokens'].apply(lambda tokens: offering_lexicon.update(tokens))

        # Add tokens from reviews to the reviews lexicon
        if 'title_tokens' in self.reviews_df.columns:
            self.reviews_df['title_tokens'].apply(lambda tokens: reviews_lexicon.update(tokens))
        if 'text_tokens' in self.reviews_df.columns:
            self.reviews_df['text_tokens'].apply(lambda tokens: reviews_lexicon.update(tokens))

        # Save lexicons to separate JSON files
        with open('../dat/offering_lexicon.json', 'w') as f:
            json.dump(list(offering_lexicon), f)
        with open('../dat/reviews_lexicon.json', 'w') as f:
            json.dump(list(reviews_lexicon), f)

# FastAPI endpoint to trigger tokenization, lexicon creation, and saving
# # @app.post("/process")d
# async def process_files():
#     tokenizer = Tokenizer(offering_path='offerings.csv', reviews_path='reviews.csv')
#     tokenizer.apply_tokenization()
#     tokenizer.save_tokenized_output()
#     tokenizer.create_lexicon()
#     return {"message": "Processing completedsuccessfully."}

tokenizer = Tokenizer(offering_path='../dat/offerings.csv', reviews_path='../dat/reviews.csv')
tokenizer.apply_tokenization()
tokenizer.save_tokenized_output()
tokenizer.create_lexicon()