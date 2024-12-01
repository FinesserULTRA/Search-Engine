import re
import string
import nltk
from nltk.corpus import stopwords, wordnet
import contractions
import spacy
import json
import pandas as pd
from collections import defaultdict
from pathlib import Path
# =======================================================================
# from multiprocessing import Pool, cpu_count
# from concurrent.futures import ProcessPoolExecutor

# import spacy_transformers


class Tokenizer:
    PUNCT_TO_REMOVE = string.punctuation
    STOPWORDS = set(stopwords.words("english"))

    def __init__(self):
        nltk.download("stopwords", quiet=True)
        self.nlp = spacy.load("en_core_web_trf", disable=["ner", "parser"])
        self.nlp = spacy.load("en_core_web_sm", disable=["ner", "parser"])

    def tokenize_with_spacy(self, text):
        text = self.__remove_urls(text)
        text = self.__expand_contractions(text)
        text = self.__remove_punctuation(text)
        text = self.__remove_stopwords(text)
        text = text.lower()

        doc = self.nlp(text)
        tokens = [token.lemma_ if token.pos_ != "NOUN" else token.text for token in doc]
        return tokens

    # =======================================================================
    # def process_large_text_parallel(self, texts):
    #     with ProcessPoolExecutor() as executor:
    #         results = executor.map(self.tokenize_with_spacy, texts)
    #     return [token for result in results for token in result]

    def __remove_urls(self, text):
        url_pattern = re.compile(r"https?://\S+|www\.\S+")
        return url_pattern.sub(r"", text)

    def __remove_punctuation(self, text):
        return text.translate(str.maketrans("", "", self.PUNCT_TO_REMOVE))

    def __remove_stopwords(self, text):
        return " ".join(
            [word for word in str(text).split() if word not in self.STOPWORDS]
        )

    def __expand_contractions(self, text):
        return contractions.fix(text)

    def __get_wordnet_pos(self, treebank_tag):
        if treebank_tag.startswith("V"):
            return wordnet.VERB
        elif treebank_tag.startswith("N"):
            return wordnet.NOUN
        elif treebank_tag.startswith("J"):
            return wordnet.ADJ
        elif treebank_tag.startswith("R"):
            return wordnet.ADV
        else:
            return None


class ForwardInverseIndexGenerator:
    def __init__(self, review_files_path: str, index_data_path: str):
        self.review_files_path = Path(review_files_path)
        self.index_data_path = Path(index_data_path)

    def process_review_file(self, file_path):
        tokenizer = Tokenizer()
        return tokenizer.process_large_text_parallel()

    def generate_lexicon(self):
        # Get list of review CSV files
        review_files = list(self.review_files_path.glob("reviews_*.csv"))

        # Use Pool to process files concurrently
        # =======================================================================
        # with Pool(cpu_count()) as pool:
        #     lexicons = pool.map(self.process_review_file, review_files)

        # Combine individual lexicons into a single lexicon
        combined_lexicon = defaultdict(int)
        # =======================================================================
        # for lexicon in lexicons:
        #     for word, count in lexicon.items():
        #         combined_lexicon[word] += count

        # Write the combined lexicon to a JSON file
        with open(self.index_data_path / "forward.json", "w", encoding='utf-8-sig') as f:
            json.dump(combined_lexicon, f)

        return "Lexicon generation completed successfully!"


def generate_lexicon():
    generator = ForwardInverseIndexGenerator(
        review_files_path="../reviews", index_data_path="../index data"
    )
    result = generator.generate_lexicon()
    return {"message": result}


print(generate_lexicon())
