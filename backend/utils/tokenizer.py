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
        return text.translate(str.maketrans(self.PUNCT_TO_REMOVE, " " * len(self.PUNCT_TO_REMOVE)))

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
