import re
import string
import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from textblob import TextBlob
import contractions
import spacy


class Tokenizer:
    # Attributes
    PUNCT_TO_REMOVE = string.punctuation
    STOPWORDS = set(stopwords.words("english"))
    nlp = spacy.load("en_core_web_sm")

    # Constructor
    def __init__(self):
        nltk.download("stopwords")

    # Tokenization
    def tokenize(self, text):
        text = self.__remove_urls(text)
        text = self.__expand_contractions(text)
        text = self.__remove_punctuation(text)
        text = self.__remove_stopwords(text)
        text = text.lower()

        text_tokens = text.split()
        tokens = []
        for token in text_tokens:
            tokens.append(self.__get_root_word(token))

        return tokens

    # Tokenization using Spacy
    def tokenize_with_spacy(self, text):
        text = self.__remove_urls(text)
        text = self.__expand_contractions(text)
        text = self.__remove_punctuation(text)
        text = self.__remove_stopwords(text)
        text = text.lower()

        doc = self.nlp(text)
        text = " ".join(
            [token.lemma_ for token in doc if not token.is_stop and token.is_alpha]
        )
        tokens = text.split()

        return tokens

    # Removal of URLS
    def __remove_urls(self, text):
        url_pattern = re.compile(r"https?://\S+|www\.\S+")
        return url_pattern.sub(r"", text)

    # Removal of punctuations
    def __remove_punctuation(self, text):
        return text.translate(str.maketrans("", "", self.PUNCT_TO_REMOVE))

    # Removal of stopwords
    def __remove_stopwords(self, text):
        return " ".join(
            [word for word in str(text).split() if word not in self.STOPWORDS]
        )

    # Correct spelling
    def __correct_spelling(self, text):
        return str(TextBlob(text).correct())

    # Get the root word
    def __get_root_word(self, word):
        ps = PorterStemmer()
        return self.__correct_spelling(ps.stem(word))

    # Remove numbers
    def __remove_numbers(self, text):
        return re.sub(r"\d+", "", text)

    # Expand contractions
    def __expand_contractions(self, text):
        return contractions.fix(text)
