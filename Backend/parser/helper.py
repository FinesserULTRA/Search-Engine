import nltk
from nltk.corpus import stopwords
import re
import string

PUNCT_TO_REMOVE = string.punctuation
nltk.download("stopwords")
STOPWORDS = set(stopwords.words("english"))


def remove_punctuation(text):
    """custom function to remove the punctuation"""
    return text.translate(str.maketrans("", "", PUNCT_TO_REMOVE))


def remove_stopwords(text):
    """custom function to remove the stopwords"""
    return " ".join([word for word in str(text).split() if word not in STOPWORDS])


## Removal of URLS
def remove_urls(text):
    url_pattern = re.compile(r"https?://\S+|www\.\S+")
    return url_pattern.sub(r"", text)
