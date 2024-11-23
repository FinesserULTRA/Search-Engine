import pandas as pd
import numpy as np
import json
from helper import remove_punctuation, remove_stopwords, remove_urls


def normalize_and_input_csv():
    # Import and view datasets to understand the content
    df = pd.read_csv("../dat/reviews.csv")

    # deconstruct json columns
    json_cols = ["ratings"]

    def clean_json(x):
        "Create apply function for decoding JSON"
        return json.loads(x)

    for x in json_cols:
        df[x] = df[x].str.replace("'", '"')
        df[x] = df[x].apply(clean_json)

    normalized_cols = pd.json_normalize(df["ratings"])
    df = df.join(normalized_cols)
    df = df.drop(json_cols, axis=1)
    df = df.drop(
        [
            "num_helpful_votes",
            "id",
            "author",
            "via_mobile",
            "date",
            "check_in_front_desk",
            "business_service_(e_g_internet_access)",
        ],
        axis=1,
    )

    ## Lower Casing
    df["text"] = df["text"].str.lower()

    df["text"] = df["text"].apply(lambda text: remove_punctuation(text))

    df["text"] = df["text"].apply(lambda text: remove_stopwords(text))

    df["text"] = df["text"].apply(lambda text: remove_urls(text))

    print(df.info())
    print(df.head())

    df.to_csv("../dat/cleaned_reviews.csv", index=False)

    return df


normalize_and_input_csv()
