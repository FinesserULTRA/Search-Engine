import pandas as pd
import numpy as np
import ast
import json
import re
import time
from collections import defaultdict
from fastapi import HTTPException
from helper import remove_punctuation, remove_stopwords, remove_urls


def round_to_half(x):
    return np.round(x * 2) / 2


def process_and_normalize_hotel_data():
    try:
        # Load the CSV files into pandas dataframes
        reviews_df = pd.read_csv("../data/cleaned_reviews.csv", encoding="utf-8-sig")
        offerings_df = pd.read_csv("../data/offerings.csv", encoding="utf-8-sig")

        # Rename 'id' column in offerings_df to 'offering_id' for consistency
        offerings_df = offerings_df.rename(columns={"id": "offering_id"})

        # Merge reviews_df and offerings_df on 'offering_id'
        combined_df = pd.merge(reviews_df, offerings_df, on="offering_id", how="right")

        # Handle 'address' column in combined_df
        combined_df["address"] = combined_df["address"].apply(ast.literal_eval)
        address_df = combined_df["address"].apply(pd.Series)
        combined_df = pd.concat(
            [combined_df.drop(columns=["address"]), address_df], axis=1
        )

        # Ensure all address components are properly extracted
        if "locality" in address_df.columns:
            combined_df["locality"] = address_df["locality"].astype(str)

        # Reorder columns in combined_df
        combined_df = combined_df[
            [
                "offering_id",
                "name",
                "hotel_class",
                "region_id",
                "region",
                "street-address",
                "locality",
            ]
            # + [
            #     col
            #     for col in address_df.columns
            #     if col not in ["region", "street-address", "locality"]
            # ]
            + [
                col
                for col in combined_df.columns
                if col
                not in [
                    "offering_id",
                    "name",
                    "hotel_class",
                    "region_id",
                    "region",
                    "street-address",
                    "locality",
                ]
            ]
        ]

        combined_df = combined_df.drop(
            columns=[
                "phone",
                "details",
                "postal-code",
            ]
        )

        combined_df = combined_df.dropna()

        hotels_grouped = (
            combined_df.groupby(
                [
                    "offering_id",
                    "name",
                    "hotel_class",
                    "region_id",
                    "region",
                    "street-address",
                    "locality",
                ],
                dropna=False,
            )
            .mean(numeric_only=True)
            .reset_index()
        )

        # Round numeric columns to the nearest half
        numeric_columns = [
            "service",
            "cleanliness",
            "overall",
            "value",
            "location",
            "sleep_quality",
            "rooms",
        ]
        hotels_grouped[numeric_columns] = hotels_grouped[numeric_columns].apply(
            round_to_half
        )

        hotels_grouped["average_score"] = (
            hotels_grouped[numeric_columns[:-1]].mean(axis=1).apply(round_to_half)
        )

        # Save the aggregated hotel data to a CSV file
        hotels_grouped.to_csv("../data/hotels_aggregated.csv", index=False)

        # lexicon_inverted()

        return {
            "message": "hotel data aggregated and saved to CSV",
            "hotels_grouped": hotels_grouped,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def lexicon_inverted():
    # Prepare the lexicon with tokens from multiple columns in hotels_grouped
    lexicon = defaultdict(int)
    columns_to_tokenize = [
        "name",
        "text",
        "title",
        "region",
        "street-address",
        "locality",
    ]

    for _, row in hotels_grouped.iterrows():
        for column in columns_to_tokenize:
            if pd.notna(row.get(column)):
                cleaned_tokens = re.findall(r"\w+", str(row[column]).lower())
                for token in cleaned_tokens:
                    if token not in lexicon:
                        lexicon[token] = len(lexicon) + 1

    # Sort the lexicon keys alphabetically and assign sorted word IDs
    sorted_lexicon_keys = sorted(lexicon.keys())
    sorted_lexicon = {token: idx + 1 for idx, token in enumerate(sorted_lexicon_keys)}

    # Save the sorted lexicon to a CSV file
    sorted_lexicon_df = pd.DataFrame(
        sorted_lexicon.items(), columns=["Token", "Word ID"]
    )
    sorted_lexicon_df.to_csv("../dat/sorted_lexicon.csv", index=False)

    # Create a new offering_id to wordID mapping based on the sorted lexicon (forward index)
    forward_index = {}
    for _, row in hotels_grouped.iterrows():
        offering_id = row["offering_id"]
        all_tokens = []
        for column in columns_to_tokenize:
            if pd.notna(row.get(column)):
                cleaned_tokens = re.findall(r"\w+", str(row[column]).lower())
                all_tokens.extend(cleaned_tokens)
        sorted_word_ids = [
            sorted_lexicon[token] for token in all_tokens if token in sorted_lexicon
        ]
        forward_index[offering_id] = sorted_word_ids

    # Save the forward index to a JSON file
    with open("../dat/forward_index.json", "w") as json_file:
        json.dump(forward_index, json_file, indent=4)

    # Create an inverted index mapping word IDs to offering_ids
    inverted_index = defaultdict(list)
    for offering_id, word_ids in forward_index.items():
        for word_id in word_ids:
            inverted_index[word_id].append(offering_id)

    # Save the inverted index to a JSON file
    with open("../dat/inverted_index.json", "w") as json_file:
        json.dump(inverted_index, json_file, indent=4)

    return {
        "message": "Sorted lexicon saved to CSV, and forward and inverted indexes saved to JSON files respectively.",
        "sorted_lexicon": sorted_lexicon_df,
        "forward_index": forward_index,
        "inverted_index": inverted_index,
    }


if __name__ == "__main__":
    start_time = time.time()
    hotels_grouped = process_and_normalize_hotel_data()["hotels_grouped"]
    print(hotels_grouped.head())
    print("\nTime taken: %.2f seconds" % (time.time() - start_time))
