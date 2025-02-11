import pandas as pd
import numpy as np
import json
import ast
import string

PUNCT_TO_REMOVE = string.punctuation


def round_to_ones(x):
    return np.round(x, 1)


def remove_punctuation(text):
    """custom function to remove the punctuation"""
    return text.translate(str.maketrans("", "", PUNCT_TO_REMOVE))


def normalize_and_input_csv():
    # Load datasets once
    reviews_df = pd.read_csv("../data/reviews.csv")
    hotel_df = pd.read_csv("../data/hotels_cleaned.csv")

    # Clean JSON columns
    reviews_df["ratings"] = (
        reviews_df["ratings"].str.replace("'", '"').apply(json.loads)
    )
    normalized_cols = pd.json_normalize(reviews_df["ratings"])
    reviews_df = pd.concat(
        [reviews_df.drop(columns=["ratings"]), normalized_cols], axis=1
    )

    # Drop unnecessary columns
    reviews_df.drop(
        columns=[
            "num_helpful_votes",
            "author",
            "via_mobile",
            "date",
            "check_in_front_desk",
            "business_service_(e_g_internet_access)",
        ],
        axis=1,
        inplace=True,
    )

    # Add custom rev_id column
    reviews_df["rev_id"] = range(1, len(reviews_df) + 1)

    # Lowercase and remove curly quotes in text and title
    reviews_df["text"] = reviews_df["text"].str.lower()
    reviews_df["text"] = reviews_df["text"].apply(lambda text: remove_punctuation(text))
    reviews_df["title"] = (
        reviews_df["title"].str.lower().str.replace("“", "").str.replace("”", "")
    )
    reviews_df["title"] = reviews_df["title"].apply(
        lambda text: remove_punctuation(text)
    )
    # Add hotel_ids to the reviews
    reviews_df = pd.merge(
        reviews_df, hotel_df[["offering_id", "hotel_id"]], on="offering_id", how="left"
    )

    # Select relevant columns
    reviews_df = reviews_df[
        [
            "rev_id",
            "hotel_id",
            "offering_id",
            "id",
            "title",
            "text",
            "overall",
            "date_stayed",
            "value",
            "location",
            "cleanliness",
            "service",
            "sleep_quality",
            "rooms",
        ]
    ]

    reviews_df.to_csv("../data/reviews_cleaned.csv", index=False)
    return reviews_df


def process_and_normalize_hotel_data():
    try:
        # Load the hotels CSV files into pandas dataframes
        offerings_df = pd.read_csv("../data/hotels.csv", encoding="utf-8-sig")

        # Rename 'id' column in offerings_df to 'offering_id' for consistency
        offerings_df.rename(columns={"id": "offering_id"}, inplace=True)
        offerings_df["hotel_id"] = range(1, len(offerings_df) + 1)

        # Deconstruct JSON in address column
        offerings_df["address"] = offerings_df["address"].apply(ast.literal_eval)
        address_df = offerings_df["address"].apply(pd.Series)
        offerings_df = pd.concat(
            [offerings_df.drop(columns=["address"]), address_df], axis=1
        )

        # Select relevant columns
        offerings_df = offerings_df[
            [
                "hotel_id",
                "offering_id",
                "name",
                "region_id",
                "region",
                "street-address",
                "locality",
                "hotel_class",
            ]
        ]

        # Export the cleaned data to a new csv file
        offerings_df.to_csv("../data/hotels_cleaned.csv", index=False)

        # Process reviews data and clean
        reviews_df = normalize_and_input_csv()

        combined_df = pd.merge(
            reviews_df, offerings_df, on=["offering_id", "hotel_id"], how="right"
        )

        # Group by hotel attributes and calculate mean for numeric columns
        hotels_grouped = (
            combined_df.groupby(
                [
                    "hotel_id",
                    "offering_id",
                    "name",
                    "region_id",
                    "region",
                    "street-address",
                    "locality",
                    "hotel_class",
                ],
                dropna=False,
            )
            .agg(
                {
                    "service": "mean",
                    "cleanliness": "mean",
                    "overall": "mean",
                    "value": "mean",
                    "location": "mean",
                    "sleep_quality": "mean",
                    "rooms": "mean",
                }
            )
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
            round_to_ones
        )

        # Calculate average score
        hotels_grouped["average_score"] = (
            hotels_grouped[numeric_columns].mean(axis=1).apply(round_to_ones)
        )
        for column in hotels_grouped.select_dtypes(include=['float64']).columns:
            hotels_grouped[column] = hotels_grouped[column].astype(str)
        hotels_grouped.fillna("", inplace=True)
        hotels_grouped.set_index("hotel_id", inplace=False)

        # Add review counts
        review_counts = reviews_df.groupby('hotel_id').size().reset_index(name='review_count')
        hotels_grouped = pd.merge(hotels_grouped, review_counts, on='hotel_id', how='left')
        hotels_grouped['review_count'] = hotels_grouped['review_count'].replace(np.nan, 0).astype(int)

        # Export aggregated data
        hotels_grouped.to_csv("../data/hotels_cleaned.csv", index=False)

    except Exception as e:
        print("Error: ", e)


def script_clean():
    process_and_normalize_hotel_data()
    return 0


script_clean()
