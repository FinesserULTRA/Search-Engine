import pandas as pd
import numpy as np
import json
import ast


def round_to_half(x):
    return np.round(x * 2) / 2


def normalize_and_input_csv():
    # Load datasets once
    reviews_df = pd.read_csv("../data/reviews.csv")
    hotel_df = pd.read_csv("../data/cleaned_hotels.csv")

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
    reviews_df["title"] = (
        reviews_df["title"].str.lower().str.replace("“", "").str.replace("”", "")
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

    reviews_df.to_csv("../data/cleaned_reviews.csv", index=False)
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
        offerings_df.to_csv("../data/cleaned_hotels.csv", index=False)

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
            round_to_half
        )

        # Calculate average score
        hotels_grouped["average_score"] = (
            hotels_grouped[numeric_columns].mean(axis=1).apply(round_to_half)
        )

        # Export aggregated data
        hotels_grouped.to_csv("../data/hotels_aggregated.csv", index=False)

    except Exception as e:
        print("Error: ", e)


def script_clean():
    process_and_normalize_hotel_data()
    return 0


script_clean()
