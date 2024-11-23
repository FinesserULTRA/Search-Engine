import ast
import pandas as pd
import numpy as np


# Function to round values to the nearest 0.5
def round_to_half(x):
    return np.round(x * 2) / 2


def separate_columns(reviews_df, offerings_df):
    # Convert the 'ratings' column from a string to a dictionary
    reviews_df["ratings"] = reviews_df["ratings"].apply(ast.literal_eval)

    # Extract individual ratings into separate columns
    ratings_df = reviews_df["ratings"].apply(pd.Series)
    reviews_df = pd.concat([reviews_df, ratings_df], axis=1)

    # Display the first few rows of the DataFrame to verify the changes
    print(reviews_df.head())

    offerings_df["address"] = offerings_df["address"].apply(ast.literal_eval)

    # Extract individual address components into separate columns
    address_df = offerings_df["address"].apply(pd.Series)

    # Combine the extracted columns with offerings_df
    offerings_df = pd.concat(
        [offerings_df.drop(columns=["address"]), address_df], axis=1
    )

    # Reorder columns in offerings_df
    desired_order = [
        "id",
        "name",
        "hotel_class",
        "region_id",
        "region",
        "street-address",
    ] + [col for col in address_df.columns if col not in ["region", "street-address"]]
    offerings_df = offerings_df[desired_order]

    offerings_df.head()

    missing_class_offering_ids = offerings_df[offerings_df["hotel_class"].isnull()][
        "id"
    ].unique()

    # Print the count of unique offering_ids with missing hotel_class
    print(
        f"Number of unique offering_ids with missing hotel_class: {len(missing_class_offering_ids)}"
    )

    # Count the number of reviews corresponding to offering_ids with missing hotel_class
    missing_class_reviews_count = reviews_df[
        reviews_df["offering_id"].isin(missing_class_offering_ids)
    ].shape[0]

    # Print the count of reviews for offering_ids with missing hotel_class
    print(
        f"Number of reviews for offering_ids with missing hotel_class: {missing_class_reviews_count}"
    )

    # offerings_df = offerings_df.dropna(subset=["hotel_class"])

    offerings_df.head()

    return drop_irrelevant(reviews_df, offerings_df)


def drop_irrelevant(reviews_df, offerings_df):
    reviews_df = reviews_df.drop(columns=["check_in_front_desk"])

    # Fill missing values in 'sleep_quality' with the mean value for each 'offering_id'
    reviews_df["sleep_quality"] = reviews_df.groupby("offering_id")[
        "sleep_quality"
    ].transform(lambda x: x.fillna(x.mean()))

    columns_to_check = [
        "service",
        "cleanliness",
        "overall",
        "value",
        "location",
        "sleep_quality",
        "rooms",
    ]

    # Drop rows where any of the specified columns have missing values
    reviews_df = reviews_df.dropna(subset=columns_to_check)

    unique_offering_ids = reviews_df["offering_id"].nunique()
    print(f"Number of unique offering_ids: {unique_offering_ids}")

    reviews_df = reviews_df.drop(
        columns=[
            "num_helpful_votes",
            "date",
            "via_mobile",
            "ratings",
            "author",
        ]
    )

    cols = reviews_df.columns.tolist()
    cols.insert(0, cols.pop(cols.index("id")))
    cols.insert(1, cols.pop(cols.index("offering_id")))
    reviews_df = reviews_df[cols]
    reviews_df.head()

    return reviews_df, offerings_df
