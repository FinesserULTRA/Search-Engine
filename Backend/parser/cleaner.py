import pandas as pd
import time
from helper import *


def clean_dataset():
    reviews_df = pd.read_csv("../dat/reviews.csv", encoding="utf-8-sig")
    offerings_df = pd.read_csv("../dat/offerings.csv", encoding="utf-8-sig")

    print("Reviews DataFrame:")
    print(reviews_df.head())

    print("\nOfferings DataFrame:")
    print(offerings_df.head())

    reviews_df, offerings_df = separate_columns(reviews_df, offerings_df)

    # Merge reviews_df with offerings_df
    combined_df = reviews_df.merge(
        offerings_df,
        left_on="offering_id",
        right_on="id",
        suffixes=("_review", "_offering"),
    )

    combined_df.dropna()

    combined_df = combined_df.drop(columns=["postal-code"])
    combined_df.dropna(inplace=True)

    print(combined_df.head())
    print(combined_df.columns)
    print(combined_df["locality"].unique())

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
            ]
        )
        .mean(numeric_only=True)
        .reset_index()
    )

    numeric_columns = [
        "service",
        "cleanliness",
        "overall",
        "value",
        "location",
        "sleep_quality",
        "rooms",
        "business_service_(e_g_internet_access)",
    ]
    hotels_grouped[numeric_columns] = hotels_grouped[numeric_columns].applymap(
        lambda x: round(x * 2) / 2
    )

    hotels_grouped["average_score"] = (
        hotels_grouped[numeric_columns[:-1]]
        .mean(axis=1)
        .apply(lambda x: round(x * 2) / 2)
    )

    print(hotels_grouped.head())

    # output to file
    hotels_grouped.to_csv("../dat/hotels_aggregated.csv", index=False)

    return hotels_grouped


start_time = time.time()
clean_dataset()

print("\nTime taken: %.2f seconds" % (time.time() - start_time))


# [
#     "id_review",
#     "offering_id",
#     "title",
#     "text",
#     "author",
#     "date_stayed",
#     "service",
#     "cleanliness",
#     "overall",
#     "value",
#     "location",
#     "sleep_quality",
#     "rooms",
#     "business_service_(e_g_internet_access)",
#     "id_offering",
#     "name",
#     "hotel_class",
#     "region_id",
#     "region",
#     "street-address",
#     "locality",
# ]

# [
#     "offering_id",
#     "name",
#     "hotel_class",
#     "region_id",
#     "region",
#     "street-address",
#     "locality",
#     "service",
#     "cleanliness",
#     "sleep_quality",
#     "rooms",
#     "location",
#     "average_score",
# ]
