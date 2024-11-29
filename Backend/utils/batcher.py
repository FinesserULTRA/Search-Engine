import pandas as pd

# Define the ranges and review counts (the dictionary you provided)
id_ranges = ["1-1000", "1001-2000", "2001-3000", "3001-4000", "4001-5000"]

# Read the reviews data
reviews_df = pd.read_csv("../data/reviews_cleaned.csv")


# Function to split the reviews based on hotel_id ranges
def split_reviews_by_range(df, ranges):
    # Create an empty list to store dataframes for each range
    dfs = {}

    for range in ranges:
        start, end = map(int, range.split("-"))
        # Filter reviews based on the range
        filtered_df = df[(df["hotel_id"] >= start) & (df["hotel_id"] <= end)]
        # Store the filtered dataframe in a dictionary
        dfs[range] = filtered_df

    return dfs


# Get the split dataframes based on the ranges
split_dataframes = split_reviews_by_range(reviews_df, id_ranges)

# Save each dataframe to a separate CSV file
for range_key, df in split_dataframes.items():
    df.to_csv(f"../reviews/reviews_{range_key}.csv", index=False)

print("Reviews have been split and saved into separate files.")