import pandas as pd

value_to_find = 3250

# Function to find the key related to a given value in the CSV file
def find_key_by_value_in_csv(value, csv_file_path):
    df = pd.read_csv(csv_file_path)
    for index, row in df.iterrows():
        if row['Word ID'] == value:
            return row['Token']
    return None

# Example usage for CSV
csv_file_path = "../dat/sorted_lexicon.csv"
key_in_csv = find_key_by_value_in_csv(value_to_find, csv_file_path)
if key_in_csv:
    print(f"The key for value {value_to_find} in CSV is: {key_in_csv}")
else:
    print(f"No key found for value {value_to_find} in CSV")