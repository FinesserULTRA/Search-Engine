import json
import os

# Directory containing the lexicon JSON files
lexicons_dir = "../index data"
lexicon_files = [os.path.join(lexicons_dir, f) for f in os.listdir(lexicons_dir) if f.startswith("lexicon_") and f.endswith(".json")]

# Initialize an empty dictionary to hold the combined lexicon
combined_lexicon = {}

# Iterate through each lexicon file
for lexicon_file in lexicon_files:
    print(f"Loading lexicon from {lexicon_file}")
    with open(lexicon_file, "r", encoding="utf-8-sig") as json_file:
        lexicon = json.load(json_file)
        for word in lexicon:
            if word not in combined_lexicon:
                combined_lexicon[word] = len(combined_lexicon)

# Save the combined lexicon to a new JSON filea
output_filename = "../index data/combined_lexicon.json"
with open(output_filename, "w", encoding="utf-8-sig") as json_file:
    json.dump(combined_lexicon, json_file, indent=4)

print(f"Combined lexicon saved to {output_filename}")
