import os
import re

# Directory containing the files
directory = "C:\\Users\\PC\\VSCODE\\Search-Engine\\backend\\index data\\forward_index\\reviews\\"

# Get all files in the directory
files = os.listdir(directory)

# Filter for forward index files
pattern = r'forward_index_(\d+)-(\d+)\.json'
files = sorted([f for f in files if re.match(pattern, f)], reverse=True)
print(files)

for filename in files:
    match = re.match(pattern, filename)
    if match:
        num1, num2 = map(int, match.groups())
        new_filename = f'forward_index_{num1-1}_{num2-1}.json'
        
        old_path = os.path.join(directory, filename)
        new_path = os.path.join(directory, new_filename)
        
        try:
            # Remove destination file if it exists
            if os.path.exists(new_path):
                os.remove(new_path)
            os.rename(old_path, new_path)
            print(f'Renamed: {filename} -> {new_filename}')
        except OSError as e:
            print(f'Error renaming {filename}: {e}')
