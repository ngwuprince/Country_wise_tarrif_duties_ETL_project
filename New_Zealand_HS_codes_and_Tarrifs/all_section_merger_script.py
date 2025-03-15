import glob
import pandas as pd
import chardet  # Library to detect file encoding

# Function to detect file encoding
def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    return result['encoding']

# Function to convert Roman numerals to integers for sorting
def roman_to_int(roman):
    roman_numerals = {
        'i': 1, 'ii': 2, 'iii': 3, 'iv': 4, 'v': 5,
        'vi': 6, 'vii': 7, 'viii': 8, 'ix': 9, 'x': 10,
        'xi': 11, 'xii': 12, 'xiii': 13, 'xiv': 14, 'xv': 15,
        'xvi': 16, 'xvii': 17, 'xviii': 18, 'xix': 19, 'xx': 20,
        'xxi': 21
    }
    return roman_numerals.get(roman.lower(), 0)  # Default to 0 if not found

# Get all the files in this directory that start with 'section'
files = glob.glob('section*.csv')

# Sort the files based on the Roman numeral in the filename
files.sort(key=lambda x: roman_to_int(x.split('-')[1].split('.')[0]))

# Read all the files into a list of DataFrames
dfs = []
for file in files:
    encoding = detect_encoding(file)  # Detect the encoding of the file
    print(f"Reading {file} with encoding: {encoding}")
    df = pd.read_csv(file, encoding=encoding)
    dfs.append(df)

# Concatenate all DataFrames row-wise
df = pd.concat(dfs, ignore_index=True)

# Write the final DataFrame to a CSV file
df.to_csv('final.csv', index=False)

# Display the first few rows of the final DataFram
print(df.head())