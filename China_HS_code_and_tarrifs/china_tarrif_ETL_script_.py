import tabula
import pandas as pd
import numpy as np
import re

#EXTRACT

# Extract tables from the PDF using the stream method
tables = tabula.read_pdf(
    "complete_china_data.pdf",
    pages="all",
    multiple_tables=True,
    lattice= True,
    pandas_options={"header": None}  # Disable automatic header detection
)

# Filter tables with the specified headers
target_headers = ["序号", "税则号列", "货品名称", "最惠国税率(%)", "协定税率(%)", "特惠税率(%)", "普通税率(%)"]
filtered_tables = []

for table in tables:
    # Check if the table contains the target headers in the first row
    if all(header in table.iloc[0].values for header in target_headers):
        # Set the first row as the header and remove it from the data
        table.columns = table.iloc[0]
        table = table[1:]
        # Select columns from the second column onward
        table = table.iloc[:, 1:]
        filtered_tables.append(table)

# Concatenate all filtered tables into one DataFrame
if filtered_tables:
    combined_table = pd.concat(filtered_tables, ignore_index=True)
    # Save the combined table to a single CSV file
    combined_table.to_csv("_combined_china.csv", index=False, encoding="utf-8-sig")
    print("All tables saved to 'tabula_stream_combined_china.csv'.")
else:
    print("No tables with the specified headers were found.")

### LOAD

data = pd.read_csv('_combined_china.csv')

#TRANSFORM

#rename the columns Unnamed: 7, Unnamed: 8 to 'new_col1', 'new_col2'
data.rename(columns={'Unnamed: 7':'new_col1', 'Unnamed: 8':'new_col2', 'Unnamed: 2' : 
                     'Preferential_Rate(Agreement rate)_1'}, inplace=True)

#add empty column named 'guard_col'
data['guard_col1'] = np.nan
data['guard_col2'] = np.nan

#get me the rows where Unnamed: 7, Unnamed: 8 are not null as dataframe
data[data['new_col1'].notnull() & data['new_col2'].notnull()]


# Convert HS_code column to string
data['HS_code'] = data['HS_code'].astype(str)

# Function to extract the last numeric value (including values like 6∆0)
def extract_last_value(value):
    # Split the row into segments based on whitespace
    segments = value.strip().split()
    # Get the last segment
    last_segment = segments[-1]
    # Check if the last segment matches the pattern for numeric or X∆Y
    if re.match(r'^\d+∆\d+$|^\d+$', last_segment):
        return last_segment
    return None

# Function to shift values to the right and insert the new value
def shift_and_insert(row):
    # Extract the last value from HS_code
    new_value = extract_last_value(row['HS_code'])
    if new_value:
        # Shift values to the right, starting from MFN_Rate
        row_values = row.tolist()
        # Find the index of MFN_Rate
        mfn_rate_index = row.index.get_loc('MFN_Rate')
        # Shift values to the right
        row_values[mfn_rate_index + 1:] = row_values[mfn_rate_index:-1]
        # Update MFN_Rate only if a match is found, otherwise keep the existing value
        row_values[mfn_rate_index] = new_value if new_value is not None else row_values[mfn_rate_index]
        # Update the row
        row = pd.Series(row_values, index=row.index)
    return row

# Apply the shift_and_insert function to each row
df = data.apply(shift_and_insert, axis=1)


# Function to check for Chinese characters
def contains_chinese(text):
    if pd.isna(text):
        return False
    return bool(re.search(r'[\u4e00-\u9fff]', str(text)))

# Load your data (replace with your actual file)
# data = pd.read_csv('your_file.csv')

# Find rows with Chinese characters in the specified column
chinese_rows = data[data['Preferential_Rate(Agreement rate)_1'].apply(contains_chinese)]

# Display the results
print(f"Found {len(chinese_rows)} rows with Chinese characters:")
print(chinese_rows)

# If you want to see just the specific column:
print("\nChinese text found in 'Preferential_Rate(Agreement rate)_1':")
print(chinese_rows['Preferential_Rate(Agreement rate)_1'].unique())

# Function to check if a value contains Chinese characters
def contains_chinese(value):
    if pd.isna(value) or value is None:
        return False  # Ignore null values
    # Regex to match Chinese characters
    return bool(re.search(r'[\u4e00-\u9fff]', value))

# Function to shift row values to the right if Chinese characters are found
def shift_row(row):
    # Check if the value in Preferential_Rate(Agreement rate)_1 contains Chinese characters
    if contains_chinese(row['Preferential_Rate(Agreement rate)_1']):
        # Get the index of Preferential_Rate(Agreement rate)_1
        start_index = row.index.get_loc('Preferential_Rate(Agreement rate)_1')
        # Shift values to the right
        row_values = row.tolist()
        row_values[start_index + 1:] = row_values[start_index:-1]
        # Add null to Preferential_Rate(Agreement rate)_1
        row_values[start_index] = None
        # Update the row
        row = pd.Series(row_values, index=row.index)
    return row

# Apply the shift_row function to each row
df2 = df.apply(shift_row, axis=1)


def process_data(data):
    # Make a copy of the original data
    df = df2.copy()
    
    # Iterate through each row
    for index, row in df.iterrows():
        # Remove whitespace from HS_code
        hs_code = str(row['HS_code']).strip()
        
        # Check if HS_code doesn't contain a decimal number pattern
        if not re.search(r'\d+\.\d+', hs_code):
            # Store the original MFN_Rate value if it exists
            if pd.notna(row['MFN_Rate']):
                mfn_value = row['MFN_Rate']
                
                # Shift values to the right
                # Move MFN_Rate to Preferential_Rate(Agreement rate)_1
                df.at[index, 'Preferential_Rate(Agreement rate)_1'] = mfn_value
                
                # Set MFN_Rate to null
                df.at[index, 'MFN_Rate'] = np.nan
    
    return df

# Apply the transformation
df2_updated = process_data(df2)


#shift columns to the right with MO in Preferential_Rate_1
# Function to process Preferential_Rate columns
def process_preferential_rates(data):
    
    # Iterate through each row
    for index, row in df.iterrows():
        pref_rate_1 = row['Preferential_Rate(Agreement rate)_1']
        
        # Check if Preferential_Rate_1 contains "MO" (case insensitive)
        if pd.notna(pref_rate_1) and re.search(r'MO', str(pref_rate_1), re.IGNORECASE):
            
            # Store the original Preferential_Rate_1 value
            original_value = pref_rate_1
            
            # Move the value to Preferential_Rate
            df2.at[index, 'Preferential_Rate(Agreement rate)'] = original_value
            
            # Set Preferential_Rate_1 to null
            df2.at[index, 'Preferential_Rate(Agreement rate)_1'] = np.nan
    
    return df2

# Apply the transformation
df3 = process_preferential_rates(df2_updated)



#Remove all Chinese characters
def remove_chinese_chars(text):
    """Remove Chinese characters from text using regex"""
    if pd.isna(text):
        return text
    return re.sub(r'[\u4e00-\u9fff\u3400-\u4dbf\U00020000-\U0002a6df\U0002a700-\U0002b73f\U0002b740-\U0002b81f\U0002b820-\U0002ceaf]+', 
                  '', str(text))

# Columns to process
columns_to_clean = [
    'HS_code', 'MFN_Rate', 'Preferential_Rate(Agreement rate)_1',
    'Preferential_Rate(Agreement rate)',
    'Preferential_Tariff_Rate(Special_Preferential_Tariff_Rate)',
    'General_Tariff_Rate', 'Return_to_General', 'new_col1', 'new_col2',
    'guard_col'
]

# Apply the cleaning function to each specified column
for col in columns_to_clean:
    if col in data.columns:
        df3[col] = df3[col].apply(remove_chinese_chars)



def remove_standalone_R(df):
    """
    Remove all standalone 'R' characters from all columns in the DataFrame using regex.
    Handles cases where 'R' is:
    - At start of string (Rvalue)
    - At end of string (valueR)
    - Surrounded by spaces (value R value)
    - Punctuation cases (value,R,value or value.R.value)
    """
    
    # Regex pattern to match standalone R (case-sensitive)
    pattern = r'(?:^|\s)R(?:\s|$)|(?:^|\s)R(\W)|(\W)R(?:\s|$)'
    
    # Iterate through all columns
    for col in df3.columns:
        # Apply regex substitution
        df3[col] = df3[col].apply(
            lambda x: re.sub(pattern, 
                           lambda m: m.group(1) if m.group(1)
                           else (m.group(2) if m.group(2) 
                           else ' '), 
                           str(x))
        )
        # Clean up any double spaces created
        df3[col] = df3[col].str.replace(r'\s+', ' ').str.strip()
        # Convert empty strings to NaN
        df3[col] = df3[col].replace({'': np.nan})
    
    return df3

# Apply the cleaning function to your DataFrame
df3_updated = remove_standalone_R(df3)



def shift_rows_left(data):
    """
    When both Preferential_Rate columns contain numbers, shift other columns left (except HS_code)
    """
    df = data.copy()
    
    # Columns to check for numbers
    pref_cols = [
        'Preferential_Rate(Agreement rate)_1',
        'Preferential_Rate(Agreement rate)'
    ]
    
    # Columns that can be shifted (all except HS_code)
    shift_cols = [col for col in df.columns if col != 'HS_code']
    
    for index, row in df.iterrows():
        # Check if both preferential columns contain numbers
        both_numeric = all(
            pd.notna(row[col]) and 
            re.match(r'^[+-]?\d*\.?\d+$', str(row[col]))
            for col in pref_cols
        )
        
        if both_numeric:
            # Get values from shiftable columns
            values = row[shift_cols].tolist()
            
            # Shift left by one position (first value gets dropped)
            shifted_values = values[1:] + [np.nan]
            
            # Update the row (except HS_code)
            for i, col in enumerate(shift_cols):
                df.at[index, col] = shifted_values[i]
    
    return df

# Apply the transformation
df4 = shift_rows_left(df3_updated)


df4.to_csv('review_data.csv', index=False)


### Used TEXTJOIN on excel to join tarrifs(last 4 columns) before proceeding to run code below


clean_V1 = pd.read_csv('review_data.csv')

# Create three new columns initialized with NaN
clean_V1['preferential_Tariff_Rate(Special_Preferential_Tariff_Rate)'] = np.nan
clean_V1['class'] = np.nan
clean_V1['General_Tariff_Rate'] = np.nan

# Iterate through each row
for index, row in clean_V1.iterrows():
    values = str(row['new_column_with_4_last_columns']).split()  # Split by space
    
    if len(values) == 3:
        # Case with 3 values
        clean_V1.at[index, 'preferential_Tariff_Rate(Special_Preferential_Tariff_Rate)'] = values[0]
        clean_V1.at[index, 'class'] = values[1]
        clean_V1.at[index, 'General_Tariff_Rate'] = values[2]
    elif len(values) == 1:
        # Case with single value
        clean_V1.at[index, 'class'] = values[0]



# Process 'hs_code' column
clean_V1['HS_CODE'] = clean_V1['HS_code'].astype(str).str.split().str[0]


clean_V1.to_csv('review_data2.csv', index=False)



data_v2 = pd.read_csv('review_data2.csv')
def process_hs_code(data):
    """
    Process HS_code column to extract decimal values and shift rates when conditions are met:
    1. Split HS_code by spaces
    2. Check if last value is a decimal number
    3. Check if Preferential_Rate(Agreement rate)_1 is empty
    4. If conditions met:
       - Move decimal value to MFN_Rate
       - Move original MFN_Rate to Preferential_Rate(Agreement rate)_1
    """
    df = data.copy()
    
    for index, row in df.iterrows():
        # Split HS_code values
        hs_values = str(row['HS_code']).split()
        
        # Check if we have values and last value is a decimal number
        if len(hs_values) > 0 and re.match(r'^\d+\.\d+$', hs_values[-1]):
            decimal_value = hs_values[-1]
            
            # Check if Preferential_Rate(Agreement rate)_1 is empty/NaN
            if pd.isna(row['Preferential_Rate(Agreement rate_1']) or row['Preferential_Rate(Agreement rate_1'] == '':
                # Store original MFN_Rate
                original_mfn = row['MFN_Rate']
                
                # Update MFN_Rate with decimal value
                df.at[index, 'MFN_Rate'] = decimal_value
                
                # Move original MFN_Rate to Preferential_Rate(Agreement rate)_1
                df.at[index, 'Preferential_Rate(Agreement rate_1'] = original_mfn
    
    return df

# Apply the transformation
V4_ = process_hs_code(data_v2)
                      
                      

V4_.to_csv('review_data3.csv', index=False)


data_v5 = pd.read_csv('review_data3.csv')
def remove_ex_rows(df):
    """
    Remove rows where HS_code values start with 'ex' (case insensitive)
    """
    # Create a boolean mask for rows to keep
    mask = ~df['HS_code'].astype(str).str.lower().str.startswith('ex')
    
    # Apply the mask to filter the dataframe
    filtered_df = df[mask].copy()
    
    return filtered_df

# Apply the transformation
filtered_data = remove_ex_rows(data_v5)

# Optional: Reset index if needed
v5_ = filtered_data.reset_index(drop=True)


#save new version
v5_.to_csv('review_data4.csv', index=False)



data_v6 = pd.read_csv('review_data4.csv')
def remove_non_alphanumeric_rows(df):
    """
    Remove rows that don't contain any letters (a-z, A-Z) or digits (0-9) across all columns.
    Preserves rows that have at least one alphanumeric character in any column.
    """
    # Create a mask to identify rows with at least one alphanumeric character
    mask = df.apply(
        lambda row: any(
            bool(re.search(r'[a-zA-Z0-9]', str(cell))) 
            for cell in row
        ),
        axis=1
    )
    
    # Filter the dataframe
    filtered_df = df[mask].copy()
    
    return filtered_df

# Apply the transformation
cleaned_data = remove_non_alphanumeric_rows(data_v6)

# Optional: Reset index
v6_ = cleaned_data.reset_index(drop=True)

v6_.to_csv('clean_7_1.csv')







