import pandas as pd

df = pd.read_csv('clean_V7_1.csv')

def reformat_hs_code(hs_code):
    # Handle empty/missing cases
    if pd.isna(hs_code) or str(hs_code).strip() in ['""', '']:
        return '""'  # Return empty quoted string
    
    # Remove existing quotes and whitespace
    code = str(hs_code).strip().strip('"')
    
    # Handle cases where code might be empty after stripping
    if not code:
        return '""'
    
    # Split into parts
    if '.' in code:
        left, right = code.split('.', 1)  # Split on first dot only
    else:
        # If no dot, split into 4+4 digits (or whatever available)
        left = code[:4]
        right = code[4:] if len(code) > 4 else ''
    
    # Pad with zeros
    left = left.zfill(4)[:4]  # Exactly 4 digits with leading zeros
    right = right.ljust(4, '0')[:4]  # Exactly 4 digits with trailing zeros
    
    # Return with quotes
    return f'"{left}.{right}"'

def process_data(df):
    # Identify rows where all other columns are empty
    other_columns = [col for col in df.columns if col != 'HS_CODE']
    
    # Check for empty rows (handling both NaN and empty strings)
    empty_rows = (
        df[other_columns].isna().all(axis=1) | 
        (df[other_columns].astype(str) == '').all(axis=1))
    
    # Apply reformatting only to non-empty rows
    df.loc[~empty_rows, 'HS_CODE'] = df.loc[~empty_rows, 'HS_CODE'].apply(reformat_hs_code)
    
    return df

# Example usage:
df = process_data(df)
df.to_csv('clean_V7_1_.csv', index=False)