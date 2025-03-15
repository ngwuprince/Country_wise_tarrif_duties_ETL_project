import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

# Function to generate URLs for all sections and chapters- URL sequencing
def generate_urls():
    base_url = "https://www.abf.gov.au/importing-exporting-and-manufacturing/tariff-classification/current-tariff/schedule-3"
    sections = {
        'I': range(1, 6),    # Section I has chapters 1-5
        'II': range(6, 15),   # Section II has chapters 6-14
        'III': range(15, 16), # Section III has chapter 15
        'IV': range(16, 25),  # Section IV has chapters 16-24
        'V': range(25, 28),   # Section V has chapters 25-27
        'VI': range(28, 39),  # Section VI has chapters 28-38
        'VII': range(39, 41), # Section VII has chapters 39-40
        'VIII': range(41, 44),# Section VIII has chapters 41-43
        'IX': range(44, 47),  # Section IX has chapters 44-46
        'X': range(47, 50),   # Section X has chapters 47-49
        'XI': range(50, 64),  # Section XI has chapters 50-63
        'XII': range(64, 68), # Section XII has chapters 64-67
        'XIII': range(68, 71),# Section XIII has chapters 68-70
        'XIV': range(71, 72), # Section XIV has chapter 71
        'XV': range(72, 84),  # Section XV has chapters 72-83
        'XVI': range(84, 86), # Section XVI has chapters 84-85
        'XVII': range(86, 90),# Section XVII has chapters 86-89
        'XVIII': range(90, 93),# Section XVIII has chapters 90-92
        'XIX': range(93, 94), # Section XIX has chapter 93
        'XX': range(94, 97),  # Section XX has chapters 94-96
        'XXI': range(97, 98)  # Section XXI has chapter 97
    }
    
    urls = []
    for section, chapters in sections.items():
        for chapter in chapters:
            url = f"{base_url}/section-{section.lower()}/chapter-{chapter}"
            urls.append(url)
    return urls

# Function to scrape a single page
def scrape_page(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Find all tables on the page
    all_tables = soup.find_all('table')
    print(f"Found {len(all_tables)} tables on {url}")
    
    # Define consistent headers based on the first table
    headers = ['StatisticalCode', 'Unit', 'Goods', 'Rate#', 'ReferenceNumber', 'Tariffconcessionorders']
    
    # Process and combine all tables
    page_data = []
    for table in all_tables:
        rows = table.find_all('tr')
        
        # Skip the header row for each table
        for row in rows[1:]:
            cols = row.find_all('td')
            
            # Skip empty rows
            if not cols:
                continue
            
            # Extract data from each column
            row_data = [col.text.strip().replace('\n', ' ').replace('\r', '') for col in cols]
            
            # Skip completely empty rows
            if not any(row_data):
                continue
            
            # Ensure row_data has the same length as headers
            if len(row_data) < len(headers):
                row_data += [''] * (len(headers) - len(row_data))
            elif len(row_data) > len(headers):
                row_data = row_data[:len(headers)]
            
            page_data.append(row_data)
    
    return page_data

# Function to clean the combined DataFrame
def clean_data(df):
    # Clean up general data: replace multiple spaces with a single space
    for col in df.columns:
        df[col] = df[col].str.replace(r'\s+', ' ', regex=True)
    
    # Clean the Reference Number column
    def extract_reference_number(text):
        if pd.isna(text) or text == '':
            return text
        
        # Try to find a pattern like xxxx.xx.xx
        match = re.search(r'\d{4}\.\d{2}\.\d{2}', text)
        if match:
            return match.group(0)
        
        # Try to find a pattern like xxxx
        match = re.search(r'\b\d{4}\b', text)
        if match:
            return match.group(0)
        
        # Try to find a pattern like xxxx.x
        match = re.search(r'\b\d{4}\.\d{1}\b', text)
        if match:
            return match.group(0)
        
        # Try to find a pattern like xxxx.xx
        match = re.search(r'\b\d{4}\.\d{2}\b', text)
        if match:
            return match.group(0)
        
        # If no specific pattern is found, return the original text
        return text
    
    # Clean the Goods column
    def clean_goods_text(text):
        if pd.isna(text) or text == '':
            return text
        
        # Replace '=-' with just '-'
        text = text.replace('=-', '-')
        
        # Replace other special character combinations that might appear
        text = text.replace('=', '')
        text = text.replace('--', '-')
        
        # Remove leading/trailing dashes and whitespace
        text = text.strip('- \t')
        
        # Normalize spaces around dashes
        text = re.sub(r'\s*-\s*', ' - ', text)
        
        # Fix any double spaces created
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    # Apply the cleaning functions to the respective columns
    df['ReferenceNumber'] = df['ReferenceNumber'].apply(extract_reference_number)
    df['Goods'] = df['Goods'].apply(clean_goods_text)
    
    # Remove empty rows without any data across the columns
    df = df.dropna(how='all')
    
    # Trim all string values in the DataFrame
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].str.strip()
    
    return df

# Step 1: Generate URLs for all sections and chapters
urls = generate_urls()
print(f"Generated {len(urls)} URLs to scrape")

# Step 2: Scrape all pages and combine data
all_data = []
for url in urls:
    print(f"Scraping {url}")
    page_data = scrape_page(url)
    all_data.extend(page_data)

# Step 3: Create DataFrame and clean data
combined_df = pd.DataFrame(all_data, columns=[
    'StatisticalCode', 'Unit', 'Goods', 'Rate#', 'ReferenceNumber', 'Tariffconcessionorders'
])
combined_df = clean_data(combined_df)

# Step 4: Save to CSV
output_file = "tariff_classification_all_section_found_all_ref.csv"
combined_df.to_csv(output_file, index=False)

print(f"Combined data from all sections saved to '{output_file}'")
print(f"Total rows in combined table: {len(combined_df)}")

# Display first 5 rows
print("\nFirst 5 rows of combined table:")
print(combined_df.head())