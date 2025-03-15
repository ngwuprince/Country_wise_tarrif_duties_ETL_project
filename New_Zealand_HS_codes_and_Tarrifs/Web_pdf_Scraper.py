import os
import requests
import pdfplumber
import pandas as pd
import re

# Step 1: Define valid sections for each year
valid_sections = {
    2024: ["i", "iii", "v", "vii", "viii", "ix", "x", "xi", "xii", "xiii", "xiv", "xv", "xvi", "xvii", "xviii", "xix", "xx", "xxi"],
    2025: ["ii", "iv", "vi"]
}

# Step 2: Generate PDF URLs dynamically for valid sections
def generate_pdf_urls(base_url, valid_sections):
    """Generate PDF URLs based on the valid sections for each year."""
    urls = []
    for year, sections in valid_sections.items():
        for section in sections:
            if year == 2024:
                url = f"{base_url}/wtd-{year}/section-{section}-july-{year}.pdf"
            else:
                url = f"{base_url}/wtd-{year}/section-{section}.pdf"
            urls.append(url)
    return urls

# Step 3: Download PDF files
def download_pdf(url, save_path):
    """Download a PDF file from a URL and save it locally."""
    try:
        response = requests.get(url, timeout=10)  # Add timeout to avoid hanging
        if response.status_code == 200:
            with open(save_path, 'wb') as f:
                f.write(response.content)
            print(f"Downloaded: {save_path}")
            return True
        else:
            print(f"Failed to download (HTTP {response.status_code}): {url}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return False

# Step 4: Extract tables from a PDF file
def extract_tables_from_pdf(pdf_path):
    """Extract tables from a PDF file using pdfplumber."""
    tables = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                # Extract tables with custom settings to improve accuracy
                page_tables = page.extract_tables({
                    "vertical_strategy": "text", 
                    "horizontal_strategy": "text",
                    "intersection_y_tolerance": 10,  # Adjust tolerance for better row detection
                })
                if page_tables:
                    tables.extend(page_tables)
        return tables
    except Exception as e:
        print(f"Error extracting tables from {pdf_path}: {e}")
        return []

# Step 5: Main function to download and process PDFs
def process_pdfs(base_url, valid_sections, output_dir):
    """Download PDFs, extract tables, and save the content."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Generate PDF URLs
    pdf_urls = generate_pdf_urls(base_url, valid_sections)

    # Initialize a list to store all extracted tables
    all_tables = []

    for i, url in enumerate(pdf_urls):
        pdf_path = os.path.join(output_dir, f"section_{i + 1}.pdf")
        
        # Download the PDF
        if download_pdf(url, pdf_path):
            # Extract tables from the PDF
            tables = extract_tables_from_pdf(pdf_path)
            if tables:
                print(f"Extracted {len(tables)} tables from: {pdf_path}")
                all_tables.extend(tables)
            else:
                print(f"No tables found in: {pdf_path}")

    # Combine all tables into a single DataFrame
    if all_tables:
        combined_df = pd.concat([pd.DataFrame(table) for table in all_tables], ignore_index=True)
        combined_csv_path = os.path.join(output_dir, "combined_tables.csv")
        combined_df.to_csv(combined_csv_path, index=False)
        print(f"All tables saved to: {combined_csv_path}")
    else:
        print("No tables were extracted.")

if __name__ == "__main__":
    # Base URL for the PDFs
    base_url = "https://www.customs.govt.nz/globalassets/documents/tariff-documents"

    # Valid sections for each year
    valid_sections = {
        2024: ["i", "iii", "v", "vii", "viii", "ix", "x", "xi", "xii", "xiii", "xiv", "xv", "xvi", "xvii", "xviii", "xix", "xx", "xxi"],
        2025: ["ii", "iv", "vi"]
    }

    # Output directory to save downloaded PDFs and extracted tables
    output_directory = "downloaded_pdfs"

    # Process the PDFs
    process_pdfs(base_url, valid_sections, output_directory)



## Using regex to assign data to appropriate columns(Normal and Prefential Tarriif column)
# Function to check if a value is numeric (int or float)
def is_numeric(value):
    return isinstance(value, (int, float))

# Filter out rows where 'Goods' is numeric
df = pd.read_csv("combined_tables_improved.csv")
df = df[~df["Goods"].apply(is_numeric)]

# Initialize new columns
df["Normal Tariff"] = None
df["Preferential Tariff"] = None

# Function to apply rules
def apply_rules(row):
    goods = row["Goods"]

    # Rule 2: Check for "FreeFree"
    if re.search(r"FreeFree", goods):
        row["Normal Tariff"] = "Free"
        row["Preferential Tariff"] = "Free"
        return row

    # Rule 4: Check for "CA", "LDC", or "RCEP" at the start
    if re.match(r"^(CA|LDC|RCEP| CPT)", goods):
        row["Preferential Tariff"] = goods
        return row

    # Rule 5: Check for "SeeBelow"
    if "SeeBelow" in goods:
        row["Preferential Tariff"] = "SeeBelow"
        return row

    # Rule 7: Check for "Free Free"
    if re.search(r"Free\sFree", goods):
        row["Normal Tariff"] = "Free"
        row["Preferential Tariff"] = "Free"
        return row

    # Rule 1: Check for digits between words
    match = re.search(r"(\d+)(\D+)", goods)
    if match:
        row["Normal Tariff"] = match.group(1)
        row["Preferential Tariff"] = match.group(2)
        return row

    # Rule 3: Check for digits followed by " Free"
    match = re.search(r"(\d+)\sFree", goods)
    if match:
        row["Normal Tariff"] = match.group(1)
        row["Preferential Tariff"] = "Free"
        return row

    # Rule 6: Check for digits followed by "Free"
    match = re.search(r"(\d+)Free", goods)
    if match:
        row["Normal Tariff"] = match.group(1)
        row["Preferential Tariff"] = "Free"
        return row

    # If no rule matches, leave columns as None
    return row

# Apply rules to each row
df = df.apply(apply_rules, axis=1)


### Cleaning Normal and preferential Tarrif columns
# Task 1: Clean Normal Tariff Column
def clean_normal_tariff(value):
    if isinstance(value, (int, float)) and value > 10:
        return ""  # Leave empty if number > 10
    return value  # Keep text or numbers <= 10

df["Normal Tariff"] = df["Normal Tariff"].apply(clean_normal_tariff)

# Task 2: Clean Preferential Tariff Column
def clean_preferential_tariff(value):
    if isinstance(value, str) and re.match(r"^(CA|LDC|RCEP|CPT|Free)", value, re.IGNORECASE):
        return value  # Keep if starts with CA, LDC, RCEP, or CPT
    return ""  # Leave empty otherwise

df["Preferential Tariff"] = df["Preferential Tariff"].apply(clean_preferential_tariff)

# Task 3: Clean Goods Column
def clean_goods(value):
    if isinstance(value, str):
        # Remove standalone 'o', 'g', 'kg'
        value = re.sub(r"\bo\b|\bg\b|\bkg\b", "", value)
        # Remove unnecessary white spaces
        value = re.sub(r"\s+", " ", value).strip()
    return value

df["Goods"] = df["Goods"].apply(clean_goods)

#save to csv
df.to_csv("new_zealand_tarrifs.csv", index=False)
# # Display the cleaned DataFrame