# -*- coding: utf-8 -*-
"""VAT_auto_scraper.ipynb

Original file is located at
    https://colab.research.google.com/drive/1LXG71q8ptzfoGsl7ALfENWQG7KPx2GSS
"""

import requests
from bs4 import BeautifulSoup
import re
from time import sleep
import pandas as pd


url = "https://www.transcustoms.com/HS_tree.htm"

# Fetch the webpage
response = requests.get(url)
response.raise_for_status()  # Check for HTTP errors

# Parse HTML
soup = BeautifulSoup(response.text, 'html.parser')

# Extract all text from the page
text = soup.get_text()

# Use regex to find all "Heading XXXX" patterns
hs_headings = re.findall(r'Heading (\d{4}):', text)

# Remove duplicates (if any) and sort
unique_hs_headings = sorted(list(set(hs_headings)))

print(f"Extracted {len(unique_hs_headings)} unique HS Headings:", unique_hs_headings)



def scrape_hs_codes_for_heading(heading, max_pages=4):
    """Scrape HS codes for a given heading across paginated results (pages 0-4)."""
    all_hs_codes = set()

    for page in range(max_pages):
        url = f"https://www.transcustoms.com/Hscode/HScode_search.asp?word={heading}&selectT=&page={page}"
        headers = {"User-Agent": "Mozilla/5.0"}

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            page_text = soup.get_text()

            # Extract 10-digit HS codes
            hs_codes = re.findall(r'\b\d{10}\b', page_text)

            if not hs_codes:  # Stop if no codes found on this page
                break

            all_hs_codes.update(hs_codes)
            #sleep(1)  # Respectful delay

        except Exception as e:
            print(f"Error scraping {heading} (page {page}): {e}")
            break

    return sorted(all_hs_codes)

# Scrape all HS codes for each heading
all_hs_codes = []
for heading in unique_hs_headings:
    codes = scrape_hs_codes_for_heading(heading)
    print(f"Heading {heading}: Found {len(codes)} HS codes")
    all_hs_codes.extend(codes)

# Remove duplicates (optional, if headings overlap)
final_hs_codes = sorted(list(set(all_hs_codes)))
print("\nTotal unique HS codes:", len(final_hs_codes))



def fetch_vat_rate(hs_code):
    """Fetch VAT rate for a single HS code."""
    url = f"https://www.transcustoms.com/China_HS_Code/China_Tariff.asp?HS_Code={hs_code}"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        vat_label = soup.find("td", text="Import VAT (Value-Added Tax)")
        if vat_label:
            return vat_label.find_next("td").text.strip()
        return "Not Found"

    except Exception as e:
        return f"Error: {e}"

# Scrape VAT rates for all HS codes
results = []
for i, code in enumerate(final_hs_codes):
    vat_rate = fetch_vat_rate(code)
    results.append({"HS_Code": code, "VAT_Rate": vat_rate})
    print(f"Processed {i+1}/{len(final_hs_codes)}: {code} → {vat_rate}")
    #sleep(1)  # Avoid rate-limiting

# Export to CSV
df = pd.DataFrame(results)
df.to_csv("hs_codes_vat_rates.csv", index=False)
print("Data saved to 'hs_codes_vat_rates.csv'")

