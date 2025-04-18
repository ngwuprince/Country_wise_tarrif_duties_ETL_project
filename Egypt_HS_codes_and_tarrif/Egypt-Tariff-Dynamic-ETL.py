import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

def scrape_customs_data():
    base_url = "https://customs.gov.eg/Services/Tarif?page={}&type=1&chapterId={}"
    data = []

    for chapter in range(1, 4):  # Chapters 1-99
        for page in range(1, 10):   # Pages 1-100 (stops if no data)
            url = base_url.format(page, chapter)
            response = requests.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')

            # Find the table with tariff data
            table = soup.find('table', {'class': 'table'})
            if not table:
                break  # No table found, exit page loop

            rows = table.find_all('tr')[1:]  # Skip header row
            if not rows:
                break  # No data rows, exit page loop

            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 2:  # Ensure 'Text of the item' and 'Item' exist
                    #text_of_item = cols[1].get_text(strip=True)
                    item = cols[0].get_text(strip=True).replace('/', '')

                    if item:#text_of_item or item  # Only append if data exists
                        data.append({
                            'Chapter': chapter,
                            'Page': page,
                            #'Text of the item': text_of_item,
                            'Item': item
                        })
            print(f"Chapter {chapter}, Page {page} - Scraped {len(rows)} items")

    return pd.DataFrame(data)

# Run the scraper
df_customs = scrape_customs_data()
df_customs.head()



def scrape_fei_data(hscode):
    # First try with 6-digit code
    hscode_6digit = str(hscode)[:6] if len(str(hscode)) >= 6 else str(hscode)
    result_6digit = scrape_fei_with_code(hscode, hscode_6digit)

    # Check conditions for retrying with full code:
    # 1. If all fields are N/A (original condition)
    all_na = all(val == 'N/A' for key, val in result_6digit.items()
                if key not in ['HS_6digit', 'Original_HS_Code'])

    # 2. New condition: If VAT is N/A but other fields have values
    vat_na_but_others_have_data = (
        result_6digit['VAT'] == 'N/A' and
        any(val != 'N/A' for key, val in result_6digit.items()
            if key not in ['HS_6digit', 'Original_HS_Code', 'VAT'])
    )

    # If either condition is met and code is longer than 6 digits, try with full code
    if len(str(hscode)) > 6 and (all_na or vat_na_but_others_have_data):
        print(f"Retrying with full code {hscode} (condition: {'all N/A' if all_na else 'VAT N/A with other data'})")
        result_full = scrape_fei_with_code(hscode, hscode)

        # Only use full code results if we got something better
        if not all(val == 'N/A' for key, val in result_full.items()
                  if key not in ['HS_6digit', 'Original_HS_Code']):
            return result_full
        # If full code returned all N/A, revert to 6-digit results
        else:
            print(f"Full code returned all N/A, reverting to 6-digit results")
            return result_6digit

    return result_6digit

def scrape_fei_with_code(original_hscode, lookup_code):
    url = f"http://www.fei.org.eg/tariff/tariff.php?hscode={lookup_code}&keywords=&submit=#"

    try:
        response = requests.get(url, timeout=20)
        soup = BeautifulSoup(response.text, 'html.parser')

        result = {
            'Description': 'N/A',
            'Unit': 'N/A',
            'Custom Fee': 'N/A',
            'VAT': 'N/A',
            'Product Type': 'N/A',
            'Agreement details': 'N/A',
            'HS_6digit': lookup_code[:6] if len(lookup_code) >= 6 else lookup_code,
            'Original_HS_Code': original_hscode
        }

        # EXTRACT DESCRIPTION
        desc_div = soup.find('div', class_='span12 content')
        if desc_div:
            desc_strong = desc_div.find('strong', string='Description: ')
            if desc_strong:
                # Get all text after the Description strong tag
                desc_text = ''.join(desc_strong.find_next_siblings(string=True)).strip()
                result['Description'] = desc_text

        # EXTRACT ALL SPAN6 CONTENT DIVS
        content_divs = soup.find_all('div', class_='span6 content')
        for div in content_divs:
            strong_tag = div.find('strong')
            if strong_tag:
                key = strong_tag.get_text(strip=True).replace(':', '')
                value = ''.join(strong_tag.find_next_siblings(string=True)).strip()

                if key == 'Unit':
                    result['Unit'] = value
                elif key == 'Custom Fee':
                    result['Custom Fee'] = value
                elif key == 'VAT':
                    result['VAT'] = value
                elif key == 'Product Type':
                    result['Product Type'] = value

        # EXTRACT AGREEMENTS
        agreements = []
        try:
            agreements_header = soup.find('p', class_='agree') or soup.find('strong', string='Agreements:')
            if agreements_header:
                agreements_table = agreements_header.find_next('div', class_='row deals')
                if agreements_table:
                    agreement_divs = agreements_table.find_all('div', class_='span6')
                    for i in range(2, len(agreement_divs), 2):
                        if i + 1 < len(agreement_divs):
                            agreement = agreement_divs[i].get_text(strip=True)
                            rate = agreement_divs[i+1].get_text(strip=True)
                            agreements.append(f"{agreement}: {rate}")
                    if agreements:
                        result['Agreement details'] = " | ".join(agreements)
        except Exception as e:
            print(f"Agreement error for {lookup_code}: {str(e)}")

        return result

    except requests.exceptions.RequestException as e:
        print(f"Request failed for {lookup_code}: {str(e)}")
        return {
            'Description': 'ERROR',
            'Unit': 'ERROR',
            'Custom Fee': 'ERROR',
            'VAT': 'ERROR',
            'Product Type': 'ERROR',
            'Agreement details': 'ERROR',
            'HS_6digit': lookup_code[:6] if len(lookup_code) >= 6 else lookup_code,
            'Original_HS_Code': original_hscode
        }

# Assuming df_customs is your input DataFrame containing HS codes in 'Item' column
# FULL PROCESSING
print("\nStarting full scraping with fallback to 10-digit...")
fei_data = []

for i, hscode in enumerate(df_customs['Item'].unique()):
    if i % 10 == 0:
        print(f"Processing {i} of {len(df_customs['Item'].unique())} HS Codes...")
    fei_data.append(scrape_fei_data(hscode))

# CREATE FINAL DATAFRAME
df_fei = pd.DataFrame(fei_data)

# Merge with original data
df_final = pd.merge(
    df_customs,
    df_fei,
    left_on='Item',
    right_on='Original_HS_Code',
    how='left'
).drop(['Original_HS_Code', 'HS_6digit'], axis=1)

# Add quotes around Item values
df_final['Item'] = df_final['Item'].apply(lambda x: f'"{x}"')

# SAVE RESULTS
df_final.to_csv('egyptian_tariff_data_with_fallb.csv', index=False, quoting=1)
print("\nScraping complete with fallback logic!")

# Display sample
print("\nSample data:")
df_final.head()