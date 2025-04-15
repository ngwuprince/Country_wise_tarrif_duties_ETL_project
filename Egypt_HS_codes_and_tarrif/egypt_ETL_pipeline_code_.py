import requests
from bs4 import BeautifulSoup
import pandas as pd
import re


def scrape_customs_data():
    base_url = "https://customs.gov.eg/Services/Tarif?page={}&type=1&chapterId={}"
    data = []

    for chapter in range(1, 100):  # Chapters 1-99
        for page in range(1, 100):   # Pages 1-100 (stops if no data)
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
    url = f"http://www.fei.org.eg/tariff/tariff.php?hscode={hscode}&keywords=&submit=#"
    
    try:
        response = requests.get(url, timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Initialize result with default values
        result = {
            'Description': 'N/A',
            'Unit': 'N/A',
            'Custom Fee': 'N/A',
            'VAT': 'N/A',
            'Agreement details': 'N/A'
        }
        
        # SAFE EXTRACTION FUNCTIONS
        def safe_extract(div_class, field_name):
            try:
                div = soup.find('div', class_=div_class)
                if div:
                    strong = div.find('strong', string=lambda t: t and field_name in t)
                    if strong:
                        return strong.parent.get_text(strip=True).replace(field_name, '').strip()
                return 'N/A'
            except:
                return 'N/A'
        
        # EXTRACT DESCRIPTION
        result['Description'] = safe_extract('span12 content', 'Description:')
        
        # EXTRACT ALL SPAN6 CONTENT DIVS
        content_divs = soup.find_all('div', class_='span6 content')
        
        # EXTRACT UNIT
        for div in content_divs:
            strong = div.find('strong', string='Unit: ')
            if strong:
                result['Unit'] = strong.parent.get_text(strip=True).replace('Unit:', '').strip()
                break
        
        # EXTRACT CUSTOM FEE
        for div in content_divs:
            strong = div.find('strong', string='Custom Fee: ')
            if strong:
                result['Custom Fee'] = strong.parent.get_text(strip=True).replace('Custom Fee:', '').strip()
                break
        
        # EXTRACT VAT
        for div in content_divs:
            strong = div.find('strong', string='VAT: ')
            if strong:
                result['VAT'] = strong.parent.get_text(strip=True).replace('VAT:', '').strip()
                break
        
        # EXTRACT AND PROCESS AGREEMENTS
        agreements = []
        try:
            agreements_header = soup.find('p', class_='agree') or soup.find('strong', string='Agreements:')
            if agreements_header:
                agreement_divs = soup.find_all('div', class_='span6')
                current_agreement = None
                
                for div in agreement_divs:
                    # Skip header rows
                    if div.find('strong', string='Agreement') or div.find('strong', string='Rate'):
                        continue
                    
                    text = div.get_text(strip=True)
                    if not text:  # Skip empty divs
                        continue
                        
                    if current_agreement is None:
                        current_agreement = text
                    else:
                        agreements.append(f"{current_agreement}: {text}")
                        current_agreement = None
                
                if agreements:
                    full_agreement_text = " | ".join(agreements)
                    # Keep from 'Egypt' to end of string
                    egypt_index = full_agreement_text.find('Egypt')
                    result['Agreement details'] = full_agreement_text[egypt_index:] if egypt_index != -1 else full_agreement_text
        except Exception as e:
            print(f"Agreement extraction error for {hscode}: {str(e)}")
        
        return result
    
    except Exception as e:
        print(f"Request error for HS Code {hscode}: {str(e)}")
        return {k: 'ERROR' for k in result.keys()}

# FULL PROCESSING
print("\nStarting full scraping...")
fei_data = []
for i, hscode in enumerate(df_customs['Item'].unique()):
    if i % 10 == 0:
        print(f"Processed {i} HS Codes...")
    fei_data.append(scrape_fei_data(hscode))

# CREATE FINAL DATAFRAME
df_fei = pd.DataFrame(fei_data)
df_final = pd.concat([df_customs, df_fei], axis=1)

# ADD QUOTES AROUND ITEM VALUES TO PRESERVE LEADING ZEROS
df_final['Item'] = df_final['Item'].apply(lambda x: f'"{x}"')

# SAVE RESULTS
df_final.to_csv('egyptian_tariff_data_.csv', index=False, quoting=1)
print("\nScraping complete! Data saved with quoted Item values.")

# Display sample of results
print("\nSample of extracted data:")
df_final.head()