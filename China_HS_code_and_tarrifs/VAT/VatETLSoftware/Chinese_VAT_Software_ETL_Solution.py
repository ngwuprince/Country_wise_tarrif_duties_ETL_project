#!pip install playwright
#!playwright install

#ETL software developed by Ogbonna Prince(ngwuogbonnaprince@gmail.com)

#!/usr/bin/env python3
import pandas as pd
import re
import time
import os
import json
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import asyncio
import nest_asyncio
from pathlib import Path

# Apply nest_asyncio to allow running async code in environments with existing event loops
nest_asyncio.apply()

# Constants
RESULTS_FILE = "tax_id_VAT.json"
LOG_FILE = "scrape_progress.log"

def setup_logging():
    """Set up logging to file"""
    with open(LOG_FILE, 'a') as f:
        f.write(f"\n\n=== New Run Started at {time.ctime()} ===\n")

def log_message(message):
    """Log a message with timestamp"""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}\n"
    print(log_entry.strip())
    with open(LOG_FILE, 'a') as f:
        f.write(log_entry)

def load_existing_results():
    """Load existing results from file if it exists"""
    if os.path.exists(RESULTS_FILE):
        try:
            with open(RESULTS_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            log_message(f"Error loading existing results: {e}")
    return {}

def save_results(results):
    """Save results to file"""
    try:
        with open(RESULTS_FILE, 'w') as f:
            json.dump(results, f, indent=2)
    except Exception as e:
        log_message(f"Error saving results: {e}")

def extract_tax_id(hs_code):
    if not isinstance(hs_code, str):
        return None
    match = re.match(r'^"(\d+)\.(\d+)"$', hs_code)
    if match:
        digits = match.group(1) + match.group(2)
        if len(digits) >= 4:
            return digits
    return None

async def scrape_vat_rates_from_detail_page(page, tax_id, debug_mode=False):
    vat_rate = "N/A"
    detail_page_html_path = f'detail_page_{tax_id}.html'
    screenshot_path = f'error_{tax_id}.png'

    try:
        log_message(f"Processing VAT rate for Tax ID: {tax_id}")

        # VAT Extraction Logic
        try:
            expand_link_selector = "a:has-text('进口消费税税率、增值税税率')"
            expand_link = page.locator(expand_link_selector).first
            await expand_link.wait_for(state="visible", timeout=15000)
            log_message("Found VAT section link. Clicking to expand/load...")
            await expand_link.click()

            vat_table_content_selector = "table.showTable tbody#add tr"
            await page.locator(vat_table_content_selector).first.wait_for(state="visible", timeout=20000)
            log_message("VAT table content appeared.")

            vat_rate_selector = f"{vat_table_content_selector} td:nth-child(2)"
            vat_cell = page.locator(vat_rate_selector).first
            vat_rate = await vat_cell.text_content()
            vat_rate = vat_rate.strip() if vat_rate else "N/A"
            log_message(f"Found VAT Rate: {vat_rate}")

        except PlaywrightTimeoutError as e_vat_extract:
            log_message(f"Error during VAT extraction on detail page: {e_vat_extract}")
            vat_rate = "Error: Rate extraction failed"
            try:
                await page.screenshot(path=screenshot_path)
                log_message(f"Saved error screenshot to {screenshot_path}")
            except Exception as screenshot_error:
                log_message(f"Could not save screenshot: {screenshot_error}")

    except Exception as e:
        log_message(f"Error processing Tax ID {tax_id}: {e}")
        try:
            if not os.path.exists(f'error_detail_extract_{tax_id}.png'):
                await page.screenshot(path=screenshot_path)
                log_message(f"Screenshot saved to {screenshot_path}")
        except Exception as screenshot_error:
            log_message(f"Could not save screenshot: {screenshot_error}")
        if vat_rate == "N/A":
            vat_rate = "Error"

    return vat_rate

async def process_results_table(page, original_tax_id, debug_mode=False):
    results = {}
    results_table_selector = "div#result[style*='display: block'] table#listData"
    existing_results = load_existing_results()  # Load existing results before processing

    try:
        # Get all rows in the results table
        rows = await page.locator(f"{results_table_selector} tbody tr").count()
        log_message(f"Found {rows} tax IDs in results table")

        for i in range(rows):
            tax_id = None
            try:
                # Get the tax ID from each row (second column)
                tax_id_cell = page.locator(f"{results_table_selector} tbody tr:nth-child({i+1}) td:nth-child(2)")
                tax_id = await tax_id_cell.text_content()
                tax_id = tax_id.strip() if tax_id else None

                # Validate tax ID format before processing
                if not tax_id or not tax_id.isdigit() or len(tax_id) < 4:
                    log_message(f"Invalid tax ID format: {tax_id} - Skipping")
                    continue

                # Skip if already successfully processed
                if tax_id in existing_results and existing_results[tax_id] not in ["Error", "No data found"]:
                    log_message(f"Tax ID {tax_id} already processed - Skipping")
                    continue

                log_message(f"Processing result #{i+1}: {tax_id}")

                # Get the detail link for this row
                link_anchor_selector = f"{results_table_selector} tbody tr:nth-child({i+1}) td a.showDetailA"
                link_locator = page.locator(link_anchor_selector).first
                await link_locator.wait_for(state="visible", timeout=10000)
                name_attribute = await link_locator.get_attribute("name")

                if name_attribute:
                    log_message(f"Found name attribute for {tax_id}")
                    base_detail_url = "https://online.customs.gov.cn/ociswebserver/pages/jckspsl/detail.html"
                    detail_url = f"{base_detail_url}?id={name_attribute}"
                    log_message(f"Navigating to detail URL for {tax_id}")

                    # Open detail page in new tab
                    context = page.context
                    new_page = await context.new_page()
                    await new_page.goto(detail_url, wait_until="networkidle", timeout=60000)

                    # Scrape VAT rate from detail page
                    vat_rate = await scrape_vat_rates_from_detail_page(new_page, tax_id, debug_mode)

                    # Only update results if we got a valid VAT rate
                    if vat_rate not in ["N/A", "Error"]:
                        results[tax_id] = vat_rate
                        existing_results[tax_id] = vat_rate  # Update in-memory cache
                        save_results(existing_results)  # Immediate save
                    else:
                        log_message(f"Invalid VAT rate for {tax_id}: {vat_rate}")

                    # Close the detail tab
                    await new_page.close()
                else:
                    log_message(f"No detail link found for {tax_id}")
                    results[tax_id] = "Error: No detail link"

            except Exception as e:
                log_message(f"Error processing row {i+1}: {e}")
                if tax_id:
                    results[tax_id] = f"Error: {str(e)}"
                    # Mark as error in results but don't save to prevent pollution
                    # We'll retry these on next run

    except Exception as e:
        log_message(f"Error processing results table: {e}")
        # Only mark original tax ID as error if we didn't process any results
        if not results:
            results[original_tax_id] = f"Error: Results table processing failed - {str(e)}"

    return results

async def scrape_vat_rates(page, tax_id, debug_mode=False):
    results_page_html_path = f'results_page_{tax_id}.html'
    screenshot_path = f'error_{tax_id}.png'
    results = {}

    try:
        log_message(f"Querying Tax ID: {tax_id}")
        await page.goto("https://online.customs.gov.cn/ociswebserver/pages/jckspsl/", wait_until="networkidle", timeout=60000)

        # Input Tax ID
        tax_id_input_selector = "input#dutySpa"
        await page.locator(tax_id_input_selector).wait_for(state="visible", timeout=15000)
        await page.locator(tax_id_input_selector).fill(tax_id)
        log_message(f"Filled Tax ID: {tax_id}")

        # Click Query button
        query_button_selector = "button#queryBtn"
        query_button = page.locator(query_button_selector)
        await query_button.wait_for(state="visible", timeout=10000)
        if not await query_button.is_disabled(timeout=5000):
            await query_button.click()
            log_message("Clicked Query button")
        else:
            raise PlaywrightTimeoutError("Query button was visible but disabled")

        # Wait for results
        results_table_selector = "div#result[style*='display: block'] table#listData"
        no_results_table_selector = "div#noResult[style*='display: block']"
        combined_wait_selector = f"{results_table_selector}, {no_results_table_selector}"
        await page.wait_for_selector(combined_wait_selector, state="visible", timeout=25000)
        log_message("Results appeared")

        # Check if no results
        if await page.locator(no_results_table_selector).is_visible():
            log_message(f"No data found for Tax ID {tax_id}")
            results[tax_id] = "No data found"
        elif await page.locator(f"{results_table_selector} div.ant-empty-description:has-text('暂无数据')").is_visible(timeout=1000):
            log_message(f"No data found for Tax ID {tax_id}")
            results[tax_id] = "No data found"
        elif await page.locator(results_table_selector).is_visible():
            log_message("Results table found, processing...")
            results = await process_results_table(page, tax_id, debug_mode)
        else:
            raise Exception("Could not determine results status")

    except Exception as e:
        log_message(f"Error processing Tax ID {tax_id}: {e}")
        try:
            await page.screenshot(path=screenshot_path)
            log_message(f"Screenshot saved to {screenshot_path}")
        except Exception as screenshot_error:
            log_message(f"Could not save screenshot: {screenshot_error}")
        results[tax_id] = "Error: Initial query failed"

    return results

async def process_tax_ids(valid_tax_ids, run_in_debug_mode=False):
    all_results = load_existing_results()
    processed_ids = set(all_results.keys())
    remaining_ids = [tid for tid in valid_tax_ids if tid not in processed_ids]

    log_message(f"Resuming scraping. Already processed: {len(processed_ids)}, Remaining: {len(remaining_ids)}")

    async with async_playwright() as p:
        browser = None
        try:
            browser = await p.chromium.launch(headless=True)
            log_message("Launched Chromium browser")
        except Exception as e_chromium:
            log_message(f"Failed to launch Chromium: {e_chromium}. Trying Firefox.")
            try:
                browser = await p.firefox.launch(headless=True)
                log_message("Launched Firefox browser")
            except Exception as e_firefox:
                log_message(f"Failed to launch Firefox: {e_firefox}. Exiting.")
                return all_results

        if browser:
            context = await browser.new_context()
            page = await context.new_page()

            for i, tax_id in enumerate(remaining_ids, 1):
                try:
                    log_message(f"Processing {i}/{len(remaining_ids)}: {tax_id}")
                    results = await scrape_vat_rates(page, tax_id, run_in_debug_mode)
                    all_results.update(results)

                    # Save progress after each tax ID
                    save_results(all_results)

                    # Add delay between requests
                    await asyncio.sleep(2)

                except Exception as e:
                    log_message(f"Fatal error processing {tax_id}: {e}")
                    # Save what we have before exiting
                    save_results(all_results)
                    break

            await context.close()
            await browser.close()
            log_message("Browser closed.")
        else:
            log_message("Failed to launch any browser.")

    return all_results

def run_async_code(valid_tax_ids, run_in_debug_mode=False):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop:
        log_message("Running in existing event loop")
        return loop.run_until_complete(process_tax_ids(valid_tax_ids, run_in_debug_mode))
    else:
        log_message("Running in new event loop")
        return asyncio.run(process_tax_ids(valid_tax_ids, run_in_debug_mode))

if __name__ == "__main__":
    setup_logging()

    input_csv_path = "clean_V7_1_Rate_without_footnotes.csv"
    output_csv_path = "clean_V7_1_1_with_vat.csv"

    try:
        df = pd.read_csv(input_csv_path)
        log_message(f"Successfully loaded CSV: {input_csv_path}")
    except Exception as e:
        log_message(f"Error loading CSV {input_csv_path}: {e}")
        exit()

    # Extract all unique tax IDs from the input file
    df['Tax_ID'] = df['HS_CODE'].apply(extract_tax_id)
    input_tax_ids = df['Tax_ID'].dropna().unique().tolist()
    log_message(f"Found {len(input_tax_ids)} unique input Tax IDs to process.")

    run_in_debug_mode = False

    # Run the scraping process
    vat_rates_map = run_async_code(input_tax_ids, run_in_debug_mode)

    log_message("\n--- Final Results ---")
    for tax_id, rate in vat_rates_map.items():
        log_message(f"Tax ID: {tax_id}, VAT Rate: {rate}")
    log_message("-------------------\n")

    # Create a new DataFrame with all discovered tax IDs and their VAT rates
    results_df = pd.DataFrame({
        'Tax_ID': list(vat_rates_map.keys()),
        'VAT_Rate': list(vat_rates_map.values())
    })

    # Save the results to CSV
    try:
        results_df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')
        log_message(f"Successfully saved results to: {output_csv_path}")
    except Exception as e:
        log_message(f"Error saving results CSV {output_csv_path}: {e}")

    log_message("Script finished.")