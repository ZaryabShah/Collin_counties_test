# scrape_collin_foreclosures_proxy_fast.py
import csv, json, re, time
from typing import Optional, Dict, Any

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

URL   = "https://apps2.collincountytx.gov/ForeclosureNotices/"
PROXY = "192.151.147.90:17093"  # your residential proxy

def make_driver(headless=True):
    from selenium.webdriver.chrome.options import Options
    opts = Options()
    # if headless:
    #     opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1440,1000")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--ignore-certificate-errors")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--enable-unsafe-swiftshader")  # hush WebGL warnings
    opts.add_argument(f"--proxy-server=http://{PROXY}")
    drv = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    drv.set_page_load_timeout(60)
    drv.set_script_timeout(15)
    return drv

HOOK_JS = r"""
// install once
if (!window.__captured_urls__) {
  window.__captured_urls__ = [];
  (function() {
    const _push = history.pushState;
    const _rep  = history.replaceState;
    history.pushState = function(s,t,u){
      try { window.__captured_urls__.push(String(u||"")); } catch(e){}
      return _push.apply(this, arguments);
    };
    history.replaceState = function(s,t,u){
      try { if (u) window.__captured_urls__.push(String(u)); } catch(e){}
      return _rep.apply(this, arguments);
    };
  })();
}
"""

def wait_for_table(driver, timeout=40):
    WebDriverWait(driver, timeout).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table.mud-table-root tbody tr"))
    )
    driver.execute_script("return new Promise(r=>requestAnimationFrame(()=>requestAnimationFrame(()=>r())));")

def current_page_badge(driver) -> str:
    return driver.execute_script(
        "const b=document.querySelector('button[aria-current=\"page\"]'); return b?b.textContent.trim():'';"
    ) or ""

def row_count(driver) -> int:
    return int(driver.execute_script("return document.querySelectorAll('table.mud-table-root tbody tr').length || 0;"))

def snapshot_row(driver, index:int) -> Optional[Dict[str,Any]]:
    return driver.execute_script(r"""
      const trs = document.querySelectorAll('table.mud-table-root tbody tr');
      const tr = trs[arguments[0]];
      if (!tr) return null;
      const info = {};
      tr.querySelectorAll('span.list-subheader').forEach(span=>{
        const label = span.textContent.trim().replace(/:$/,'');
        const val = (span.parentElement?.textContent||'').replace(span.textContent,'').trim();
        info[label]=val;
      });
      const address = (tr.querySelector('.list-header')?.textContent||'').trim();
      return {
        address,
        city: info["City"]||"",
        sale_date: info["Sale Date"]||"",
        file_date: info["File Date"]||"",
        property_type: info["Property Type"]||info["Type"]||""
      };
    """, index)

def scrape_detail_page_in_new_tab(driver, index: int, timeout=15):
    """Open detail page in new tab, scrape all data including HTML, then close tab."""
    # Click row to open in new tab (website automatically opens in new tab)
    clicked = driver.execute_script("""
      const trs = document.querySelectorAll('table.mud-table-root tbody tr');
      const tr = trs[arguments[0]];
      if (!tr) return false;
      const target = tr.querySelector('.list-header') || tr.querySelector('td') || tr;
      target.scrollIntoView({block:'center'});
      
      // Simple click - website opens in new tab automatically
      target.click();
      return true;
    """, index)
    
    if not clicked:
        return None
    
    # Wait a moment for new tab to open
    time.sleep(1)
    
    # Switch to the new tab (should be the last one)
    original_window = driver.current_window_handle
    all_windows = driver.window_handles
    
    if len(all_windows) <= 1:
        # If new tab didn't open, try regular click and get URL
        driver.execute_script("""
          const trs = document.querySelectorAll('table.mud-table-root tbody tr');
          const tr = trs[arguments[0]];
          if (tr) {
            const target = tr.querySelector('.list-header') || tr.querySelector('td') || tr;
            target.click();
          }
        """, index)
        
        # Wait for navigation
        try:
            WebDriverWait(driver, timeout).until(lambda d: "/DetailPage/" in d.current_url)
        except TimeoutException:
            return None
            
        detail_data = scrape_current_detail_page(driver)
        
        # Go back to list
        driver.execute_script("history.back();")
        try:
            WebDriverWait(driver, timeout).until(lambda d: "/DetailPage/" not in d.current_url)
            wait_for_table(driver, timeout=20)
        except TimeoutException:
            pass
            
        return detail_data
    
    # Switch to new tab
    new_window = [w for w in all_windows if w != original_window][-1]
    driver.switch_to.window(new_window)
    
    try:
        # Wait for detail page to load
        WebDriverWait(driver, timeout).until(lambda d: "/DetailPage/" in d.current_url)
        
        # Scrape detail page data
        detail_data = scrape_current_detail_page(driver)
        
    except TimeoutException:
        detail_data = None
    finally:
        # Close current tab and switch back to original
        driver.close()
        driver.switch_to.window(original_window)
    
    return detail_data

def scrape_current_detail_page(driver):
    """Scrape all data from the current detail page."""
    try:
        # Wait for page to load
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Get basic info
        url = driver.current_url
        detail_id_match = re.search(r"/DetailPage/(\d+)", url)
        detail_id = detail_id_match.group(1) if detail_id_match else None
        
        # Get HTML content
        html_content = driver.page_source
        
        # Extract structured data from the page
        detail_info = driver.execute_script("""
            const data = {};
            
            // Function to extract field values based on field labels
            function extractFieldValue(label) {
                const titleElements = document.querySelectorAll('.item-title');
                for (let titleEl of titleElements) {
                    if (titleEl.textContent.trim() === label) {
                        const gridItem = titleEl.closest('.mud-grid-item');
                        if (gridItem && gridItem.nextElementSibling) {
                            const valueEl = gridItem.nextElementSibling.querySelector('p');
                            if (valueEl) {
                                return valueEl.textContent.trim();
                            }
                        }
                    }
                }
                return null;
            }
            
            // Function to extract multiple paragraphs from a field
            function extractFieldValues(label) {
                const titleElements = document.querySelectorAll('.item-title');
                for (let titleEl of titleElements) {
                    if (titleEl.textContent.trim() === label) {
                        const gridItem = titleEl.closest('.mud-grid-item');
                        if (gridItem && gridItem.nextElementSibling) {
                            const valueEls = gridItem.nextElementSibling.querySelectorAll('p');
                            if (valueEls.length > 0) {
                                return Array.from(valueEls).map(el => el.textContent.trim());
                            }
                        }
                    }
                }
                return [];
            }
            
            // Extract basic property information
            data.full_address = extractFieldValue('Address');
            data.property_type = extractFieldValue('Type');
            data.mapsco = extractFieldValue('Mapsco');
            data.deed_date = extractFieldValue('Deed Date');
            data.deed_number = extractFieldValue('Deed Number');
            data.last_updated = extractFieldValue('Last Updated');
            
            // Extract owner information
            data.owner_name = extractFieldValue('Name');
            const ownerAddressLines = extractFieldValues('Address');
            if (ownerAddressLines.length >= 2) {
                data.owner_street_address = ownerAddressLines[0];
                data.owner_city_state_zip = ownerAddressLines[1];
            } else if (ownerAddressLines.length === 1) {
                data.owner_address = ownerAddressLines[0];
            }
            
            // Extract tax information
            data.appraised_value_2025 = extractFieldValue('Appraised Value (2025)');
            data.appraised_value_2026 = extractFieldValue('Appraised Value (2026)');
            
            // Extract main address from h1
            const mainAddressEl = document.querySelector('h1');
            if (mainAddressEl) {
                data.main_address = mainAddressEl.textContent.trim();
            }
            
            // Extract external links
            const links = [];
            const linkEls = document.querySelectorAll('a[href]');
            linkEls.forEach(linkEl => {
                const href = linkEl.getAttribute('href');
                const text = linkEl.textContent.trim();
                if (href && text) {
                    links.push({
                        url: href,
                        text: text
                    });
                }
            });
            data.external_links = links;
            
            // Find PDF download link
            const pdfButton = document.querySelector('button .mud-button-label');
            if (pdfButton && pdfButton.textContent.includes('Download PDF')) {
                data.has_pdf_download = true;
            }
            
            // Extract deed search URL
            const deedSearchLink = document.querySelector('a[href*="publicsearch.us"]');
            if (deedSearchLink) {
                data.deed_search_url = deedSearchLink.getAttribute('href');
            }
            
            // Extract appraisal district URL
            const appraisalLink = document.querySelector('a[href*="collincad.org"]');
            if (appraisalLink) {
                data.appraisal_url = appraisalLink.getAttribute('href');
            }
            
            // Parse owner name into first and last names
            if (data.owner_name) {
                const nameParts = data.owner_name.replace(/&/g, 'and').split(/\s+/);
                if (nameParts.length >= 2) {
                    // Simple parsing - first word is first name, last word is last name
                    data.owner_1_first_name = nameParts[0];
                    data.owner_1_last_name = nameParts[nameParts.length - 1];
                    
                    // If there are more than 2 words, check for "and" indicating second owner
                    const nameStr = data.owner_name.toLowerCase();
                    if (nameStr.includes(' and ') || nameStr.includes(' & ')) {
                        const parts = data.owner_name.split(/\s+(?:and|&)\s+/i);
                        if (parts.length >= 2) {
                            const owner1Parts = parts[0].trim().split(/\s+/);
                            const owner2Parts = parts[1].trim().split(/\s+/);
                            
                            data.owner_1_first_name = owner1Parts[0] || '';
                            data.owner_1_last_name = owner1Parts[owner1Parts.length - 1] || '';
                            data.owner_2_first_name = owner2Parts[0] || '';
                            data.owner_2_last_name = owner2Parts[owner2Parts.length - 1] || '';
                        }
                    }
                }
            }
            
            // Parse address components
            if (data.main_address) {
                const lines = data.main_address.split('\\n').map(line => line.trim()).filter(line => line);
                if (lines.length >= 2) {
                    data.street_address = lines[0];
                    const cityStateZip = lines[1];
                    
                    // Parse "City, ST ZIP"
                    const match = cityStateZip.match(/^(.+),\\s*([A-Z]{2})\\s+(\\d{5}(?:-\\d{4})?)$/);
                    if (match) {
                        data.city = match[1].trim();
                        data.state = match[2];
                        data.zip = match[3];
                    }
                }
            }
            
            return {
                extracted_data: data,
                title: document.title || '',
                url: window.location.href
            };
        """)
        
        extracted_data = detail_info.get("extracted_data", {})
        
        return {
            "detail_id": detail_id,
            "detail_url": url,
            "html_content": html_content,
            "page_title": detail_info.get("title", ""),
            "scrape_timestamp": time.time(),
            
            # Main fields for your spreadsheet
            "full_address": extracted_data.get("full_address", ""),
            "county": "Collin",  # This is always Collin County
            "list_name": "",  # Not sure what this field represents
            "street_address": extracted_data.get("street_address", ""),
            "city": extracted_data.get("city", ""),
            "state": extracted_data.get("state", ""),
            "zip": extracted_data.get("zip", ""),
            "owner_1_first_name": extracted_data.get("owner_1_first_name", ""),
            "owner_1_last_name": extracted_data.get("owner_1_last_name", ""),
            "owner_2_first_name": extracted_data.get("owner_2_first_name", ""),
            "owner_2_last_name": extracted_data.get("owner_2_last_name", ""),
            # sale_date and sale_time will come from list data
            "recorded_date": extracted_data.get("deed_date", ""),
            "recorded_time": "",  # Time not available on this page
            "document_id": extracted_data.get("deed_number", ""),
            "document_type": "Deed of Trust",  # Assumed based on foreclosure context
            "legal_description": "",  # Will need to extract from foreclosure notice
            "url_to_lead": url,  # The detail page URL
            "pdf_url": "",  # Will need to extract PDF download URL
            "deed_of_trust_number": extracted_data.get("deed_number", ""),
            
            # Additional extracted data
            "owner_name": extracted_data.get("owner_name", ""),
            "property_type": extracted_data.get("property_type", ""),
            "deed_date": extracted_data.get("deed_date", ""),
            "deed_number": extracted_data.get("deed_number", ""),
            "appraised_value_2025": extracted_data.get("appraised_value_2025", ""),
            "appraisal_url": extracted_data.get("appraisal_url", ""),
            "deed_search_url": extracted_data.get("deed_search_url", ""),
            "external_links": extracted_data.get("external_links", [])
        }
        
    except Exception as e:
        print(f"Error scraping detail page: {e}")
        return {
            "detail_id": detail_id if 'detail_id' in locals() else None,
            "detail_url": driver.current_url if driver else None,
            "html_content": None,
            "error": str(e),
            "scrape_timestamp": time.time()
        }

def click_next_page(driver, timeout=25) -> bool:
    before = current_page_badge(driver)
    first_txt = driver.execute_script("const tr=document.querySelector('table.mud-table-root tbody tr');return tr?tr.textContent:'';")

    can = driver.execute_script("""
      const next=document.querySelector('button[aria-label="Next page"]');
      if(!next || next.disabled) return false;
      next.scrollIntoView({block:'center'}); next.click(); return true;
    """)
    if not can: return False

    def changed(drv):
        try:
            if current_page_badge(drv) != before: return True
            const = drv.execute_script("const tr=document.querySelector('table.mud-table-root tbody tr');return tr?tr.textContent:'';")
            return const != first_txt
        except Exception:
            return False
    WebDriverWait(driver, timeout).until(changed)
    return True

def scrape_all(max_pages: int = 20, max_listings: int = None, stop_after_first_page: bool = False):
    """
    Scrape foreclosure data with flexible stopping conditions.
    
    Args:
        max_pages: Maximum number of pages to scrape (default: 20)
        max_listings: Maximum number of listings to scrape (default: None, no limit)
        stop_after_first_page: If True, stop after completing the first page (default: False)
    """
    drv = make_driver(headless=True)
    drv.get(URL)
    wait_for_table(drv)
    drv.execute_script(HOOK_JS)  # install pushState hook once

    all_rows = []
    seen = set()
    pages_done = 0

    while pages_done < max_pages:
        badge = current_page_badge(drv) or "?"
        if badge in seen: break
        seen.add(badge); pages_done += 1
        print(f"Scraping list page {badge} …")

        n = row_count(drv)
        for i in range(n):
            snap = snapshot_row(drv, i)
            if not snap or not snap.get("address"): continue

            # Scrape detail page in new tab
            detail_data = scrape_detail_page_in_new_tab(drv, i)
            if detail_data:
                # Preserve sale_date from list data and merge with detail data
                list_sale_date = snap.get("sale_date", "")
                list_file_date = snap.get("file_date", "")
                snap.update(detail_data)
                # Ensure sale_date from list is preserved
                snap["sale_date"] = list_sale_date
                snap["file_date"] = list_file_date
            
            all_rows.append(snap)
            
            # Check if we've reached the maximum number of listings
            if max_listings and len(all_rows) >= max_listings:
                print(f"Reached maximum listings limit ({max_listings}). Stopping.")
                drv.quit()
                return all_rows

        # Check if we should stop after first page
        if stop_after_first_page:
            print("Stopping after first page as requested.")
            break

        if not click_next_page(drv): break

    drv.quit()
    return all_rows

def write_outputs(rows, json_path="collin_foreclosures.json", csv_path="collin_foreclosures.csv"):
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    if rows:
        # Main spreadsheet fields as requested
        keys = [
            "full_address", "county", "list_name", "street_address", "city", "state", "zip",
            "owner_1_first_name", "owner_1_last_name", "owner_2_first_name", "owner_2_last_name",
            "sale_date", "sale_time", "recorded_date", "recorded_time", "document_id", 
            "document_type", "legal_description", "url_to_lead", "pdf_url", "deed_of_trust_number",
            # Additional useful fields
            "detail_id", "file_date", "property_type", "owner_name", "appraised_value_2025", 
            "appraisal_url", "deed_search_url", "scrape_timestamp"
        ]
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=keys); w.writeheader()
            for r in rows: 
                # Merge basic list data with detail data for backward compatibility
                row_data = {}
                for k in keys:
                    if k == "sale_time":
                        row_data[k] = ""  # Not available
                    elif k == "recorded_time":
                        row_data[k] = ""  # Not available  
                    elif k == "legal_description":
                        row_data[k] = ""  # Need to extract from foreclosure notice
                    elif k == "pdf_url":
                        row_data[k] = ""  # Need to extract PDF download URL
                    else:
                        row_data[k] = r.get(k, "")
                w.writerow(row_data)
    
    # Also save HTML files separately for easier inspection
    import os
    html_dir = "html_pages"
    if not os.path.exists(html_dir):
        os.makedirs(html_dir)
    
    for i, row in enumerate(rows):
        if row.get("html_content") and row.get("detail_id"):
            html_filename = f"{html_dir}/detail_{row['detail_id']}.html"
            try:
                with open(html_filename, "w", encoding="utf-8") as f:
                    f.write(row["html_content"])
            except Exception as e:
                print(f"Error saving HTML file {html_filename}: {e}")

if __name__ == "__main__":
    # Examples of different usage:
    # data = scrape_all()  # Default: scrape up to 20 pages
    # data = scrape_all(max_pages=5)  # Scrape maximum 5 pages
    # data = scrape_all(max_listings=10)  # Stop after collecting 10 listings
    # data = scrape_all(stop_after_first_page=True)  # Stop after first page only
    # data = scrape_all(max_pages=3, max_listings=15)  # Stop at 3 pages OR 15 listings, whichever comes first
    
    data = scrape_all()
  # For testing - only first page
    print(f"Collected {len(data)} rows with detailed page data.")
    write_outputs(data)
    print("Saved: collin_foreclosures.json, collin_foreclosures.csv, and HTML files in html_pages/ directory")
