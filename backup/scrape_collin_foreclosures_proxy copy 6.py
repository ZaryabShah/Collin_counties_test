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
    import os
    
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
    
    # Set up PDF download preferences
    pdf_dir = os.path.abspath("pdf_files")
    if not os.path.exists(pdf_dir):
        os.makedirs(pdf_dir)
    
    prefs = {
        "download.default_directory": pdf_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True
    }
    opts.add_experimental_option("prefs", prefs)
    
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

def download_pdf_for_filed_entry(driver, detail_id):
    """Download PDF for FILED entries with robust stale element handling."""
    import os
    import time
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By
    from selenium.common.exceptions import StaleElementReferenceException, TimeoutException, NoSuchElementException
    
    pdf_dir = os.path.abspath("pdf_files")
    if not os.path.exists(pdf_dir):
        os.makedirs(pdf_dir)
    
    try:
        # Get list of files before download
        files_before = set(os.listdir(pdf_dir))
        
        # Wait 3 seconds as requested by user
        time.sleep(3)
        
        # Try multiple selectors for the PDF download button with explicit waits
        selectors_with_types = [
            ("xpath", "/html/body/div[3]/div/div[3]/div[2]/div/div[1]/div[1]/button"),
            ("xpath", "//button[contains(text(), 'Download')]"),
            ("xpath", "//button[contains(@class, 'mud-button')]"),
            ("css", "button .mud-button-label"),
            ("css", ".mud-button-label"),
            ("xpath", "//button[@type='button']"),
            ("css", "button[type='button']")
        ]
        
        pdf_button = None
        successful_selector = None
        
        for selector_type, selector in selectors_with_types:
            try:
                wait = WebDriverWait(driver, 5)
                if selector_type == "xpath":
                    pdf_button = wait.until(EC.element_to_be_clickable((By.XPATH, selector)))
                else:
                    pdf_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
                
                successful_selector = (selector_type, selector)
                print(f"Found PDF button using {selector_type}: {selector}")
                break
                
            except (TimeoutException, NoSuchElementException):
                continue
            except Exception as e:
                print(f"Error with selector {selector}: {e}")
                continue
        
        if not pdf_button or not successful_selector:
            print(f"Could not find PDF download button for {detail_id}")
            return None

        # Try clicking with multiple strategies to handle stale elements
        click_successful = False
        
        for attempt in range(3):
            try:
                # Re-find the element to avoid stale reference
                selector_type, selector = successful_selector
                wait = WebDriverWait(driver, 5)
                
                if selector_type == "xpath":
                    fresh_button = wait.until(EC.element_to_be_clickable((By.XPATH, selector)))
                else:
                    fresh_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
                
                # Try regular click first
                fresh_button.click()
                click_successful = True
                print(f"Successfully clicked PDF download button for {detail_id} (attempt {attempt + 1})")
                break
                
            except StaleElementReferenceException:
                print(f"Stale element reference on attempt {attempt + 1}, retrying...")
                time.sleep(1)
                continue
            except Exception as e:
                # Try JavaScript click as fallback
                try:
                    selector_type, selector = successful_selector
                    if selector_type == "xpath":
                        js_button = driver.find_element(By.XPATH, selector)
                    else:
                        js_button = driver.find_element(By.CSS_SELECTOR, selector)
                    
                    driver.execute_script("arguments[0].click();", js_button)
                    click_successful = True
                    print(f"Successfully clicked PDF download button using JavaScript for {detail_id}")
                    break
                except Exception as js_error:
                    print(f"Both regular and JavaScript click failed on attempt {attempt + 1}: {e}, {js_error}")
                    if attempt < 2:
                        time.sleep(1)
                    continue
        
        if not click_successful:
            print(f"Failed to click PDF download button after all attempts for {detail_id}")
            return None
        
        # Wait for download to complete (check for new files)
        max_wait = 20
        wait_time = 0
        new_file = None
        downloaded_file_path = None
        
        print(f"Waiting for PDF download for {detail_id}...")
        print(f"Files before download: {files_before}")
        
        while wait_time < max_wait:
            time.sleep(1)
            wait_time += 1
            
            try:
                files_after = set(os.listdir(pdf_dir))
                new_files = files_after - files_before
                
                # Remove any .crdownload files from new_files as they're still downloading
                actual_new_files = [f for f in new_files if not f.endswith('.crdownload')]
                
                if actual_new_files:
                    new_file = actual_new_files[0]
                    print(f"Found new file: {new_file}")
                    break
                    
                # Check if download is in progress
                temp_files = [f for f in files_after if f.endswith('.crdownload')]
                if temp_files:
                    print(f"Download in progress: {temp_files}")
                    if wait_time < max_wait - 3:  # Give it more time if download started
                        continue
                        
                # Also check if a file with the detail_id already exists (maybe it was downloaded directly with the right name)
                expected_filename = f"{detail_id}.pdf"
                if expected_filename in files_after and expected_filename not in files_before:
                    new_file = expected_filename
                    print(f"Found file with expected name: {new_file}")
                    break
                    
            except Exception as e:
                print(f"Error checking files: {e}")
                continue
        
        print(f"Files after download attempt: {set(os.listdir(pdf_dir)) if os.path.exists(pdf_dir) else 'directory not found'}")
        
        if new_file:
            old_path = os.path.join(pdf_dir, new_file)
            new_path = os.path.join(pdf_dir, f"{detail_id}.pdf")
            
            # If the file is already named correctly, just return it
            if new_file == f"{detail_id}.pdf":
                print(f"PDF already has correct name: {detail_id}.pdf")
                return old_path
            
            # Otherwise, rename it
            try:
                if os.path.exists(new_path):
                    os.remove(new_path)  # Remove existing file if any
                    print(f"Removed existing file: {new_path}")
                
                if os.path.exists(old_path):
                    os.rename(old_path, new_path)
                    print(f"Successfully downloaded and renamed PDF: {new_file} -> {detail_id}.pdf")
                    return new_path
                else:
                    print(f"Source file does not exist: {old_path}")
                    return None
                    
            except Exception as e:
                print(f"Error renaming PDF file from {old_path} to {new_path}: {e}")
                # Return the original file path if rename failed but file exists
                if os.path.exists(old_path):
                    return old_path
                return None
        else:
            print(f"PDF download timeout for {detail_id} after {max_wait} seconds")
            return None
        
    except Exception as e:
        print(f"Error downloading PDF for {detail_id}: {e}")
        return None

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
        
        # Check if this is a FILED entry by looking at the page title or content
        page_title = driver.title
        h1_text = ""
        try:
            h1_element = driver.find_element(By.TAG_NAME, "h1")
            h1_text = h1_element.text
        except:
            pass
            
        is_filed_entry = "FILED" in page_title.upper() or "FILED" in h1_text.upper()
        
        # If it's a FILED entry, download the PDF
        pdf_filename = None
        pdf_downloaded = False
        if is_filed_entry and detail_id:
            pdf_filename = download_pdf_for_filed_entry(driver, detail_id)
            if pdf_filename:
                pdf_downloaded = True
                print(f"Downloaded PDF for FILED entry: {detail_id}")
            else:
                print(f"Failed to download PDF for FILED entry: {detail_id}")
            
            # For FILED entries, return basic info with PDF reference
            return {
                "detail_id": detail_id,
                "detail_url": url,
                "html_content": html_content,
                "page_title": page_title,
                "scrape_timestamp": time.time(),
                "is_filed_entry": True,
                "pdf_filename": pdf_filename,
                "pdf_downloaded": pdf_downloaded,
                "county": "Collin",
                "document_type": "Foreclosure Notice Filing",
                "url_to_lead": url,
                "pdf_url": pdf_filename if pdf_filename else "",
                "full_address": h1_text,  # Use h1 text as address for FILED entries
            }
        
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
                const nameParts = data.owner_name.replace(/&/g, 'and').split(/\\s+/);
                if (nameParts.length >= 2) {
                    // Simple parsing - first word is first name, last word is last name
                    data.owner_1_first_name = nameParts[0];
                    data.owner_1_last_name = nameParts[nameParts.length - 1];
                    
                    // If there are more than 2 words, check for "and" indicating second owner
                    const nameStr = data.owner_name.toLowerCase();
                    if (nameStr.includes(' and ') || nameStr.includes(' & ')) {
                        const parts = data.owner_name.split(/\\s+(?:and|&)\\s+/i);
                        if (parts.length >= 2) {
                            const owner1Parts = parts[0].trim().split(/\\s+/);
                            const owner2Parts = parts[1].trim().split(/\\s+/);
                            
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

def load_processed_checkpoints():
    """Load already processed listings from checkpoint file with address normalization."""
    import os
    checkpoint_file = "processed_checkpoints.json"
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, "r", encoding="utf-8") as f:
                checkpoints = json.load(f)
            
            # Normalize addresses in existing checkpoints
            normalized_checkpoints = []
            for cp in checkpoints:
                if cp and isinstance(cp, dict):
                    # Normalize the address field
                    if "address" in cp:
                        address = cp["address"]
                        if address:
                            normalized_address = " ".join(address.replace("\r\n", " ").replace("\n", " ").split())
                            cp["address"] = normalized_address
                    normalized_checkpoints.append(cp)
            
            print(f"Loaded {len(normalized_checkpoints)} processed checkpoints")
            return set(tuple(sorted(cp.items())) for cp in normalized_checkpoints if cp)
        except Exception as e:
            print(f"Error loading checkpoints: {e}")
    return set()

def save_checkpoint(processed_checkpoints):
    """Save processed checkpoints to file."""
    checkpoint_file = "processed_checkpoints.json"
    checkpoints_list = [dict(cp) for cp in processed_checkpoints]
    with open(checkpoint_file, "w", encoding="utf-8") as f:
        json.dump(checkpoints_list, f, ensure_ascii=False, indent=2)

def create_checkpoint_key(snap):
    """Create checkpoint key from basic listing info with normalized address."""
    # Normalize address by removing newlines and extra whitespace
    address = snap.get("address", "").strip()
    address = " ".join(address.replace("\r\n", " ").replace("\n", " ").split())
    
    return {
        "address": address,
        "city": snap.get("city", "").strip(), 
        "sale_date": snap.get("sale_date", "").strip(),
        "file_date": snap.get("file_date", "").strip(),
        "property_type": snap.get("property_type", "").strip(),
        "pdf_downloaded": snap.get("pdf_downloaded", False)
    }

def is_already_processed(snap, processed_checkpoints, existing_results):
    """Check if a listing is already processed using multiple methods."""
    
    # Method 1: Check checkpoint system
    checkpoint_key = create_checkpoint_key(snap)
    checkpoint_tuple = tuple(sorted(checkpoint_key.items()))
    if checkpoint_tuple in processed_checkpoints:
        return True, "checkpoint"
    
    # Method 2: Check existing detailed results by address matching
    snap_address = (snap.get("address") or "").strip().upper()
    snap_city = (snap.get("city") or "").strip().upper()
    
    for existing in existing_results:
        if not existing:  # Skip None entries
            continue
            
        # Check various address fields that might match (with null safety)
        existing_addresses = [
            (existing.get("address") or "").strip().upper(),
            (existing.get("full_address") or "").strip().upper(),
            (existing.get("street_address") or "").strip().upper(),
        ]
        
        existing_cities = [
            (existing.get("city") or "").strip().upper(),
        ]
        
        # If we find a match on both address and city, it's already processed
        for addr in existing_addresses:
            if addr and snap_address and (snap_address in addr or addr in snap_address):
                for city in existing_cities:
                    if city and snap_city and snap_city == city:
                        return True, "existing_data"
    
    # Method 3: Simple address matching (fallback)
    for existing in existing_results:
        if not existing:  # Skip None entries
            continue
            
        if snap_address and snap_city:
            existing_addr = existing.get('address') or ""
            existing_city = existing.get('city') or ""
            existing_full = f"{existing_addr} {existing_city}".strip().upper()
            snap_full = f"{snap_address} {snap_city}".strip()
            if existing_full and snap_full and (snap_full in existing_full or existing_full in snap_full):
                return True, "address_match"
    
    return False, None

def load_existing_results():
    """Load existing detailed results from JSON file."""
    import os
    json_path = "collin_foreclosures.json"
    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
            print(f"Loaded {len(existing_data)} existing detailed results")
            return existing_data
        except Exception as e:
            print(f"Error loading existing results: {e}")
    return []

def find_filed_entries_without_pdf(existing_data):
    """Find FILED entries that don't have PDF downloaded yet."""
    import os
    pdf_dir = os.path.abspath("pdf_files")
    filed_without_pdf = []
    
    for entry in existing_data:
        # Check if it's a FILED entry
        if (entry.get("is_filed_entry") or 
            "FILED" in str(entry.get("address", "")).upper() or 
            "FILED" in str(entry.get("full_address", "")).upper()):
            
            detail_id = entry.get("detail_id")
            if detail_id:
                pdf_path = os.path.join(pdf_dir, f"{detail_id}.pdf")
                if not os.path.exists(pdf_path):
                    filed_without_pdf.append(entry)
    
    return filed_without_pdf

def download_missing_pdfs(driver, filed_entries):
    """Download PDFs for FILED entries that are missing them."""
    print(f"Found {len(filed_entries)} FILED entries without PDFs")
    
    for entry in filed_entries:
        detail_id = entry.get("detail_id")
        detail_url = entry.get("detail_url")
        
        if not detail_id or not detail_url:
            continue
            
        print(f"Downloading PDF for FILED entry: {detail_id}")
        
        try:
            # Navigate to the detail page
            driver.get(detail_url)
            time.sleep(2)
            
            # Download the PDF
            pdf_filename = download_pdf_for_filed_entry(driver, detail_id)
            
            if pdf_filename:
                # Update the entry to mark PDF as downloaded
                entry["pdf_filename"] = pdf_filename
                entry["pdf_downloaded"] = True
                entry["pdf_url"] = pdf_filename
                print(f"Successfully downloaded PDF for {detail_id}")
            else:
                print(f"Failed to download PDF for {detail_id}")
                
        except Exception as e:
            print(f"Error downloading PDF for {detail_id}: {e}")
    
    return filed_entries

def scrape_all(max_pages: int = 20, max_listings: int = None, stop_after_first_page: bool = False, download_missing_pdfs_only: bool = False):
    """
    Scrape foreclosure data with flexible stopping conditions.
    
    Args:
        max_pages: Maximum number of pages to scrape (default: 20)
        max_listings: Maximum number of listings to scrape (default: None, no limit)
        stop_after_first_page: If True, stop after completing the first page (default: False)
        download_missing_pdfs_only: If True, only download missing PDFs for existing FILED entries (default: False)
    """
    # Load processed checkpoints and existing results
    processed_checkpoints = load_processed_checkpoints()
    all_rows = load_existing_results()  # Start with existing data
    
    drv = make_driver(headless=True)
    
    # If we're only downloading missing PDFs, do that and return
    if download_missing_pdfs_only:
        filed_entries = find_filed_entries_without_pdf(all_rows)
        if filed_entries:
            download_missing_pdfs(drv, filed_entries)
            # Update the main data with the new PDF info
            for i, row in enumerate(all_rows):
                for filed_entry in filed_entries:
                    if row.get("detail_id") == filed_entry.get("detail_id"):
                        all_rows[i].update(filed_entry)
                        break
        else:
            print("No FILED entries found without PDFs")
        drv.quit()
        return all_rows
    
    # Check for missing PDFs in existing FILED entries first
    filed_entries = find_filed_entries_without_pdf(all_rows)
    if filed_entries:
        print(f"Found {len(filed_entries)} existing FILED entries without PDFs. Downloading...")
        download_missing_pdfs(drv, filed_entries)
        # Update the main data with the new PDF info
        for i, row in enumerate(all_rows):
            for filed_entry in filed_entries:
                if row.get("detail_id") == filed_entry.get("detail_id"):
                    all_rows[i].update(filed_entry)
                    break
    
    drv.get(URL)
    wait_for_table(drv)
    
    # Configure sorting by Filed Date (descending)
    try:
        print("Setting up Filed Date sorting...")
        
        # Step 1: Click dropdown to open sort options
        dropdown_selectors = [
            "/html/body/div[3]/div/div[3]/div[1]/div/div[2]/div[5]/div[1]/div/div/div[1]/div[1]",
            "/html/body/div[3]/div/div[3]/div[1]/div/div[2]/div[5]/div[1]/div/div/div[1]/div[2]/svg"
        ]
        
        dropdown_clicked = False
        for selector in dropdown_selectors:
            try:
                dropdown_element = WebDriverWait(drv, 10).until(
                    EC.element_to_be_clickable((By.XPATH, selector))
                )
                dropdown_element.click()
                print(f"Clicked dropdown using selector: {selector}")
                dropdown_clicked = True
                break
            except Exception as e:
                print(f"Failed to click dropdown with selector {selector}: {e}")
                continue
        
        if dropdown_clicked:
            # Step 2: Wait a moment for dropdown to open and select "Filed Date" (last option)
            time.sleep(1)
            try:
                # Look for Filed Date option - try different approaches to find the last option
                filed_date_options = drv.find_elements(By.XPATH, "//div[contains(text(), 'Filed Date') or contains(text(), 'File Date')]")
                if filed_date_options:
                    filed_date_options[-1].click()  # Click the last one if multiple found
                    print("Selected 'Filed Date' option")
                else:
                    # Fallback: try to click the last option in the dropdown
                    dropdown_options = drv.find_elements(By.XPATH, "//div[@role='option' or contains(@class, 'mud-list-item')]")
                    if dropdown_options:
                        dropdown_options[-1].click()
                        print("Selected last option in dropdown (assumed to be Filed Date)")
                
                # Step 3: Click the sort order switch/toggle 
                time.sleep(1)
                sort_toggle = WebDriverWait(drv, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "/html/body/div[3]/div/div[3]/div[1]/div/div[2]/div[5]/div[2]/div/label"))
                )
                sort_toggle.click()
                print("Clicked sort order toggle")
                
                # Wait for sorting to take effect
                time.sleep(2)
                print("Successfully configured Filed Date sorting")
                
            except Exception as e:
                print(f"Error configuring sort options: {e}")
        else:
            print("Could not open dropdown - continuing without sorting")
            
    except Exception as e:
        print(f"Error setting up sorting: {e}")
        print("Continuing without sorting...")
    
    drv.execute_script(HOOK_JS)  # install pushState hook once

    seen = set()
    pages_done = 0
    newly_processed_count = 0  # Track only newly processed records

    while pages_done < max_pages:
        badge = current_page_badge(drv) or "?"
        if badge in seen: break
        seen.add(badge); pages_done += 1
        print(f"Scraping list page {badge} …")

        n = row_count(drv)
        for i in range(n):
            snap = snapshot_row(drv, i)
            if not snap or not snap.get("address"): continue

            # Check if this listing was already processed using multiple methods
            already_processed, method = is_already_processed(snap, processed_checkpoints, all_rows)
            
            if already_processed:
                print(f"Skipping already processed ({method}): {snap.get('address', '')}")
                continue

            # Scrape detail page in new tab
            print(f"Processing: {snap.get('address', '')}")
            detail_data = scrape_detail_page_in_new_tab(drv, i)
            if detail_data:
                # Preserve sale_date from list data and merge with detail data
                list_sale_date = snap.get("sale_date", "")
                list_file_date = snap.get("file_date", "")
                snap.update(detail_data)
                # Ensure sale_date from list is preserved
                snap["sale_date"] = list_sale_date
                snap["file_date"] = list_file_date
                
                # Mark PDF as downloaded if it's a FILED entry and PDF was downloaded
                if detail_data.get("is_filed_entry") and detail_data.get("pdf_filename"):
                    snap["pdf_downloaded"] = True
                else:
                    snap["pdf_downloaded"] = False
            
            all_rows.append(snap)
            newly_processed_count += 1
            
            # Create updated checkpoint with PDF status
            updated_checkpoint_key = create_checkpoint_key(snap)
            updated_checkpoint_tuple = tuple(sorted(updated_checkpoint_key.items()))
            
            # Add to processed checkpoints and save
            processed_checkpoints.add(updated_checkpoint_tuple)
            save_checkpoint(processed_checkpoints)
            
            # Check if we've reached the maximum number of NEW listings
            if max_listings and newly_processed_count >= max_listings:
                print(f"Reached maximum NEW listings limit ({max_listings}). Stopping.")
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
            "appraisal_url", "deed_search_url", "scrape_timestamp", "is_filed_entry", "pdf_filename"
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
    # data = scrape_all(max_listings=5)
    data = scrape_all()
  # For testing - only first page
    print(f"Total records in database: {len(data)} rows with detailed page data.")
    write_outputs(data)
    print("Saved: collin_foreclosures.json, collin_foreclosures.csv, and HTML files in html_pages/ directory")
