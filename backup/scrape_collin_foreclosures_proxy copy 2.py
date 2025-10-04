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
            
            // Try to extract all text content and structure it
            const allElements = document.querySelectorAll('*');
            const textData = [];
            
            allElements.forEach(el => {
                if (el.children.length === 0 && el.textContent.trim()) {
                    const text = el.textContent.trim();
                    const tagName = el.tagName.toLowerCase();
                    const className = el.className || '';
                    const parent = el.parentElement ? el.parentElement.tagName.toLowerCase() : '';
                    
                    textData.push({
                        text: text,
                        tag: tagName,
                        class: className,
                        parent: parent
                    });
                }
            });
            
            // Try to find common patterns
            const labels = document.querySelectorAll('label, .label, .field-label, .form-label');
            labels.forEach(label => {
                const text = label.textContent.trim();
                const next = label.nextElementSibling;
                const parent = label.parentElement;
                
                if (next && next.textContent.trim()) {
                    data[text] = next.textContent.trim();
                } else if (parent) {
                    const siblings = Array.from(parent.children);
                    const labelIndex = siblings.indexOf(label);
                    if (labelIndex >= 0 && labelIndex < siblings.length - 1) {
                        const nextSibling = siblings[labelIndex + 1];
                        if (nextSibling.textContent.trim()) {
                            data[text] = nextSibling.textContent.trim();
                        }
                    }
                }
            });
            
            return {
                extracted_data: data,
                all_text: textData,
                title: document.title || '',
                url: window.location.href
            };
        """)
        
        return {
            "detail_id": detail_id,
            "detail_url": url,
            "html_content": html_content,
            "page_title": detail_info.get("title", ""),
            "extracted_data": detail_info.get("extracted_data", {}),
            "all_text_elements": detail_info.get("all_text", []),
            "scrape_timestamp": time.time()
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
                # Merge basic row data with detailed data
                snap.update(detail_data)
            
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
        keys = ["detail_id","detail_url","address","city","sale_date","file_date","property_type","page_title","scrape_timestamp"]
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=keys); w.writeheader()
            for r in rows: w.writerow({k:r.get(k) for k in keys})
    
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
    
    data = scrape_all(max_listings=5)
  # For testing - only first page
    print(f"Collected {len(data)} rows with detailed page data.")
    write_outputs(data)
    print("Saved: collin_foreclosures.json, collin_foreclosures.csv, and HTML files in html_pages/ directory")
