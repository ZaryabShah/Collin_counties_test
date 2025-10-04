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

def capture_detail_id_via_pushstate(driver, index:int, timeout=8):
    """Click row, wait for a new captured URL (pushState), parse id, then go back quickly."""
    before_len = int(driver.execute_script("return window.__captured_urls__ ? window.__captured_urls__.length : 0;") or 0)

    clicked = driver.execute_script("""
      const trs = document.querySelectorAll('table.mud-table-root tbody tr');
      const tr = trs[arguments[0]];
      if (!tr) return false;
      const target = tr.querySelector('.list-header') || tr.querySelector('td') || tr;
      target.scrollIntoView({block:'center'});
      target.click();
      return true;
    """, index)
    if not clicked:
        return None, None

    # wait for captured URL (pushState), not full page render
    def got_url(drv):
        return int(drv.execute_script("return window.__captured_urls__.length;") or 0) > before_len
    try:
        WebDriverWait(driver, timeout).until(got_url)
    except TimeoutException:
        # fall back: tiny check if URL actually changed to detail
        if "/DetailPage/" in driver.current_url:
            url = driver.current_url
        else:
            return None, None
    else:
        url = driver.execute_script("return window.__captured_urls__[window.__captured_urls__.length-1];") or ""

    m = re.search(r"/DetailPage/(\d+)", url)
    detail_id = m.group(1) if m else None

    # hop back to list without waiting for render
    driver.execute_script("history.back();")
    try:
        WebDriverWait(driver, 12).until(lambda d: "/DetailPage/" not in d.current_url)
        wait_for_table(driver, timeout=20)
    except TimeoutException:
        pass

    return url if detail_id else None, detail_id

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

def scrape_all(max_pages: int = 20):
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

            url, did = capture_detail_id_via_pushstate(drv, i)
            snap["detail_url"] = url
            snap["detail_id"]  = did
            all_rows.append(snap)

        if not click_next_page(drv): break

    drv.quit()
    return all_rows

def write_outputs(rows, json_path="collin_foreclosures.json", csv_path="collin_foreclosures.csv"):
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    if rows:
        keys = ["detail_id","detail_url","address","city","sale_date","file_date","property_type"]
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=keys); w.writeheader()
            for r in rows: w.writerow({k:r.get(k) for k in keys})

if __name__ == "__main__":
    data = scrape_all()
    print(f"Collected {len(data)} rows.")
    write_outputs(data)
    print("Saved: collin_foreclosures.json and collin_foreclosures.csv")
