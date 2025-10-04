# scrape_collin_foreclosures_tabs.py
import csv, json, re, time
from typing import Dict, Any, List, Optional

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait as wa
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

URL   = "https://apps2.collincountytx.gov/ForeclosureNotices/"
PROXY = "192.151.147.90:17093"

def make_driver(headless=True):
    from selenium.webdriver.chrome.options import Options
    opts = Options()
    # if headless: opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1440,1000")
    opts.add_argument("--disable-gpu"); opts.add_argument("--no-sandbox")
    opts.add_argument("--ignore-certificate-errors")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--enable-unsafe-swiftshader")
    opts.add_argument(f"--proxy-server=http://{PROXY}")
    drv = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    drv.set_page_load_timeout(60); drv.set_script_timeout(20)
    return drv

# ---------- list helpers ----------
def wait_list_loaded(driver, timeout=45):
    wa(driver, timeout).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table.mud-table-root tbody tr"))
    )
    driver.execute_script("return new Promise(r=>requestAnimationFrame(()=>requestAnimationFrame(()=>r())));")
    t0=time.time(); last=-1
    while time.time()-t0<8:
        n = driver.execute_script("return document.querySelectorAll('table.mud-table-root tbody tr').length||0;")
        if n>=20 and n==last: break
        last=n; time.sleep(0.2)

def row_count(driver)->int:
    return int(driver.execute_script("return document.querySelectorAll('table.mud-table-root tbody tr').length||0;"))

def current_page_badge(driver)->str:
    return driver.execute_script("let b=document.querySelector('button[aria-current=\"page\"]');return b?b.textContent.trim():'';") or ""

def snapshot_row(driver, idx:int)->Optional[Dict[str,str]]:
    return driver.execute_script(r"""
      const trs=document.querySelectorAll('table.mud-table-root tbody tr');
      const tr=trs[arguments[0]]; if(!tr) return null;
      const info={};
      tr.querySelectorAll('span.list-subheader').forEach(s=>{
        const k=s.textContent.trim().replace(/:$/,'');
        const v=(s.parentElement?.textContent||'').replace(s.textContent,'').trim();
        info[k]=v;
      });
      const address=(tr.querySelector('.list-header')?.textContent||'').trim();
      return {address, city:info["City"]||"", sale_date:info["Sale Date"]||"", file_date:info["File Date"]||"", property_type:info["Property Type"]||info["Type"]||""};
    """, idx)

def click_row_get_detail_url(driver, idx:int, timeout=20)->str:
    list_url = driver.current_url
    driver.execute_script("""
      const tr=document.querySelectorAll('table.mud-table-root tbody tr')[arguments[0]];
      const t=tr?.querySelector('.list-header')||tr?.querySelector('td')||tr;
      if(t){ t.scrollIntoView({block:'center'}); t.click(); }
    """, idx)
    wa(driver, timeout).until(lambda d: "/DetailPage/" in d.current_url)
    detail_url = driver.current_url
    return detail_url

def click_next_page(driver, timeout=25)->bool:
    before = current_page_badge(driver)
    first_txt = driver.execute_script("const tr=document.querySelector('table.mud-table-root tbody tr');return tr?tr.textContent:'';")
    can = driver.execute_script("""
      const n=document.querySelector('button[aria-label=\"Next page\"]');
      if(!n || n.disabled) return false; n.scrollIntoView({block:'center'}); n.click(); return true;
    """)
    if not can: return False
    def changed(drv):
        try:
            if current_page_badge(drv)!=before: return True
            const = drv.execute_script("const tr=document.querySelector('table.mud-table-root tbody tr');return tr?tr.textContent:'';")
            return const!=first_txt
        except: return False
    wa(driver, timeout).until(changed)
    return True

# ---------- detail helpers ----------
def _text_after_label(driver, label:str)->str:
    xps = [
        f"//*[normalize-space(text())='{label}']/following-sibling::*[1]",
        f"(//*[contains(@class,'mud-typography') and normalize-space(.)='{label}']/ancestor::div[contains(@class,'mud-grid-item')])[1]/following-sibling::div[contains(@class,'mud-grid-item')][1]//*[self::div or self::span][1]"
    ]
    for xp in xps:
        try:
            t = driver.find_element(By.XPATH, xp).text.strip()
            if t: return t
        except NoSuchElementException:
            continue
    return ""

def _find_pdf_url(driver)->str:
    tries = [
        "//a[contains(@href,'.pdf')]", "//a[contains(.,'PDF')]",
        "//button[contains(.,'PDF')]/ancestor::a[1]",
        "//a[contains(@href,'GetPdf') or contains(@href,'GetPDF') or contains(@href,'/PDF')]",
    ]
    for xp in tries:
        for el in driver.find_elements(By.XPATH, xp):
            href = el.get_attribute("href") or ""
            if href: return href
    return ""

def _split_first_last(n:str):
    parts=[p for p in re.split(r"\s+", n.strip()) if p]
    if not parts: return "",""
    if len(parts)==1: return parts[0],""
    return parts[0], parts[-1]

def _split_owner(full:str):
    full=(full or "").strip()
    if not full: return "","","",""
    m=re.split(r"\s+(?:&|and)\s+", full, 1, flags=re.I)
    if len(m)==2:
        f1,l1=_split_first_last(m[0]); f2,l2=_split_first_last(m[1]); return f1,l1,f2,l2
    f1,l1=_split_first_last(full); return f1,l1,"",""

def scrape_detail(driver)->Dict[str,str]:
    wa(driver,40).until(EC.presence_of_element_located((By.CSS_SELECTOR,"body")))
    detail_url = driver.current_url
    out = {"url_to_lead": detail_url,
           "detail_id": (re.search(r"/DetailPage/(\d+)", detail_url) or [None,None])[1] or ""}
    full_addr = _text_after_label(driver,"Address")
    out["full_address"]=full_addr
    out["street_address"]=full_addr.split(",")[0].strip() if full_addr else ""
    m=re.search(r",\s*([A-Za-z\s]+),\s*TX\s*(\d{5})", full_addr or "")
    out["city"]=m.group(1).strip() if m else ""
    out["state"]="TX" if m else ""
    out["zip"]=m.group(2) if m else ""
    out["county"]="Collin"
    out["list_name"]="Prop Code 51 Foreclosure Notices"
    out["legal_description"]=_text_after_label(driver,"Legal Description")
    out["deed_of_trust"]=_text_after_label(driver,"Deed Number")
    owner=_text_after_label(driver,"Name")
    o1f,o1l,o2f,o2l=_split_owner(owner)
    out.update({"owner_1_first":o1f,"owner_1_last":o1l,"owner_2_first":o2f,"owner_2_last":o2l})
    out["document_id"]=_text_after_label(driver,"Document ID") or _text_after_label(driver,"Doc Number")
    out["document_type"]=_text_after_label(driver,"Document Type")
    out["sale_time"]=_text_after_label(driver,"Sale Time")
    out["recorded_date"]=_text_after_label(driver,"Recorded Date")
    out["recorded_time"]=_text_after_label(driver,"Recorded Time")
    out["pdf_url"]=_find_pdf_url(driver)
    return out

# ---------- main ----------
def scrape_all(max_pages:int=20)->List[Dict[str,Any]]:
    drv = make_driver(headless=True)
    rows: List[Dict[str,Any]] = []
    try:
        drv.get(URL)
        wait_list_loaded(drv)
        list_handle = drv.current_window_handle

        seen=set()
        while len(seen)<max_pages:
            badge=current_page_badge(drv) or "?"
            if badge in seen: break
            seen.add(badge)
            print(f"Scraping list page {badge} …")

            n=row_count(drv)
            for i in range(n):
                snap = snapshot_row(drv, i)
                if not snap or not snap.get("address"):
                    continue

                # 1) navigate to detail in this tab to learn URL
                detail_url = click_row_get_detail_url(drv, i)
                # 2) open a REAL new tab and load that URL
                drv.switch_to.new_window('tab')
                detail_handle = drv.current_window_handle
                drv.get(detail_url)
                # 3) restore list in the original tab
                drv.switch_to.window(list_handle)
                drv.execute_script("history.back();")
                wa(drv, 25).until(lambda d: "/DetailPage/" not in d.current_url)
                wait_list_loaded(drv)

                # 4) scrape in the detail tab
                drv.switch_to.window(detail_handle)
                detail = scrape_detail(drv)
                drv.close()                      # close detail tab
                drv.switch_to.window(list_handle) # back to list tab

                # 5) merge + store
                rows.append({
                    "full_address": detail.get("full_address",""),
                    "county": "Collin",
                    "list_name": "Prop Code 51 Foreclosure Notices",
                    "street_address": detail.get("street_address",""),
                    "city": detail.get("city", snap.get("city","")),
                    "state": detail.get("state","TX") or "TX",
                    "zip": detail.get("zip",""),
                    "owner_1_first": detail.get("owner_1_first",""),
                    "owner_1_last":  detail.get("owner_1_last",""),
                    "owner_2_first": detail.get("owner_2_first",""),
                    "owner_2_last":  detail.get("owner_2_last",""),
                    "sale_date": snap.get("sale_date",""),
                    "sale_time": detail.get("sale_time",""),
                    "recorded_date": detail.get("recorded_date",""),
                    "recorded_time": detail.get("recorded_time",""),
                    "document_id":  detail.get("document_id",""),
                    "document_type":detail.get("document_type",""),
                    "legal_description": detail.get("legal_description",""),
                    "url_to_lead": detail.get("url_to_lead", detail_url),
                    "pdf_url": detail.get("pdf_url",""),
                    "deed_of_trust": detail.get("deed_of_trust",""),
                    "address": snap.get("address",""),
                    "file_date": snap.get("file_date",""),
                    "property_type": snap.get("property_type",""),
                    "detail_id": detail.get("detail_id","")
                })

            if not click_next_page(drv): break
            wait_list_loaded(drv)

    finally:
        try: drv.quit()
        except: pass
    return rows

def write_outputs(rows, json_path="collin_foreclosures.json", csv_path="collin_foreclosures.csv"):
    with open(json_path,"w",encoding="utf-8") as f: json.dump(rows,f,ensure_ascii=False,indent=2)
    cols = ["full_address","county","list_name","street_address","city","state","zip",
            "owner_1_first","owner_1_last","owner_2_first","owner_2_last",
            "sale_date","sale_time","recorded_date","recorded_time",
            "document_id","document_type","legal_description",
            "url_to_lead","pdf_url","deed_of_trust",
            "address","file_date","property_type","detail_id"]
    with open(csv_path,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=cols); w.writeheader()
        for r in rows: w.writerow({k:r.get(k,"") for k in cols})

if __name__ == "__main__":
    data = scrape_all()
    print(f"Collected {len(data)} rows.")
    write_outputs(data)
    print("Saved: collin_foreclosures.json and collin_foreclosures.csv")
asdaklfnlhaikhnj