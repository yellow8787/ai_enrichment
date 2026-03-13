#!/usr/bin/env python3
"""
Merchant Enrichment Pipeline (Complete)
=========================================
Phase 1: Has domain -> Store Leads GMV (screenshot + OCR)
Phase 2: Find missing IG handles (website scrape + Serper site:instagram.com)
Phase 3: Has IG handle -> IG followers (screenshot + OCR)
Phase 4: No domain -> Serper + Gemini find domain -> can re-run phase1
Phase 5: Filter GMV>=400K or IG>=10K -> high_performing_sellers.csv

CSV columns:
  seller_name, domain, instagram_handle, ig_followers,
  storeleads_gmv_usd, register_country_code, tiktok_account_name

Install:
  pip install requests beautifulsoup4 pandas pyautogui pytesseract Pillow

  Tesseract OCR: https://github.com/UB-Mannheim/tesseract/wiki

Usage:
  set SERPER_API_KEY=xxx
  set GEMINI_API_KEY=xxx

  python pipeline.py --phase1 --input data.csv   (GMV)
  python pipeline.py --phase2 --input data.csv   (find IG handles)
  python pipeline.py --phase3 --input data.csv   (IG followers)
  python pipeline.py --phase4 --input data.csv   (find missing domains)
  python pipeline.py --phase5 --input data.csv   (filter)
  python pipeline.py --find-position
  python pipeline.py --debug-gmv --input data.csv
  python pipeline.py --debug-ig --input data.csv
"""

import os, re, time, logging, argparse, json, sys
from typing import Optional
from urllib.parse import urlparse

import requests
import pandas as pd
from bs4 import BeautifulSoup
import pyautogui
import webbrowser

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pipeline")

# =====================================================================
#  CONFIG
# =====================================================================

TESSERACT_CMD = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

# API Keys (set via environment or paste here)
SERPER_API_KEY = os.environ.get("SERPER_API_KEY", "YOUR_SERPER_API_KEY")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "YOUR_GEMINI_API_KEY")

# Store Leads extension icon
EXTENSION_X = 2584
EXTENSION_Y = 128

# Store Leads panel screenshot area
SL_LEFT   = 951
SL_TOP    = 170
SL_RIGHT  = 2500
SL_BOTTOM = 973

# Store Leads error popup Close button
CLOSE_X = 1450
CLOSE_Y = 970

# IG followers screenshot area
IG_LEFT   = 1085
IG_TOP    = 455
IG_RIGHT  = 1648
IG_BOTTOM = 509

# Timing
PAGE_WAIT = 6
EXT_WAIT  = 3
IG_WAIT   = 4
AUTOSAVE  = 20
DELAY     = 1.0

# Filter thresholds
FILTER_GMV = 400_000
FILTER_IG  = 10_000

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
}
TIMEOUT = 15


# =====================================================================
#  CSV I/O
# =====================================================================

def load_csv(path):
    for enc in ["utf-8-sig", "utf-8", "latin-1", "cp1252"]:
        try:
            df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding=enc)
            break
        except (UnicodeDecodeError, Exception):
            continue
    else:
        df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="latin-1")
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    for c in ["domain", "instagram_handle", "ig_followers", "storeleads_gmv_usd",
              "register_country_code", "tiktok_account_name"]:
        if c not in df.columns:
            df[c] = ""
    return df


def save_csv(df, path):
    try:
        df.to_csv(path, index=False)
        log.info(f"  Saved -> {path}")
    except PermissionError:
        alt = path.replace(".csv", f"_{int(time.time())}.csv")
        log.warning(f"  Locked! -> {alt}")
        df.to_csv(alt, index=False)


def is_empty(val) -> bool:
    """Check if a cell is empty/null/NULL/nan"""
    if val is None:
        return True
    s = str(val).strip().lower()
    return s in ("", "null", "nan", "none", "0")


# =====================================================================
#  OCR Engine
# =====================================================================

def ocr_region(left, top, right, bottom, debug_name=""):
    import pytesseract
    from PIL import Image
    if os.path.exists(TESSERACT_CMD):
        pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD
    w, h = right - left, bottom - top
    shot = pyautogui.screenshot(region=(left, top, w, h))
    sw, sh = shot.size
    proc = shot.resize((sw * 2, sh * 2), Image.LANCZOS).convert("L")
    if debug_name:
        d = os.path.join(os.path.dirname(os.path.abspath(CSV_PATH)), "screenshots")
        os.makedirs(d, exist_ok=True)
        safe = re.sub(r"[^a-zA-Z0-9]", "_", debug_name)
        shot.save(os.path.join(d, f"{safe}_raw.png"))
    return pytesseract.image_to_string(proc, lang="eng")


def check_tesseract():
    try:
        import pytesseract
        from PIL import Image
        if os.path.exists(TESSERACT_CMD):
            pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD
        pytesseract.image_to_string(Image.new("RGB", (10, 10)))
        return True
    except Exception as e:
        log.error(f"Tesseract error: {e}")
        return False


# =====================================================================
#  Store Leads: Click + Handle Error Popup
# =====================================================================

def click_and_capture(max_retries=3) -> str:
    for attempt in range(1, max_retries + 1):
        pyautogui.click(EXTENSION_X, EXTENSION_Y)
        time.sleep(EXT_WAIT)
        ocr = ocr_region(SL_LEFT, SL_TOP, SL_RIGHT, SL_BOTTOM)
        if is_error_popup(ocr):
            log.warning(f"    Error popup (attempt {attempt}), clicking Close...")
            if CLOSE_X > 0:
                pyautogui.click(CLOSE_X, CLOSE_Y)
            else:
                pyautogui.press("escape")
            time.sleep(2)
            if attempt < max_retries:
                continue
        return ocr
    return ""


def is_error_popup(text: str) -> bool:
    if not text:
        return False
    lower = text.lower()
    return any(p in lower for p in [
        "something has gone wrong", "gone wrong", "max_write_operations",
        "exceeds the", "quota", "closing this window",
    ])


# =====================================================================
#  Parse: Store Leads GMV
# =====================================================================

def parse_estimated_sales(text):
    if not text or not text.strip():
        return {"monthly": None, "annual": None, "info": "empty OCR"}
    cleaned = re.sub(r"[\r\n\t]+", " ", text)
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = cleaned.replace("\u00a7", "$").replace("S$", "$")
    cleaned = cleaned.replace("USS", "USD").replace("USO", "USD")

    m = re.search(r"\$\s*([\d,.]+)\s*/\s*m(?:onth|o)", cleaned, re.IGNORECASE)
    if m:
        v = _dollar(m.group(1))
        if v >= 0: return {"monthly": v, "annual": v * 12, "info": "$/month"}

    m = re.search(r"\$\s*([\d,.]+)\s*/\s*y(?:ear|r)", cleaned, re.IGNORECASE)
    if m:
        v = _dollar(m.group(1))
        if v >= 0: return {"monthly": v // 12, "annual": v, "info": "$/year"}

    m = re.search(r"ESTIMATED\s+SALES.{0,30}?\$\s*([\d,.]+)", cleaned, re.IGNORECASE)
    if m:
        v = _dollar(m.group(1))
        if v >= 0: return {"monthly": v, "annual": v * 12, "info": "EST+$"}

    lines = text.split("\n")
    for i, line in enumerate(lines):
        if re.search(r"ESTIMATED\s+SALES", line, re.IGNORECASE):
            for j in range(i, min(i + 6, len(lines))):
                lf = lines[j].replace("\u00a7", "$")
                dm = re.search(r"\$\s*([\d,.]+)", lf)
                if dm:
                    v = _dollar(dm.group(1))
                    if v >= 0:
                        yr = bool(re.search(r"/\s*y", lf, re.IGNORECASE))
                        if yr: return {"monthly": v // 12, "annual": v, "info": "line$/yr"}
                        else: return {"monthly": v, "annual": v * 12, "info": "line$/mo"}
            break

    for amt in re.findall(r"\$\s*([\d,.]+)", cleaned):
        v = _dollar(amt)
        if v > 0: return {"monthly": v, "annual": v * 12, "info": "loose$"}

    return {"monthly": None, "annual": None, "info": "no match"}


# =====================================================================
#  Parse: IG Followers
# =====================================================================

def parse_ig_followers(text):
    if not text or not text.strip():
        return {"count": None, "info": "empty OCR"}
    cleaned = re.sub(r"[\r\n\t]+", " ", text)
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = re.sub(r"[Ff]oll[oO0a]w[eo]rs", "followers", cleaned)

    m = re.search(r"([\d,.]+\s*[KMBkmb]?)\s*followers", cleaned, re.IGNORECASE)
    if m:
        v = _num(m.group(1))
        if v and v > 0: return {"count": v, "info": f"'{m.group(1).strip()}'"}

    m = re.search(r"posts?\s+([\d,.]+\s*[KMBkmb]?)\s+follow", cleaned, re.IGNORECASE)
    if m:
        v = _num(m.group(1))
        if v and v > 0: return {"count": v, "info": "posts->X->follow"}

    lines = text.strip().split("\n")
    for i, line in enumerate(lines):
        if re.search(r"followers", line, re.IGNORECASE):
            dm = re.search(r"([\d,.]+\s*[KMBkmb]?)", line)
            if dm:
                v = _num(dm.group(1))
                if v and v > 0: return {"count": v, "info": "same line"}
            if i > 0:
                dm = re.search(r"([\d,.]+\s*[KMBkmb]?)", lines[i - 1])
                if dm:
                    v = _num(dm.group(1))
                    if v and v > 0: return {"count": v, "info": "line above"}
            break

    nums = re.findall(r"([\d,.]+\s*[KMBkmb]?)", cleaned)
    if len(nums) >= 3:
        v = _num(nums[1])
        if v and v > 0: return {"count": v, "info": f"3-nums 2nd"}

    return {"count": None, "info": "no followers"}


# =====================================================================
#  Number Parsers
# =====================================================================

def _num(s):
    s = s.strip().replace(" ", "")
    if not s: return None
    try:
        u = s.upper()
        if u.endswith("B"): return int(float(u[:-1].replace(",", "")) * 1e9)
        if u.endswith("M"): return int(float(u[:-1].replace(",", "")) * 1e6)
        if u.endswith("K"): return int(float(u[:-1].replace(",", "")) * 1e3)
        return int(float(s.replace(",", "")))
    except: return None


def _dollar(s):
    s = s.strip().replace("$", "").replace(",", "").replace(" ", "")
    if not s: return 0
    try:
        u = s.upper()
        if u.endswith("B"): return int(float(u[:-1]) * 1e9)
        if u.endswith("M"): return int(float(u[:-1]) * 1e6)
        if u.endswith("K"): return int(float(u[:-1]) * 1e3)
        return int(float(s))
    except: return 0


# =====================================================================
#  Phase 1: Store Leads GMV (has domain -> screenshot + OCR)
# =====================================================================

def run_phase1(path, limit=0):
    if not check_tesseract(): return
    pyautogui.FAILSAFE = True; pyautogui.PAUSE = 0.3

    df = load_csv(path)
    total = len(df)
    todo = [i for i in range(total)
            if not is_empty(df.at[i, "domain"]) and is_empty(df.at[i, "storeleads_gmv_usd"])]
    if limit > 0: todo = todo[:limit]

    log.info("=" * 60)
    log.info("  PHASE 1: Store Leads GMV (screenshot + OCR)")
    log.info(f"  Total: {total} | To scrape: {len(todo)} | Est: ~{len(todo)*10//60}min")
    log.info(f"  !! DO NOT TOUCH MOUSE !! Emergency: mouse to top-left")
    log.info("=" * 60)

    for i in range(5, 0, -1): log.info(f"  {i}..."); time.sleep(1)

    ok, fail, done = 0, 0, 0
    for idx in todo:
        seller = df.at[idx, "seller_name"]
        domain = df.at[idx, "domain"]
        done += 1
        eta = (len(todo) - done) * 10 // 60
        log.info(f"\n  [{done}/{len(todo)}] {seller} ({domain}) ETA:{eta}min")

        try:
            webbrowser.open(f"https://{domain}")
            time.sleep(PAGE_WAIT)
            ocr = click_and_capture()
            r = parse_estimated_sales(ocr)
            if r["monthly"] is not None:
                df.at[idx, "storeleads_gmv_usd"] = str(r["annual"])
                log.info(f"    ${r['monthly']:,}/mo -> ${r['annual']:,}/yr [{r['info']}]")
                ok += 1
            else:
                log.warning(f"    NOT FOUND ({r['info']})")
                fail += 1
            pyautogui.hotkey("ctrl", "w"); time.sleep(1)
        except pyautogui.FailSafeException:
            log.error("EMERGENCY STOP"); save_csv(df, path); return
        except Exception as e:
            log.error(f"    {e}"); fail += 1
            try: pyautogui.hotkey("ctrl", "w"); time.sleep(1)
            except: pass

        if done % AUTOSAVE == 0:
            log.info(f"  [AUTO-SAVE] {ok}ok {fail}fail"); save_csv(df, path)

    log.info(f"\n  Phase 1 Done: {ok} ok, {fail} fail")
    save_csv(df, path)


# =====================================================================
#  Phase 2: Find IG handles (website scrape + Serper)
# =====================================================================

def find_ig_from_website(domain: str) -> Optional[str]:
    if not domain: return None
    try:
        r = requests.get(f"https://{domain}", headers=BROWSER_HEADERS,
                        timeout=TIMEOUT, allow_redirects=True)
        soup = BeautifulSoup(r.text, "html.parser")
        skip = {"p", "reel", "reels", "stories", "explore", "accounts",
                "about", "developer", "tv", "directory", "legal", "privacy"}
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "instagram.com" in href:
                h = href.rstrip("/").split("/")[-1].split("?")[0].lower()
                if h and h not in skip and len(h) > 1:
                    return h
        for m in re.findall(r'instagram\.com/([a-zA-Z0-9_.]+)', r.text):
            h = m.lower()
            if h not in skip and len(h) > 1:
                return h
    except: pass
    return None


def find_ig_from_serper(seller_name: str, domain: str = "", country: str = "", tiktok: str = "") -> Optional[str]:
    if not SERPER_API_KEY: return None
    queries = [f'"{seller_name}" site:instagram.com']
    if domain:
        brand = seller_name.split()[0]
        queries.insert(0, f'"{brand}" "{domain}" site:instagram.com')
    if country:
        queries.append(f'"{seller_name}" {country} site:instagram.com')
    # TikTok handle often = IG handle, use as search hint
    if tiktok and tiktok.lower() not in ("null", "nan", "none", ""):
        tt = tiktok.strip().lstrip("@")
        queries.append(f'"{tt}" site:instagram.com')

    skip = {"p", "reel", "reels", "explore", "stories", "accounts", "about", "tags"}
    for q in queries:
        try:
            r = requests.post(
                "https://google.serper.dev/search",
                headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
                json={"q": q, "num": 5}, timeout=TIMEOUT,
            )
            if r.status_code == 200:
                for item in r.json().get("organic", []):
                    link = item.get("link", "")
                    if "instagram.com/" in link:
                        h = link.rstrip("/").split("/")[-1].split("?")[0].lower()
                        if h and h not in skip and len(h) > 1:
                            return h
        except: pass
    return None


def run_phase2(path, limit=0):
    log.info("=" * 60)
    log.info("  PHASE 2: Find IG Handles")
    log.info("  Works with OR without domain!")
    log.info("  Method: website scrape (if domain) -> Serper search (always)")
    log.info("=" * 60)

    df = load_csv(path)
    total = len(df)
    todo = [i for i in range(total) if is_empty(df.at[i, "instagram_handle"])]
    if limit > 0: todo = todo[:limit]

    has_domain = sum(1 for i in todo if not is_empty(df.at[i, "domain"]))
    no_domain = len(todo) - has_domain
    log.info(f"  {len(todo)} missing IG handle ({has_domain} with domain, {no_domain} without)")

    ok, fail = 0, 0
    for idx in todo:
        seller = df.at[idx, "seller_name"]
        domain = df.at[idx, "domain"].strip()
        country = df.at[idx, "register_country_code"].strip()
        tiktok = df.at[idx, "tiktok_account_name"].strip()
        done = ok + fail + 1
        log.info(f"\n  [{done}/{len(todo)}] {seller}" +
                (f" ({domain})" if domain else " (no domain)"))

        handle = None

        # Method 1: scrape website (only if has domain)
        if domain:
            handle = find_ig_from_website(domain)
            if handle:
                log.info(f"    Website: @{handle}")

        # Method 2: Serper Google search (always, even without domain)
        if not handle:
            handle = find_ig_from_serper(seller, domain, country, tiktok)
            if handle:
                log.info(f"    Serper: @{handle}")

        if handle:
            df.at[idx, "instagram_handle"] = f"@{handle}" if not handle.startswith("@") else handle
            ok += 1
        else:
            log.warning(f"    NOT FOUND")
            fail += 1

        time.sleep(DELAY)

        if (ok + fail) % AUTOSAVE == 0:
            save_csv(df, path)

    log.info(f"\n  Phase 2 Done: {ok} found, {fail} not found")
    save_csv(df, path)


# =====================================================================
#  Phase 3a: IG Followers via Serper snippet (FAST, no browser)
# =====================================================================

def get_followers_from_serper(handle: str, debug=False) -> Optional[int]:
    """
    Search: site:instagram.com {handle}
    Parse followers from ANY field in Google results:
    snippet, title, knowledgeGraph, sitelinks, etc.
    """
    if not SERPER_API_KEY:
        return None
    h = handle.strip().lstrip("@")
    try:
        r = requests.post(
            "https://google.serper.dev/search",
            headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
            json={"q": f"site:instagram.com {h}", "num": 5},
            timeout=TIMEOUT,
        )
        if r.status_code != 200:
            if debug: log.info(f"    Serper HTTP {r.status_code}")
            return None

        data = r.json()

        if debug:
            import json as jmod
            log.info(f"    --- Serper raw response ---")
            log.info(jmod.dumps(data, indent=2, ensure_ascii=False)[:2000])
            log.info(f"    --- end ---")

        # Search ALL text in the entire response for "XXX followers"
        all_text = jmod_dumps_flat(data) if not debug else ""
        if not all_text:
            all_text = str(data)

        # Method 1: Search in organic results
        for item in data.get("organic", []):
            for key in ["snippet", "title", "description"]:
                text = item.get(key, "")
                if text:
                    m = re.search(r"([\d,.]+\s*[KMBkmb]?)\s*[Ff]ollowers", text)
                    if m:
                        v = _num(m.group(1))
                        if v and v > 0:
                            if debug: log.info(f"    Found in organic.{key}: {m.group(0)}")
                            return v

        # Method 2: Search in knowledgeGraph
        kg = data.get("knowledgeGraph", {})
        if kg:
            kg_text = str(kg)
            m = re.search(r"([\d,.]+\s*[KMBkmb]?)\s*[Ff]ollowers", kg_text)
            if m:
                v = _num(m.group(1))
                if v and v > 0:
                    if debug: log.info(f"    Found in knowledgeGraph: {m.group(0)}")
                    return v

        # Method 3: Search entire response as string
        m = re.search(r"([\d,.]+\s*[KMBkmb]?)\s*[Ff]ollowers", all_text)
        if m:
            v = _num(m.group(1))
            if v and v > 0:
                if debug: log.info(f"    Found in full response: {m.group(0)}")
                return v

    except Exception as e:
        if debug: log.error(f"    Serper error: {e}")
    return None


def jmod_dumps_flat(obj) -> str:
    """Flatten any JSON object to a single string for regex search"""
    try:
        return json.dumps(obj, ensure_ascii=False)
    except:
        return str(obj)


def run_phase3a(path, limit=0):
    """Phase 3a: Fast IG followers via Serper (2s/row, no browser)"""
    if not SERPER_API_KEY:
        log.error("  SERPER_API_KEY not set!"); return

    df = load_csv(path)
    total = len(df)
    todo = [i for i in range(total)
            if not is_empty(df.at[i, "instagram_handle"]) and is_empty(df.at[i, "ig_followers"])]
    if limit > 0: todo = todo[:limit]

    log.info("=" * 60)
    log.info("  PHASE 3a: IG Followers via Serper (FAST, no browser)")
    log.info(f"  To check: {len(todo)} | Est: ~{len(todo)*2//60}min")
    log.info("=" * 60)

    ok, fail, done = 0, 0, 0
    for idx in todo:
        seller = df.at[idx, "seller_name"]
        handle = df.at[idx, "instagram_handle"].strip().lstrip("@")
        done += 1

        count = get_followers_from_serper(handle)
        if count is not None:
            df.at[idx, "ig_followers"] = str(count)
            log.info(f"  [{done}/{len(todo)}] @{handle} -> {count:,}")
            ok += 1
        else:
            if done % 20 == 0:
                log.info(f"  [{done}/{len(todo)}] progress... ({ok} found so far)")
            fail += 1

        time.sleep(DELAY)

        if done % AUTOSAVE == 0:
            save_csv(df, path)

    log.info(f"\n  Phase 3a Done: {ok} found via Serper, {fail} need screenshot")
    if fail > 0:
        log.info(f"  Run --phase3b for remaining {fail} (screenshot + OCR)")
    save_csv(df, path)


# =====================================================================
#  Phase 3b: IG Followers screenshot + OCR (fallback for 3a misses)
# =====================================================================

def run_phase3b(path, limit=0):
    """Phase 3b: Screenshot fallback for rows Serper couldn't find"""
    if not check_tesseract(): return
    if IG_LEFT == 0:
        log.error("Set IG coordinates! --find-position"); return
    pyautogui.FAILSAFE = True; pyautogui.PAUSE = 0.3

    df = load_csv(path)
    total = len(df)
    todo = [i for i in range(total)
            if not is_empty(df.at[i, "instagram_handle"]) and is_empty(df.at[i, "ig_followers"])]
    if limit > 0: todo = todo[:limit]

    log.info("=" * 60)
    log.info("  PHASE 3b: IG Followers (screenshot + OCR fallback)")
    log.info(f"  Remaining: {len(todo)} | Est: ~{len(todo)*8//60}min")
    log.info(f"  !! DO NOT TOUCH MOUSE !!")
    log.info("=" * 60)

    if len(todo) == 0:
        log.info("  All followers filled! Nothing to do."); return

    for i in range(5, 0, -1): log.info(f"  {i}..."); time.sleep(1)

    ok, fail, done = 0, 0, 0
    for idx in todo:
        seller = df.at[idx, "seller_name"]
        handle = df.at[idx, "instagram_handle"].strip().lstrip("@")
        done += 1
        eta = (len(todo) - done) * 8 // 60
        log.info(f"\n  [{done}/{len(todo)}] {seller} (@{handle}) ETA:{eta}min")

        try:
            url = f"https://www.instagram.com/{handle}/"
            webbrowser.open(url)
            time.sleep(IG_WAIT)

            ocr = ocr_region(IG_LEFT, IG_TOP, IG_RIGHT, IG_BOTTOM)
            r = parse_ig_followers(ocr)

            if r["count"] is not None:
                df.at[idx, "ig_followers"] = str(r["count"])
                log.info(f"    {r['count']:,} followers [{r['info']}]")
                ok += 1
            else:
                log.warning(f"    NOT FOUND ({r['info']})")
                fail += 1

            pyautogui.hotkey("ctrl", "w"); time.sleep(1)
        except pyautogui.FailSafeException:
            log.error("EMERGENCY STOP"); save_csv(df, path); return
        except Exception as e:
            log.error(f"    {e}"); fail += 1
            try: pyautogui.hotkey("ctrl", "w"); time.sleep(1)
            except: pass

        if done % AUTOSAVE == 0:
            save_csv(df, path)

    log.info(f"\n  Phase 3b Done: {ok} ok, {fail} fail")
    save_csv(df, path)


# =====================================================================
#  Phase 4: Find missing domains (Serper + Gemini)
# =====================================================================

def serper_search(query: str, num=5) -> list:
    if not SERPER_API_KEY: return []
    try:
        r = requests.post(
            "https://google.serper.dev/search",
            headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
            json={"q": query, "num": num}, timeout=TIMEOUT,
        )
        if r.status_code == 200:
            return r.json().get("organic", [])
    except: pass
    return []


def gemini_pick_domain(seller_name: str, country: str, tiktok: str, results: list) -> Optional[str]:
    """Use Gemini to pick the correct official domain from search results"""
    if not GEMINI_API_KEY or not results:
        return None

    # Build context
    candidates = []
    for i, item in enumerate(results):
        candidates.append(f"{i+1}. {item.get('title','')} | {item.get('link','')} | {item.get('snippet','')[:100]}")
    candidates_text = "\n".join(candidates)

    prompt = f"""You are helping identify the official e-commerce website for a merchant.

Merchant name: {seller_name}
Country: {country or 'unknown'}
TikTok: {tiktok or 'unknown'}

Google search results:
{candidates_text}

Which result is most likely the merchant's official e-commerce website?
Rules:
- Skip Wikipedia, news sites, social media (facebook, instagram, tiktok, youtube, linkedin)
- Skip Amazon, eBay, Etsy marketplace listings
- Skip review sites (trustpilot, yelp)
- Prefer .com, country TLDs, or Shopify/ecommerce domains
- If none match, say "NONE"

Reply with ONLY the domain (e.g. "example.com") or "NONE". Nothing else."""

    try:
        r = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}",
            headers={"Content-Type": "application/json"},
            json={"contents": [{"parts": [{"text": prompt}]}]},
            timeout=30,
        )
        if r.status_code == 200:
            data = r.json()
            text = data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "").strip()
            # Clean response
            text = text.strip().strip('"').strip("'").lower()
            text = re.sub(r"^https?://", "", text)
            text = re.sub(r"^www\.", "", text)
            text = text.rstrip("/")
            if text and text != "none" and "." in text and " " not in text:
                return text
    except Exception as e:
        log.debug(f"    Gemini error: {e}")
    return None


def run_phase4(path, limit=0):
    log.info("=" * 60)
    log.info("  PHASE 4: Find Missing Domains (Serper + Gemini)")
    log.info("=" * 60)

    if not SERPER_API_KEY:
        log.error("  SERPER_API_KEY not set!"); return
    if not GEMINI_API_KEY:
        log.warning("  GEMINI_API_KEY not set, will use basic heuristic")

    df = load_csv(path)
    total = len(df)
    todo = [i for i in range(total) if is_empty(df.at[i, "domain"])]
    if limit > 0: todo = todo[:limit]
    log.info(f"  {len(todo)}/{total} missing domain")

    ok, fail = 0, 0
    for idx in todo:
        seller = df.at[idx, "seller_name"]
        country = df.at[idx, "register_country_code"].strip()
        tiktok = df.at[idx, "tiktok_account_name"].strip()
        log.info(f"\n  [{idx+1}/{total}] {seller} (country:{country}, tt:{tiktok})")

        # Build search query
        query = f"{seller} official website"
        if country:
            query += f" {country}"
        if tiktok and tiktok.lower() not in ("null", "nan", "none", ""):
            query += f" {tiktok}"

        results = serper_search(query)
        if not results:
            log.warning(f"    No search results")
            fail += 1
            continue

        # Method 1: Gemini picks the domain
        domain = gemini_pick_domain(seller, country, tiktok, results)
        if domain:
            log.info(f"    Gemini: {domain}")
            df.at[idx, "domain"] = domain
            ok += 1
        else:
            # Method 2: heuristic — first non-social-media domain
            skip_domains = {"facebook.com", "instagram.com", "tiktok.com", "twitter.com",
                           "youtube.com", "linkedin.com", "reddit.com", "amazon.com",
                           "wikipedia.org", "yelp.com", "trustpilot.com", "ebay.com",
                           "etsy.com", "x.com", "crunchbase.com"}
            found = None
            for item in results:
                d = urlparse(item.get("link", "")).netloc.replace("www.", "").lower()
                if d and d not in skip_domains and "." in d:
                    found = d
                    break
            if found:
                log.info(f"    Heuristic: {found}")
                df.at[idx, "domain"] = found
                ok += 1
            else:
                log.warning(f"    NOT FOUND")
                fail += 1

        time.sleep(DELAY)
        if (ok + fail) % AUTOSAVE == 0:
            save_csv(df, path)

    log.info(f"\n  Phase 4 Done: {ok} found, {fail} not found")
    save_csv(df, path)


# =====================================================================
#  Phase 5: Filter High Performers
# =====================================================================

def run_phase5(path):
    log.info("=" * 60)
    log.info(f"  PHASE 5: Filter (GMV >= ${FILTER_GMV:,}/yr OR IG >= {FILTER_IG:,})")
    log.info("=" * 60)

    df = load_csv(path)
    out_dir = os.path.dirname(os.path.abspath(path))
    hp_path = os.path.join(out_dir, "high_performing_sellers.csv")

    results = []
    for _, row in df.iterrows():
        try: ig = int(float(row.get("ig_followers", "0") or "0"))
        except: ig = 0
        try: gmv = int(float(row.get("storeleads_gmv_usd", "0") or "0"))
        except: gmv = 0

        if ig >= FILTER_IG or gmv >= FILTER_GMV:
            results.append(row)
            r = []
            if ig >= FILTER_IG: r.append(f"IG {ig:,}")
            if gmv >= FILTER_GMV: r.append(f"GMV ${gmv:,}")
            log.info(f"  PASS {row.get('seller_name','')} ({' | '.join(r)})")
        else:
            log.info(f"  FAIL {row.get('seller_name','')} (IG:{ig:,} GMV:{gmv:,})")

    hp_df = pd.DataFrame(results)
    save_csv(hp_df, hp_path)
    log.info(f"\n  {len(results)}/{len(df)} qualify -> {hp_path}")


# =====================================================================
#  Find Position
# =====================================================================

def find_position():
    print("=" * 55)
    print("  Coordinate Finder (5 positions)")
    print("=" * 55)

    print("\n  [1/5] Store Leads extension icon")
    for i in range(5, 0, -1): print(f"    {i}..."); time.sleep(1)
    ex, ey = pyautogui.position()
    print(f"    => ({ex}, {ey})")

    print("\n  [2/5] Store Leads panel BOTTOM-RIGHT")
    for i in range(8, 0, -1): print(f"    {i}..."); time.sleep(1)
    srx, sry = pyautogui.position()
    print(f"    => ({srx}, {sry})")

    print("\n  [3/5] Store Leads 'Close' button (trigger error first)")
    for i in range(8, 0, -1): print(f"    {i}..."); time.sleep(1)
    cx, cy = pyautogui.position()
    print(f"    => ({cx}, {cy})")

    print("\n  [4/5] IG followers area TOP-LEFT")
    for i in range(8, 0, -1): print(f"    {i}..."); time.sleep(1)
    ilx, ily = pyautogui.position()
    print(f"    => ({ilx}, {ily})")

    print("\n  [5/5] IG followers area BOTTOM-RIGHT")
    for i in range(8, 0, -1): print(f"    {i}..."); time.sleep(1)
    irx, iry = pyautogui.position()
    print(f"    => ({irx}, {iry})")

    print(f"\n{'='*55}")
    print(f"  EXTENSION_X = {ex}")
    print(f"  EXTENSION_Y = {ey}")
    print(f"  SL_RIGHT  = {srx}")
    print(f"  SL_BOTTOM = {sry}")
    print(f"  CLOSE_X   = {cx}")
    print(f"  CLOSE_Y   = {cy}")
    print(f"  IG_LEFT   = {ilx}")
    print(f"  IG_TOP    = {ily}")
    print(f"  IG_RIGHT  = {irx}")
    print(f"  IG_BOTTOM = {iry}")


# =====================================================================
#  Debug
# =====================================================================

def run_debug_gmv(path):
    if not check_tesseract(): return
    pyautogui.FAILSAFE = True; pyautogui.PAUSE = 0.3
    df = load_csv(path)
    d, s = None, None
    for _, r in df.iterrows():
        if not is_empty(r.get("domain", "")):
            d, s = r["domain"], r["seller_name"]; break
    if not d: print("No domain!"); return
    print(f"\n  DEBUG GMV: {s} ({d})")
    for i in range(3, 0, -1): print(f"  {i}..."); time.sleep(1)
    webbrowser.open(f"https://{d}"); time.sleep(PAGE_WAIT)
    ocr = click_and_capture()
    print(f"\n{'='*60}\nOCR:\n{'='*60}\n{ocr}\n{'='*60}")
    r = parse_estimated_sales(ocr)
    if r["monthly"] is not None:
        print(f"\n  ${r['monthly']:,}/mo -> ${r['annual']:,}/yr [{r['info']}]")
    else: print(f"\n  FAILED: {r['info']}")
    pyautogui.hotkey("ctrl", "w")


def run_debug_ig(path):
    if not check_tesseract(): return
    pyautogui.FAILSAFE = True; pyautogui.PAUSE = 0.3
    df = load_csv(path)
    h, s = None, None
    for _, r in df.iterrows():
        if not is_empty(r.get("instagram_handle", "")):
            h, s = r["instagram_handle"], r["seller_name"]; break
    if not h: print("No IG!"); return
    handle = h.strip().lstrip("@")
    url = f"https://www.instagram.com/{handle}/"
    print(f"\n  DEBUG IG: {s} (@{handle}) -> {url}")
    for i in range(3, 0, -1): print(f"  {i}..."); time.sleep(1)
    webbrowser.open(url); time.sleep(IG_WAIT)
    ocr = ocr_region(IG_LEFT, IG_TOP, IG_RIGHT, IG_BOTTOM, f"ig_{handle}")
    print(f"\n{'='*60}\nOCR:\n{'='*60}\n{ocr}\n{'='*60}")
    r = parse_ig_followers(ocr)
    if r["count"]: print(f"\n  {r['count']:,} followers [{r['info']}]")
    else: print(f"\n  FAILED: {r['info']}")
    pyautogui.hotkey("ctrl", "w")


# =====================================================================
#  CLI
# =====================================================================

CSV_PATH = ""

if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description="Merchant Enrichment Pipeline",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p.add_argument("--input", help="CSV path")
    p.add_argument("--limit", type=int, default=0, help="Only process first N rows (0=all)")
    p.add_argument("--phase1", action="store_true", help="GMV (has domain)")
    p.add_argument("--phase2", action="store_true", help="Find IG handles")
    p.add_argument("--phase3a", action="store_true", help="IG followers via Serper (fast)")
    p.add_argument("--phase3b", action="store_true", help="IG followers screenshot (fallback)")
    p.add_argument("--phase4", action="store_true", help="Find missing domains")
    p.add_argument("--phase5", action="store_true", help="Filter high performers")
    p.add_argument("--find-position", action="store_true")
    p.add_argument("--debug-gmv", action="store_true")
    p.add_argument("--debug-ig", action="store_true")
    p.add_argument("--debug-serper", action="store_true", help="Test Serper follower lookup for 1 handle")
    a = p.parse_args()

    if a.input: CSV_PATH = a.input
    LIMIT = a.limit

    if a.find_position: find_position()
    elif a.debug_gmv: run_debug_gmv(a.input)
    elif a.debug_ig: run_debug_ig(a.input)
    elif a.debug_serper:
        df = load_csv(a.input)
        for _, r in df.iterrows():
            h = r.get("instagram_handle", "").strip()
            if h and not is_empty(h):
                log.info(f"Testing Serper for: {h}")
                result = get_followers_from_serper(h, debug=True)
                if result:
                    log.info(f"  => {result:,} followers")
                else:
                    log.warning(f"  => NOT FOUND")
                break
    elif a.phase1: run_phase1(a.input, LIMIT)
    elif a.phase2: run_phase2(a.input, LIMIT)
    elif a.phase3a: run_phase3a(a.input, LIMIT)
    elif a.phase3b: run_phase3b(a.input, LIMIT)
    elif a.phase4: run_phase4(a.input, LIMIT)
    elif a.phase5: run_phase5(a.input)
    else:
        p.print_help()
        print("""
=== Pipeline Flow ===

  Phase 1: GMV (rows with domain)
    python pipeline.py --phase1 --input data.csv

  Phase 2: Find IG handles (with OR without domain)
    python pipeline.py --phase2 --input data.csv
    Uses: website scrape + Serper + tiktok handle + country

  Phase 3a: IG followers via Serper (FAST, no browser!)
    python pipeline.py --phase3a --input data.csv

  Phase 3b: IG followers screenshot (only for 3a misses)
    python pipeline.py --phase3b --input data.csv

  Phase 4: Find missing domains (Serper + Gemini)
    python pipeline.py --phase4 --input data.csv
    then re-run --phase1 for new domains

  Phase 5: Filter
    python pipeline.py --phase5 --input data.csv
    -> high_performing_sellers.csv

  Add --limit N to any phase to test with N rows:
    python pipeline.py --phase2 --input data.csv --limit 5
""")
