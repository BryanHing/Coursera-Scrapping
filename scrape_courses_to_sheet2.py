
# -*- coding: utf-8 -*-
"""
Coursera course scraper (XPath-first) with DEBUG HTML output — v3 (fixed)
What’s new (v3):
- Stronger description extractor:
 * Prefers div[4] description; fallback div[5]
 * Filters marketing/testimonial lines
 * Looks for headings like "About this Course" to lock onto canonical copy
 * Tries CSS data-testid, JSON-LD, and meta tags
 * Ranks candidates to pick the most accurate (least marketing, richest text)
- More robust num_registered: primary → instructor fallback → labelled containers → strict phrase scan
- Generates per-page debug HTML showing the extracted summary, XPath hit counts, and text previews.
FIX in this file:
- Restored helper: extract_offered_by(...) to avoid NameError.
- Language extraction fixed to use user-provided XPath, stripping "Taught in ".
- Description filter: drop any line that begins with "Offered by ..." to avoid meta snippets.
- Rating guard: avoid misreading durations (e.g., "4 weeks") as numeric ratings.
- Time-to-complete (fixed as requested): prefer exact duration via primary XPath; else fallback XPath; else default "Flexible schedule".

USAGE:
 1) Set config (URLS_FILE, SERVICE_ACCOUNT_JSON, SPREADSHEET_URL).
 2) Run: python only_time_bug.py

DEPENDENCIES:
 pip install requests beautifulsoup4 lxml gspread google-auth
"""
import os
import re
import time
import traceback
import pathlib
import json
import requests
from bs4 import BeautifulSoup
from lxml import html as lxml_html
from lxml import etree
try:
    import gspread
    from google.oauth2.service_account import Credentials
except Exception:
    gspread = None
    Credentials = None

# --------------------------- CONFIG ---------------------------
URLS_FILE = r"C:\\Web Scrapping\\xpaths.txt"  # one URL per line
SERVICE_ACCOUNT_JSON = r"C:\\Web Scrapping\\n8n-integration-bryan-c9074da0d443.json"
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1-8cKeEunV0oc1O_8sZCRviU98D6EfA8yR0HOqnFinRo/edit?gid=0#gid=0"
WORKSHEET_NAME = "Courses"
REQUEST_TIMEOUT = 25
REQUEST_DELAY_SEC = 2.0
MAX_RETRIES = 3
ENABLE_SHEETS = True  # set False to skip Google Sheets writes
DEBUG_OUTPUT_DIR = pathlib.Path("debug_html")
BATCH_SIZE = 5

# --------------------------- COLUMNS ---------------------------
COLUMNS = [
    "course_url", "title", "course_category", "course_subcategory",
    "rating", "language", "Time to complete", "num_modules",
    "skill_acquire", "description", "experience_required",
    "num_registered", "course content", "offered_by",
]

# --------------------------- XPaths ---------------------------
XPATHS = {
    # Title fallbacks
    "title_h1": "//h1",
    "title_h2": "//h2",
    "title_tag": "//title/text()",

    # Breadcrumbs
    "course_category": "/html/body/div[2]/div/main/section[1]/div/div/div/div[1]/nav/ol/li[3]/a",
    "course_subcategory": "/html/body/div[2]/div/main/section[1]/div/div/div/div[1]/nav/ol/li[4]/a",

    # Rating (primary + fallback)
    "rating_primary": "/html/body/div[2]/div/main/section[2]/div/div/div[2]/div/div[2]/div[2]/div/div/div[1]",
    "rating_fallback": "/html/body/div[2]/div/main/section[2]/div/div/div[2]/div/div[2]/div[2]/div/div/div[1]/font/font",

    # Language (user-provided XPath)
    "language": "/html/body/div[2]/div/main/section[2]/div/div/div[4]/div/div/div[2]/div[2]/div[3]/div/span",

    # Time to complete (primary + fallback provided by you)
    "time_primary": "/html/body/div[2]/div/main/section[2]/div/div/div[2]/div/div[2]/div[3]/div/div/div[1]",
    "time_flexible": "/html/body/div[2]/div/main/section[2]/div/div/div[2]/div/div[2]/div[4]/div/div/div[1]",

    # num_modules: read number from span (primary: div[5], fallback: div[4])
    "num_modules_span_primary": "/html/body/div[2]/div/main/div[5]/div/div/div/div[1]/h2/span",
    "num_modules_span_fallback": "/html/body/div[2]/div/main/div[4]/div/div/div/div[1]/h2/span",

    # anchor-count container (module tiles/links)
    "num_modules_anchor_container": "/html/body/div[2]/div/main/section[2]/div/div/div[2]/div/div[2]/div[1]/div/div/div[1]/div/a",

    # skill_acquire ULs
    "skill_acquire_ul_primary": "/html/body/div[2]/div/main/section[2]/div/div/div[4]/div/div/div[2]/ul",
    "skill_acquire_ul_fallback": "/html/body/div[2]/div/main/section[2]/div/div/div[4]/div/div/div[1]/ul",

    # description containers (prefer div[4], fallback div[5])
    "description_container_primary": "/html/body/div[2]/div/main/div[4]/div/div/div/div[1]/div/div",
    "description_container_fallback": "/html/body/div[2]/div/main/div[5]/div/div/div/div[1]/div/div",

    # experience_required
    "experience_level": "/html/body/div[2]/div/main/section[2]/div/div/div[2]/div/div[2]/div[3]/div/div/div[1]",

    # num_registered
    "num_registered": "/html/body/div[2]/div/main/section[2]/div/div/div[1]/div[1]/div/div/div/div[2]/div[4]/p/span/strong/span",
    # user-requested fallback path (instructor details area)
    "num_registered_fallback_instructor": "/html/body/div[2]/div/main/div[4]/div/div/div/div[3]/div/div[1]/div[2]/div/div[2]/div[3]/span[3]/span",

    # course content containers (prefer div[4], fallback div[5])
    "course_content_container_primary": "/html/body/div[2]/div/main/div[4]/div/div/div/div[2]/div/div",
    "course_content_container_fallback": "/html/body/div[2]/div/main/div[5]/div/div/div/div[2]/div/div",

    # offered_by (primary + alt)
    "offered_by_primary": "/html/body/div[2]/div/main/div[5]/div/div/div/div[3]/div/div[2]/div[2]/div/div[2]/a/span",
    "offered_by_alt": "/html/body/div[2]/div/main/div[4]/div/div/div/div[3]/div/div[2]/div[2]/div/div[2]/a/span",
}

# --------------------------- Utilities ---------------------------
def clean_text(t: str) -> str:
    if not t:
        return ""
    return re.sub(r"\s+", " ", t).strip()

def fix_text_encoding(s: str) -> str:
    if not s:
        return ""
    replacements = {
        "â€™": "’", "â€œ": "“", "â€\u009D": "”", "â€“": "–", "â€”": "—",
        "â€¢": "•", "â€˜": "‘", "â€": "”", "Â": " ", "â€¢": "•", "â€¦": "…",
    }
    out = s
    for bad, good in replacements.items():
        out = out.replace(bad, good)
    return clean_text(out)

def extract_numbers(text: str):
    if not text:
        return []
    return [float(x.replace(",", "")) for x in re.findall(r"\b\d[\d,]*\.?\d*\b", text)]

def first_number(text: str):
    nums = extract_numbers(text)
    return nums[0] if nums else None

LANGUAGE_MAP = {
    "EN": "English", "ENG": "English", "ENGLISH": "English",
    "BM": "Malay", "MS": "Malay", "MALAY": "Malay",
    "ZH": "Chinese", "CN": "Chinese", "CHINESE": "Chinese",
    "ES": "Spanish", "ESP": "Spanish", "SPANISH": "Spanish",
    "FR": "French", "FRENCH": "French",
    "DE": "German", "GERMAN": "German",
}

def normalize_language_full(term: str) -> str:
    t = clean_text(term).upper()
    if not t:
        return ""
    return LANGUAGE_MAP.get(t, clean_text(term))

def create_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    return s

def fetch_url(session: requests.Session, url: str):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            if not resp.encoding or resp.encoding.lower() == "iso-8859-1":
                resp.encoding = resp.apparent_encoding or "utf-8"
            if 200 <= resp.status_code < 300:
                return resp
            elif resp.status_code in (403, 429):
                time.sleep(REQUEST_DELAY_SEC * attempt)
            else:
                time.sleep(REQUEST_DELAY_SEC)
        except requests.RequestException:
            time.sleep(REQUEST_DELAY_SEC * attempt)
    raise RuntimeError(f"Failed to fetch URL after {MAX_RETRIES} attempts: {url}")

def load_urls(file_path: str):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"URLs file not found: {file_path}")
    with open(file_path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip() and not line.startswith("#")]

# --------------------------- Filters ---------------------------
def is_level(s: str) -> bool:
    return bool(re.search(r"\b(Beginner|Intermediate|Advanced|All Levels)\b", s or "", re.I))

def is_duration(s: str) -> bool:
    return bool(re.search(r"\b(week|weeks|hour|hours|hr|hrs|minute|minutes|min)\b", s or "", re.I))

def is_noise(line: str) -> bool:
    if not line:
        return True
    l = line.strip()
    return bool(re.search(r"^(Explore more|Status: Preview|Preview|Learn more)$", l, re.I))

def is_modules_line(line: str) -> bool:
    if not line:
        return False
    l = line.strip().lower()
    return bool(re.search(r"^\d+\s+modules$", l)) or bool(re.search(r"there are\s+\d+\s+modules", l))

MARKETING_PHRASES = [
    r"Build your subject-matter expertise",
    r"This course is part of the .* Specialization",
    r"When you enroll in this course, you'll also be enrolled",
    r"Learn new concepts from industry experts",
    r"Gain a foundational understanding",
    r"Develop job-relevant skills",
    r"Earn a shareable career certificate",
]

def is_marketing(line: str) -> bool:
    if not line:
        return False
    l = line.strip()
    for pat in MARKETING_PHRASES:
        if re.search(pat, l, re.I):
            return True
    return False

def is_testimonial(line: str) -> bool:
    if not line:
        return False
    l = line.strip()
    return bool(re.search(r"\bLearner since\b", l, re.I)) or bool(re.search(r"Coursera allows me to learn without limits", l, re.I))

# --------------------------- XPath helpers ---------------------------
def xp_text(doc, xp: str) -> str:
    try:
        nodes = doc.xpath(xp)
        if not nodes:
            return ""
        n = nodes[0]
        txt = n if isinstance(n, str) else n.text_content()
        return clean_text(txt)
    except Exception:
        return ""

def xp_norm(doc, xp: str) -> str:
    try:
        return clean_text(doc.xpath(f"normalize-space({xp})"))
    except Exception:
        return ""

# --------------------------- NEW: offered_by cleaner ---------------------------
def extract_offered_by(raw: str) -> str:
    """
    Normalize the 'offered_by' institution string:
    - Remove 'Offered by'/'Learn more'
    - Cut trailing marketing ('has', 'is')
    - Keep first entity-like chunk
    - Map known short forms
    """
    t = clean_text(raw)
    if not t:
        return ""
    # Remove 'Offered by' and 'Learn more'
    t = re.sub(r"\bOffered by\b.*", "", t, flags=re.I)
    t = re.sub(r"\bLearn more\b", "", t, flags=re.I)
    # Keep first chunk (split by dot/bullet/newline)
    t = re.split(r"[\.•\n]+", t)[0].strip()
    # Cut at ' has ' / ' is ' to drop marketing blurbs
    t = re.split(r"\s+(has|is)\s+", t)[0].strip()
    SHORT_MAP = {"CalArts": "California Institute of the Arts", "MoMA": "The Museum of Modern Art"}
    return SHORT_MAP.get(t, t)

# --------------------------- Description extraction (hardened) ---------------------------
def extract_description(doc, soup):
    raw_source = {}

    def filter_lines(text):
        parts = re.split(r"(?<=[.!?])\s+|\n+", text)
        cleaned = []
        for p in parts:
            if not p:
                continue
            # NEW: drop meta-style opener "Offered by ..."
            if re.match(r"^\s*Offered by\b", p, flags=re.I):
                continue
            if is_marketing(p) or is_testimonial(p):
                continue
            cleaned.append(p)
        return clean_text(" ".join(cleaned))

    candidates = []

    # 1) containers (prefer div[4], then div[5])
    for label, xp in ("div4", XPATHS["description_container_primary"]), ("div5", XPATHS["description_container_fallback"]):
        try:
            nodes = doc.xpath(xp)
            if nodes:
                txt = clean_text(nodes[0].text_content())
                filtered = filter_lines(txt)
                if filtered:
                    candidates.append(("container_" + label, filtered))
                raw_source["container_" + label] = filtered
        except Exception:
            pass

    # 2) look for headings indicating canonical description
    heading_labels = [
        "About this Course", "About the Course", "Course description",
        "What you'll learn", "Overview"
    ]
    for hxp in ["//h2", "//h3"]:
        try:
            for h in doc.xpath(hxp):
                ht = clean_text(h.text_content())
                if any(ht.lower() == hl.lower() for hl in heading_labels):
                    block = h.getparent()
                    if block is not None:
                        txt = clean_text(block.text_content())
                        filtered = filter_lines(txt)
                        if filtered:
                            key = "heading_" + ht
                            candidates.append((key, filtered))
                            raw_source[key] = filtered
        except Exception:
            pass

    # 3) CSS data-testid description
    try:
        block = soup.select_one('[data-testid="description"], [data-test="description"]')
        if block:
            txt = clean_text(block.get_text(" "))
            filtered = filter_lines(txt)
            if filtered:
                candidates.append(("css_data_testid", filtered))
                raw_source["css_data_testid"] = filtered
    except Exception:
        pass

    # 4) JSON-LD description
    try:
        for sc in soup.select('script[type="application/ld+json"]'):
            try:
                data = json.loads(sc.string or sc.text or "{}")
            except Exception:
                continue
            nodes = data if isinstance(data, list) else [data]
            for obj in nodes:
                if isinstance(obj, dict) and (obj.get("@type") in ("Course", "CreativeWork", None)):
                    desc = obj.get("description") or obj.get("about")
                    if isinstance(desc, str) and clean_text(desc):
                        txt = clean_text(desc)
                        filtered = filter_lines(txt)
                        if filtered:
                            candidates.append(("json_ld", filtered))
                            raw_source["json_ld"] = filtered
    except Exception:
        pass

    # 5) meta tags
    for sel, key in ("meta[property='og:description']", "og:description"), ("meta[name='description']", "meta:description"):
        try:
            tag = soup.select_one(sel)
            if tag and tag.get("content"):
                txt = clean_text(tag["content"])
                filtered = filter_lines(txt)
                if filtered:
                    candidates.append((key, filtered))
                    raw_source[key] = filtered
        except Exception:
            pass

    if not candidates:
        return "", "", raw_source

    # Choose best candidate by score: non-marketing length minus marketing penalty
    def score(text):
        length = len(text)
        penalty = sum(bool(re.search(pat, text, re.I)) for pat in MARKETING_PHRASES) * 100
        return length - penalty

    best = max(candidates, key=lambda c: score(c[1]))
    return best[1], best[0], raw_source

# --------------------------- Duration fallback (NEW, compact) ---------------------------
def extract_duration_from_page(soup) -> str:
    """
    Scan raw page text for a simple duration phrase like '4 weeks', '20 hours', etc.
    Returns a cleaned phrase or '' if not found.
    """
    text = clean_text(soup.get_text(" "))
    m = re.search(r"\b(\d{1,3})\s*(weeks?|hours?|hrs?|minutes?|mins?)\b", text, flags=re.I)
    if m:
        qty = m.group(1)
        unit = m.group(2)
        # normalize unit pluralization
        unit_norm = unit.lower()
        if unit_norm.startswith("hr"):
            unit_norm = "hours" if qty != "1" else "hour"
        elif unit_norm.startswith("min"):
            unit_norm = "minutes" if qty != "1" else "minute"
        elif unit_norm.startswith("week"):
            unit_norm = "weeks" if qty != "1" else "week"
        elif unit_norm.startswith("hour"):
            unit_norm = "hours" if qty != "1" else "hour"
        elif unit_norm.startswith("minute"):
            unit_norm = "minutes" if qty != "1" else "minute"
        return f"{qty} {unit_norm}"
    return ""

# --------------------------- Extractor ---------------------------
def extract_by_xpaths(html_text: str, url: str) -> dict:
    doc = lxml_html.fromstring(html_text)
    soup = BeautifulSoup(html_text, "lxml")
    raw_debug = {}

    # Title
    title = xp_text(doc, XPATHS["title_h1"]) or xp_text(doc, XPATHS["title_h2"]) or xp_text(doc, XPATHS["title_tag"])

    # Category/Subcategory
    course_category = xp_norm(doc, XPATHS["course_category"])
    course_subcategory = xp_norm(doc, XPATHS["course_subcategory"])
    raw_debug["course_category_raw"] = course_category
    raw_debug["course_subcategory_raw"] = course_subcategory

    # Rating (with guard to avoid durations)
    rating_txt = xp_text(doc, XPATHS["rating_primary"]) or xp_text(doc, XPATHS["rating_fallback"])
    rating_val = None
    if rating_txt and not is_duration(rating_txt):
        # Look for patterns typical of ratings, e.g., "4.8", "4.8 stars", "4.8 out of 5"
        m = re.search(r"\b(\d\.\d)\b(?:\s*(?:stars?|out of 5))?", rating_txt, flags=re.I)
        if m:
            try:
                rating_val = float(m.group(1))
            except Exception:
                rating_val = None
        else:
            # fallback to first number only if it does NOT look like weeks/hours
            if not re.search(r"\b(week|weeks|hour|hours|hr|hrs|minute|minutes|min)\b", rating_txt, re.I):
                rating_val = first_number(rating_txt)
    rating = rating_val if isinstance(rating_val, (int, float)) else "N/A"
    raw_debug["rating_raw"] = rating_txt

    # Language (UPDATED logic)
    language_raw = xp_text(doc, XPATHS["language"]) or (soup.find("html").get("lang") if soup.find("html") else "")
    # Strip "Taught in " prefix if present
    if language_raw:
        language_raw = re.sub(r"^\s*Taught in\s+", "", language_raw, flags=re.I).strip()
    language = normalize_language_full(language_raw) if language_raw else "N/A"
    raw_debug["language_raw"] = language_raw

    # -------------------- Time to complete (ONLY CHANGE) --------------------
    # Prefer explicit duration via primary XPath;
    # else use fallback XPath;
    # else default to "Flexible schedule".
    time_txt_primary = xp_text(doc, XPATHS["time_primary"])
    time_txt = None
    if is_duration(time_txt_primary):
        time_txt = time_txt_primary
    else:
        time_txt_fallback = xp_text(doc, XPATHS["time_flexible"])
        time_txt = time_txt_fallback if is_duration(time_txt_fallback) else (time_txt_fallback or "Flexible schedule")

    time_to_complete = clean_text(time_txt) if time_txt else "Flexible schedule"
    raw_debug["time_primary_raw"] = time_txt_primary
    raw_debug["time_flexible_raw"] = xp_text(doc, XPATHS["time_flexible"])
    # -----------------------------------------------------------------------

    # experience_required
    exp_candidate = time_txt_primary
    experience_required = clean_text(exp_candidate) if is_level(exp_candidate) else ""
    if not experience_required:
        tag = soup.select_one('[data-testid="level"], [data-test="level"]')
        if tag and is_level(tag.get_text()):
            experience_required = clean_text(tag.get_text())
    if not experience_required:
        try:
            parent_nodes = doc.xpath("/html/body/div[2]/div/main/section[2]/div/div/div[2]/div/div[2]")
            if parent_nodes:
                txt_block = clean_text(parent_nodes[0].text_content())
                m = re.search(r"\b(Beginner|Intermediate|Advanced|All Levels)\b", txt_block, re.I)
                if m:
                    experience_required = m.group(0)
        except Exception:
            pass
    if not experience_required:
        experience_required = "N/A"
    raw_debug["experience_required_raw"] = exp_candidate

    # num_modules (span → fallback → anchor count → module headings)
    num_modules_span_primary = xp_text(doc, XPATHS["num_modules_span_primary"])
    num_modules_span_fallback = xp_text(doc, XPATHS["num_modules_span_fallback"]) if not num_modules_span_primary else ""
    num_modules_txt = num_modules_span_primary or num_modules_span_fallback
    num_modules_val = first_number(num_modules_txt)

    num_modules_anchor_count = 0
    if num_modules_val is None:
        try:
            anchor_nodes = doc.xpath(XPATHS["num_modules_anchor_container"])
            num_modules_anchor_count = len(anchor_nodes)
            if num_modules_anchor_count > 0:
                num_modules_val = float(num_modules_anchor_count)
        except Exception:
            pass

    if num_modules_val is None:
        mod_headings = soup.select('[data-testid="module"] h3')
        num_modules_val = len(mod_headings) if mod_headings else None

    num_modules = int(num_modules_val) if isinstance(num_modules_val, (int, float)) else "N/A"
    raw_debug["num_modules_span_primary_raw"] = num_modules_span_primary
    raw_debug["num_modules_span_fallback_raw"] = num_modules_span_fallback
    raw_debug["num_modules_anchor_count"] = num_modules_anchor_count

    # skill_acquire
    skill_items = []
    for xp in (XPATHS["skill_acquire_ul_primary"], XPATHS["skill_acquire_ul_fallback"]):
        try:
            ul_nodes = doc.xpath(xp)
            if ul_nodes:
                for ul in ul_nodes:
                    for li in ul.xpath(".//li"):
                        txt_li = clean_text(li.text_content())
                        if txt_li and not re.search(r"^view all skills$", txt_li, re.I):
                            skill_items.append(txt_li)
                break
        except Exception:
            continue
    skill_acquire = "; ".join(skill_items) if skill_items else "N/A"
    raw_debug["skill_ul_count"] = len(skill_items)

    # description (robust extractor)
    description_txt, desc_source, desc_sources_raw = extract_description(doc, soup)
    description = fix_text_encoding(description_txt) if description_txt else "N/A"
    raw_debug.update({f"description_source": desc_source, **{f"desc_src_{k}": v for k, v in desc_sources_raw.items()}})

    # --- num_registered with multiple fallbacks ---
    reg_txt = xp_text(doc, XPATHS["num_registered"]) or xp_text(doc, XPATHS["num_registered_fallback_instructor"])
    reg_val = first_number(reg_txt)
    if reg_val is None:
        probable_xpaths = [
            "//section[contains(., 'learners') or contains(., 'students')]",
            "//div[contains(., 'learners') or contains(., 'students')]",
            "//span[contains(., 'learners') or contains(., 'students')]",
        ]
        for xp in probable_xpaths:
            try:
                nodes = doc.xpath(xp)
                for n in nodes:
                    txt = clean_text(n.text_content())
                    m = re.search(r"\b([\d,]+)\b\s*(learners|students|enrolled)", txt, re.I)
                    if m:
                        reg_val = float(m.group(1).replace(",", ""))
                        break
                if reg_val is not None:
                    break
            except Exception:
                continue
    if reg_val is None:
        page_text = clean_text(soup.get_text(" "))
        m = re.search(r"\b([\d,]+)\b\s*(learners|students|enrolled)", page_text, re.I)
        if m:
            try:
                reg_val = float(m.group(1).replace(",", ""))
            except Exception:
                reg_val = None
    num_registered = int(reg_val) if isinstance(reg_val, (int, float)) else "N/A"
    raw_debug["num_registered_raw"] = reg_txt

    # course content (prefer div[4], then div[5], then fallbacks)
    course_content_txt = ""
    for xp in (XPATHS["course_content_container_primary"], XPATHS["course_content_container_fallback"]):
        try:
            nodes = doc.xpath(xp)
            if nodes:
                parts = []
                container = nodes[0]
                for h in container.xpath(".//h2|.//h3"):
                    ht = clean_text(h.text_content())
                    if ht and not is_noise(ht) and not is_modules_line(ht):
                        parts.append(ht)
                for li in container.xpath(".//li"):
                    lt = clean_text(li.text_content())
                    if lt and not is_noise(lt) and not is_modules_line(lt):
                        parts.append(lt)
                if not parts:
                    for p in container.xpath(".//p"):
                        pt = clean_text(p.text_content())
                        if pt and not is_noise(pt) and not is_modules_line(pt):
                            parts.append(pt)
                course_content_txt = "\n".join(parts) if parts else clean_text(container.text_content())
                course_content_txt = "\n".join([
                    l for l in course_content_txt.splitlines()
                    if l.strip() and not is_noise(l) and not is_modules_line(l)
                ])
                if course_content_txt:
                    break
        except Exception:
            continue

    # Fallbacks only if the strict containers were empty
    if not course_content_txt:
        parts_fb = []
        try:
            anchor_nodes = doc.xpath(XPATHS["num_modules_anchor_container"])
            for a in anchor_nodes:
                atxt = clean_text(a.text_content())
                if atxt and not is_noise(atxt) and not is_modules_line(atxt):
                    parts_fb.append(atxt)
        except Exception:
            pass
        for h in soup.select('[data-testid="module"] h3'):
            ht = clean_text(h.get_text())
            if ht and not is_noise(ht) and not is_modules_line(ht):
                parts_fb.append(ht)
        for li in soup.select('[data-testid="syllabus"] li'):
            lt = clean_text(li.get_text())
            if lt and not is_noise(lt) and not is_modules_line(lt):
                parts_fb.append(lt)
        if parts_fb:
            course_content_txt = "\n".join(parts_fb)

    course_content = course_content_txt if course_content_txt else "N/A"
    raw_debug["course_content_raw"] = course_content_txt

    # offered_by: primary → alt → text fallback ("Offered by …") → clean
    offered_by_raw = ""
    try:
        nodes = doc.xpath(XPATHS["offered_by_primary"])
        if nodes:
            direct_texts = nodes[0].xpath("text()")
            offered_by_raw = clean_text(" ".join(direct_texts))
        if not offered_by_raw:
            nodes_alt = doc.xpath(XPATHS["offered_by_alt"])
            if nodes_alt:
                direct_texts_alt = nodes_alt[0].xpath("text()")
                offered_by_raw = clean_text(" ".join(direct_texts_alt))
    except Exception:
        offered_by_raw = ""

    offered_by_fallback_text = ""
    if not offered_by_raw:
        page_text = soup.get_text(" ")
        m = re.search(r"Offered by\s*[:\-]?\s*(.+?)\s{2,}$", page_text, re.I)
        if m:
            offered_by_fallback_text = clean_text(m.group(1))
            offered_by_raw = offered_by_fallback_text

    offered_by = extract_offered_by(offered_by_raw) if offered_by_raw else "Coursera"
    raw_debug["offered_by_raw_primary"] = offered_by_raw
    raw_debug["offered_by_fallback_text"] = offered_by_fallback_text

    row = {
        "course_url": url,
        "title": title if title else "N/A",
        "course_category": course_category if course_category else "N/A",
        "course_subcategory": course_subcategory if course_subcategory else "N/A",
        "rating": rating,
        "language": language,
        "Time to complete": time_to_complete,
        "num_modules": num_modules,
        "skill_acquire": skill_acquire,
        "description": description,
        "experience_required": experience_required,
        "num_registered": num_registered,
        "course content": course_content,
        "offered_by": offered_by,
    }

    debug_print_focus(url, row, raw_debug)
    return row

# --------------------------- Google Sheets I/O ---------------------------
def get_gsheet_client():
    if not ENABLE_SHEETS:
        return None
    if gspread is None or Credentials is None:
        raise RuntimeError("gspread/google-auth not installed. Set ENABLE_SHEETS=False to skip.")
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON, scopes=scopes)
    return gspread.authorize(creds)

def ensure_sheet_and_header(client):
    if not client:
        return None, None
    sh = client.open_by_url(SPREADSHEET_URL)
    try:
        ws = sh.worksheet(WORKSHEET_NAME)
    except Exception:
        ws = sh.add_worksheet(title=WORKSHEET_NAME, rows=2000, cols=len(COLUMNS))
    header = ws.row_values(1)
    if header != COLUMNS:
        ws.update("A1", [COLUMNS])
    return sh, ws

def append_rows(ws, rows: list):
    if not ws or not rows:
        return
    values = []
    for r in rows:
        row = []
        for col in COLUMNS:
            v = r.get(col, "")
            row.append("N/A" if (isinstance(v, str) and v == "") or v is None else v)
        values.append(row)
    ws.append_rows(values, value_input_option="USER_ENTERED")

# --------------------------- Debug HTML helper ---------------------------
HIGHLIGHT_COLORS = ["#e53935", "#8e24aa", "#3949ab", "#1e88e5", "#00897b",
                    "#7cb342", "#fdd835", "#fb8c00", "#6d4c41", "#546e7a"]

def _wrap_node_with_style(node, color, label):
    if not isinstance(node, etree._Element):
        return
    existing_style = node.get("style", "")
    outline = f"outline: 3px dashed {color}; outline-offset: 2px;"
    node.set("style", (existing_style + "; " + outline).strip("; "))
    node.set("data-debug-label", label)

def _find_nodes(doc, xp):
    try:
        return doc.xpath(xp)
    except Exception:
        return []

def _collect_text_preview(node, max_len=800):
    try:
        txt = node.text_content()
    except Exception:
        txt = ""
    txt = re.sub(r"\s+", " ", txt).strip()
    return (txt[:max_len] + "…") if len(txt) > max_len else txt

def highlight_html_with_xpaths(html_text, xpaths_dict):
    doc = lxml_html.fromstring(html_text)
    matches = {}
    for i, (label, xp) in enumerate(xpaths_dict.items()):
        color = HIGHLIGHT_COLORS[i % len(HIGHLIGHT_COLORS)]
        nodes = _find_nodes(doc, xp)
        previews = []
        for n in nodes:
            _wrap_node_with_style(n, color, label)
            previews.append(_collect_text_preview(n))
        matches[label] = {"count": len(nodes), "color": color, "xpath": xp, "previews": previews}
    modified_html = etree.tostring(doc, encoding="unicode", method="html")
    return modified_html, matches

def _summary_table(row):
    headers = COLUMNS
    tr_rows = []
    from html import escape
    for h in headers:
        v = escape(str(row.get(h, "")))
        tr_rows.append(f"<tr><th>{escape(h)}</th><td>{v}</td></tr>")
    return "\n".join(tr_rows)

def save_debug_html(url, html_text, row, xpaths_dict, out_path):
    from html import escape
    modified_html, matches = highlight_html_with_xpaths(html_text, xpaths_dict)

    legend_items = []
    for label, info in matches.items():
        color, count, xp = info["color"], info["count"], escape(info["xpath"])
        legend_items.append(
            f"<li><span class='swatch' style='background:{color}'></span>"
            f"<strong>{escape(label)}</strong> — <code>{xp}</code> • hits: <strong>{count}</strong></li>"
        )
    legend_html = "\n".join(legend_items)

    previews = []
    for label, info in matches.items():
        items, color, xp = info["previews"], info["color"], escape(info["xpath"])
        body = "\n".join(
            f"<li><div class='preview' style='border-left:6px solid {color}'>"
            f"<div class='preview-xp'><code>{xp}</code></div>"
            f"<div class='preview-txt'>{escape(p)}</div>"
            f"</div></li>" for p in items
        )
    previews.append(
        f"<details class='preview-block' open><summary><span class='swatch' style='background:{color}'></span>"
        f"{escape(label)} ({len(items)} matches)</summary><ul>{body}</ul></details>"
    )
    previews_html = "\n".join(previews)

    srcdoc = escape(modified_html)

    out = f"""<!DOCTYPE html>
<html lang='en'>
<head><meta charset='utf-8' /><title>Coursera Debug — {escape(url)}</title>
<style>
body {{ margin:0; font-family: system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, sans-serif; }}
header {{ position: sticky; top: 0; background: #0f172a; color: #fff; padding: 10px 16px; z-index: 9999; }}
header h1 {{ font-size: 16px; margin: 0; }}
.container {{ display: grid; grid-template-columns: 420px 1fr; min-height: 100vh; }}
.sidebar {{ padding: 12px 16px; border-right: 1px solid #e2e8f0; overflow: auto; max-height: calc(100vh - 48px); }}
.main {{ height: calc(100vh - 48px); }}
iframe {{ width: 100%; height: 100%; border: 0; background: #fff; }}
.swatch {{ display:inline-block; width:14px; height:14px; border-radius:2px; margin-right:6px; vertical-align:middle; }}
.sidebar h2 {{ font-size:15px; margin:16px 0 8px; color:#0f172a; }}
.sidebar table {{ width:100%; border-collapse:collapse; font-size:13px; }}
.sidebar th {{ text-align:left; width:160px; color:#334155; vertical-align:top; padding:6px 4px; border-bottom:1px solid #e2e8f0; }}
.sidebar td {{ padding:6px 4px; border-bottom:1px solid #e2e8f0; }}
.preview-block summary {{ cursor:pointer; font-weight:600; }}
.preview {{ background:#f8fafc; margin:4px 0; padding:6px 8px; border-radius:4px; }}
.preview-xp {{ color:#64748b; font-size:12px; margin-bottom:4px; }}
.preview-txt {{ white-space: pre-wrap; font-size:13px; }}
footer {{ font-size:12px; color:#64748b; padding:8px 16px; border-top:1px solid #e2e8f0; }}
</style></head>
<body>
<header><h1>Debugging extraction for: {escape(url)}</h1></header>
<div class='container'>
  <aside class='sidebar'>
    <h2>Extracted summary</h2>
    <table>{_summary_table(row)}</table>
    <h2>XPath legend</h2>
    <ul>{legend_html}</ul>
    <h2>Matched text previews</h2>
    {previews_html}
  </aside>
  <main class='main'>
    <iframe srcdoc="{srcdoc}"></iframe>
  </main>
</div>
<footer>
 This debug page outlines nodes matched by your XPaths (dashed boxes). If counts are 0, the absolute XPath likely does not exist for this course variation.
</footer>
</body></html>"""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(out)
    return str(out_path)

# --------------------------- Printing ---------------------------
def debug_print_focus(url: str, row: dict, raw_debug: dict):
    print("\n================ XPATH DEBUG ================")
    print(f"course_url : {url}")
    print(f"title : {row.get('title')}")
    print(f"course_category : {row.get('course_category')}")
    print(f"course_subcategory: {row.get('course_subcategory')}")
    print(f"rating : {row.get('rating')}")
    print(f"language : {row.get('language')}")
    print(f"Time to complete : {row.get('Time to complete')}")
    print(f"num_modules : {row.get('num_modules')}")
    print(f"skill_acquire : {row.get('skill_acquire')}")
    print(f"description : {row.get('description')[:800]}")
    print(f"experience_required: {row.get('experience_required')}")
    print(f"num_registered : {row.get('num_registered')}")
    print(f"course content : {row.get('course content')[:400]}")
    print(f"offered_by : {row.get('offered_by')}")
    print("---- RAW MATCHES ----")
    for k, v in raw_debug.items():
        pv = v if isinstance(v, str) else str(v)
        print(f"{k:<30}: {pv[:400]}")
    print("============================================\n")

# --------------------------- Main ---------------------------
def sanitize_filename(s: str) -> str:
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", s)
    return s[:100]

def main():
    urls = load_urls(URLS_FILE)
    session = create_session()
    DEBUG_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    ENABLE_SHEETS_FLAG = False
    client = None
    ws = None
    if ENABLE_SHEETS:
        try:
            client = get_gsheet_client()
            sh, ws = ensure_sheet_and_header(client)
            ENABLE_SHEETS_FLAG = True
        except Exception as e:
            print(f"[WARN] Google Sheets disabled due to: {e}")
            ENABLE_SHEETS_FLAG = False

    batch = []
    for i, url in enumerate(urls, start=1):
        try:
            print(f"[{i}/{len(urls)}] Fetching: {url}")
            resp = fetch_url(session, url)

            # Extract
            row = extract_by_xpaths(resp.text, url)
            batch.append(row)

            # Save raw HTML
            raw_name = sanitize_filename(f"{i:03d}_raw.html")
            with open(DEBUG_OUTPUT_DIR / raw_name, "w", encoding="utf-8") as f:
                f.write(resp.text)

            # Debug HTML with highlighted nodes + previews
            xpaths_to_check = {
                "course_content (div[4])": XPATHS["course_content_container_primary"],
                "course_content (div[5])": XPATHS["course_content_container_fallback"],
                "description (div[4])": XPATHS["description_container_primary"],
                "description (div[5])": XPATHS["description_container_fallback"],
                "num_registered (primary)": XPATHS["num_registered"],
                "num_registered (instructor fallback)": XPATHS["num_registered_fallback_instructor"],
            }
            dbg_name = sanitize_filename(f"{i:03d}_debug.html")
            save_debug_html(url, resp.text, row, xpaths_to_check, DEBUG_OUTPUT_DIR / dbg_name)

            # Push to Sheets in small batches
            if ENABLE_SHEETS_FLAG and len(batch) >= BATCH_SIZE:
                append_rows(ws, batch)
                batch = []

            time.sleep(REQUEST_DELAY_SEC)
        except Exception as e:
            print(f"Error processing {url}: {e}")
            traceback.print_exc()
            time.sleep(REQUEST_DELAY_SEC)

    if ENABLE_SHEETS_FLAG and batch:
        append_rows(ws, batch)

    print("Done.")

if __name__ == "__main__":
    print("Starting Coursera scrape (XPath-first) with debug HTML v3…")
    main()

