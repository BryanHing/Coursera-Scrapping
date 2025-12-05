
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Reads course links from xpaths.txt, scrapes details, and writes to your Google Sheet.

Google Sheet:
- Opened directly by URL (SPREADSHEET_URL)
- Creates/uses a worksheet/tab named WORKSHEET_NAME ("Courses" by default)
- Adds header if missing and appends rows

Dependencies:
   pip install gspread google-auth google-auth-oauthlib google-auth-httplib2
"""

import os
import re
import json
import time
import traceback
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

# Google Sheets
import gspread
from google.oauth2.service_account import Credentials

# ------------- Configuration -------------
URLS_FILE = r"C:\Web Scrapping\xpaths.txt"  # one URL per line
SERVICE_ACCOUNT_JSON = r"C:\Web Scrapping\n8n-integration-bryan-c9074da0d443.json"  # your service account key
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1-8cKeEunV0oc1O_8sZCRviU98D6EfA8yR0HOqnFinRo/edit?gid=0#gid=0"
WORKSHEET_NAME = "Courses"  # tab name; will be created if missing

REQUEST_TIMEOUT = 25
REQUEST_DELAY_SEC = 2.0
MAX_RETRIES = 3

COLUMNS = [
    "course_url",
    "title",
    "course_category",
    "rating",
    "language",
    "total_hours",
    "num_modules",
    "skill_acquire",
    "description",
    "experience_required",
    "instructor_name",
    "instructor_rating",
    "instructor_total_students",
    "instructor_total_courses",
    "provider",
    "num_registered",
    "course content"
]

# ------------- Utilities -------------

def clean_text(t: str) -> str:
    if not t:
        return ""
    return re.sub(r"\s+", " ", t).strip()

def extract_numbers(text: str):
    if not text:
        return []
    return [float(x.replace(",", "")) for x in re.findall(r"\b\d[\d,]*\.?\d*\b", text)]

def normalize_number(val):
    if val in (None, "", [], {}):
        return ""
    try:
        return float(str(val).replace(",", ""))
    except Exception:
        return ""

def iso8601_duration_to_hours(dur: str):
    if not dur:
        return None
    pattern = re.compile(
        r"P(?:(?P<weeks>\d+)W)?(?:(?P<days>\d+)D)?"
        r"(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)?"
    )
    m = pattern.match(dur)
    if not m:
        return None
    w = int(m.group("weeks") or 0)
    d = int(m.group("days") or 0)
    h = int(m.group("hours") or 0)
    mins = int(m.group("minutes") or 0)
    s = int(m.group("seconds") or 0)
    return round(w*168 + d*24 + h + mins/60 + s/3600, 2)

def join_list_str(items, sep="; "):
    items = [clean_text(i) for i in items if i and clean_text(i)]
    return sep.join(items)

def get_domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return ""

def load_urls(file_path: str):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"URLs file not found: {file_path}")
    urls = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            u = line.strip()
            if u and not u.startswith("#"):
                urls.append(u)
    return urls

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
            if 200 <= resp.status_code < 300:
                return resp
            elif resp.status_code in (403, 429):
                time.sleep(REQUEST_DELAY_SEC * attempt)
            else:
                time.sleep(REQUEST_DELAY_SEC)
        except requests.RequestException:
            time.sleep(REQUEST_DELAY_SEC * attempt)
    raise RuntimeError(f"Failed to fetch URL after {MAX_RETRIES} attempts: {url}")

def parse_jsonld_blocks(soup: BeautifulSoup):
    blocks = []
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            text = (tag.string or tag.get_text() or "").strip()
            if not text:
                continue
            data = json.loads(text)
            if isinstance(data, list):
                blocks.extend([d for d in data if isinstance(d, dict)])
            elif isinstance(data, dict):
                blocks.append(data)
        except Exception:
            continue
    return blocks

def find_language(soup: BeautifulSoup):
    html = soup.find("html")
    if html and html.has_attr("lang"):
        return clean_text(html["lang"])
    meta_lang = soup.find("meta", attrs={"http-equiv": "content-language"})
    if meta_lang and meta_lang.get("content"):
        return clean_text(meta_lang["content"])
    for label in soup.select("div,span,li,p"):
        t = clean_text(label.get_text())
        if re.search(r"\bLanguage\b", t, flags=re.I):
            parts = t.split(":")
            return clean_text(parts[1] if len(parts) > 1 else t)
    return ""

def merge_dicts(base: dict, *overrides):
    out = dict(base)
    for d in overrides:
        for k, v in d.items():
            if v not in (None, "", [], {}):
                out[k] = v
    return out

# ------------- Parsers -------------

def parse_from_jsonld(jsonlds: list):
    data = {}
    course_objs = [j for j in jsonlds if j.get("@type") in ("Course", "CreativeWork", "Product")]
    if not course_objs and jsonlds:
        course_objs = jsonlds

    for j in course_objs:
        name = j.get("name") or j.get("headline")
        desc = j.get("description")

        provider = ""
        prov = j.get("provider") or j.get("brand") or j.get("publisher")
        if isinstance(prov, dict):
            provider = prov.get("name") or ""
        elif isinstance(prov, str):
            provider = prov

        agg = j.get("aggregateRating") or {}
        rating = agg.get("ratingValue") or agg.get("rating") or None

        num_registered = ""
        inter = j.get("interactionStatistic") or []
        if isinstance(inter, dict):
            inter = [inter]
        for is_obj in inter:
            if is_obj.get("@type") == "InteractionCounter":
                count = is_obj.get("userInteractionCount")
                if count:
                    num_registered = count

        duration = j.get("timeRequired") or j.get("duration") or ""
        total_hours = iso8601_duration_to_hours(duration)

        instructor_name = ""
        instr = j.get("instructor") or j.get("author") or []
        if isinstance(instr, dict):
            instructor_name = instr.get("name") or ""
        elif isinstance(instr, list) and instr:
            names = []
            for x in instr:
                if isinstance(x, dict) and x.get("name"):
                    names.append(x.get("name"))
                elif isinstance(x, str):
                    names.append(x)
            instructor_name = join_list_str(names)

        skills = []
        about = j.get("about")
        if isinstance(about, list):
            for item in about:
                if isinstance(item, dict) and item.get("name"):
                    skills.append(item.get("name"))
                elif isinstance(item, str):
                    skills.append(item)
        elif isinstance(about, dict) and about.get("name"):
            skills.append(about.get("name"))
        elif isinstance(about, str):
            skills.append(about)

        data.update({
            "title": clean_text(name) if name else "",
            "description": clean_text(desc) if desc else "",
            "provider": clean_text(provider) if provider else "",
            "rating": normalize_number(rating),
            "total_hours": total_hours if total_hours is not None else "",
            "instructor_name": clean_text(instructor_name),
            "skill_acquire": join_list_str(skills),
            "num_registered": normalize_number(num_registered) if num_registered else "",
        })
    return data

def parse_coursera(soup: BeautifulSoup):
    data = {}
    title_tag = soup.find("h1") or soup.find("h2")
    data["title"] = clean_text(title_tag.get_text()) if title_tag else ""

    desc = ""
    desc_tag = soup.select_one('[data-testid="description"]') or soup.find("meta", attrs={"name": "description"})
    if desc_tag:
        desc = desc_tag.get("content") if desc_tag.name == "meta" else desc_tag.get_text()
    data["description"] = clean_text(desc)

    rating = ""
    rating_tag = soup.find(attrs={"data-test": "ratings-count"}) or soup.find("span", string=re.compile(r"\d\.\d"))
    if rating_tag:
        nums = extract_numbers(rating_tag.get_text())
        rating = nums[0] if nums else ""
    data["rating"] = rating

    skills = [clean_text(li.get_text()) for li in soup.select('[data-testid="skill"]')]
    if not skills:
        skills = [clean_text(li.get_text()) for li in soup.select('ul li') if re.search(r"skill|learn|you will", li.get_text(), re.I)]
    data["skill_acquire"] = join_list_str(skills)

    crumbs = [clean_text(x.get_text()) for x in soup.select('[data-testid="breadcrumb"] a, nav[aria-label="breadcrumb"] a')]
    data["course_category"] = crumbs[-2] if len(crumbs) >= 2 else (crumbs[0] if crumbs else "")

    data["language"] = find_language(soup)

    modules = [clean_text(x.get_text()) for x in soup.select('[data-testid="syllabus"] h3, [data-testid="module"] h3, section h3')]
    data["num_modules"] = len([m for m in modules if m])
    data["course content"] = "\n".join([m for m in modules if m][:50])

    exp = ""
    for li in soup.select("li, p"):
        t = li.get_text()
        if re.search(r"Prerequisite|Level|Experience", t, re.I):
            exp = clean_text(t); break
    data["experience_required"] = exp

    data["provider"] = "Coursera"

    instructors = [clean_text(x.get_text()) for x in soup.select('[data-testid="instructor-name"], [data-testid="instructor"] a, [data-testid="instructor-card"] h3')]
    data["instructor_name"] = join_list_str(instructors)

    enrolled_tag = soup.find(string=re.compile(r"enrolled|learners", re.I))
    if enrolled_tag:
        nums = extract_numbers(str(enrolled_tag))
        data["num_registered"] = nums[0] if nums else ""

    return data

def parse_udemy(soup: BeautifulSoup):
    data = {}
    title_tag = soup.find("h1") or soup.find("h2")
    data["title"] = clean_text(title_tag.get_text()) if title_tag else ""

    meta_desc = soup.find("meta", attrs={"name": "description"})
    desc = meta_desc.get("content") if meta_desc and meta_desc.get("content") else ""
    if not desc:
        desc_tag = soup.select_one("[data-purpose='lead-course-description'], .course-description")
        desc = desc_tag.get_text() if desc_tag else ""
    data["description"] = clean_text(desc)

    rating_tag = soup.find("span", attrs={"data-purpose": "rating-number"}) or soup.find("span", string=re.compile(r"\d\.\d"))
    data["rating"] = normalize_number(rating_tag.get_text()) if rating_tag else ""

    data["language"] = find_language(soup)

    skills = [clean_text(li.get_text()) for li in soup.select("[data-purpose='course-objectives'] li")]
    data["skill_acquire"] = join_list_str(skills)

    sections = [clean_text(h.get_text()) for h in soup.select("h3, .section--title")]
    data["course content"] = "\n".join([s for s in sections if s][:50])
    data["num_modules"] = len([s for s in sections if s])

    exp_tag = soup.find(string=re.compile(r"Prerequisites|Requirements|Level", re.I))
    data["experience_required"] = clean_text(exp_tag) if exp_tag else ""

    instructor = [clean_text(x.get_text()) for x in soup.select("[data-purpose='instructor-name'], .instructor--instructor__title, .instructor-links__link")]
    data["instructor_name"] = join_list_str(instructor)

    instr_rating = instr_students = instr_courses = ""
    for t in soup.select("span, div"):
        txt = t.get_text()
        if re.search(r"Instructor Rating", txt, re.I):
            nums = extract_numbers(txt); instr_rating = nums[0] if nums else instr_rating
        if re.search(r"Students|learners", txt, re.I):
            nums = extract_numbers(txt); instr_students = nums[0] if nums else instr_students
        if re.search(r"courses", txt, re.I):
            nums = extract_numbers(txt); instr_courses = nums[0] if nums else instr_courses
    data["instructor_rating"] = instr_rating
    data["instructor_total_students"] = instr_students
    data["instructor_total_courses"] = instr_courses

    en_tag = soup.find(string=re.compile(r"students|enrolled|learners", re.I))
    data["num_registered"] = extract_numbers(str(en_tag))[0] if en_tag and extract_numbers(str(en_tag)) else ""

    crumbs = [clean_text(a.get_text()) for a in soup.select("nav[aria-label='breadcrumb'] a")]
    data["course_category"] = crumbs[-2] if len(crumbs) >= 2 else (crumbs[0] if crumbs else "")

    data["provider"] = "Udemy"
    return data

def parse_edx(soup: BeautifulSoup):
    data = {}
    title_tag = soup.find("h1") or soup.find("h2")
    data["title"] = clean_text(title_tag.get_text()) if title_tag else ""
    desc_tag = soup.find("meta", attrs={"name": "description"})
    desc = desc_tag.get("content") if desc_tag and desc_tag.get("content") else ""
    if not desc:
        d = soup.select_one(".course-description, [data-testid='course-description'], .about-course")
        desc = d.get_text() if d else ""
    data["description"] = clean_text(desc)
    data["rating"] = ""
    data["language"] = find_language(soup)
    skills = [clean_text(li.get_text()) for li in soup.select("[data-testid='skills'] li, .skills-list li")]
    data["skill_acquire"] = join_list_str(skills)
    modules = [clean_text(h.get_text()) for h in soup.select("section h3, .syllabus h3, .course-section h3")]
    data["course content"] = "\n".join([m for m in modules if m][:50])
    data["num_modules"] = len([m for m in modules if m])
    exp_tag = soup.find(string=re.compile(r"Prerequisites|Level|Background", re.I))
    data["experience_required"] = clean_text(exp_tag) if exp_tag else ""
    crumbs = [clean_text(x.get_text()) for x in soup.select("nav[aria-label='breadcrumb'] a")]
    data["course_category"] = crumbs[-2] if len(crumbs) >= 2 else (crumbs[0] if crumbs else "")
    instr = [clean_text(x.get_text()) for x in soup.select(".instructor h3, [data-testid='instructor-name']")]
    data["instructor_name"] = join_list_str(instr)
    data["num_registered"] = ""
    data["provider"] = "edX"
    return data

def parse_linkedin_learning(soup: BeautifulSoup):
    data = {}
    title_tag = soup.find("h1") or soup.find("h2")
    data["title"] = clean_text(title_tag.get_text()) if title_tag else ""
    desc_tag = soup.find("meta", attrs={"name": "description"})
    data["description"] = clean_text(desc_tag.get("content") if desc_tag and desc_tag.get("content") else "")
    r = soup.find("span", string=re.compile(r"\d\.\d"))
    data["rating"] = normalize_number(r.get_text()) if r else ""
    data["language"] = find_language(soup)
    skills = [clean_text(x.get_text()) for x in soup.select("[data-test-id='skills-list'] li, .skills li")]
    data["skill_acquire"] = join_list_str(skills)
    modules = [clean_text(x.get_text()) for x in soup.select("ol li, .toc__item, .toc-section__title")]
    data["course content"] = "\n".join([m for m in modules if m][:50])
    data["num_modules"] = len([m for m in modules if m])
    lvl_tag = soup.find(string=re.compile(r"Beginner|Intermediate|Advanced|Level", re.I))
    data["experience_required"] = clean_text(lvl_tag) if lvl_tag else ""
    instr = [clean_text(x.get_text()) for x in soup.select(".instructor__name, [data-test-id='instructor-name']")]
    data["instructor_name"] = join_list_str(instr)
    data["instructor_rating"] = ""
    data["instructor_total_students"] = ""
    data["instructor_total_courses"] = ""
    data["num_registered"] = ""
    data["provider"] = "LinkedIn Learning"
    return data

def generic_parse(soup: BeautifulSoup, domain: str):
    data = {}
    title_tag = soup.find("h1") or soup.find("h2") or soup.title
    data["title"] = clean_text(title_tag.get_text()) if hasattr(title_tag, "get_text") else clean_text(str(title_tag))
    desc_tag = soup.find("meta", attrs={"name": "description"}) or soup.find("meta", attrs={"property": "og:description"})
    desc = desc_tag.get("content") if desc_tag and desc_tag.get("content") else ""
    if not desc:
        p = soup.find("p"); desc = p.get_text() if p else ""
    data["description"] = clean_text(desc)
    r = soup.find(string=re.compile(r"\b\d\.\d\b"))
    data["rating"] = extract_numbers(str(r))[0] if r and extract_numbers(str(r)) else ""
    data["language"] = find_language(soup)
    skills = [clean_text(li.get_text()) for li in soup.select("ul li") if re.search(r"learn|skill|you will|outcomes", li.get_text(), re.I)]
    data["skill_acquire"] = join_list_str(skills)
    modules = [clean_text(h.get_text()) for h in soup.select("section h3, section h2, .module h3")]
    data["course content"] = "\n".join([m for m in modules if m][:50])
    data["num_modules"] = len([m for m in modules if m])
    exp_tag = soup.find(string=re.compile(r"Prerequisites|Requirements|Level|Experience", re.I))
    data["experience_required"] = clean_text(exp_tag) if exp_tag else ""
    crumbs = [clean_text(a.get_text()) for a in soup.select("nav[aria-label='breadcrumb'] a, .breadcrumb a")]
    data["course_category"] = crumbs[-2] if len(crumbs) >= 2 else (crumbs[0] if crumbs else "")
    instr = [clean_text(x.get_text()) for x in soup.select(".instructor, .teacher, .author a, .author")]
    data["instructor_name"] = join_list_str(instr)
    data["provider"] = domain
    reg_tag = soup.find(string=re.compile(r"enrolled|students|learners|registered", re.I))
    data["num_registered"] = extract_numbers(str(reg_tag))[0] if reg_tag and extract_numbers(str(reg_tag)) else ""
    data["instructor_rating"] = ""
    data["instructor_total_students"] = ""
    data["instructor_total_courses"] = ""
    return data

def extract_course_details(session: requests.Session, url: str) -> dict:
    resp = fetch_url(session, url)
    soup = BeautifulSoup(resp.text, "lxml")

    result = {col: "" for col in COLUMNS}
    result["course_url"] = url

    jsonlds = parse_jsonld_blocks(soup)
    from_ld = parse_from_jsonld(jsonlds)
    domain = get_domain(url)

    if "coursera.org" in domain:
        specific = parse_coursera(soup)
    elif "udemy.com" in domain:
        specific = parse_udemy(soup)
    elif "edx.org" in domain or "learning.edx.org" in domain:
        specific = parse_edx(soup)
    elif "linkedin.com" in domain and "learning" in domain:
        specific = parse_linkedin_learning(soup)
    else:
        specific = generic_parse(soup, domain)

    merged = merge_dicts(result, from_ld, specific)

    merged["rating"] = normalize_number(merged.get("rating"))
    merged["total_hours"] = normalize_number(merged.get("total_hours"))
    merged["num_modules"] = int(merged.get("num_modules") or 0)
    merged["instructor_rating"] = normalize_number(merged.get("instructor_rating"))
    merged["instructor_total_students"] = normalize_number(merged.get("instructor_total_students"))
    merged["instructor_total_courses"] = normalize_number(merged.get("instructor_total_courses"))
    merged["num_registered"] = normalize_number(merged.get("num_registered"))

    merged["description"] = clean_text(merged.get("description", ""))[:4000]
    merged["skill_acquire"] = clean_text(merged.get("skill_acquire", ""))[:2000]
    merged["course content"] = clean_text(merged.get("course content", ""))[:8000]
    merged["experience_required"] = clean_text(merged.get("experience_required", ""))[:2000]
    merged["title"] = clean_text(merged.get("title", ""))[:512]
    merged["course_category"] = clean_text(merged.get("course_category", ""))[:256]
    merged["language"] = clean_text(merged.get("language", ""))[:64]
    merged["provider"] = clean_text(merged.get("provider", ""))[:128]
    merged["instructor_name"] = clean_text(merged.get("instructor_name", ""))[:512]

    return merged

# ------------- Google Sheets I/O -------------

def get_gsheet_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON, scopes=scopes)
    return gspread.authorize(creds)

def ensure_sheet_and_header(client):
    # Open by the exact URL you provided
    sh = client.open_by_url(SPREADSHEET_URL)
    try:
        ws = sh.worksheet(WORKSHEET_NAME)
    except gspread.WorksheetNotFound:
        # Create the worksheet if it does not exist
        ws = sh.add_worksheet(title=WORKSHEET_NAME, rows=2000, cols=len(COLUMNS))
    # Ensure header row
    header = ws.row_values(1)
    if header != COLUMNS:
        ws.update("A1", [COLUMNS])
    return sh, ws

def append_rows(ws, rows: list):
    if not rows:
        return
    values = []
    for r in rows:
        values.append([r.get(col, "") for col in COLUMNS])
    ws.append_rows(values, value_input_option="USER_ENTERED")

# ------------- Main -------------

def main():
    urls = load_urls(URLS_FILE)
    session = create_session()
    client = get_gsheet_client()
    sh, ws = ensure_sheet_and_header(client)

    batch = []
    for i, url in enumerate(urls, start=1):
        try:
            print(f"[{i}/{len(urls)}] Fetching: {url}")
            data = extract_course_details(session, url)
            batch.append(data)
            if len(batch) >= 5:
                append_rows(ws, batch)
                batch = []
            time.sleep(REQUEST_DELAY_SEC)  # polite delay
        except Exception as e:
            print(f"Error processing {url}: {e}")
            traceback.print_exc()
            time.sleep(REQUEST_DELAY_SEC)

    if batch:
        append_rows(ws, batch)
    print("Done.")

if __name__ == "__main__":
    main()



