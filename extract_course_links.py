from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from lxml import html
import time
import os
from datetime import datetime

# ------------------------------
# Config
# ------------------------------
BASE_URL = "https://www.coursera.org/browse"  # your base url for Explore
# Your Explore Categories container XPath
EXPLORE_CONTAINER_XPATH = "/html/body/div[2]/div/div/div/div[3]/div[1]/div/div[1]"
# Optional “Show more” within Explore, if it exists
SHOW_MORE_XPATHS = [
    ".//button[normalize-space()='Show more']",
    ".//button[contains(., 'Show') and contains(., 'more')]",
    ".//button[contains(@aria-label, 'Show') and contains(@aria-label, 'more')]",
]
# Your exact Next button XPath (button, not SVG)
NEXT_BUTTON_XPATH_PRIMARY = "/html/body/div[2]/div/div/div/div[3]/div[1]/div/div[10]/div/section/div/div/div/div[3]/div/div/div[3]/div[2]/div/nav/ul/li[9]/button"
# Robust fallbacks for Next button across UI variants
NEXT_BUTTON_FALLBACKS = [
    "//*[@aria-label='Next Page' and not(@disabled)]",
    "//nav[contains(@class,'pagination')]//button[not(@disabled)][@aria-label='Next']",
    "//nav[contains(@class,'pagination')]//li[contains(@class,'next')]//button[not(@disabled)]",
    "//button[.//span[contains(.,'Next')]][not(@disabled)]",
    "//button[contains(@class,'pagination')]//span[contains(.,'Next')]/ancestor::button[not(@disabled)]",
]
# Your original absolute XPath for course links
ABS_LINK_XPATH = "/html/body/div[2]/div/div/div/div[3]/div[1]/div/div[10]/div/section/div/div/div/div[3]/div/div/div[2]/div[1]/div/ul/li/div/div/div/div/div/div[2]/div[1]/div[2]/a/@href"
# Robust fallback that finds course detail links regardless of layout
FALLBACK_LINK_XPATH = "//a[contains(@href, '/learn/')]/@href"
# Output path (as requested)
OUT_PATH = r"C:\Web Scrapping\xpaths.txt"  # same as your original
# Safety cap on pages
MAX_PAGES = 500
PAGE_DELAY_SEC = 1.0

# ------------------------------
# Selenium setup
# ------------------------------
chrome_options = Options()
# If Next button isn't found in headless mode, try commenting this line to run non-headless:
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
# Help headless reliability
chrome_options.add_argument(
    "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)
chrome_options.add_argument("--disable-dev-shm-usage")

service = Service(r"C:\Users\Admin\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe")
driver = webdriver.Chrome(service=service, options=chrome_options)
wait = WebDriverWait(driver, 20)

# ------------------------------
# Timing + streaming write helpers
# ------------------------------
def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def load_already_written(path):
    """
    Load existing links from xpaths.txt so we don't re-write duplicates.
    If the file doesn't exist yet, return an empty set.
    """
    if not os.path.exists(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            # only keep lines that look like URLs
            return set(line.strip() for line in f if line.strip().startswith("http"))
    except Exception:
        return set()

def append_run_header(path, start_ts):
    """
    Append a visible run header to help auditing:
    ---- RUN START [timestamp] ----
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"\n---- RUN START [{start_ts}] ----\n")
        f.flush()

def append_run_footer(path, end_ts, total_new, total_unique, duration_sec):
    """
    Append the run footer:
    ---- RUN END [timestamp] (new: X, unique: Y, duration: Zs) ----
    """
    with open(path, "a", encoding="utf-8") as f:
        f.write(
            f"---- RUN END   [{end_ts}] (new_written={total_new}, unique_all={total_unique}, duration={duration_sec:.2f}s) ----\n"
        )
        f.flush()

def append_links(path, links, already_written):
    """
    Write each new link immediately (append mode).
    Returns how many NEW lines were written.
    """
    new_count = 0
    with open(path, "a", encoding="utf-8") as f:
        for link in sorted(links):
            if link not in already_written:
                f.write(link + "\n")
                new_count += 1
                # update the set so later pages/categories don't re-write
                already_written.add(link)
        f.flush()
    return new_count

# ------------------------------
# Helpers (unchanged logic, minor edits for streaming)
# ------------------------------
def accept_cookies_if_present():
    """
    Dismiss common cookie banners so elements behind are interactable.
    Coursera uses OneTrust frequently.
    """
    selectors = [
        "//*[@id='onetrust-accept-btn-handler']",
        "//button[normalize-space()='Accept']",
        "//button[contains(., 'Accept all')]",
        "//button[contains(@class,'ot-sdk-container')]//button[contains(@id,'accept')]",
    ]
    for xp in selectors:
        try:
            btns = driver.find_elements(By.XPATH, xp)
            for b in btns:
                if b.is_displayed():
                    driver.execute_script("arguments[0].click();", b)
                    time.sleep(0.5)
                    return
        except Exception:
            continue

def expand_show_more_if_present(container):
    """Reveal hidden chips if 'Show more' exists."""
    for xp in SHOW_MORE_XPATHS:
        try:
            btns = container.find_elements(By.XPATH, xp)
            for b in btns:
                if b.is_displayed():
                    driver.execute_script("arguments[0].click();", b)
                    time.sleep(0.5)
                    return
        except Exception:
            continue

def discover_categories_from_explore_container():
    """
    Use your Explore container XPath to find all /browse/* anchors.
    Returns dict {visible_text: absolute_url}
    """
    try:
        container = wait.until(EC.presence_of_element_located((By.XPATH, EXPLORE_CONTAINER_XPATH)))
    except Exception:
        return {}
    expand_show_more_if_present(container)

    anchors = container.find_elements(By.XPATH, ".//a[contains(@href, '/browse/')]")
    categories = {}
    for a in anchors:
        try:
            href = (a.get_attribute("href") or "").strip()
            text = (a.text or "").strip()
            if not href:
                continue
            if not href.startswith("http"):
                href = "https://www.coursera.org" + href
            if not text:
                text = (
                    a.get_attribute("aria-label")
                    or a.get_attribute("data-click-value")
                    or href.rsplit("/", 1)[-1]
                )
            categories[text] = href
        except Exception:
            continue
    return categories

def discover_categories_fallback_from_page_source():
    """
    If Explore container fails (e.g., headless rendering),
    scan the whole page for /browse/<category> anchors.
    """
    tree = html.fromstring(driver.page_source)
    hrefs = set(tree.xpath("//a[starts-with(@href, '/browse/')]/@href"))
    categories = {}
    for href in hrefs:
        full = "https://www.coursera.org" + href if not href.startswith("http") else href
        key = href.strip("/").split("/")[-1] or full
        categories[key] = full
    return categories

def extract_links_from_page_source(page_source):
    """
    Parse the rendered HTML with lxml, try your absolute XPath first,
    and fall back to a resilient relative XPath if needed.
    """
    tree = html.fromstring(page_source)
    links = tree.xpath(ABS_LINK_XPATH)
    if not links:
        links = tree.xpath(FALLBACK_LINK_XPATH)

    # Normalize to full URLs
    full_links = set()
    for href in links:
        if not href:
            continue
        if href.startswith("http"):
            full_links.add(href)
        else:
            full_links.add("https://www.coursera.org" + href)
    return full_links

def find_clickable_next():
    """
    Locate the Next button via your XPath and verify it's clickable (not disabled).
    Try your primary first, then fallbacks.
    """
    try:
        btn = wait.until(EC.presence_of_element_located((By.XPATH, NEXT_BUTTON_XPATH_PRIMARY)))
        if (btn.get_attribute("disabled") is None) and (btn.get_attribute("aria-disabled") != "true"):
            return btn
    except Exception:
        pass
    for xp in NEXT_BUTTON_FALLBACKS:
        try:
            btn = wait.until(EC.presence_of_element_located((By.XPATH, xp)))
            if (btn.get_attribute("disabled") is None) and (btn.get_attribute("aria-disabled") != "true"):
                return btn
        except Exception:
            continue
    return None

def click_next(btn):
    """Scroll to and click the Next button; try JS click for reliability."""
    try:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
        time.sleep(0.3)
        driver.execute_script("arguments[0].click();", btn)
        return True
    except Exception:
        try:
            btn.click()
            return True
        except Exception:
            return False

def wait_for_course_grid():
    """Ensure some anchors exist before scraping links."""
    try:
        wait.until(EC.presence_of_all_elements_located((By.XPATH, "//a")))
    except Exception:
        time.sleep(1.0)

def scrape_category(category_name, category_url, already_written, global_link_set):
    """
    Visit a category URL, paginate with Next, and collect all /learn/ links.
    Streams links to disk on every page.
    """
    driver.get(category_url)
    time.sleep(1.5)
    accept_cookies_if_present()
    wait_for_course_grid()
    page_index = 1
    total_new_written = 0

    while page_index <= MAX_PAGES:
        # Parse current page
        page_source = driver.page_source
        page_links = extract_links_from_page_source(page_source)

        # Add to global set and append immediately to file
        before_size = len(global_link_set)
        global_link_set |= page_links
        # write only new-to-file links to disk
        new_written = append_links(OUT_PATH, page_links, already_written)
        total_new_written += new_written

        print(f"[{category_name}] Page {page_index}: +{len(page_links)} "
              f"(page-new-written={new_written}, total {len(global_link_set)})")

        # Keep a reference element to detect DOM staleness after clicking next
        old_anchor = None
        try:
            anchors_now = driver.find_elements(By.XPATH, "//a[contains(@href, '/learn/')]")
            if anchors_now:
                old_anchor = anchors_now[0]
        except Exception:
            pass

        # Find & click Next
        next_btn = find_clickable_next()
        if not next_btn:
            print(f"[{category_name}] Next button not found or disabled. Stopping.")
            break
        clicked = click_next(next_btn)
        if not clicked:
            print(f"[{category_name}] Failed to click Next button. Stopping.")
            break

        # Wait for page to change (staleness) or fallback sleep
        try:
            if old_anchor is not None:
                wait.until(EC.staleness_of(old_anchor))
            else:
                time.sleep(2.0)
        except Exception:
            time.sleep(2.0)

        page_index += 1
        time.sleep(PAGE_DELAY_SEC)  # polite delay

    return total_new_written

# ------------------------------
# Main
# ------------------------------
all_links_global = set()
already_written = load_already_written(OUT_PATH)

start_ts = now_str()
start_wall = time.time()
append_run_header(OUT_PATH, start_ts)
print(f"== RUN START [{start_ts}] ==")

try:
    # 1) Go to base browse page and enumerate categories from Explore
    driver.get(BASE_URL)
    time.sleep(2.0)
    accept_cookies_if_present()
    categories = discover_categories_from_explore_container()
    if not categories:
        # Fallback if Explore container couldn't be located in headless
        categories = discover_categories_fallback_from_page_source()
    print(f"Found {len(categories)} categories in Explore.")
    for name, href in sorted(categories.items()):
        print(f"- {name}: {href}")

    if not categories:
        print("WARNING: No categories discovered. Try running non-headless or verify the XPath on the base URL.")
    else:
        # 2) Scrape each discovered category
        total_new_written_run = 0
        for cat_name, cat_url in categories.items():
            print(f"\n=== Scraping category: {cat_name} ===")
            new_written_cat = scrape_category(cat_name, cat_url, already_written, all_links_global)
            total_new_written_run += new_written_cat
            print(f"=== {cat_name}: {len(all_links_global)} unique course links so far (new-written-this-cat={new_written_cat}) ===")

finally:
    driver.quit()

# ------------------------------
# End-of-run summary + footer
# ------------------------------
end_ts = now_str()
duration_sec = time.time() - start_wall
print(f"\nTotal unique course links across all categories: {len(all_links_global)}\n")
for link in sorted(all_links_global):
    print(link)

append_run_footer(OUT_PATH, end_ts, total_new=len(already_written), total_unique=len(all_links_global), duration_sec=duration_sec)
print(f"\n== RUN END   [{end_ts}] (unique_all={len(all_links_global)}, duration={duration_sec:.2f}s) ==")
print(f"Streaming writes completed. File: {OUT_PATH}")
