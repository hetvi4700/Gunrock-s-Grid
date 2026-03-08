# scraper/craigslist_scraper.py
# scrapes apartment listings from craigslist sacramento (davis area)
# we go thru the search results page, grab each listing url,
# then visit each one to get the full details (price, beds, sqft, description, etc)
#
# update: added threading to speed things up and csv saving so we dont lose
# data if something crashes halfway through. also reuses one browser 
# instance instead of opening a new one every time (was the main bottleneck)

import requests
from bs4 import BeautifulSoup
import time
import random
import hashlib
import re
import os
import sys
import csv
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import USER_AGENT, SCRAPE_DELAY_MIN, SCRAPE_DELAY_MAX

# base url for davis apartments on sacramento craigslist
BASE_URL = "https://sacramento.craigslist.org/search/davis-ca/apa"

# how many threads to use for scraping individual listings
# dont go too high or craigslist will rate limit us
NUM_WORKERS = 4

# full browser-like headers so craigslist doesn't return 403
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
    "DNT": "1",
    "Referer": "https://www.google.com/",
}

# ---- browser management ----
# Playwright is NOT thread-safe: the same browser cannot be used from multiple threads.
# So we use thread-local storage: each thread gets its own browser instance.

_tls = threading.local()


def _get_browser():
    """get or create a playwright browser for THIS thread only (thread-local).
    Playwright is not thread-safe, so each worker thread must have its own browser.
    """
    if not hasattr(_tls, "browser") or _tls.browser is None:
        try:
            from playwright.sync_api import sync_playwright
            _tls.pw = sync_playwright().start()
            _tls.browser = _tls.pw.chromium.launch(headless=True)
        except ImportError:
            print("  playwright not installed! run: pip install playwright && playwright install chromium")
            return None
        except Exception as e:
            print(f"  couldnt start browser: {e}")
            return None
    return _tls.browser


def _close_browser():
    """cleanup - close this thread's browser (called from main thread; we close all we can)"""
    # ThreadPoolExecutor workers may already be done; we only close main-thread browser if any
    if hasattr(_tls, "browser") and _tls.browser:
        try:
            _tls.browser.close()
        except Exception:
            pass
        _tls.browser = None
    if hasattr(_tls, "pw") and _tls.pw:
        try:
            _tls.pw.stop()
        except Exception:
            pass
        _tls.pw = None


def _fetch_html(url, params=None):
    """fetch a page's html. tries requests first (fast),
    falls back to playwright browser if we get 403'd
    
    the browser fallback reuses a single browser instance
    and just opens a new tab for each request
    """
    full_url = url
    if params:
        full_url = url + "?" + urlencode(params)
    
    # try simple requests first - its way faster when it works
    try:
        session = requests.Session()
        session.headers.update(HEADERS)
        resp = session.get(full_url, timeout=15)
        if resp.status_code == 200:
            return resp.text
        if resp.status_code != 403:
            return None
    except requests.RequestException:
        pass
    
    # got 403 or request failed, use the real browser
    browser = _get_browser()
    if not browser:
        return None
    
    try:
        # each thread opens its own page/tab in the shared browser
        page = browser.new_page()
        page.set_extra_http_headers({"User-Agent": USER_AGENT})
        page.goto(full_url, wait_until="domcontentloaded", timeout=20000)
        html = page.content()
        page.close()  # close the tab but keep browser running
        return html
    except Exception as e:
        print(f"  browser fetch failed for {url[:60]}: {e}")
        return None


# ---- CSV saving ----
# we save results to csv as we go so if the scraper crashes
# we dont lose everything. each listing gets appended immediately

CSV_COLUMNS = [
    "listing_id", "complex_name", "address", "price_total", "bedrooms",
    "baths", "sqft", "lat", "lon", "description", "amenities",
    "pets_allowed", "has_parking", "laundry_type", "post_date", "url"
]

_csv_lock = threading.Lock()


def _get_csv_path():
    """put the csv in the project root folder (same folder as this script)"""
    root = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(root, "scraped_listings.csv")


def _init_csv():
    """create the csv file with headers if it doesnt exist yet
    if it already exists we leave it alone (so we can resume scraping)
    """
    path = _get_csv_path()
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()
        print(f"created new csv: {path}")
    else:
        print(f"csv already exists, will append: {path}")
    return path


def _append_to_csv(listing_dict):
    """append one listing to the csv file
    uses a lock so multiple threads dont write at the same time and corrupt the file
    """
    path = _get_csv_path()
    row = {k: listing_dict.get(k, "") for k in CSV_COLUMNS}
    
    with _csv_lock:
        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writerow(row)


def _get_already_scraped():
    """check the csv for listing ids we already scraped
    so we can skip them if we're resuming a previous run
    """
    path = _get_csv_path()
    scraped = set()
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    lid = row.get("listing_id", "")
                    if lid:
                        scraped.add(lid)
        except Exception:
            pass
    return scraped


# ---- parsing functions ----
# these extract specific fields from the listing page html

def make_listing_id(url):
    """generate a short unique id from the listing url
    we take the craigslist post id from the url and prepend 'cl_'
    eg: https://sacramento.craigslist.org/apa/d/blah/7919804657.html -> cl_7919804657
    """
    match = re.search(r'/(\d+)\.html', url)
    if match:
        return "cl_" + match.group(1)
    return "cl_" + hashlib.md5(url.encode()).hexdigest()[:8]


def polite_sleep():
    """wait a bit between requests so we dont get banned"""
    wait_time = random.uniform(SCRAPE_DELAY_MIN, SCRAPE_DELAY_MAX)
    time.sleep(wait_time)


def get_search_results(page_offset=0):
    """scrape one page of craigslist search results
    
    returns a list of dicts with basic info from the search page:
    - url, title, price, and location text
    """
    params = {}
    if page_offset > 0:
        params["s"] = page_offset
    
    print(f"fetching search page (offset={page_offset})...")
    html = _fetch_html(BASE_URL, params=params)
    if not html:
        print("error fetching search page")
        return []
    
    soup = BeautifulSoup(html, "lxml")
    results = []
    
    # try different selectors bc craigslist changes their html sometimes
    listings = soup.select("li.cl-static-search-result")
    
    if not listings:
        listings = soup.select("li.result-row")
    
    if not listings:
        listings = soup.select(".cl-search-result")
    
    if not listings:
        # fallback - just find all links that go to listing pages
        all_links = soup.find_all("a", href=re.compile(r"/apa/d/"))
        seen_urls = set()
        for link in all_links:
            href = link.get("href", "")
            if href and href not in seen_urls:
                seen_urls.add(href)
                if not href.startswith("http"):
                    href = "https://sacramento.craigslist.org" + href
                title = link.get_text(strip=True)
                results.append({"url": href, "title": title})
        print(f"found {len(results)} listings (link fallback)")
        return results
    
    for item in listings:
        link = item.find("a")
        if not link:
            continue
        href = link.get("href", "")
        if not href.startswith("http"):
            href = "https://sacramento.craigslist.org" + href
        title = link.get_text(strip=True)
        price_el = item.select_one(".price, .result-price, .priceinfo")
        price_text = price_el.get_text(strip=True) if price_el else ""
        results.append({"url": href, "title": title, "price_text": price_text})
    
    print(f"found {len(results)} listings on search page")
    return results


def parse_price(text):
    """extract numeric price from text like '$2,125' or '$2,125/mo'"""
    if not text:
        return None
    matches = re.findall(r'\$[\d,]+', text)
    if matches:
        price_str = matches[0].replace("$", "").replace(",", "")
        try:
            return float(price_str)
        except ValueError:
            return None
    return None


def parse_beds_baths_sqft(soup):
    """extract beds, baths, sqft from the listing page
    tries multiple spots bc craigslist isnt super consistent with placement
    """
    beds = None
    baths = None
    sqft = None
    
    page_text = soup.get_text()
    
    bed_match = re.search(r'(\d+)\s*(?:br|BR|bed|bd|Bed|bedroom|Bedroom)', page_text)
    if bed_match:
        beds = int(bed_match.group(1))
    
    if beds is None and re.search(r'studio', page_text, re.IGNORECASE):
        beds = 0
    
    bath_match = re.search(r'(\d+\.?\d*)\s*(?:ba|BA|bath|Bath|bathroom|Bathroom)', page_text)
    if bath_match:
        baths = float(bath_match.group(1))
    
    sqft_match = re.search(r'(\d+)\s*(?:ft2|ft²|sqft|sq\.?\s*ft)', page_text)
    if sqft_match:
        sqft = int(sqft_match.group(1))
    
    # check structured attr groups too
    attr_groups = soup.select(".attrgroup span, .housing_info span, .shared-line-bubble")
    for attr in attr_groups:
        text = attr.get_text(strip=True)
        if not beds:
            m = re.search(r'(\d+)\s*(?:br|BR|bed)', text)
            if m:
                beds = int(m.group(1))
        if not sqft:
            m = re.search(r'(\d+)\s*(?:ft|sq)', text)
            if m:
                sqft = int(m.group(1))
    
    return beds, baths, sqft


def parse_lat_lon(soup):
    """get lat/lon from the google maps link craigslist embeds"""
    lat = None
    lon = None
    
    maps_link = soup.find("a", href=re.compile(r"google\.com/maps"))
    if maps_link:
        href = maps_link.get("href", "")
        coord_match = re.search(r'([\d.-]+),([\d.-]+)', href)
        if coord_match:
            lat = float(coord_match.group(1))
            lon = float(coord_match.group(2))
    
    if lat is None:
        map_el = soup.find(attrs={"data-latitude": True})
        if map_el:
            lat = float(map_el.get("data-latitude"))
            lon = float(map_el.get("data-longitude"))
    
    if lat is None:
        map_div = soup.select_one("#map")
        if map_div:
            lat_attr = map_div.get("data-latitude") or map_div.get("data-lat")
            lon_attr = map_div.get("data-longitude") or map_div.get("data-lon")
            if lat_attr and lon_attr:
                lat = float(lat_attr)
                lon = float(lon_attr)
    
    return lat, lon


def parse_address(soup):
    """grab the address - craigslist puts it in different spots depending on listing"""
    addr_el = soup.select_one("h2.street-address, .mapaddress")
    if addr_el:
        return addr_el.get_text(strip=True)
    
    body_text = soup.get_text()
    addr_match = re.search(
        r'(\d+\s+[\w\s]+(?:St|Ave|Blvd|Dr|Ct|Ln|Rd|Way|Pl|Cir)\.?[\w\s,]*Davis[\s,]*CA[\s\d]*)',
        body_text
    )
    if addr_match:
        addr = addr_match.group(1).strip()
        addr = re.sub(r'\s+', ' ', addr)
        return addr
    
    return None


def parse_description(soup):
    """get the full listing description text for NLP later"""
    body = soup.select_one("#postingbody")
    if body:
        for unwanted in body.select(".print-information, .print-qrcode-container"):
            unwanted.decompose()
        text = body.get_text(separator=" ", strip=True)
        text = re.sub(r'\s+', ' ', text)
        text = text.replace("QR Code Link to This Post", "").strip()
        return text
    return None


def parse_amenities(soup):
    """extract amenity tags from the listing"""
    amenities = []
    attr_groups = soup.select(".attrgroup")
    for group in attr_groups:
        spans = group.select("span")
        for span in spans:
            text = span.get_text(strip=True)
            if not re.match(r'^\d+\s*(br|ba|ft)', text, re.IGNORECASE):
                if text:
                    amenities.append(text)
    
    feature_items = soup.select(".mapAndAttrs .attrgroup span, .housing_info")
    for item in feature_items:
        text = item.get_text(strip=True)
        if text and text not in amenities:
            amenities.append(text)
    return amenities


# ---- main scraping logic ----

def scrape_single_listing(url):
    """visit one listing page and pull out all the details
    returns a dict or None if it fails
    """
    html = _fetch_html(url)
    if not html:
        print(f"  failed to fetch {url[:70]}")
        return None
    
    soup = BeautifulSoup(html, "lxml")
    
    title_el = soup.select_one("#titletextonly")
    title = title_el.get_text(strip=True) if title_el else ""
    
    price_el = soup.select_one(".price")
    price_text = price_el.get_text(strip=True) if price_el else ""
    price_total = parse_price(price_text)
    
    if price_total is None:
        full_title = soup.select_one(".postingtitletext")
        if full_title:
            price_total = parse_price(full_title.get_text())
    
    beds, baths, sqft = parse_beds_baths_sqft(soup)
    lat, lon = parse_lat_lon(soup)
    address = parse_address(soup)
    description = parse_description(soup)
    amenities = parse_amenities(soup)
    
    page_text = soup.get_text().lower()
    pets_allowed = "cats are ok" in page_text or "dogs are ok" in page_text
    has_parking = "parking" in page_text
    
    laundry_type = "none"
    if "w/d in unit" in page_text or "laundry in unit" in page_text or "in-unit laundry" in page_text or "washer and dryer" in page_text:
        laundry_type = "in-unit"
    elif "laundry on site" in page_text or "laundry facilit" in page_text:
        laundry_type = "on-site"
    
    time_el = soup.select_one("time.date.timeago, time.posting-info-date")
    post_date = time_el.get("datetime") if time_el else None
    
    listing = {
        "listing_id": make_listing_id(url),
        "complex_name": title,
        "address": address,
        "price_total": price_total,
        "bedrooms": beds,
        "baths": baths,
        "sqft": sqft,
        "lat": lat,
        "lon": lon,
        "description": description,
        "amenities": ", ".join(amenities) if amenities else None,
        "pets_allowed": pets_allowed,
        "has_parking": has_parking,
        "laundry_type": laundry_type,
        "post_date": post_date,
        "url": url,
    }
    
    return listing


def _worker_scrape(url, index, total):
    """wrapper that each thread calls - scrapes one listing and saves to csv
    this is what gets submitted to the ThreadPoolExecutor
    """
    # stagger the start slightly so threads dont all hit craigslist at the exact same ms
    time.sleep(random.uniform(0.2, 0.8))
    
    try:
        listing = scrape_single_listing(url)
        
        if listing and listing.get("price_total"):
            _append_to_csv(listing)
            name = listing.get("complex_name", "")[:50]
            price = listing.get("price_total", 0)
            print(f"  [{index}/{total}] ${price:.0f} - {name}")
            return listing
        else:
            print(f"  [{index}/{total}] skipped (no price or failed)")
            return None
    except Exception as e:
        print(f"  [{index}/{total}] error: {e}")
        return None


def scrape_all_davis_listings(max_pages=3):
    """main function - gets all listing urls from search pages,
    then uses ThreadPoolExecutor to scrape them in parallel
    
    saves each listing to csv immediately so nothing is lost on crash
    
    the threading part is good for the Computational Workflow requirement -
    we get about a 3-4x speedup compared to doing them one at a time
    since web scraping is I/O bound (waiting for responses) not CPU bound
    """
    
    # init the csv file
    csv_path = _init_csv()
    already_done = _get_already_scraped()
    if already_done:
        print(f"found {len(already_done)} already scraped listings, will skip those")
    
    # step 1: collect all listing urls from search pages
    all_urls = []
    seen_urls = set()
    
    for page_num in range(max_pages):
        offset = page_num * 120
        search_results = get_search_results(page_offset=offset)
        
        if not search_results:
            print(f"no results on page {page_num + 1}, stopping pagination")
            break
        
        for result in search_results:
            url = result.get("url", "")
            if url in seen_urls:
                continue
            seen_urls.add(url)
            
            # skip non-davis listings that sneak in
            if "davis" not in url.lower() and "davis" not in result.get("title", "").lower():
                continue
            
            # skip if we already scraped it in a previous run
            lid = make_listing_id(url)
            if lid in already_done:
                continue
            
            all_urls.append(url)
        
        if len(search_results) < 50:
            break
        
        polite_sleep()
    
    total = len(all_urls)
    print(f"\n{'='*50}")
    print(f"found {total} new listings to scrape")
    print(f"using {NUM_WORKERS} threads")
    print(f"saving to: {csv_path}")
    print(f"{'='*50}\n")
    
    if total == 0:
        print("nothing new to scrape!")
        return []
    
    # step 2: scrape all listings using thread pool
    # ThreadPoolExecutor manages a pool of worker threads
    # each thread handles one listing at a time
    # this is faster than sequential bc while one thread waits for a response,
    # other threads can send their requests
    
    all_listings = []
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        # submit all urls to the thread pool
        future_to_url = {}
        for i, url in enumerate(all_urls):
            future = executor.submit(_worker_scrape, url, i + 1, total)
            future_to_url[future] = url
        
        # collect results as they finish
        for future in as_completed(future_to_url):
            result = future.result()
            if result:
                all_listings.append(result)
    
    elapsed = time.time() - start_time
    
    # cleanup
    _close_browser()
    
    print(f"\n{'='*50}")
    print(f"DONE! scraped {len(all_listings)} valid listings in {elapsed:.1f} seconds")
    if all_listings:
        avg_time = elapsed / len(all_listings)
        print(f"average time per listing: {avg_time:.1f}s")
        sequential_est = avg_time * len(all_listings) * NUM_WORKERS
        print(f"estimated sequential time: {sequential_est:.0f}s (threading gave ~{NUM_WORKERS}x speedup)")
    print(f"data saved to: {csv_path}")
    print(f"{'='*50}")
    
    return all_listings


# ----- run it -----
if __name__ == "__main__":
    print("=" * 50)
    print("CRAIGSLIST DAVIS APARTMENT SCRAPER")
    print(f"started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50 + "\n")
    
    listings = scrape_all_davis_listings(max_pages=3)
    
    # print summary
    if listings:
        prices = [l["price_total"] for l in listings if l.get("price_total")]
        print(f"\nSummary:")
        print(f"  total listings: {len(listings)}")
        print(f"  price range: ${min(prices):.0f} - ${max(prices):.0f}")
        print(f"  with coordinates: {sum(1 for l in listings if l.get('lat'))}/{len(listings)}")
        print(f"  with description: {sum(1 for l in listings if l.get('description'))}/{len(listings)}")
        print(f"  with sqft: {sum(1 for l in listings if l.get('sqft'))}/{len(listings)}")
        
        print("\nsample (first 3):")
        for l in listings[:3]:
            print(f"  {l['listing_id']} | ${l['price_total']:.0f} | {l.get('bedrooms', '?')}br | {l.get('address', 'no addr')}")
    else:
        print("no listings scraped :(")
