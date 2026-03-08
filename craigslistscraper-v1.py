# scraper/craigslist_scraper.py
# scrapes apartment listings from craigslist sacramento (davis area)
# we go thru the search results page, grab each listing url,
# then visit each one to get the full details (price, beds, sqft, description, etc)

import requests
from bs4 import BeautifulSoup
import time
import random
import hashlib
import re
import os
import sys
from urllib.parse import urlencode

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import USER_AGENT, SCRAPE_DELAY_MIN, SCRAPE_DELAY_MAX

# base url for davis apartments on sacramento craigslist
BASE_URL = "https://sacramento.craigslist.org/search/davis-ca/apa"

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

# reuse one session so we get consistent cookies (more browser-like)
_session = None


def _get_session():
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update(HEADERS)
    return _session


def _fetch_html(url, params=None):
    """Fetch page HTML. Try requests first; on 403, fall back to Playwright (real browser)."""
    full_url = url
    if params:
        full_url = url + ("?" + urlencode(params) if params else "")
    try:
        resp = _get_session().get(url, params=params, timeout=15)
        if resp.status_code == 200:
            return resp.text
        if resp.status_code != 403:
            return None
    except requests.RequestException:
        pass
    # 403 or request failed — try Playwright so we look like a real browser
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("  (install playwright: pip install playwright && playwright install chromium)")
        return None
    print("  (403 from Craigslist — using browser fallback...)")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.set_extra_http_headers({"User-Agent": USER_AGENT})
            page.goto(full_url, wait_until="domcontentloaded", timeout=20000)
            html = page.content()
            browser.close()
            return html
    except Exception as e:
        print(f"  playwright fetch failed: {e}")
        return None


def make_listing_id(url):
    """generate a short unique id from the listing url
    we take the craigslist post id from the url and prepend 'cl_'
    eg: https://sacramento.craigslist.org/apa/d/blah/7919804657.html -> cl_7919804657
    """
    # the post id is the number before .html at the end
    match = re.search(r'/(\d+)\.html', url)
    if match:
        return "cl_" + match.group(1)
    # fallback - just hash the whole url
    return "cl_" + hashlib.md5(url.encode()).hexdigest()[:8]


def polite_sleep():
    """wait a random amount of time between requests
    craigslist is pretty chill about scraping but we still want to be nice
    and not hammer their servers. also helps avoid getting ip banned
    """
    wait_time = random.uniform(SCRAPE_DELAY_MIN, SCRAPE_DELAY_MAX)
    time.sleep(wait_time)


def get_search_results(page_offset=0):
    """scrape one page of craigslist search results
    
    returns a list of dicts with basic info from the search page:
    - url, title, price, and location text
    
    page_offset is for pagination - craigslist uses #search=1~list~{offset}~0
    but actually the newer craigslist loads results dynamically... 
    we'll just grab whatever shows up on the main page for now
    """
    params = {}
    if page_offset > 0:
        params["s"] = page_offset  # craigslist pagination param
    
    print(f"fetching search page (offset={page_offset})...")
    html = _fetch_html(BASE_URL, params=params)
    if not html:
        print("error fetching search page (403 or network error). tried browser fallback if available.")
        return []
    soup = BeautifulSoup(html, "lxml")
    results = []
    
    # craigslist search results are in <li> tags with class "cl-static-search-result"
    # or they might be in <a> tags inside the results container
    # the structure changed a bit in recent years so we try multiple selectors
    
    # try the newer structure first - each result is a <li> with an <a> inside
    listings = soup.select("li.cl-static-search-result")
    
    if not listings:
        # try older structure 
        listings = soup.select("li.result-row")
    
    if not listings:
        # sometimes results are just anchor tags in a list
        listings = soup.select(".cl-search-result")
    
    if not listings:
        # last resort - find all links that look like listing urls
        # craigslist listing urls have /apa/d/ in them
        all_links = soup.find_all("a", href=re.compile(r"/apa/d/"))
        seen_urls = set()
        for link in all_links:
            href = link.get("href", "")
            if href and href not in seen_urls:
                seen_urls.add(href)
                # make sure its a full url
                if not href.startswith("http"):
                    href = "https://sacramento.craigslist.org" + href
                
                title = link.get_text(strip=True)
                results.append({
                    "url": href,
                    "title": title,
                })
        print(f"found {len(results)} listings using link fallback method")
        return results
    
    for item in listings:
        link = item.find("a")
        if not link:
            continue
        
        href = link.get("href", "")
        if not href.startswith("http"):
            href = "https://sacramento.craigslist.org" + href
        
        title = link.get_text(strip=True)
        
        # try to grab price from search result
        price_el = item.select_one(".price, .result-price, .priceinfo")
        price_text = price_el.get_text(strip=True) if price_el else ""
        
        results.append({
            "url": href,
            "title": title,
            "price_text": price_text,
        })
    
    print(f"found {len(results)} listings on search page")
    return results


def parse_price(text):
    """extract numeric price from text like '$2,125' or '$2,125/mo'
    returns float or None if we cant parse it
    """
    if not text:
        return None
    # find all dollar amounts in the text
    matches = re.findall(r'\$[\d,]+', text)
    if matches:
        # take the first one, strip $ and commas
        price_str = matches[0].replace("$", "").replace(",", "")
        try:
            return float(price_str)
        except ValueError:
            return None
    return None


def parse_beds_baths_sqft(soup):
    """try to extract beds, baths, and sqft from the listing page
    
    craigslist puts these in different places depending on the listing
    sometimes its in the title like "$2,125 / 2br - 800ft2"
    sometimes theres structured attr groups
    """
    beds = None
    baths = None
    sqft = None
    
    # first check the title/header area which usually has the format:
    # $2,125 / 2br - 800ft2 - Title (Location)
    title_el = soup.select_one(".postingtitletext, #titletextonly")
    page_text = soup.get_text()
    
    # look for bedroom count - patterns like "2br", "2BR", "2 br", "2 bed", "2bd"
    bed_match = re.search(r'(\d+)\s*(?:br|BR|bed|bd|Bed|bedroom|Bedroom)', page_text)
    if bed_match:
        beds = int(bed_match.group(1))
    
    # studio detection
    if beds is None and re.search(r'studio', page_text, re.IGNORECASE):
        beds = 0
    
    # look for bathroom count
    bath_match = re.search(r'(\d+\.?\d*)\s*(?:ba|BA|bath|Bath|bathroom|Bathroom)', page_text)
    if bath_match:
        baths = float(bath_match.group(1))
    
    # look for sqft - patterns like "800ft2", "800 sq ft", "800 sqft", "800ft²"
    sqft_match = re.search(r'(\d+)\s*(?:ft2|ft²|sqft|sq\.?\s*ft)', page_text)
    if sqft_match:
        sqft = int(sqft_match.group(1))
    
    # also check the structured attributes that craigslist sometimes has
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
    """extract latitude and longitude from the listing
    
    craigslist embeds coords in a google maps link on the page
    the link looks like: https://www.google.com/maps/search/38.559577,-121.741559
    we just grab the numbers from that
    """
    lat = None
    lon = None
    
    # check for google maps link
    maps_link = soup.find("a", href=re.compile(r"google\.com/maps"))
    if maps_link:
        href = maps_link.get("href", "")
        coord_match = re.search(r'([\d.-]+),([\d.-]+)', href)
        if coord_match:
            lat = float(coord_match.group(1))
            lon = float(coord_match.group(2))
    
    # fallback: check for data attributes (older craigslist style)
    if lat is None:
        map_el = soup.find(attrs={"data-latitude": True})
        if map_el:
            lat = float(map_el.get("data-latitude"))
            lon = float(map_el.get("data-longitude"))
    
    # another fallback: some listings embed coords in a #map element
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
    """grab the address from the listing page
    
    on the detail page craigslist usually shows it as a subtitle or
    in a specific address element. the format varies a lot tho
    """
    # look for the address in the small subtitle under the main title
    # eg: "920 Cranbrook Ct., Davis, CA 95616"
    addr_el = soup.select_one("h2.street-address, .mapaddress")
    if addr_el:
        return addr_el.get_text(strip=True)
    
    # check for nearby text after "google map" link
    # sometimes address is just floating in the body
    body_text = soup.get_text()
    
    # look for davis CA pattern in the text
    addr_match = re.search(r'(\d+\s+[\w\s]+(?:St|Ave|Blvd|Dr|Ct|Ln|Rd|Way|Pl|Cir)\.?[\w\s,]*Davis[\s,]*CA[\s\d]*)', body_text)
    if addr_match:
        addr = addr_match.group(1).strip()
        # clean up weird whitespace
        addr = re.sub(r'\s+', ' ', addr)
        return addr
    
    return None


def parse_description(soup):
    """get the full listing description text
    this is the main body text that landlords write
    we'll use this for NLP sentiment analysis later
    
    craigslist puts the posting body in a section with id=postingbody
    """
    body = soup.select_one("#postingbody")
    if body:
        # remove the "QR Code Link to This Post" text that craigslist adds
        for unwanted in body.select(".print-information, .print-qrcode-container"):
            unwanted.decompose()
        
        text = body.get_text(separator=" ", strip=True)
        # clean up the text a bit
        text = re.sub(r'\s+', ' ', text)
        # remove "QR Code Link to This Post" if still there
        text = text.replace("QR Code Link to This Post", "").strip()
        return text
    
    return None


def parse_amenities(soup):
    """try to extract amenity/feature info from the listing
    not all listings have these but its nice extra data
    returns a list of amenity strings
    """
    amenities = []
    
    # look for the attribute groups that have features
    attr_groups = soup.select(".attrgroup")
    for group in attr_groups:
        spans = group.select("span")
        for span in spans:
            text = span.get_text(strip=True)
            # skip the ones that are just bed/bath info
            if not re.match(r'^\d+\s*(br|ba|ft)', text, re.IGNORECASE):
                if text:
                    amenities.append(text)
    
    # also check for the structured feature lists some listings have
    feature_items = soup.select(".mapAndAttrs .attrgroup span, .housing_info")
    for item in feature_items:
        text = item.get_text(strip=True)
        if text and text not in amenities:
            amenities.append(text)
    
    return amenities


def scrape_single_listing(url):
    """visit one listing page and extract all the details
    
    returns a dict with all the fields we need, or None if the page fails
    this is the main workhorse function - it calls all the parse_* helpers
    """
    html = _fetch_html(url)
    if not html:
        print(f"  failed to fetch {url}")
        return None
    soup = BeautifulSoup(html, "lxml")
    
    # get the title from the page
    title_el = soup.select_one("#titletextonly")
    title = title_el.get_text(strip=True) if title_el else ""
    
    # parse price from the header area
    # format is usually "$2,125 / 2br - 800ft2 - Title (Location)"
    price_el = soup.select_one(".price")
    price_text = price_el.get_text(strip=True) if price_el else ""
    price_total = parse_price(price_text)
    
    # if price wasnt in the .price element, try the full title text
    if price_total is None:
        full_title = soup.select_one(".postingtitletext")
        if full_title:
            price_total = parse_price(full_title.get_text())
    
    beds, baths, sqft = parse_beds_baths_sqft(soup)
    lat, lon = parse_lat_lon(soup)
    address = parse_address(soup)
    description = parse_description(soup)
    amenities = parse_amenities(soup)
    
    # figure out pets allowed from amenity tags
    page_text = soup.get_text().lower()
    pets_allowed = "cats are ok" in page_text or "dogs are ok" in page_text
    
    # check for parking info
    has_parking = "parking" in page_text
    
    # check for laundry
    laundry_type = "none"
    if "w/d in unit" in page_text or "laundry in unit" in page_text or "in-unit laundry" in page_text or "washer and dryer" in page_text:
        laundry_type = "in-unit"
    elif "laundry on site" in page_text or "laundry facilit" in page_text:
        laundry_type = "on-site"
    
    # get the posting date
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


def scrape_all_davis_listings(max_pages=3):
    """main function - scrapes multiple pages of search results
    then visits each listing to get full details
    
    max_pages controls how many search result pages we go thru
    each page has roughly 120 results (craiglist changed this recently)
    
    returns a list of listing dicts ready for the database
    """
    all_listings = []
    seen_urls = set()
    
    for page_num in range(max_pages):
        offset = page_num * 120  # craigslist paginates by 120 now
        
        search_results = get_search_results(page_offset=offset)
        
        if not search_results:
            print(f"no results on page {page_num + 1}, stopping")
            break
        
        print(f"\n--- page {page_num + 1}: {len(search_results)} results ---")
        
        for i, result in enumerate(search_results):
            url = result.get("url", "")
            
            # skip if we already scraped this one (duplicates happen across pages)
            if url in seen_urls:
                continue
            seen_urls.add(url)
            
            # only scrape actual davis listings (skip if url doesnt have davis)
            # some results from nearby areas sneak in
            if "davis" not in url.lower() and "davis" not in result.get("title", "").lower():
                continue
            
            print(f"  [{i+1}/{len(search_results)}] scraping: {result.get('title', 'untitled')[:60]}...")
            
            listing = scrape_single_listing(url)
            
            if listing and listing.get("price_total"):
                all_listings.append(listing)
            else:
                print(f"    skipped (no price or failed)")
            
            polite_sleep()
        
        # if we got fewer results than expected, probably no more pages
        if len(search_results) < 50:
            break
        
        polite_sleep()
    
    print(f"\n=== done! scraped {len(all_listings)} valid listings ===")
    return all_listings


# ----- run it -----
if __name__ == "__main__":
    print("starting craigslist davis apartment scraper...")
    print("this will take a few minutes bc we're being polite with request timing\n")
    
    listings = scrape_all_davis_listings(max_pages=2)
    
    # print a summary
    if listings:
        print(f"\nGot {len(listings)} listings")
        print(f"Price range: ${min(l['price_total'] for l in listings if l['price_total']):.0f} - ${max(l['price_total'] for l in listings if l['price_total']):.0f}")
        
        with_coords = sum(1 for l in listings if l.get('lat'))
        with_desc = sum(1 for l in listings if l.get('description'))
        with_sqft = sum(1 for l in listings if l.get('sqft'))
        
        print(f"With coordinates: {with_coords}/{len(listings)}")
        print(f"With description: {with_desc}/{len(listings)}")
        print(f"With sqft: {with_sqft}/{len(listings)}")
        
        # show first 3 as sample
        print("\n--- sample listings ---")
        for l in listings[:3]:
            print(f"  {l['listing_id']} | ${l['price_total']:.0f} | {l.get('bedrooms', '?')}br | {l.get('sqft', '?')}sqft | {l.get('address', 'no addr')}")
            if l.get('description'):
                print(f"    desc preview: {l['description'][:100]}...")
    else:
        print("no listings scraped :(")
