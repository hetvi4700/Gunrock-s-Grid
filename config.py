# config.py
# just has all the constants and stuff we reuse across files
# easier to change things in one place instead of hunting thru every file

# ---- Memorial Union coordinates (our reference point for distance calc) ----
# got these from google maps, right clicked on the MU building
MU_LAT = 38.5424
MU_LON = -121.7483

# radius in miles for transit score 
# basically how far from the apartment we look for bus stops
TRANSIT_RADIUS_MILES = 0.25

# database file name
DB_NAME = "davis_housing.db"

# how many seconds to wait between requests so we dont get blocked
# apartments.com will ban you if you hit them too fast lol
SCRAPE_DELAY_MIN = 1.5
SCRAPE_DELAY_MAX = 3.0

# neighborhoood boundaries (rough lat/lon cutoffs)
# i drew these out on google maps, they arent perfect but close enough
# downtown is basically the grid around 3rd/B street area
NEIGHBORHOOD_BOUNDS = {
    "Downtown": {"lat_min": 38.540, "lat_max": 38.550, "lon_min": -121.755, "lon_max": -121.738},
    "North Davis": {"lat_min": 38.555, "lat_max": 38.575, "lon_min": -121.770, "lon_max": -121.730},
    "South Davis": {"lat_min": 38.525, "lat_max": 38.540, "lon_min": -121.770, "lon_max": -121.730},
    "East Davis": {"lat_min": 38.540, "lat_max": 38.560, "lon_min": -121.730, "lon_max": -121.710},
    "West Davis": {"lat_min": 38.540, "lat_max": 38.565, "lon_min": -121.790, "lon_max": -121.755},
}

# craigslist base url for davis apartments (under sacramento region)
CRAIGSLIST_URL = "https://sacramento.craigslist.org/search/davis-ca/apa"

# max search result pages to scrape (each page has ~120 listings)
MAX_PAGES = 3

# user agent so we look like a normal browser
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
