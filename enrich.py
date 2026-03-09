# enrich.py
# takes the raw scraped csv and adds computed columns:
#   - neighborhood (North/South/East/West/Downtown Davis)
#   - dist_to_mu (distance to memorial union in miles)
#   - price_per_bed (monthly rent / number of bedrooms)
#   - price_per_sqft (rent / square footage)
#   - distances to all points of interest
#
# run this after the scraper finishes. reads scraped_listings.csv
# and writes enriched_listings.csv with the new columns

import pandas as pd
import numpy as np
import os
import sys
from math import radians, sin, cos, sqrt, atan2

from config import MU_LAT, MU_LON, NEIGHBORHOOD_BOUNDS


def get_project_root():
    """project root = same folder this script is in"""
    return os.path.dirname(os.path.abspath(__file__))


def haversine_miles(lat1, lon1, lat2, lon2):
    """calculate distance between two lat/lon points in miles
    using the haversine formula

    we could use geopy for this but haversine is simple enough
    to implement ourselves and it avoids an extra dependency.
    plus its good to show in the report that we understand the math

    formula from: https://en.wikipedia.org/wiki/Haversine_formula
    earth radius = 3958.8 miles
    """
    R = 3958.8  # earths radius in miles

    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return R * c


def assign_neighborhood(lat, lon):
    """figure out which neighborhood a listing is in based on coordinates

    we defined rough bounding boxes for each Davis neighborhood in config.py
    these arent perfect but they cover the main areas. if a listing doesnt
    fall into any box we call it 'Other Davis'

    the boundaries were drawn by looking at google maps and noting where
    each neighborhood roughly starts and ends. downtown is the small grid
    around 3rd street / B street area
    """
    if pd.isna(lat) or pd.isna(lon):
        return "Unknown"

    for name, bounds in NEIGHBORHOOD_BOUNDS.items():
        if (
            bounds["lat_min"] <= lat <= bounds["lat_max"]
            and bounds["lon_min"] <= lon <= bounds["lon_max"]
        ):
            return name

    # if it didnt match any neighborhood, try to guess based on
    # position relative to downtown center (~38.545, -121.745)
    center_lat = 38.545
    center_lon = -121.745

    if lat > 38.555:
        return "North Davis"
    elif lat < 38.535:
        return "South Davis"
    elif lon > -121.730:
        return "East Davis"
    elif lon < -121.755:
        return "West Davis"
    else:
        return "Downtown"


def calc_price_per_bed(price, beds):
    """price per bedroom - for studios we just use the full price
    since theres no separate bedroom
    """
    if pd.isna(price) or price <= 0:
        return None
    if pd.isna(beds) or beds <= 0:
        # studio or missing bedroom count - price_per_bed = total price
        return price
    return round(price / beds, 2)


def calc_price_per_sqft(price, sqft):
    """monthly rent divided by square footage
    gives us a standardized cost metric regardless of unit size
    """
    if pd.isna(price) or pd.isna(sqft) or sqft <= 0:
        return None
    return round(price / sqft, 2)


def enrich_dataframe(df):
    """take the raw scraped dataframe and add all the computed columns

    this is the main function - it does:
    1. neighborhood assignment from lat/lon
    2. distance to memorial union (haversine)
    3. price per bedroom
    4. price per sqft
    5. basic data cleaning (drop rows with no price, filter outliers)
    """
    print(f"starting enrichment on {len(df)} listings...")

    # --- step 1: clean up the data a bit ---

    # drop rows where theres no price (useless for analysis)
    before = len(df)
    df = df.dropna(subset=["price_total"])
    df = df[df["price_total"] > 0]
    print(f"  dropped {before - len(df)} rows with no price")

    # convert types just in case they came in as strings from csv
    df["price_total"] = pd.to_numeric(df["price_total"], errors="coerce")
    df["bedrooms"] = pd.to_numeric(df["bedrooms"], errors="coerce")
    df["sqft"] = pd.to_numeric(df["sqft"], errors="coerce")
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")

    # filter out obvious outliers / non-apartment listings
    # some craigslist posts are whole houses at $5000+ or rooms at $400
    # we keep a wide range but remove the extreme stuff
    df = df[(df["price_total"] >= 500) & (df["price_total"] <= 6000)]
    print(f"  after price filter (500-6000): {len(df)} listings")

    # filter coords to davis area only (some listings have wrong coords)
    # davis is roughly between 38.52-38.58 lat, -121.80 to -121.70 lon
    if "lat" in df.columns:
        davis_mask = (
            (df["lat"] >= 38.51)
            & (df["lat"] <= 38.59)
            & (df["lon"] >= -121.81)
            & (df["lon"] <= -121.69)
        )
        outside = (~davis_mask).sum()
        if outside > 0:
            print(f"  filtered out {outside} listings outside Davis boundaries")
        df = df[davis_mask]

    # --- step 2: compute neighborhood ---
    print("  assigning neighborhoods...")
    df["neighborhood"] = df.apply(
        lambda row: assign_neighborhood(row["lat"], row["lon"]), axis=1
    )

    # print neighborhood distribution so we can sanity check
    hood_counts = df["neighborhood"].value_counts()
    for hood, count in hood_counts.items():
        print(f"    {hood}: {count} listings")

    # --- step 3: distance to memorial union ---
    print("  calculating distance to MU...")
    df["dist_to_mu"] = df.apply(
        lambda row: (
            round(haversine_miles(row["lat"], row["lon"], MU_LAT, MU_LON), 2)
            if pd.notna(row["lat"]) and pd.notna(row["lon"])
            else None
        ),
        axis=1,
    )

    avg_dist = df["dist_to_mu"].mean()
    print(f"    average distance to MU: {avg_dist:.2f} miles")
    print(
        f"    closest: {df['dist_to_mu'].min():.2f} mi, farthest: {df['dist_to_mu'].max():.2f} mi"
    )

    # --- step 4: price per bed ---
    print("  computing price_per_bed...")
    df["price_per_bed"] = df.apply(
        lambda row: calc_price_per_bed(row["price_total"], row["bedrooms"]), axis=1
    )

    # --- step 5: price per sqft ---
    print("  computing price_per_sqft...")
    df["price_per_sqft"] = df.apply(
        lambda row: calc_price_per_sqft(row["price_total"], row["sqft"]), axis=1
    )

    print(f"\nenrichment done! {len(df)} listings with all computed fields")
    return df


def run_enrichment():
    """load csv, enrich it, save the result"""

    root = get_project_root()

    # force using the csv inside the project folder
    input_path = os.path.join(root, "scraped_listings.csv")
    output_path = os.path.join(root, "enriched_listings.csv")

    print("\n--- FILE PATH CHECK ---")
    print(f"Script location : {root}")
    print(f"Reading CSV from: {input_path}")
    print(f"Output will be  : {output_path}")
    print("-----------------------\n")

    if not os.path.exists(input_path):
        print("ERROR: scraped_listings.csv not found in project folder.")
        print(f"Expected here: {input_path}")
        print("Make sure the scraper saved the file in Gunrock-s-Grid.")
        return None

    df = pd.read_csv(input_path)
    print(f"loaded {len(df)} rows")

    df = enrich_dataframe(df)

    from places_of_interest import compute_all_distances

    df = compute_all_distances(df)

    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"\nsaved enriched data to {output_path}")

    return df


if __name__ == "__main__":
    run_enrichment()
