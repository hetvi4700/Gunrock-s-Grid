"""
on_campus_listings.py
---------------------
Adds hardcoded on-campus UC Davis housing listings to scraped_listings.csv.
These can't be scraped from Craigslist/Apartments.com because they're
university-operated or P3 (public-private partnership) housing.

Data sourced from official 2025-2026 UC Davis Student Housing fee schedules
and property websites. Each listing gets a unique listing_id with prefix
"oc_" (on-campus) to distinguish from "cl_" (craigslist) entries.

Run this BEFORE enrich.py so the on-campus rows get enriched alongside
the scraped data.

Usage:
    python on_campus_listings.py
"""

import csv
import os

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

# path setup -- same flat structure as the rest of the project
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(SCRIPT_DIR, "scraped_listings.csv")

# CSV columns -- must match what craigslist_scraper.py writes
FIELDNAMES = [
    "listing_id",
    "complex_name",
    "address",
    "price_total",
    "bedrooms",
    "sqft",
    "lat",
    "lon",
    "description",
    "url",
    "posted_date",
]

# ON-CAMPUS HOUSING DATA (2025-2026 fee schedules)
# Notes on pricing:
#   - The Green: quarterly fees, converted to monthly (annual / 12)
#     Studio: $23,040/yr = $1,920/mo per person
#     Single: $15,888/yr = $1,324/mo per person (single bed in shared apt)
#     Double: $10,584/yr = $882/mo per person (shared bed in shared apt)
#   - Orchard Park: monthly rent from fee schedule (already monthly)
#   - Primero Grove: monthly unit lease (whole apartment)
#   - ANOVA Aggie Square: monthly rent per bed from fee schedule
#   - Sol at West Village: per-bed pricing from Apartments.com / property site
#   - Colleges at La Rue: per-bed pricing from RentCafe / Apartments.com
#
# For bed-lease properties (The Green, Orchard Park bed lease, ANOVA, Sol,
# Colleges at La Rue), price_total = monthly cost PER PERSON and bedrooms =
# the bed count that person occupies (so bedrooms=1 for a single bed in a
# 4BR apartment). This makes price_per_bed calculations consistent.
#
# For unit-lease properties (Primero Grove, Orchard Park family/grad),
# price_total = whole unit rent and bedrooms = actual bedroom count.
#
# sqft: estimated from floor plans where exact numbers weren't available.
#       The Green studios ~280-350 sqft, 2BR ~700-800 sqft, 4BR ~1100-1200
#       Orchard Park studios 350 sqft, 2BR ~700 sqft, 4BR ~1150 sqft
#       Primero Grove studios ~400, 1BR ~550, 2BR ~800, 3BR ~1050, 4BR ~1300

ON_CAMPUS_LISTINGS = [
    # THE GREEN AT WEST VILLAGE (UC Davis operated, CHF-Davis I)
    # Location: 298 Celadon St area, West Village
    # Coordinates: 38.5426, -121.7749
    # Quarterly billing -> converted to monthly (annual / 12)
    # All units furnished, utilities + internet + TV included
    {
        "listing_id": "oc_green_studio",
        "complex_name": "The Green at West Village",
        "address": "298 Celadon St, Davis, CA 95616",
        "price_total": 1920,  # $23,040/yr / 12
        "bedrooms": 0,  # studio
        "sqft": 320,
        "lat": 38.5426,
        "lon": -121.7749,
        "description": "On-campus studio apartment at The Green at West Village. Single occupancy private apartment. Fully furnished with full kitchen. Utilities, high-speed WiFi, and Stream2 TV included. Fitness center, community center, dedicated Unitrans V Line. 4-story building, LVT plank flooring, full-size washer/dryer in building. Zero-net-energy community with solar arrays.",
        "url": "https://housing.ucdavis.edu/apartments/the-green/",
        "posted_date": "2025-08-01",
    },
    {
        "listing_id": "oc_green_single",
        "complex_name": "The Green at West Village",
        "address": "298 Celadon St, Davis, CA 95616",
        "price_total": 1324,  # $15,888/yr / 12
        "bedrooms": 1,  # single bed in shared apartment
        "sqft": 200,  # per-person share of a shared apt
        "lat": 38.5426,
        "lon": -121.7749,
        "description": "On-campus single-occupancy bedroom in a shared apartment at The Green at West Village. Fully furnished with full bed, shared kitchen and common space. Utilities, high-speed WiFi, and Stream2 TV included. Fitness center, study rooms on every floor, dedicated Unitrans V Line. Zero-net-energy community.",
        "url": "https://housing.ucdavis.edu/apartments/the-green/",
        "posted_date": "2025-08-01",
    },
    {
        "listing_id": "oc_green_double",
        "complex_name": "The Green at West Village",
        "address": "298 Celadon St, Davis, CA 95616",
        "price_total": 882,  # $10,584/yr / 12
        "bedrooms": 1,  # shared bed (double occupancy)
        "sqft": 150,  # per-person share
        "lat": 38.5426,
        "lon": -121.7749,
        "description": "On-campus double-occupancy bedroom in a shared apartment at The Green at West Village. Room shared with one roommate, fully furnished with full beds. Shared kitchen and common space. Utilities, high-speed WiFi, and Stream2 TV included. Fitness center, study rooms, dedicated Unitrans V Line.",
        "url": "https://housing.ucdavis.edu/apartments/the-green/",
        "posted_date": "2025-08-01",
    },
    # ===================================================================
    # ORCHARD PARK (UC Davis operated, CHF-Davis II)
    # Location: Russell Blvd / Blue Ridge Rd, northwest campus
    # Coordinates: 38.5440, -121.7608
    # Monthly billing, all utilities + WiFi included
    # ===================================================================
    # -- Bed leases (undergrad contracts) --
    {
        "listing_id": "oc_orchard_studio",
        "complex_name": "Orchard Park",
        "address": "Orchard Park Circle, Davis, CA 95616",
        "price_total": 1870,
        "bedrooms": 0,  # studio
        "sqft": 350,
        "lat": 38.5440,
        "lon": -121.7608,
        "description": "On-campus furnished studio at Orchard Park. Single bed lease. All utilities and WiFi included. Card-operated laundry on each floor. Fitness rooms, study/meeting spaces, tot lot, bike/ped paths. 4-story building. Located on northwest corner of UC Davis campus off Russell Blvd.",
        "url": "https://housing.ucdavis.edu/apartments/orchard-park/",
        "posted_date": "2025-08-01",
    },
    {
        "listing_id": "oc_orchard_4br_single",
        "complex_name": "Orchard Park",
        "address": "Orchard Park Circle, Davis, CA 95616",
        "price_total": 1173,
        "bedrooms": 1,  # single bed in 4BR
        "sqft": 290,  # per-person share of ~1150 sqft
        "lat": 38.5440,
        "lon": -121.7608,
        "description": "On-campus single bedroom in a furnished 4-bedroom apartment at Orchard Park. Per-bed lease. All utilities and WiFi included. Card-operated laundry on each floor. Fitness rooms, study spaces, bike paths. Northwest campus off Russell Blvd.",
        "url": "https://housing.ucdavis.edu/apartments/orchard-park/",
        "posted_date": "2025-08-01",
    },
    {
        "listing_id": "oc_orchard_2br_single",
        "complex_name": "Orchard Park",
        "address": "Orchard Park Circle, Davis, CA 95616",
        "price_total": 1323,
        "bedrooms": 1,  # single bed in 2BR
        "sqft": 350,  # per-person share of ~700 sqft
        "lat": 38.5440,
        "lon": -121.7608,
        "description": "On-campus single bedroom in a furnished 2-bedroom apartment at Orchard Park. Per-bed lease. All utilities and WiFi included. Card-operated laundry on each floor. Fitness rooms, study spaces. Northwest campus off Russell Blvd.",
        "url": "https://housing.ucdavis.edu/apartments/orchard-park/",
        "posted_date": "2025-08-01",
    },
    {
        "listing_id": "oc_orchard_2br_double",
        "complex_name": "Orchard Park",
        "address": "Orchard Park Circle, Davis, CA 95616",
        "price_total": 702,
        "bedrooms": 1,  # shared bed in 2BR (double occupancy)
        "sqft": 175,  # per-person share
        "lat": 38.5440,
        "lon": -121.7608,
        "description": "On-campus double-occupancy bedroom in a furnished 2-bedroom apartment at Orchard Park. Per-bed lease, limited availability. Room shared with one roommate. All utilities and WiFi included. Laundry on each floor. Northwest campus.",
        "url": "https://housing.ucdavis.edu/apartments/orchard-park/",
        "posted_date": "2025-08-01",
    },
    # -- Unit leases (families & grads) --
    {
        "listing_id": "oc_orchard_2br_furn_unit",
        "complex_name": "Orchard Park (Family/Grad)",
        "address": "Orchard Park Circle, Davis, CA 95616",
        "price_total": 2727,
        "bedrooms": 2,
        "sqft": 700,
        "lat": 38.5440,
        "lon": -121.7608,
        "description": "On-campus furnished 2-bedroom apartment at Orchard Park for families and graduate students. Unit lease with monthly rent. All utilities and WiFi included. Card-operated laundry, fitness rooms, tot lot. $250 security deposit required. Northwest campus off Russell Blvd.",
        "url": "https://housing.ucdavis.edu/apartments/orchard-park/",
        "posted_date": "2025-08-01",
    },
    {
        "listing_id": "oc_orchard_2br_unfurn_unit",
        "complex_name": "Orchard Park (Family/Grad)",
        "address": "Orchard Park Circle, Davis, CA 95616",
        "price_total": 2602,
        "bedrooms": 2,
        "sqft": 700,
        "lat": 38.5440,
        "lon": -121.7608,
        "description": "On-campus unfurnished 2-bedroom apartment at Orchard Park for families and graduate students. Unit lease with monthly rent. All utilities and WiFi included. Card-operated laundry, fitness rooms, tot lot. $250 security deposit. Northwest campus off Russell Blvd.",
        "url": "https://housing.ucdavis.edu/apartments/orchard-park/",
        "posted_date": "2025-08-01",
    },
    # ===================================================================
    # PRIMERO GROVE (UC Davis operated, grad/family housing)
    # Location: near Primero/Russell area, west-central campus
    # Coordinates: 38.5455, -121.7570
    # Monthly unit lease, unfurnished, utilities included
    # ===================================================================
    {
        "listing_id": "oc_primero_studio",
        "complex_name": "Primero Grove",
        "address": "Primero Grove, Davis, CA 95616",
        "price_total": 1359,
        "bedrooms": 0,
        "sqft": 400,
        "lat": 38.5455,
        "lon": -121.7570,
        "description": "On-campus unfurnished studio at Primero Grove. Unit lease for graduate students and families. Utilities included. Located near Segundo/Tercero area, close to campus core. Quiet residential community with bike paths.",
        "url": "https://housing.ucdavis.edu/apartments/primero-grove/",
        "posted_date": "2025-08-01",
    },
    {
        "listing_id": "oc_primero_1br",
        "complex_name": "Primero Grove",
        "address": "Primero Grove, Davis, CA 95616",
        "price_total": 1427,
        "bedrooms": 1,
        "sqft": 550,
        "lat": 38.5455,
        "lon": -121.7570,
        "description": "On-campus unfurnished 1-bedroom apartment at Primero Grove. Unit lease for graduate students and families. Utilities included. Near campus core, bike-friendly. Quiet residential community.",
        "url": "https://housing.ucdavis.edu/apartments/primero-grove/",
        "posted_date": "2025-08-01",
    },
    {
        "listing_id": "oc_primero_2br",
        "complex_name": "Primero Grove",
        "address": "Primero Grove, Davis, CA 95616",
        "price_total": 1831,
        "bedrooms": 2,
        "sqft": 800,
        "lat": 38.5455,
        "lon": -121.7570,
        "description": "On-campus unfurnished 2-bedroom apartment at Primero Grove. Unit lease for graduate students and families. Utilities included. Near campus core. Quiet community with walking and biking paths.",
        "url": "https://housing.ucdavis.edu/apartments/primero-grove/",
        "posted_date": "2025-08-01",
    },
    {
        "listing_id": "oc_primero_3br",
        "complex_name": "Primero Grove",
        "address": "Primero Grove, Davis, CA 95616",
        "price_total": 2499,
        "bedrooms": 3,
        "sqft": 1050,
        "lat": 38.5455,
        "lon": -121.7570,
        "description": "On-campus unfurnished 3-bedroom apartment at Primero Grove. Unit lease for families and graduate students. Utilities included. Walking distance to campus core. Spacious layout with bike-friendly paths.",
        "url": "https://housing.ucdavis.edu/apartments/primero-grove/",
        "posted_date": "2025-08-01",
    },
    {
        "listing_id": "oc_primero_4br",
        "complex_name": "Primero Grove",
        "address": "Primero Grove, Davis, CA 95616",
        "price_total": 3232,
        "bedrooms": 4,
        "sqft": 1300,
        "lat": 38.5455,
        "lon": -121.7570,
        "description": "On-campus unfurnished 4-bedroom apartment at Primero Grove. Unit lease for families and graduate students. Utilities included. Largest floor plan. Near campus core with easy bike access.",
        "url": "https://housing.ucdavis.edu/apartments/primero-grove/",
        "posted_date": "2025-08-01",
    },
    # ===================================================================
    # ANOVA AGGIE SQUARE (UC Davis operated)
    # Location: Aggie Square, Sacramento campus area
    # Coordinates: 38.5530, -121.4560 (Sacramento, NOT Davis main campus)
    # NOTE: ANOVA is at the Sacramento Aggie Square campus, ~15 mi from
    #       main Davis campus. Including it adds geographic diversity but
    #       dist_to_mu will be very large. Consider excluding if you only
    #       want Davis-proper listings. Leaving it in for completeness.
    # Monthly per-bed lease, furnished
    # ===================================================================
    {
        "listing_id": "oc_anova_micro_studio",
        "complex_name": "ANOVA Aggie Square",
        "address": "ANOVA Aggie Square, Sacramento, CA 95817",
        "price_total": 1510,
        "bedrooms": 0,
        "sqft": 300,
        "lat": 38.5530,
        "lon": -121.4560,
        "description": "Micro studio at ANOVA Aggie Square in Sacramento. Single bed lease for UC Davis students. Furnished. Modern graduate student housing at the new Aggie Square campus development. Easy access to UC Davis Health campus.",
        "url": "https://housing.ucdavis.edu/how-to-apply/anova-aggie-square/",
        "posted_date": "2025-08-01",
    },
    {
        "listing_id": "oc_anova_2br_single",
        "complex_name": "ANOVA Aggie Square",
        "address": "ANOVA Aggie Square, Sacramento, CA 95817",
        "price_total": 1328,
        "bedrooms": 1,
        "sqft": 250,
        "lat": 38.5530,
        "lon": -121.4560,
        "description": "Single bedroom in a 2-bedroom apartment at ANOVA Aggie Square in Sacramento. Per-bed lease for UC Davis students. Furnished. Located at the new Aggie Square campus near UC Davis Health.",
        "url": "https://housing.ucdavis.edu/how-to-apply/anova-aggie-square/",
        "posted_date": "2025-08-01",
    },
    {
        "listing_id": "oc_anova_4br_single",
        "complex_name": "ANOVA Aggie Square",
        "address": "ANOVA Aggie Square, Sacramento, CA 95817",
        "price_total": 1138,
        "bedrooms": 1,
        "sqft": 200,
        "lat": 38.5530,
        "lon": -121.4560,
        "description": "Single bedroom in a 4-bedroom apartment at ANOVA Aggie Square in Sacramento. Per-bed lease for UC Davis students. Furnished. Modern building at the Aggie Square campus development near UC Davis Health.",
        "url": "https://housing.ucdavis.edu/how-to-apply/anova-aggie-square/",
        "posted_date": "2025-08-01",
    },
    # ===================================================================
    # SOL AT WEST VILLAGE (P3, managed by Landmark Properties)
    # Location: 1580 Jade St, West Village
    # Coordinates: 38.5398, -121.7722
    # Per-bed leasing, furnished, utilities NOT included in base rent
    # Prices from Apartments.com and property website (approximate mid-range)
    # ===================================================================
    {
        "listing_id": "oc_sol_1br",
        "complex_name": "Sol at West Village",
        "address": "1580 Jade St, Davis, CA 95616",
        "price_total": 1099,
        "bedrooms": 1,
        "sqft": 790,
        "lat": 38.5398,
        "lon": -121.7722,
        "description": "1-bedroom apartment at Sol at West Village. Per-bed lease. Furnished units available. Walk to campus, pool, spa, fitness center, media theater, yoga studio, sand volleyball, outdoor kitchen with BBQs. Pet-friendly with dog park. In-unit washer/dryer, granite countertops, walk-in closets. EV charging available.",
        "url": "https://solatwestvillage.com/",
        "posted_date": "2025-08-01",
    },
    {
        "listing_id": "oc_sol_2br",
        "complex_name": "Sol at West Village",
        "address": "1580 Jade St, Davis, CA 95616",
        "price_total": 1350,
        "bedrooms": 1,  # per-bed in 2BR
        "sqft": 500,  # per-person share of ~1000 sqft
        "lat": 38.5398,
        "lon": -121.7722,
        "description": "Single bedroom in a 2-bedroom apartment at Sol at West Village. Per-bed lease with individual locking bedrooms and private bathroom. Furnished. Pool, spa, fitness center, media theater. In-unit washer/dryer. Walk to campus. Pet-friendly. EV charging.",
        "url": "https://solatwestvillage.com/",
        "posted_date": "2025-08-01",
    },
    {
        "listing_id": "oc_sol_4br",
        "complex_name": "Sol at West Village",
        "address": "1580 Jade St, Davis, CA 95616",
        "price_total": 1150,
        "bedrooms": 1,  # per-bed in 4BR
        "sqft": 370,  # per-person share of ~1467 sqft
        "lat": 38.5398,
        "lon": -121.7722,
        "description": "Single bedroom in a 4-bedroom apartment at Sol at West Village. Per-bed lease with individual locking bedrooms and private bathroom. Furnished. Pool, spa, fitness center, media theater, yoga studio. Walk to campus. Pet-friendly.",
        "url": "https://solatwestvillage.com/",
        "posted_date": "2025-08-01",
    },
    # ===================================================================
    # THE COLLEGES AT LA RUE (P3, managed by Tandem Properties)
    # Location: 164 Orchard Park Dr / La Rue Rd
    # Coordinates: 38.5394, -121.7580
    # Right across from ARC and Rec Pool
    # Joint lease / individual lease available
    # Prices from Apartments.com (starting rates)
    # ===================================================================
    {
        "listing_id": "oc_colleges_1br",
        "complex_name": "The Colleges at La Rue",
        "address": "164 La Rue Rd, Davis, CA 95616",
        "price_total": 1566,
        "bedrooms": 1,
        "sqft": 480,
        "lat": 38.5394,
        "lon": -121.7580,
        "description": "1-bedroom apartment at The Colleges at La Rue. On campus across from the ARC and Recreation Pool. Managed by Tandem Properties. Dog and cat friendly. In-unit washer/dryer, microwave, granite countertops. Fitness center, pool, clubhouse. Walk to any class in minutes. For UC Davis continuing undergrad, grad, and transfer students only.",
        "url": "https://colleges.tandemproperties.com/",
        "posted_date": "2025-08-01",
    },
    {
        "listing_id": "oc_colleges_2br",
        "complex_name": "The Colleges at La Rue",
        "address": "164 La Rue Rd, Davis, CA 95616",
        "price_total": 2100,
        "bedrooms": 2,
        "sqft": 900,
        "lat": 38.5394,
        "lon": -121.7580,
        "description": "2-bedroom apartment at The Colleges at La Rue. On campus across from the ARC. Managed by Tandem Properties. Geothermal heating/cooling. In-unit washer/dryer, modern kitchen. Pet-friendly. Fitness center, pool, study rooms. Voted Best Place to Live in Davis by UC Davis students 3 years running.",
        "url": "https://colleges.tandemproperties.com/",
        "posted_date": "2025-08-01",
    },
    {
        "listing_id": "oc_colleges_4br",
        "complex_name": "The Colleges at La Rue",
        "address": "164 La Rue Rd, Davis, CA 95616",
        "price_total": 3400,
        "bedrooms": 4,
        "sqft": 1590,
        "lat": 38.5394,
        "lon": -121.7580,
        "description": "4-bedroom apartment at The Colleges at La Rue. Largest floor plan, great for groups. On campus right across from ARC. Managed by Tandem Properties. Pet-friendly. In-unit washer/dryer. Geothermal climate control. Fitness center, pool, study rooms, BBQ areas.",
        "url": "https://colleges.tandemproperties.com/",
        "posted_date": "2025-08-01",
    },
]


# ---------------------------------------------------------------------------
# MAIN -- append on-campus rows to scraped_listings.csv
# ---------------------------------------------------------------------------


def main():
    # check if CSV exists and read existing listing IDs to avoid duplicates
    existing_ids = set()
    file_exists = os.path.exists(CSV_PATH)

    if file_exists:
        with open(CSV_PATH, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_ids.add(row.get("listing_id", ""))

    # filter out any on-campus listings already in the CSV
    new_listings = [
        L for L in ON_CAMPUS_LISTINGS if L["listing_id"] not in existing_ids
    ]

    if not new_listings:
        print("all on-campus listings already present in CSV, nothing to add")
        return

    # if file doesnt exist yet, write header first
    write_header = not file_exists

    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if write_header:
            writer.writeheader()
        for listing in new_listings:
            writer.writerow(listing)

    print(f"added {len(new_listings)} on-campus listings to {CSV_PATH}")
    print(f"  (skipped {len(ON_CAMPUS_LISTINGS) - len(new_listings)} already present)")
    print()

    # quick summary
    print("--- ON-CAMPUS LISTINGS ADDED ---")
    for L in new_listings:
        beds = "Studio" if L["bedrooms"] == 0 else f"{L['bedrooms']}BR"
        print(
            f"  {L['listing_id']:30s}  {L['complex_name']:30s}  {beds:8s}  ${L['price_total']:,}/mo"
        )
    print(f"\ntotal on-campus listings: {len(ON_CAMPUS_LISTINGS)}")
    print(f"total new rows added: {len(new_listings)}")

    # heads up about ANOVA
    anova_count = sum(1 for L in new_listings if "anova" in L["listing_id"])
    if anova_count > 0:
        print(f"\n  NOTE: {anova_count} ANOVA Aggie Square listings are in SACRAMENTO")
        print("  (not Davis proper). dist_to_mu will be ~15 miles.")
        print("  remove the oc_anova_* entries from this script if you want")
        print("  Davis-only listings.")


if __name__ == "__main__":
    main()
