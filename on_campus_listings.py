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

# CSV columns -- must EXACTLY match what craigslist_scraper.py writes
FIELDNAMES = [
    "listing_id",
    "complex_name",
    "address",
    "price_total",
    "bedrooms",
    "baths",
    "sqft",
    "lat",
    "lon",
    "description",
    "amenities",
    "pets_allowed",
    "has_parking",
    "laundry_type",
    "post_date",
    "url",
]

# ---------------------------------------------------------------------------
# ON-CAMPUS HOUSING DATA (2025-2026 fee schedules)
# ---------------------------------------------------------------------------
# Notes on pricing:
#   - The Green: quarterly fees converted to monthly (annual / 12)
#   - Orchard Park: monthly rent from fee schedule (already monthly)
#   - Primero Grove: estimated from comparable UC Davis apt pricing
#   - ANOVA Aggie Square: monthly rent per bed from fee schedule
#   - Sol at West Village: per-bed pricing from Apartments.com / property site
#   - Colleges at La Rue: per-bed pricing from RentCafe / Apartments.com

ON_CAMPUS_LISTINGS = [
    # ===================================================================
    # THE GREEN AT WEST VILLAGE (UC Davis operated, CHF-Davis I)
    # 298 Celadon St area | 38.5426, -121.7749
    # Quarterly billing -> converted to monthly
    # All furnished, utilities + internet + TV included
    # ===================================================================
    {
        "listing_id": "oc_green_studio",
        "complex_name": "The Green at West Village",
        "address": "298 Celadon St, Davis, CA 95616",
        "price_total": 1920,
        "bedrooms": 0,
        "baths": 1,
        "sqft": 320,
        "lat": 38.5426,
        "lon": -121.7749,
        "description": "On-campus studio apartment at The Green at West Village. Single occupancy private apartment. Fully furnished with full kitchen. Utilities, high-speed WiFi, and Stream2 TV included. Fitness center, community center, dedicated Unitrans V Line. 4-story building, LVT plank flooring, full-size washer/dryer in building. Zero-net-energy community with solar arrays.",
        "amenities": "furnished, wifi included, tv included, utilities included, fitness center, community center, study rooms, unitrans V line, solar powered, bike parking",
        "pets_allowed": "No",
        "has_parking": "Yes",
        "laundry_type": "in-building",
        "post_date": "2025-08-01",
        "url": "https://housing.ucdavis.edu/apartments/the-green/",
    },
    {
        "listing_id": "oc_green_single",
        "complex_name": "The Green at West Village",
        "address": "298 Celadon St, Davis, CA 95616",
        "price_total": 1324,
        "bedrooms": 1,
        "baths": 1,
        "sqft": 200,
        "lat": 38.5426,
        "lon": -121.7749,
        "description": "On-campus single-occupancy bedroom in a shared apartment at The Green at West Village. Fully furnished with full bed, shared kitchen and common space. Utilities, high-speed WiFi, and Stream2 TV included. Fitness center, study rooms on every floor, dedicated Unitrans V Line. Zero-net-energy community.",
        "amenities": "furnished, wifi included, tv included, utilities included, fitness center, study rooms, unitrans V line, solar powered, bike parking",
        "pets_allowed": "No",
        "has_parking": "Yes",
        "laundry_type": "in-building",
        "post_date": "2025-08-01",
        "url": "https://housing.ucdavis.edu/apartments/the-green/",
    },
    {
        "listing_id": "oc_green_double",
        "complex_name": "The Green at West Village",
        "address": "298 Celadon St, Davis, CA 95616",
        "price_total": 882,
        "bedrooms": 1,
        "baths": 1,
        "sqft": 150,
        "lat": 38.5426,
        "lon": -121.7749,
        "description": "On-campus double-occupancy bedroom in a shared apartment at The Green at West Village. Room shared with one roommate, fully furnished with full beds. Shared kitchen and common space. Utilities, high-speed WiFi, and Stream2 TV included. Fitness center, study rooms, dedicated Unitrans V Line.",
        "amenities": "furnished, wifi included, tv included, utilities included, fitness center, study rooms, unitrans V line, solar powered, bike parking",
        "pets_allowed": "No",
        "has_parking": "Yes",
        "laundry_type": "in-building",
        "post_date": "2025-08-01",
        "url": "https://housing.ucdavis.edu/apartments/the-green/",
    },
    # ===================================================================
    # ORCHARD PARK (UC Davis operated, CHF-Davis II)
    # Orchard Park Circle, northwest campus | 38.5440, -121.7608
    # Monthly billing, all utilities + WiFi included
    # ===================================================================
    {
        "listing_id": "oc_orchard_studio",
        "complex_name": "Orchard Park",
        "address": "Orchard Park Circle, Davis, CA 95616",
        "price_total": 1870,
        "bedrooms": 0,
        "baths": 1,
        "sqft": 350,
        "lat": 38.5440,
        "lon": -121.7608,
        "description": "On-campus furnished studio at Orchard Park. Single bed lease. All utilities and WiFi included. Card-operated laundry on each floor. Fitness rooms, study/meeting spaces, tot lot, bike/ped paths. 4-story building. Located on northwest corner of UC Davis campus off Russell Blvd.",
        "amenities": "furnished, wifi included, utilities included, fitness room, study rooms, tot lot, bike paths, card laundry",
        "pets_allowed": "No",
        "has_parking": "Yes",
        "laundry_type": "in-building",
        "post_date": "2025-08-01",
        "url": "https://housing.ucdavis.edu/apartments/orchard-park/",
    },
    {
        "listing_id": "oc_orchard_4br_single",
        "complex_name": "Orchard Park",
        "address": "Orchard Park Circle, Davis, CA 95616",
        "price_total": 1173,
        "bedrooms": 1,
        "baths": 1,
        "sqft": 290,
        "lat": 38.5440,
        "lon": -121.7608,
        "description": "On-campus single bedroom in a furnished 4-bedroom apartment at Orchard Park. Per-bed lease. All utilities and WiFi included. Card-operated laundry on each floor. Fitness rooms, study spaces, bike paths. Northwest campus off Russell Blvd.",
        "amenities": "furnished, wifi included, utilities included, fitness room, study rooms, bike paths, card laundry",
        "pets_allowed": "No",
        "has_parking": "Yes",
        "laundry_type": "in-building",
        "post_date": "2025-08-01",
        "url": "https://housing.ucdavis.edu/apartments/orchard-park/",
    },
    {
        "listing_id": "oc_orchard_2br_single",
        "complex_name": "Orchard Park",
        "address": "Orchard Park Circle, Davis, CA 95616",
        "price_total": 1323,
        "bedrooms": 1,
        "baths": 1,
        "sqft": 350,
        "lat": 38.5440,
        "lon": -121.7608,
        "description": "On-campus single bedroom in a furnished 2-bedroom apartment at Orchard Park. Per-bed lease. All utilities and WiFi included. Card-operated laundry on each floor. Fitness rooms, study spaces. Northwest campus off Russell Blvd.",
        "amenities": "furnished, wifi included, utilities included, fitness room, study rooms, card laundry",
        "pets_allowed": "No",
        "has_parking": "Yes",
        "laundry_type": "in-building",
        "post_date": "2025-08-01",
        "url": "https://housing.ucdavis.edu/apartments/orchard-park/",
    },
    {
        "listing_id": "oc_orchard_2br_double",
        "complex_name": "Orchard Park",
        "address": "Orchard Park Circle, Davis, CA 95616",
        "price_total": 702,
        "bedrooms": 1,
        "baths": 1,
        "sqft": 175,
        "lat": 38.5440,
        "lon": -121.7608,
        "description": "On-campus double-occupancy bedroom in a furnished 2-bedroom apartment at Orchard Park. Per-bed lease, limited availability. Room shared with one roommate. All utilities and WiFi included. Laundry on each floor. Northwest campus.",
        "amenities": "furnished, wifi included, utilities included, fitness room, card laundry",
        "pets_allowed": "No",
        "has_parking": "Yes",
        "laundry_type": "in-building",
        "post_date": "2025-08-01",
        "url": "https://housing.ucdavis.edu/apartments/orchard-park/",
    },
    {
        "listing_id": "oc_orchard_2br_furn_unit",
        "complex_name": "Orchard Park (Family/Grad)",
        "address": "Orchard Park Circle, Davis, CA 95616",
        "price_total": 2727,
        "bedrooms": 2,
        "baths": 1,
        "sqft": 700,
        "lat": 38.5440,
        "lon": -121.7608,
        "description": "On-campus furnished 2-bedroom apartment at Orchard Park for families and graduate students. Unit lease with monthly rent. All utilities and WiFi included. Card-operated laundry, fitness rooms, tot lot. $250 security deposit required. Northwest campus off Russell Blvd.",
        "amenities": "furnished, wifi included, utilities included, fitness room, tot lot, card laundry",
        "pets_allowed": "No",
        "has_parking": "Yes",
        "laundry_type": "in-building",
        "post_date": "2025-08-01",
        "url": "https://housing.ucdavis.edu/apartments/orchard-park/",
    },
    {
        "listing_id": "oc_orchard_2br_unfurn_unit",
        "complex_name": "Orchard Park (Family/Grad)",
        "address": "Orchard Park Circle, Davis, CA 95616",
        "price_total": 2602,
        "bedrooms": 2,
        "baths": 1,
        "sqft": 700,
        "lat": 38.5440,
        "lon": -121.7608,
        "description": "On-campus unfurnished 2-bedroom apartment at Orchard Park for families and graduate students. Unit lease with monthly rent. All utilities and WiFi included. Card-operated laundry, fitness rooms, tot lot. $250 security deposit. Northwest campus off Russell Blvd.",
        "amenities": "wifi included, utilities included, fitness room, tot lot, card laundry",
        "pets_allowed": "No",
        "has_parking": "Yes",
        "laundry_type": "in-building",
        "post_date": "2025-08-01",
        "url": "https://housing.ucdavis.edu/apartments/orchard-park/",
    },
    # ===================================================================
    # PRIMERO GROVE (UC Davis operated, grad/family housing)
    # Near Primero/Russell area | 38.5455, -121.7570
    # Monthly unit lease, unfurnished, utilities included
    # ===================================================================
    {
        "listing_id": "oc_primero_studio",
        "complex_name": "Primero Grove",
        "address": "Primero Grove, Davis, CA 95616",
        "price_total": 1359,
        "bedrooms": 0,
        "baths": 1,
        "sqft": 400,
        "lat": 38.5455,
        "lon": -121.7570,
        "description": "On-campus unfurnished studio at Primero Grove. Unit lease for graduate students and families. Utilities included. Located near Segundo/Tercero area, close to campus core. Quiet residential community with bike paths.",
        "amenities": "utilities included, bike paths, quiet community",
        "pets_allowed": "No",
        "has_parking": "Yes",
        "laundry_type": "shared",
        "post_date": "2025-08-01",
        "url": "https://housing.ucdavis.edu/apartments/primero-grove/",
    },
    {
        "listing_id": "oc_primero_1br",
        "complex_name": "Primero Grove",
        "address": "Primero Grove, Davis, CA 95616",
        "price_total": 1427,
        "bedrooms": 1,
        "baths": 1,
        "sqft": 550,
        "lat": 38.5455,
        "lon": -121.7570,
        "description": "On-campus unfurnished 1-bedroom apartment at Primero Grove. Unit lease for graduate students and families. Utilities included. Near campus core, bike-friendly. Quiet residential community.",
        "amenities": "utilities included, bike paths, quiet community",
        "pets_allowed": "No",
        "has_parking": "Yes",
        "laundry_type": "shared",
        "post_date": "2025-08-01",
        "url": "https://housing.ucdavis.edu/apartments/primero-grove/",
    },
    {
        "listing_id": "oc_primero_2br",
        "complex_name": "Primero Grove",
        "address": "Primero Grove, Davis, CA 95616",
        "price_total": 1831,
        "bedrooms": 2,
        "baths": 1,
        "sqft": 800,
        "lat": 38.5455,
        "lon": -121.7570,
        "description": "On-campus unfurnished 2-bedroom apartment at Primero Grove. Unit lease for graduate students and families. Utilities included. Near campus core. Quiet community with walking and biking paths.",
        "amenities": "utilities included, bike paths, walking paths, quiet community",
        "pets_allowed": "No",
        "has_parking": "Yes",
        "laundry_type": "shared",
        "post_date": "2025-08-01",
        "url": "https://housing.ucdavis.edu/apartments/primero-grove/",
    },
    {
        "listing_id": "oc_primero_3br",
        "complex_name": "Primero Grove",
        "address": "Primero Grove, Davis, CA 95616",
        "price_total": 2499,
        "bedrooms": 3,
        "baths": 2,
        "sqft": 1050,
        "lat": 38.5455,
        "lon": -121.7570,
        "description": "On-campus unfurnished 3-bedroom apartment at Primero Grove. Unit lease for families and graduate students. Utilities included. Walking distance to campus core. Spacious layout with bike-friendly paths.",
        "amenities": "utilities included, bike paths, walking paths, quiet community",
        "pets_allowed": "No",
        "has_parking": "Yes",
        "laundry_type": "shared",
        "post_date": "2025-08-01",
        "url": "https://housing.ucdavis.edu/apartments/primero-grove/",
    },
    {
        "listing_id": "oc_primero_4br",
        "complex_name": "Primero Grove",
        "address": "Primero Grove, Davis, CA 95616",
        "price_total": 3232,
        "bedrooms": 4,
        "baths": 2,
        "sqft": 1300,
        "lat": 38.5455,
        "lon": -121.7570,
        "description": "On-campus unfurnished 4-bedroom apartment at Primero Grove. Unit lease for families and graduate students. Utilities included. Largest floor plan. Near campus core with easy bike access.",
        "amenities": "utilities included, bike paths, quiet community",
        "pets_allowed": "No",
        "has_parking": "Yes",
        "laundry_type": "shared",
        "post_date": "2025-08-01",
        "url": "https://housing.ucdavis.edu/apartments/primero-grove/",
    },
    # ===================================================================
    # ANOVA AGGIE SQUARE (UC Davis operated, Sacramento campus)
    # Sacramento | 38.5530, -121.4560
    # WARNING: ~15 mi from Davis main campus. Delete if Davis-only.
    # ===================================================================
    {
        "listing_id": "oc_anova_micro_studio",
        "complex_name": "ANOVA Aggie Square",
        "address": "ANOVA Aggie Square, Sacramento, CA 95817",
        "price_total": 1510,
        "bedrooms": 0,
        "baths": 1,
        "sqft": 300,
        "lat": 38.5530,
        "lon": -121.4560,
        "description": "Micro studio at ANOVA Aggie Square in Sacramento. Single bed lease for UC Davis students. Furnished. Modern graduate student housing at the new Aggie Square campus development. Easy access to UC Davis Health campus.",
        "amenities": "furnished, modern building, near UC Davis Health",
        "pets_allowed": "No",
        "has_parking": "Yes",
        "laundry_type": "in-building",
        "post_date": "2025-08-01",
        "url": "https://housing.ucdavis.edu/how-to-apply/anova-aggie-square/",
    },
    {
        "listing_id": "oc_anova_2br_single",
        "complex_name": "ANOVA Aggie Square",
        "address": "ANOVA Aggie Square, Sacramento, CA 95817",
        "price_total": 1328,
        "bedrooms": 1,
        "baths": 1,
        "sqft": 250,
        "lat": 38.5530,
        "lon": -121.4560,
        "description": "Single bedroom in a 2-bedroom apartment at ANOVA Aggie Square in Sacramento. Per-bed lease for UC Davis students. Furnished. Located at the new Aggie Square campus near UC Davis Health.",
        "amenities": "furnished, modern building, near UC Davis Health",
        "pets_allowed": "No",
        "has_parking": "Yes",
        "laundry_type": "in-building",
        "post_date": "2025-08-01",
        "url": "https://housing.ucdavis.edu/how-to-apply/anova-aggie-square/",
    },
    {
        "listing_id": "oc_anova_4br_single",
        "complex_name": "ANOVA Aggie Square",
        "address": "ANOVA Aggie Square, Sacramento, CA 95817",
        "price_total": 1138,
        "bedrooms": 1,
        "baths": 1,
        "sqft": 200,
        "lat": 38.5530,
        "lon": -121.4560,
        "description": "Single bedroom in a 4-bedroom apartment at ANOVA Aggie Square in Sacramento. Per-bed lease for UC Davis students. Furnished. Modern building at the Aggie Square campus development near UC Davis Health.",
        "amenities": "furnished, modern building, near UC Davis Health",
        "pets_allowed": "No",
        "has_parking": "Yes",
        "laundry_type": "in-building",
        "post_date": "2025-08-01",
        "url": "https://housing.ucdavis.edu/how-to-apply/anova-aggie-square/",
    },
    # ===================================================================
    # SOL AT WEST VILLAGE (P3, Landmark Properties)
    # 1580 Jade St | 38.5398, -121.7722
    # Per-bed leasing, furnished, utilities NOT all included
    # ===================================================================
    {
        "listing_id": "oc_sol_1br",
        "complex_name": "Sol at West Village",
        "address": "1580 Jade St, Davis, CA 95616",
        "price_total": 1099,
        "bedrooms": 1,
        "baths": 1,
        "sqft": 790,
        "lat": 38.5398,
        "lon": -121.7722,
        "description": "1-bedroom apartment at Sol at West Village. Per-bed lease. Furnished units available. Walk to campus, pool, spa, fitness center, media theater, yoga studio, sand volleyball, outdoor kitchen with BBQs. Pet-friendly with dog park. In-unit washer/dryer, granite countertops, walk-in closets. EV charging available.",
        "amenities": "furnished, pool, spa, fitness center, media theater, yoga studio, volleyball, outdoor kitchen, BBQ, dog park, EV charging, washer/dryer in-unit, granite countertops, walk-in closets",
        "pets_allowed": "Yes",
        "has_parking": "Yes",
        "laundry_type": "in-unit",
        "post_date": "2025-08-01",
        "url": "https://solatwestvillage.com/",
    },
    {
        "listing_id": "oc_sol_2br",
        "complex_name": "Sol at West Village",
        "address": "1580 Jade St, Davis, CA 95616",
        "price_total": 1350,
        "bedrooms": 1,
        "baths": 1,
        "sqft": 500,
        "lat": 38.5398,
        "lon": -121.7722,
        "description": "Single bedroom in a 2-bedroom apartment at Sol at West Village. Per-bed lease with individual locking bedrooms and private bathroom. Furnished. Pool, spa, fitness center, media theater. In-unit washer/dryer. Walk to campus. Pet-friendly. EV charging.",
        "amenities": "furnished, pool, spa, fitness center, media theater, locking bedrooms, private bathroom, washer/dryer in-unit, EV charging, dog park",
        "pets_allowed": "Yes",
        "has_parking": "Yes",
        "laundry_type": "in-unit",
        "post_date": "2025-08-01",
        "url": "https://solatwestvillage.com/",
    },
    {
        "listing_id": "oc_sol_4br",
        "complex_name": "Sol at West Village",
        "address": "1580 Jade St, Davis, CA 95616",
        "price_total": 1150,
        "bedrooms": 1,
        "baths": 1,
        "sqft": 370,
        "lat": 38.5398,
        "lon": -121.7722,
        "description": "Single bedroom in a 4-bedroom apartment at Sol at West Village. Per-bed lease with individual locking bedrooms and private bathroom. Furnished. Pool, spa, fitness center, media theater, yoga studio. Walk to campus. Pet-friendly.",
        "amenities": "furnished, pool, spa, fitness center, media theater, yoga studio, locking bedrooms, private bathroom, washer/dryer in-unit, dog park",
        "pets_allowed": "Yes",
        "has_parking": "Yes",
        "laundry_type": "in-unit",
        "post_date": "2025-08-01",
        "url": "https://solatwestvillage.com/",
    },
    # ===================================================================
    # THE COLLEGES AT LA RUE (P3, Tandem Properties)
    # 164 La Rue Rd | 38.5394, -121.7580
    # Across from ARC and Rec Pool
    # ===================================================================
    {
        "listing_id": "oc_colleges_1br",
        "complex_name": "The Colleges at La Rue",
        "address": "164 La Rue Rd, Davis, CA 95616",
        "price_total": 1566,
        "bedrooms": 1,
        "baths": 1,
        "sqft": 480,
        "lat": 38.5394,
        "lon": -121.7580,
        "description": "1-bedroom apartment at The Colleges at La Rue. On campus across from the ARC and Recreation Pool. Managed by Tandem Properties. Dog and cat friendly. In-unit washer/dryer, microwave, granite countertops. Fitness center, pool, clubhouse. Walk to any class in minutes. For UC Davis continuing undergrad, grad, and transfer students only.",
        "amenities": "washer/dryer in-unit, microwave, granite countertops, fitness center, pool, clubhouse, study rooms, geothermal heating/cooling, bike racks",
        "pets_allowed": "Yes",
        "has_parking": "Yes",
        "laundry_type": "in-unit",
        "post_date": "2025-08-01",
        "url": "https://colleges.tandemproperties.com/",
    },
    {
        "listing_id": "oc_colleges_2br",
        "complex_name": "The Colleges at La Rue",
        "address": "164 La Rue Rd, Davis, CA 95616",
        "price_total": 2100,
        "bedrooms": 2,
        "baths": 2,
        "sqft": 900,
        "lat": 38.5394,
        "lon": -121.7580,
        "description": "2-bedroom apartment at The Colleges at La Rue. On campus across from the ARC. Managed by Tandem Properties. Geothermal heating/cooling. In-unit washer/dryer, modern kitchen. Pet-friendly. Fitness center, pool, study rooms. Voted Best Place to Live in Davis by UC Davis students 3 years running.",
        "amenities": "washer/dryer in-unit, modern kitchen, fitness center, pool, study rooms, clubhouse, geothermal, bike racks, BBQ area",
        "pets_allowed": "Yes",
        "has_parking": "Yes",
        "laundry_type": "in-unit",
        "post_date": "2025-08-01",
        "url": "https://colleges.tandemproperties.com/",
    },
    {
        "listing_id": "oc_colleges_4br",
        "complex_name": "The Colleges at La Rue",
        "address": "164 La Rue Rd, Davis, CA 95616",
        "price_total": 3400,
        "bedrooms": 4,
        "baths": 2,
        "sqft": 1590,
        "lat": 38.5394,
        "lon": -121.7580,
        "description": "4-bedroom apartment at The Colleges at La Rue. Largest floor plan, great for groups. On campus right across from ARC. Managed by Tandem Properties. Pet-friendly. In-unit washer/dryer. Geothermal climate control. Fitness center, pool, study rooms, BBQ areas.",
        "amenities": "washer/dryer in-unit, fitness center, pool, study rooms, clubhouse, geothermal, bike racks, BBQ area, storage",
        "pets_allowed": "Yes",
        "has_parking": "Yes",
        "laundry_type": "in-unit",
        "post_date": "2025-08-01",
        "url": "https://colleges.tandemproperties.com/",
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

    anova_count = sum(1 for L in new_listings if "anova" in L["listing_id"])
    if anova_count > 0:
        print(f"\n  NOTE: {anova_count} ANOVA Aggie Square listings are in SACRAMENTO")
        print("  (not Davis proper). dist_to_mu will be ~15 miles.")
        print("  remove the oc_anova_* entries from this script if you want")
        print("  Davis-only listings.")


if __name__ == "__main__":
    main()
