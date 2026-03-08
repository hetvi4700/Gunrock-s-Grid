# data/database.py
# handles all the sqlite stuff - creating tables, inserting rows, reading data
# we use sqlite bc its simple and doesnt need a server running

import sqlite3
import pandas as pd
import os

# go up one folder to find config
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_NAME


def get_db_path():
    """returns full path to the database file
    we keep the db in the project root folder"""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(root, DB_NAME)


def create_tables():
    """makes the listings table if it doesnt exist yet
    
    the schema has all 17 columns from our project doc.
    listing_id is the primary key - its a combo of source prefix + hash
    so we dont get duplicate entries if we scrape the same listing twice
    """
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS listings (
            listing_id TEXT PRIMARY KEY,
            complex_name TEXT,
            address TEXT,
            neighborhood TEXT,
            price_per_bed REAL,
            price_total REAL,
            bedrooms INTEGER,
            sqft INTEGER,
            price_per_sqft REAL,
            lat REAL,
            lon REAL,
            dist_to_mu REAL,
            transit_score INTEGER,
            review_count INTEGER,
            avg_star_rating REAL,
            sentiment_score REAL,
            value_index REAL
        )
    """)

    # also make a table for raw reviews so we can reprocess them later if needed
    # like if we want to try a different NLP method
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS reviews (
            review_id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id TEXT,
            review_text TEXT,
            star_rating REAL,
            review_date TEXT,
            compound_score REAL,
            FOREIGN KEY (listing_id) REFERENCES listings(listing_id)
        )
    """)

    conn.commit()
    conn.close()
    print("tables created (or already existed)")


def insert_listing(listing_dict):
    """insert one listing into the db
    uses INSERT OR IGNORE so duplicates just get skipped
    
    listing_dict should have keys matching the column names
    if some keys are missing thats ok, they'll just be NULL
    """
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()

    # build the sql dynamically based on what keys are in the dict
    cols = ", ".join(listing_dict.keys())
    placeholders = ", ".join(["?" for _ in listing_dict])
    vals = list(listing_dict.values())

    sql = f"INSERT OR IGNORE INTO listings ({cols}) VALUES ({placeholders})"
    cursor.execute(sql, vals)

    conn.commit()
    conn.close()


def insert_many_listings(list_of_dicts):
    """bulk insert a bunch of listings at once
    way faster than calling insert_listing in a loop
    
    expects a list of dicts, all with the same keys
    """
    if not list_of_dicts:
        print("nothing to insert, list is empty")
        return
    
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()

    cols = ", ".join(list_of_dicts[0].keys())
    placeholders = ", ".join(["?" for _ in list_of_dicts[0]])
    sql = f"INSERT OR IGNORE INTO listings ({cols}) VALUES ({placeholders})"

    rows = [list(d.values()) for d in list_of_dicts]
    cursor.executemany(sql, rows)

    conn.commit()
    inserted = cursor.rowcount
    conn.close()
    print(f"inserted {inserted} listings into db")
    return inserted


def insert_review(review_dict):
    """insert a single review into the reviews table"""
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()

    cols = ", ".join(review_dict.keys())
    placeholders = ", ".join(["?" for _ in review_dict])
    vals = list(review_dict.values())

    sql = f"INSERT INTO reviews ({cols}) VALUES ({placeholders})"
    cursor.execute(sql, vals)

    conn.commit()
    conn.close()


def get_all_listings():
    """pull everything from the listings table into a pandas dataframe
    this is what the streamlit app will call to get data for charts
    """
    conn = sqlite3.connect(get_db_path())
    df = pd.read_sql("SELECT * FROM listings", conn)
    conn.close()
    return df


def get_reviews_for_listing(listing_id):
    """get all reviews for a specifc listing
    useful for when we want to show review details in the app"""
    conn = sqlite3.connect(get_db_path())
    df = pd.read_sql("SELECT * FROM reviews WHERE listing_id = ?", conn, params=[listing_id])
    conn.close()
    return df


def get_listing_count():
    """quick count of how many listings we have - good for sanity checks"""
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM listings")
    count = cursor.fetchone()[0]
    conn.close()
    return count


def update_listing_field(listing_id, field_name, value):
    """update a single field for a listing
    we use this after enrichment steps like geocoding or sentiment scoring
    instead of rewriting the whole row
    """
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()
    sql = f"UPDATE listings SET {field_name} = ? WHERE listing_id = ?"
    cursor.execute(sql, [value, listing_id])
    conn.commit()
    conn.close()


# run this if you execute the file directly - good for testing
if __name__ == "__main__":
    create_tables()
    print(f"db path: {get_db_path()}")
    print(f"current listing count: {get_listing_count()}")
