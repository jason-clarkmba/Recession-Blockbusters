#!/usr/bin/env python3
"""
backfill_omdb.py

Back-fill missing Plot and rating fields in your box office dataset using the OMDb API.
This script uses a persistent cache to avoid re-fetching already-retrieved entries and can
be safely stopped and resumed at any time.

Usage:
    1. Install dependencies:
        pip install pandas requests

    2. Export your OMDb API key:
        export OMDB_API_KEY="4903cdae"

    3. Place this script in the same folder as 'box_office_2000_2023_emotions_v2.csv'.

    4. Run:
        python backfill_omdb.py

Outputs:
    - box_office_2000_2023_emotions_v3.csv : the enriched dataset
    - omdb_cache.json                    : cache file for fetched OMDb responses
"""

import os
import time
import json
import pandas as pd
import requests
from datetime import datetime

# Configuration
INPUT_CSV     = "box_office_2000_2023_emotions_v2.csv"
OUTPUT_CSV    = "box_office_2000_2023_emotions_v3.csv"
CACHE_FILE    = "omdb_cache.json"
API_KEY       = os.getenv("OMDB_API_KEY")
REQUEST_DELAY = 0.2    # seconds between OMDb requests
SAVE_INTERVAL = 500    # save progress every N rows

if not API_KEY:
    raise RuntimeError("OMDB_API_KEY environment variable not set")

# Load dataset
df = pd.read_csv(INPUT_CSV)

# Cast rating columns to object dtype to accept empty strings
# prevents dtype warnings when assigning empty strings
df["IMDb_Rating"] = df["IMDb_Rating"].astype(object)
df["Metascore"]   = df["Metascore"].astype(object)

# Load or create cache
if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, 'r') as f:
        cache = json.load(f)
else:
    cache = {}


def fetch_omdb(title, year):
    """Fetch Plot and Ratings from OMDb for a given title and year."""
    params = {"t": title, "y": str(year), "apikey": API_KEY}
    try:
        resp = requests.get("http://www.omdbapi.com/", params=params, timeout=10)
        data = resp.json() if resp.ok else {}
    except Exception as e:
        print(f"Error fetching {title} ({year}): {e}")
        return {"Plot": "", "IMDb_Rating": "", "Metascore": ""}
    return {
        "Plot":        data.get("Plot", ""),
        "IMDb_Rating": data.get("imdbRating", ""),
        "Metascore":   data.get("Metascore", "")
    }

# Iterate and back-fill
total = len(df)
start_time = datetime.now()
print(f"Starting backfill of {total} rows...")

for idx, row in df.iterrows():
    title = row.get("Movie_Title", "").strip()
    year  = row.get("Year", "")
    key   = f"{title} ({year})"

    # Determine if we need to fetch
    needs_plot   = pd.isna(row.get("Plot")) or not str(row.get("Plot", "")).strip()
    needs_rating = pd.isna(row.get("IMDb_Rating"))

    if not (needs_plot or needs_rating) and key in cache:
        continue

    # Fetch and cache
    cache[key] = fetch_omdb(title, year)
    time.sleep(REQUEST_DELAY)

    # Update DataFrame
    omdb = cache[key]
    if needs_plot:
        df.at[idx, "Plot"] = omdb["Plot"]
    if needs_rating:
        df.at[idx, "IMDb_Rating"] = omdb["IMDb_Rating"]
        df.at[idx, "Metascore"]    = omdb["Metascore"]

    # Periodic save and log
    if (idx + 1) % SAVE_INTERVAL == 0:
        with open(CACHE_FILE, 'w') as f:
            json.dump(cache, f, indent=2)
        df.to_csv(OUTPUT_CSV, index=False)
        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"Processed {idx+1}/{total} rows. Elapsed {elapsed:.1f}s.")

# Final save
with open(CACHE_FILE, 'w') as f:
    json.dump(cache, f, indent=2)
df.to_csv(OUTPUT_CSV, index=False)
elapsed = (datetime.now() - start_time).total_seconds()
print(f"Done! {total} rows processed in {elapsed:.1f}s. Enriched dataset saved to {OUTPUT_CSV}")
