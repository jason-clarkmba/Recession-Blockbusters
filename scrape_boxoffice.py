# Box Office Mojo + OMDb Combined Scraper (2000–2023)

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import os

# ========== CONFIG ========== #
API_KEY = "4903cdae"  # OMDb API Key
BASE_URL = "https://www.boxofficemojo.com/year/{}/"
OUTPUT_CSV = "box_office_2000_2023.csv"
YEARS = list(range(2000, 2024))

# ========== SCRAPE BOX OFFICE MOJO ========== #
if not os.path.exists(OUTPUT_CSV):
    all_movies = []
    for year in YEARS:
        url = BASE_URL.format(year)
        print(f"Scraping Box Office Mojo for {year}...")
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")

        table = soup.find("table")
        if not table:
            print(f"⚠️ No table found for year {year}, skipping.")
            continue

        rows = table.find_all("tr")[1:]
        for row in rows:
            cols = row.find_all("td")
            try:
                rank = cols[0].text.strip()
                title = cols[1].text.strip()
                gross = cols[7].text.strip().replace('$','').replace(',','')
                all_movies.append({
                    "Year": year,
                    "Rank": int(rank),
                    "Movie_Title": title,
                    "Domestic_Revenue": int(gross)
                })
            except Exception as e:
                print(f"Row skipped due to error: {e}")
                continue

        time.sleep(random.uniform(1, 2))

    df = pd.DataFrame(all_movies)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"✅ Initial scrape complete! Data saved to '{OUTPUT_CSV}'")
else:
    df = pd.read_csv(OUTPUT_CSV)
    print(f"📂 Loaded existing file: {OUTPUT_CSV}")

# ========== ENRICH WITH OMDb GENRE & RELEASE DATE ========== #
for col in ["Genre", "Release_Date", "OMDb_Found"]:
    if col not in df.columns:
        df[col] = ""

pending = df[df['Genre'].isna() | (df['Genre'] == "")]
print(f"🔍 Enriching {len(pending)} titles with OMDb metadata...")

OMDB_URL = "http://www.omdbapi.com/"

for idx in pending.index:
    title = df.at[idx, "Movie_Title"]
    year = df.at[idx, "Year"]
    print(f"Querying OMDb: {title} ({year})")

    params = {
        't': title,
        'y': year,
        'apikey': API_KEY
    }
    response = requests.get(OMDB_URL, params=params)
    data = response.json()

    if data.get('Response') != 'True':
        print("  ✗ Not found with year. Retrying without year...")
        params = {
            't': title,
            'apikey': API_KEY
        }
        response = requests.get(OMDB_URL, params=params)
        data = response.json()

    if data.get('Response') == 'True':
        df.at[idx, 'Genre'] = data.get('Genre', '')
        df.at[idx, 'Release_Date'] = data.get('Released', '')
        df.at[idx, 'OMDb_Found'] = "Yes"
        print(f"  ✓ Found → {data.get('Genre')}")
    else:
        df.at[idx, 'OMDb_Found'] = "No"
        print(f"  ✗ Still not found — {data.get('Error')}")

    if idx % 10 == 0:
        df.to_csv(OUTPUT_CSV, index=False)
        print("💾 Progress saved.")

    time.sleep(random.uniform(0.5, 1.3))

# Final save
df.to_csv(OUTPUT_CSV, index=False)
print("✅ Enrichment complete!")
