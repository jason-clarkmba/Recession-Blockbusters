import pandas as pd
import os
import re

def normalize_title(title):
    return re.sub(r"[^\w\s]", "", title).strip().lower() if pd.notna(title) else ""

def append_urls():
    master_file = "box_office_2000_2023_emotions.csv"
    if not os.path.exists(master_file):
        print(f"❌ Master file '{master_file}' not found.")
        return

    print(f"📥 Loading master file: {master_file}")
    master = pd.read_csv(master_file, dtype=str).fillna("")
    master["Title_norm"] = master["Movie_Title"].apply(normalize_title)
    master["Year_norm"] = master["Year"].astype(str)

    print(f"🔢 Master rows: {len(master)}")
    print("🔁 Starting to process cohort files...\n")

    for start in range(2021, 1999, -3):  # descending order
        end = start + 2
        cohort_file = f"box_office_{start}_{end}.csv"
        if not os.path.exists(cohort_file):
            print(f"⚠️  Skipping missing file: {cohort_file}")
            continue

        print(f"📄 Reading {cohort_file} ...")
        df = pd.read_csv(cohort_file, dtype=str).fillna("")
        df["Title_norm"] = df["Title"].apply(normalize_title)
        df["Year_norm"] = df["Year"].astype(str)

        before_merge = len(df)
        df = df.merge(
            master[["Title_norm", "Year_norm", "ReleaseURL"]],
            on=["Title_norm", "Year_norm"],
            how="left"
        )
        matched = df["ReleaseURL"].notna().sum()
        print(f"🔍 Matched {matched} of {before_merge} titles")

        df["URL"] = df["ReleaseURL"]
        df.drop(columns=["Title_norm", "Year_norm", "ReleaseURL"], inplace=True)

        df.to_csv(cohort_file, index=False)
        print(f"✅ Saved updated file with URLs: {cohort_file}\n")

if __name__ == "__main__":
    append_urls()
import pandas as pd
import requests
import time
import os

BASE_URL = "https://www.boxofficemojo.com"
COHORTS = [
    (2021, 2023),
    (2018, 2020),
    (2015, 2017),
    (2012, 2014),
    (2009, 2011),
    (2006, 2008),
    (2003, 2005),
    (2000, 2002),
]

HEADERS = {"User-Agent": "Mozilla/5.0"}

def fetch_five_week_grosses(url):
    if pd.isna(url) or not isinstance(url, str):
        return [None] * 5
    if url.startswith("/release/"):
        url = BASE_URL + url

    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        print(f"⚠️ Failed to fetch {url}: {e}")
        return [None] * 5

    tables = pd.read_html(resp.text)
    week_table = next((t for t in tables if "Week" in t.columns[0]), None)
    if week_table is None:
        print(f"⚠️ No weekly table found at {url}")
        return [None] * 5

    gross_col = [c for c in week_table.columns if "Gross" in c][0]
    five_weeks = week_table[gross_col].head(5).fillna("$0").tolist()
    return five_weeks + [None] * (5 - len(five_weeks))  # pad to 5 entries

def build_weekly_dataset():
    records = []
    for start, end in COHORTS:
        filename = f"box_office_{start}_{end}.csv"
        if not os.path.exists(filename):
            print(f"❌ File missing: {filename}")
            continue

        print(f"🔄 Processing {filename}...")
        df = pd.read_csv(filename)

        if "ReleaseURL" not in df.columns:
            print(f"⚠️ No ReleaseURL column in {filename}; skipping.")
            continue

        for _, row in df.iterrows():
            url = row.get("ReleaseURL", None)
            title = row.get("Title", None)
            year = row.get("Year", None)

            week1, week2, week3, week4, week5 = fetch_five_week_grosses(url)
            records.append({
                "Title": title,
                "Year": year,
                "ReleaseURL": url,
                "Week1_Gross": week1,
                "Week2_Gross": week2,
                "Week3_Gross": week3,
                "Week4_Gross": week4,
                "Week5_Gross": week5,
            })

            time.sleep(1.5)

    pd.DataFrame(records).to_csv("weekly_gross_5w_combined.csv", index=False)
    print("✅ Saved: weekly_gross_5w_combined.csv")

if __name__ == "__main__":
    build_weekly_dataset()
