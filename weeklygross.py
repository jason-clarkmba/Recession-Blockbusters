import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import os
import re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Setup a requests Session with retries
session = requests.Session()
retries = Retry(total=3, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retries))

HEADERS = {'User-Agent': 'Mozilla/5.0'}
BASE_URL = 'https://www.boxofficemojo.com'


def fetch_weekly_grosses(url):
    """
    Scrape cumulative weekly grosses for the first five weeks from the weekly table.
    """
    suffix = url.split('?')[0].rstrip('/')
    full_url = f"{BASE_URL}{suffix}/weekly/"
    try:
        resp = session.get(full_url, headers=HEADERS, timeout=(5, 5))
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')

        # Find all tables with mojo-body-table class
        tables = soup.find_all('table', class_=lambda c: c and 'mojo-body-table' in c)
        weekly_table = None
        weekly_idx = None

        for table in tables:
            # Determine header cells
            thead = table.find('thead')
            if thead:
                header_cells = thead.find_all('th')
            else:
                header_cells = table.find('tr').find_all(['th', 'td'])

            headers = [cell.get_text(strip=True) for cell in header_cells]
            # Look for header titled 'Weekly'
            for i, txt in enumerate(headers):
                if re.fullmatch(r'Weekly', txt, re.I):
                    weekly_table = table
                    weekly_idx = i
                    break
            if weekly_table:
                break

        if not weekly_table or weekly_idx is None:
            print(f"⚠️ 'Weekly' column not found at {full_url}")
            return [None] * 5

        # Extract first five data rows
        tbody = weekly_table.find('tbody') or weekly_table
        rows = tbody.find_all('tr')[1:6]
        grosses = []
        for row in rows:
            cells = row.find_all('td')
            if weekly_idx < len(cells):
                raw = cells[weekly_idx].get_text(strip=True).replace('$', '').replace(',', '')
                try:
                    grosses.append(int(raw))
                except ValueError:
                    grosses.append(None)
            else:
                grosses.append(None)

        # Pad to length 5
        grosses += [None] * (5 - len(grosses))
        return grosses

    except Exception as e:
        print(f"❌ Error fetching/parsing {full_url}: {e}")
        return [None] * 5


def process_cohort(file_path):
    print(f"\n🔄 Processing {file_path} ...")
    df = pd.read_csv(file_path)
    df.columns = df.columns.str.strip()

    # Standardize URL column
    if 'URL' not in df.columns and 'ReleaseURL' in df.columns:
        df['URL'] = df['ReleaseURL']
    if 'URL' not in df.columns:
        print(f"⚠️ No URL column in {file_path}. Columns: {list(df.columns)}")
        return

    # Initialize weekly gross columns
    for i in range(1, 6):
        df[f'Week{i}_Gross'] = None

    # Fetch and assign weekly grosses
    for idx in tqdm(df.index, desc=f"⏳ Fetching weekly grosses for {os.path.basename(file_path)}"):
        url = df.at[idx, 'URL']
        if not isinstance(url, str) or not url.startswith('/release/'):
            continue
        values = fetch_weekly_grosses(url)
        for week_num, val in enumerate(values, start=1):
            df.at[idx, f'Week{week_num}_Gross'] = val

    df.to_csv(file_path, index=False)
    print(f"✅ Saved: {file_path}")


def main():
    pattern = re.compile(r'^box_office_20\d{2}_20\d{2}\.csv$')
    files = sorted(f for f in os.listdir('.') if pattern.match(f))
    for f in files:
        process_cohort(f)


if __name__ == '__main__':
    main()
