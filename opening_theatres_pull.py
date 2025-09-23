import pandas as pd
import requests
from bs4 import BeautifulSoup
import time

BASE_URL = 'https://www.boxofficemojo.com'
HEADERS = {'User-Agent': 'Mozilla/5.0'}

def fetch_opening_week_theaters(release_url):
    if not isinstance(release_url, str) or not release_url.startswith('/release/'):
        return None
    url = f"{BASE_URL}{release_url.split('?')[0].rstrip('/')}/weekly/"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        table = soup.find('table', class_=lambda c: c and 'mojo-body-table' in c)
        if not table:
            return None
        header_cells = table.find('tr').find_all(['th', 'td'])
        headers = [cell.get_text(strip=True) for cell in header_cells]
        try:
            theaters_idx = headers.index('Theaters')
        except ValueError:
            return None
        rows = table.find_all('tr')
        if len(rows) < 2:
            return None
        first_row = rows[1]
        cells = first_row.find_all('td')
        if len(cells) > theaters_idx:
            val = cells[theaters_idx].get_text(strip=True).replace(',', '')
            return int(val) if val.isdigit() else None
    except Exception:
        return None
    return None

def process_file(input_csv, output_csv):
    df = pd.read_csv(input_csv, encoding="latin1")
    if 'ReleaseURL' not in df.columns:
        raise Exception("No 'ReleaseURL' column found in your CSV")
    df['Opening_Theaters'] = None
    for idx, row in df.iterrows():
        release_url = row['ReleaseURL']
        count = fetch_opening_week_theaters(release_url)
        df.at[idx, 'Opening_Theaters'] = count
        print(f"{idx+1}/{len(df)}: {release_url} → {count}")
        time.sleep(1.5)
    df.to_csv(output_csv, index=False)
    print(f"\nDone. Results written to {output_csv}")

if __name__ == "__main__":
    input_csv = "box_office_2000_2023_plot_ratings.csv"
    output_csv = "box_office_2000_2023_with_theaters.csv"
    process_file(input_csv, output_csv)
