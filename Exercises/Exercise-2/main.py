import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from pathlib import Path
from typing import Optional

BASE_URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
TARGET_TIMESTAMP = "2024-01-19 15:45"

downloads_dir = Path("downloads")
downloads_dir.mkdir(exist_ok=True)

def fetch_directory_index(url: str) -> str:
    resp = requests.get(url, timeout=(5, 30))
    resp.raise_for_status()
    return resp.text

def normalize_space(s: str) -> str:
    return " ".join(s.split())

def find_filename_by_modified(html: str, target: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    for row in soup.select("tr"):
        cells = row.find_all("td")
        if len(cells) >= 2:
            modified = normalize_space(cells[1].get_text(strip=True))
            if modified.startswith(target):
                link = cells[0].find("a")
                if link and link.get("href"):
                    name = link.get_text(strip=True)
                    if name.lower().endswith(".csv"):
                        return name
    return None

def download_file(base_url: str, filename: str, target_dir: Path) -> Path:
    url = urljoin(base_url, filename)
    path = target_dir / filename
    with requests.get(url, stream=True, timeout=(5, 60)) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    return path

def print_max_temperature_records(csv_path: Path) -> None:
    df = pd.read_csv(csv_path)
    col = "HourlyDryBulbTemperature"
    series = pd.to_numeric(df[col], errors="coerce")
    max_val = series.max()
    records = df[series == max_val]
    print(records[['STATION', 'DATE', 'NAME', 'HourlyDryBulbTemperature']].to_string(index=False))

def main() -> None:
    html = fetch_directory_index(BASE_URL)
    filename = find_filename_by_modified(html, TARGET_TIMESTAMP)
    if not filename:
        raise RuntimeError(f"Could not find file with Last Modified starting with '{TARGET_TIMESTAMP}'")
    csv_path = download_file(BASE_URL, filename, downloads_dir)
    print_max_temperature_records(csv_path)

if __name__ == "__main__":
    main()
