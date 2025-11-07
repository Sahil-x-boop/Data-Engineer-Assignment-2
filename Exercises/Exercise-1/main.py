import requests
import zipfile
from pathlib import Path
from typing import Optional

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip"
]

downloads_dir = Path("downloads")
downloads_dir.mkdir(exist_ok=True)


def download_file(url: str, target_dir: Path) -> Optional[Path]:
    filename = url.split("/")[-1]
    file_path = target_dir / filename
    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        return file_path
    except requests.exceptions.RequestException:
        return None

def extract_zip(file_path: Path, extract_dir: Path) -> None:
    try:
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(extract_dir)
        file_path.unlink()
    except zipfile.BadZipFile:
        return

def main() -> None:
    for url in download_uris:
        zip_path = download_file(url, downloads_dir)
        if zip_path and zip_path.exists():
            extract_zip(zip_path, downloads_dir)

if __name__ == "__main__":
    main()
