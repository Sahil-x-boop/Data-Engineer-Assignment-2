import gzip
import requests
from io import BytesIO, TextIOWrapper

def read_first_line_from_gz_http(url: str) -> str:
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        gz = gzip.GzipFile(fileobj=r.raw)
        wrapper = TextIOWrapper(gz, encoding="utf-8")
        return wrapper.readline().strip()

def stream_and_print_gz_from_http(url: str):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        gz = gzip.GzipFile(fileobj=r.raw)
        wrapper = TextIOWrapper(gz, encoding="utf-8")
        for line in wrapper:
            print(line.rstrip())

def main():
    list_url = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-05/wet.paths.gz"

    first_file_uri = read_first_line_from_gz_http(list_url)

    if not first_file_uri.startswith("s3://"):
        first_file_uri = "s3://commoncrawl/" + first_file_uri.lstrip("/")

    # Convert S3 path → HTTPS path
    http_url = first_file_uri.replace("s3://commoncrawl/", "https://data.commoncrawl.org/")

    print(f"Downloading and streaming: {http_url}")

    stream_and_print_gz_from_http(http_url)

if __name__ == "__main__":
    main()
