import requests
from bs4 import BeautifulSoup
import time
import json
import os
import logging
import pandas as pd

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

BATCH_SIZE = 10
INDEX_FILE = "last_index.txt"
CSV_FILE = "books.csv"
OUTPUT_FOLDER = "output"

def read_last_index():
    if not os.path.exists(INDEX_FILE):
        return 0
    with open(INDEX_FILE, "r") as f:
        return int(f.read().strip())

def write_last_index(index):
    with open(INDEX_FILE, "w") as f:
        f.write(str(index))

def scrape_data(book_url):
    try:
        logging.info(f"Scraping URL: {book_url}")
        response = requests.get(book_url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')

        title_tag = soup.find("h1", {"data-testid": "bookTitle"})
        author_tag = soup.find("span", {"class": "ContributorLink__name"})
        rating_tag = soup.find("div", {"data-testid": "averageRating"})
        desc_tag = soup.find("div", {"data-testid": "description"})

        return {
            "title": title_tag.get_text(strip=True) if title_tag else None,
            "author": author_tag.get_text(strip=True) if author_tag else None,
            "rating": rating_tag.get_text(strip=True) if rating_tag else None,
            "description": desc_tag.get_text(strip=True) if desc_tag else None,
            "url": book_url
        }
    except Exception as e:
        logging.error(f"Scraping error at {book_url}: {e}")
        return None

def save_data_json(data, filename):
    try:
        os.makedirs(OUTPUT_FOLDER, exist_ok=True)
        filepath = os.path.join(OUTPUT_FOLDER, f"{filename}.json")
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logging.info(f"Saved: {filepath}")
    except Exception as e:
        logging.error(f"Save error: {e}")

def main():
    if not os.path.exists(CSV_FILE):
        logging.error(f"Missing: {CSV_FILE}")
        return

    # Load the CSV directly assuming it contains URLs without a header
    df = pd.read_csv(CSV_FILE, header=None, names=["url"])
    urls = df["url"].dropna().tolist()
    total = len(urls)

    last_index = read_last_index()
    next_index = min(last_index + BATCH_SIZE, total)

    logging.info(f"Scraping from index {last_index} to {next_index - 1}")

    for i in range(last_index, next_index):
        url = urls[i].strip()
        if not url.startswith("http"):
            continue

        filename_base = f"book_{i}"
        data = scrape_data(url)
        if data and data["title"]:
            save_data_json(data, filename_base)
        else:
            logging.warning(f"No data found or skipped: {url}")

        time.sleep(2)  # Be polite to Goodreads

    write_last_index(next_index)
    logging.info("Batch completed.")

if __name__ == "__main__":
    main()

