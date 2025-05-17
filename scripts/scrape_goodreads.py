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

BATCH_SIZE = 15
INDEX_FILE = "last_index.txt"
CSV_FILE = "books.csv"

def read_last_index():
    if not os.path.exists(INDEX_FILE):
        return 0
    with open(INDEX_FILE, "r") as f:
        return int(f.read().strip() or 0)

def write_last_index(index):
    with open(INDEX_FILE, "w") as f:
        f.write(str(index))

def get_book_url(book_name):
    try:
        logging.info(f"Searching Goodreads URL for: {book_name}")
        search_url = "https://duckduckgo.com/html/"
        params = {"q": f"site:goodreads.com {book_name}"}
        response = requests.get(search_url, params=params, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        results = soup.find_all('a', href=True)

        for link in results:
            href = link['href']
            if 'goodreads.com/book/show' in href:
                if href.startswith("/l/"):
                    href = href.split("/l/?kh=-1&uddg=")[-1]
                    href = requests.utils.unquote(href)
                return href
        return None
    except Exception as e:
        logging.error(f"Search error: {e}")
        return None

def scrape_data(book_url):
    try:
        logging.info(f"Scraping URL: {book_url}")
        response = requests.get(book_url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')

        title = soup.find("h1", {"data-testid": "bookTitle"})
        author = soup.find("span", {"class": "ContributorLink__name"})
        rating = soup.find("div", {"data-testid": "averageRating"})
        description = soup.find("div", {"data-testid": "description"})

        return {
            "title": title.get_text(strip=True) if title else None,
            "author": author.get_text(strip=True) if author else None,
            "rating": rating.get_text(strip=True) if rating else None,
            "description": description.get_text(strip=True) if description else None,
            "url": book_url
        }
    except Exception as e:
        logging.error(f"Scraping error: {e}")
        return None

def save_data_json(data, filename):
    try:
        os.makedirs("output", exist_ok=True)
        filepath = os.path.join("output", f"{filename}.json")
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        logging.info(f"Saved: {filepath}")
    except Exception as e:
        logging.error(f"Save error: {e}")

def main():
    if not os.path.exists(CSV_FILE):
        logging.error(f"Missing: {CSV_FILE}")
        return

    df = pd.read_csv(CSV_FILE, header=None)  # No column names
    books = df[0].dropna().astype(str).tolist()
    total_books = len(books)
    last_index = read_last_index()
    next_index = min(last_index + BATCH_SIZE, total_books)

    logging.info(f"Scraping batch: {last_index} to {next_index - 1}")

    for i in range(last_index, next_index):
        book_name = books[i].strip()
        if not book_name:
            continue

        filename_base = book_name.lower().replace(' ', '_').replace(':', '').replace('\'', '').replace('/', '').replace('\\', '')[:50]
        logging.info(f"Processing: {book_name}")

        book_url = get_book_url(book_name)
        if book_url:
            scraped_data = scrape_data(book_url)
            if scraped_data and any(scraped_data.values()):
                save_data_json(scraped_data, filename_base)
            else:
                logging.warning(f"No data found for {book_name} at: {book_url}")
        else:
            logging.warning(f"No URL found for: {book_name}")

        time.sleep(3)

    write_last_index(next_index)
    logging.info("Batch completed.")

if __name__ == "__main__":
    main()
