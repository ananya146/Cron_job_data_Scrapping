import requests
from bs4 import BeautifulSoup
import time
import json
import os
import logging
import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
}

def get_book_url(book_name):
    """Searches DuckDuckGo for the Goodreads URL of a book."""
    try:
        logging.info(f"Searching for Goodreads URL for: {book_name}")
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
        logging.warning("No Goodreads URL found.")
        return None
    except Exception as e:
        logging.error(f"Error during DuckDuckGo search: {e}")
        return None

def scrape_data(book_url):
    """Scrapes Goodreads book data from the provided URL."""
    try:
        logging.info(f"Scraping data from URL: {book_url}")
        response = requests.get(book_url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')

        title = soup.find("h1", {"data-testid": "bookTitle"})
        author = soup.find("span", {"class": "ContributorLink__name"})
        rating = soup.find("div", {"data-testid": "averageRating"})
        description = soup.find("div", {"data-testid": "description"})

        data = {
            "title": title.get_text(strip=True) if title else None,
            "author": author.get_text(strip=True) if author else None,
            "rating": rating.get_text(strip=True) if rating else None,
            "description": description.get_text(strip=True) if description else None,
            "url": book_url
        }

        return data
    except Exception as e:
        logging.error(f"Error while scraping Goodreads page: {e}")
        return None

def save_data_json(data, filename):
    """Saves scraped data to a JSON file."""
    try:
        if not os.path.exists('output'):
            os.makedirs('output')
        filepath = os.path.join('output', f"{filename}.json")
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
        logging.info(f"Data saved to {filepath}")
    except Exception as e:
        logging.error(f"Failed to save JSON: {e}")

def main():
    csv_file = 'books.csv'
    if not os.path.exists(csv_file):
        logging.error(f"CSV file '{csv_file}' not found.")
        return

    try:
        df = pd.read_csv(csv_file)
        if 'book' not in df.columns:
            logging.error("CSV must contain a column named 'book'.")
            return

        for book_name in df['book']:
            book_name = str(book_name).strip()
            if not book_name:
                continue

            filename_base = book_name.lower().replace(' ', '_').replace(':', '').replace('\'', '').replace('/', '').replace('\\', '')[:50]

            logging.info(f"\n\n--- Starting Goodreads Scraper for: {book_name} ---")
            book_url = get_book_url(book_name)

            if book_url:
                scraped_data = scrape_data(book_url)
                if scraped_data:
                    save_data_json(scraped_data, filename_base)
                else:
                    logging.error(f"Failed to scrape data for book URL: {book_url}")
            else:
                logging.error(f"Could not find a valid Goodreads URL for: {book_name}")

            time.sleep(3)  # Delay to avoid rate limiting

    except Exception as e:
        logging.error(f"Critical error while reading or processing the CSV: {e}")
    finally:
        logging.info("\n--- All scraping finished ---")

if __name__ == "__main__":
    main()
