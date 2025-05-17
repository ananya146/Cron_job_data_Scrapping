# import requests
# from bs4 import BeautifulSoup
# import time
# import json
# import os
# import logging
# import pandas as pd

# # Setup logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# HEADERS = {
#     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
# }

# BATCH_SIZE = 10
# INDEX_FILE = "last_index.txt"
# CSV_FILE = "books.csv"
# OUTPUT_FOLDER = "output"

# def read_last_index():
#     if not os.path.exists(INDEX_FILE):
#         return 0
#     with open(INDEX_FILE, "r") as f:
#         return int(f.read().strip())

# def write_last_index(index):
#     with open(INDEX_FILE, "w") as f:
#         f.write(str(index))

# def scrape_data(book_url):
#     try:
#         logging.info(f"Scraping URL: {book_url}")
#         response = requests.get(book_url, headers=HEADERS, timeout=15)
#         soup = BeautifulSoup(response.text, 'html.parser')

#         title_tag = soup.find("h1", {"data-testid": "bookTitle"})
#         author_tag = soup.find("span", {"class": "ContributorLink__name"})
#         rating_tag = soup.find("div", {"data-testid": "averageRating"})
#         desc_tag = soup.find("div", {"data-testid": "description"})

#         return {
#             "title": title_tag.get_text(strip=True) if title_tag else None,
#             "author": author_tag.get_text(strip=True) if author_tag else None,
#             "rating": rating_tag.get_text(strip=True) if rating_tag else None,
#             "description": desc_tag.get_text(strip=True) if desc_tag else None,
#             "url": book_url
#         }
#     except Exception as e:
#         logging.error(f"Scraping error at {book_url}: {e}")
#         return None

# def save_data_json(data, filename):
#     try:
#         os.makedirs(OUTPUT_FOLDER, exist_ok=True)
#         filepath = os.path.join(OUTPUT_FOLDER, f"{filename}.json")
#         with open(filepath, "w", encoding="utf-8") as f:
#             json.dump(data, f, indent=4, ensure_ascii=False)
#         logging.info(f"Saved: {filepath}")
#     except Exception as e:
#         logging.error(f"Save error: {e}")

# def main():
#     if not os.path.exists(CSV_FILE):
#         logging.error(f"Missing: {CSV_FILE}")
#         return

#     # Load the CSV directly assuming it contains URLs without a header
#     df = pd.read_csv(CSV_FILE, header=None, names=["url"])
#     urls = df["url"].dropna().tolist()
#     total = len(urls)

#     last_index = read_last_index()
#     next_index = min(last_index + BATCH_SIZE, total)

#     logging.info(f"Scraping from index {last_index} to {next_index - 1}")

#     for i in range(last_index, next_index):
#         url = urls[i].strip()
#         if not url.startswith("http"):
#             continue

#         filename_base = f"book_{i}"
#         data = scrape_data(url)
#         if data and data["title"]:
#             save_data_json(data, filename_base)
#         else:
#             logging.warning(f"No data found or skipped: {url}")

#         time.sleep(2)  # Be polite to Goodreads

#     write_last_index(next_index)
#     logging.info("Batch completed.")

# if __name__ == "__main__":
#     main()

import requests
from bs4 import BeautifulSoup
import time
import json
import os
import logging
import pandas as pd
import re

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

BATCH_SIZE = 10  # Number of links to process per run
INDEX_FILE = "last_index.txt"
CSV_FILE = "books.csv"
OUTPUT_FOLDER = "output"

def read_last_index():
    if not os.path.exists(INDEX_FILE):
        return 0
    with open(INDEX_FILE, "r") as f:
        content = f.read().strip()
        return int(content) if content.isdigit() else 0

def write_last_index(index):
    with open(INDEX_FILE, "w") as f:
        f.write(str(index))

def scrape_data(book_url):
    """
    Given a Goodreads URL, extract a rich set of information:
      - title, author, rating, rating_count, review_count,
      - description, genres,
      - page_count, format, publication_date,
      - cover_image URL,
      - about_author (basic bio text if available)
    """
    try:
        logging.info(f"Scraping URL: {book_url}")
        response = requests.get(book_url, headers=HEADERS, timeout=15)
        if response.status_code != 200:
            logging.error(f"Non-200 status code: {response.status_code} for URL: {book_url}")
            return None
        soup = BeautifulSoup(response.text, 'html.parser')

        ## Title
        title_tag = soup.find("h1", {"id": "bookTitle"})
        if not title_tag:
            title_tag = soup.find("h1", {"data-testid": "bookTitle"})
        title = title_tag.get_text(strip=True) if title_tag else None

        ## Author
        author_tag = soup.find("a", class_="authorName")
        author = author_tag.get_text(strip=True) if author_tag else None

        ## Rating (overall)
        rating_tag = soup.find("span", itemprop="ratingValue")
        if not rating_tag:
            rating_tag = soup.find("div", {"data-testid": "averageRating"})
        rating = rating_tag.get_text(strip=True) if rating_tag else None

        ## Rating count and review count using meta tags if available
        rating_count = None
        review_count = None
        meta_rating_count = soup.find("meta", itemprop="ratingCount")
        if meta_rating_count:
            rating_count = meta_rating_count.get("content")
        meta_review_count = soup.find("meta", itemprop="reviewCount")
        if meta_review_count:
            review_count = meta_review_count.get("content")
        # If meta tags aren't available, attempt to scrape text with "ratings" or "reviews"
        if not rating_count:
            rating_count_text = soup.find(text=re.compile(r"ratings", re.I))
            if rating_count_text:
                match = re.search(r"([\d,]+)\s+ratings", rating_count_text)
                if match:
                    rating_count = match.group(1)
        if not review_count:
            review_count_text = soup.find(text=re.compile(r"reviews", re.I))
            if review_count_text:
                match = re.search(r"([\d,]+)\s+reviews", review_count_text)
                if match:
                    review_count = match.group(1)

        ## Description extraction:
        description = None
        desc_div = soup.find("div", id="description")
        if desc_div:
            spans = desc_div.find_all("span")
            if len(spans) > 1:
                description = spans[1].get_text(" ", strip=True)
            elif spans:
                description = spans[0].get_text(" ", strip=True)

        ## Genres (try to collect all unique genres)
        genres = []
        genre_tags = soup.find_all("a", class_="actionLinkLite bookPageGenreLink")
        for tag in genre_tags:
            g = tag.get_text(strip=True)
            if g and g not in genres:
                genres.append(g)

        ## Details (page count, format, publication date)
        page_count = None
        format_info = None
        pub_date = None
        details_div = soup.find("div", id="details")
        if details_div:
            details_text = details_div.get_text(" ", strip=True)
            # Page count e.g., "374 pages"
            page_match = re.search(r"(\d+)\s+pages", details_text)
            if page_match:
                page_count = page_match.group(1)
            # Format info by checking for keywords:
            if "Hardcover" in details_text:
                format_info = "Hardcover"
            elif "Paperback" in details_text:
                format_info = "Paperback"
            # Publication date from text like "First published September 14, 2008"
            pub_match = re.search(r"First published (.+?)(?:\s{2,}|$)", details_text)
            if pub_match:
                pub_date = pub_match.group(1).strip()

        ## Cover image URL
        cover_img = None
        img_tag = soup.find("img", id="coverImage")
        if img_tag:
            cover_img = img_tag.get("src")

        ## About the author (a simple version)
        about_author = {}
        # Look for a div that contains "about the author" info. Goodreads has various layouts.
        author_about_div = soup.find("div", {"id": "aboutAuthor"})
        if author_about_div:
            about_text = author_about_div.get_text(" ", strip=True)
            if about_text:
                about_author["bio"] = about_text

        data = {
            "title": title,
            "author": author,
            "rating": rating,
            "rating_count": rating_count,
            "review_count": review_count,
            "description": description,
            "genres": genres,
            "page_count": page_count,
            "format": format_info,
            "publication_date": pub_date,
            "cover_image": cover_img,
            "about_author": about_author,
            "url": book_url
        }
        logging.info(f"Scraped Data: {data}")
        return data
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

    # Load the CSV; we assume one URL per line, no header.
    df = pd.read_csv(CSV_FILE, header=None, names=["url"])
    urls = df["url"].dropna().astype(str).tolist()
    total = len(urls)

    last_index = read_last_index()
    next_index = min(last_index + BATCH_SIZE, total)

    logging.info(f"Scraping links from index {last_index} to {next_index - 1} (Total URLs: {total})")

    for i in range(last_index, next_index):
        url = urls[i].strip()
        if not url.startswith("http"):
            logging.warning(f"Skipping invalid URL: {url}")
            continue

        filename_base = f"book_{i}"
        scraped_data = scrape_data(url)
        if scraped_data and scraped_data.get("title"):
            save_data_json(scraped_data, filename_base)
        else:
            logging.warning(f"No usable data scraped for URL: {url}")

        time.sleep(2)  # Delay to be polite

    write_last_index(next_index)
    logging.info("Batch completed.")

if __name__ == "__main__":
    main()

