import requests
from bs4 import BeautifulSoup
import csv
import os

CSV_FILE = 'books.csv'
GOODREADS_URL = 'https://www.goodreads.com/list/show/1.Best_Books_Ever?page=1'

def extract_links():
    headers = {
        'User-Agent': 'Mozilla/5.0'
    }
    response = requests.get(GOODREADS_URL, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    book_links = []
    for a in soup.select('a.bookTitle')[:10]:
        link = 'https://www.goodreads.com' + a['href']
        book_links.append(link)
    
    return book_links

def save_to_csv(links):
    # Check if file exists
    file_exists = os.path.isfile(CSV_FILE)
    
    with open(CSV_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(['Link'])
        for link in links:
            writer.writerow([link])

if __name__ == "__main__":
    links = extract_links()
    save_to_csv(links)
