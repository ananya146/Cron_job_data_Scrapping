import requests
from bs4 import BeautifulSoup
import csv
import os

CSV_FILE = 'books.csv'
PAGE_FILE = 'last_page.txt'

def get_last_page():
    if not os.path.exists(PAGE_FILE):
        with open(PAGE_FILE, 'w') as f:
            f.write('1')
        return 1
    with open(PAGE_FILE, 'r') as f:
        return int(f.read().strip())

def update_last_page(new_page):
    with open(PAGE_FILE, 'w') as f:
        f.write(str(new_page))

def extract_links(start_page, end_page):
    headers = {'User-Agent': 'Mozilla/5.0'}
    all_links = []

    for page in range(start_page, end_page + 1):
        url = f'https://www.goodreads.com/list/show/1.Best_Books_Ever?page={page}'
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        book_links = ['https://www.goodreads.com' + a['href'] for a in soup.select('a.bookTitle')]
        all_links.extend(book_links)

    return all_links

def save_to_csv(links):
    file_exists = os.path.isfile(CSV_FILE)
    with open(CSV_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(['Link'])
        for link in links:
            writer.writerow([link])

if __name__ == "__main__":
    start = get_last_page()
    end = start + 14  # 15 pages total
    links = extract_links(start, end)
    save_to_csv(links)
    update_last_page(end + 1)
