import requests
from bs4 import BeautifulSoup

def scrape_page(url):
    """Scrape tickers from a single page."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to retrieve {url}: {response.status_code}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', id='rankigns')
    if not table:
        print("Table not found.")
        return []

    tickers = []
    rows = table.tbody.find_all('tr') if table.tbody else []
    for row in rows:
        ticker_span = row.find('span', class_='font-semibold')
        if not ticker_span:
            continue
        ticker = ticker_span.get_text(strip=True)
        tickers.append(ticker)  # we only need the ticker symbol
    return tickers

def scrape_all_pages(base_url):
    """Iterate through all pages and collect tickers."""
    all_tickers = []
    page = 1
    while True:
        url = f"{base_url}?page={page}"
        print(f"Scraping page {page}...")
        page_tickers = scrape_page(url)
        if not page_tickers:
            break
        all_tickers.extend(page_tickers)
        page += 1
    return all_tickers

if __name__ == "__main__":
    base_url = "https://investidor10.com.br/acoes/"
    tickers = scrape_all_pages(base_url)
    print(f"Total tickers found: {len(tickers)}")
    # For testing, print first 10
    for t in tickers[:10]:
        print(t)