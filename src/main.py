import os
import logging
import tempfile
from dotenv import load_dotenv
from scrap_tickers import scrape_all_pages
from load_data_from_yfinance import fetch_data, save_parquet_by_date
from load_to_s3 import upload_to_s3

load_dotenv()  # load variables from .env

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    # 1. Scrape all tickers
    base_url = "https://investidor10.com.br/acoes/"
    tickers = scrape_all_pages(base_url)
    if not tickers:
        logging.error("No tickers scraped. Exiting.")
        return
    logging.info(f"Scraped {len(tickers)} tickers.")

    # 2. Fetch last 5 days of data
    days = 3
    data = fetch_data(tickers, days=days)
    if data.empty:
        logging.error("No data retrieved. Exiting.")
        return

    # 3. Save as daily Parquet files in a temporary directory
    with tempfile.TemporaryDirectory() as tmpdir:
        parquet_files = save_parquet_by_date(data, output_dir=tmpdir)
        if not parquet_files:
            logging.error("No Parquet files generated.")
            return

        # 4. Upload to S3
        bucket = os.getenv('S3_BUCKET')
        if not bucket:
            logging.error("S3_BUCKET not set in environment.")
            return
        prefix = os.getenv('S3_PREFIX', '')
        uploaded = upload_to_s3(parquet_files, bucket, s3_prefix=prefix)
        logging.info(f"Successfully uploaded {len(uploaded)} files to S3.")

if __name__ == "__main__":
    main()