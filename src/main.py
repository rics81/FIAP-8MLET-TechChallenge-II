import os
import logging
import tempfile
from datetime import date, datetime, timedelta
from dotenv import load_dotenv
from scrap_tickers import scrape_all_pages
from load_data_from_yfinance import fetch_data, save_parquet_by_date
from load_to_s3 import upload_to_s3, get_latest_date_from_s3

load_dotenv()  # load variables from .env

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def main():
    bucket = os.getenv('S3_BUCKET')
    if not bucket:
        logging.error("S3_BUCKET not set in environment.")
        return
    prefix = os.getenv('S3_PREFIX', '')
    max_days = int(os.getenv('DEFAULT_DAYS', 5))

    # 1. Check the latest date already in S3 to avoid reprocessing
    latest_date_in_s3 = get_latest_date_from_s3(bucket, s3_prefix=prefix)
    today = date.today()

    if latest_date_in_s3:
        first_date_needed = latest_date_in_s3 + timedelta(days=1)
        if first_date_needed >= today:
            logging.info(
                f"S3 is already up to date (latest: {latest_date_in_s3}). Nothing to process."
            )
            return
        logging.info(
            f"Latest S3 date: {latest_date_in_s3}. "
            f"Fetching data from {first_date_needed} onwards."
        )
        # Convert to datetime for fetch_data
        start_date = datetime.combine(first_date_needed, datetime.min.time())
    else:
        start_date = None
        logging.info(f"No existing data in S3. Fetching last {max_days} trading days.")

    # 2. Scrape all tickers
    base_url = "https://investidor10.com.br/acoes/"
    tickers = scrape_all_pages(base_url)
    if not tickers:
        logging.error("No tickers scraped. Exiting.")
        return
    logging.info(f"Scraped {len(tickers)} tickers.")

    # 3. Fetch data for the required date window
    data = fetch_data(tickers, start_date=start_date, max_days=max_days)
    if data.empty:
        logging.error("No data retrieved. Exiting.")
        return

    # 4. Filter out dates already present in S3 (safety net for edge cases)
    if latest_date_in_s3:
        before = len(data)
        data = data[data['date'].dt.date > latest_date_in_s3]
        logging.info(
            f"Filtered out dates <= {latest_date_in_s3}: {before - len(data)} rows removed, "
            f"{len(data)} rows remaining."
        )
        if data.empty:
            logging.info("No new data after filtering. Nothing to upload.")
            return

    # 5. Save as daily Parquet files in a temporary directory
    with tempfile.TemporaryDirectory() as tmpdir:
        parquet_files = save_parquet_by_date(data, output_dir=tmpdir)
        if not parquet_files:
            logging.error("No Parquet files generated.")
            return

        # 6. Upload to S3
        uploaded = upload_to_s3(parquet_files, bucket, s3_prefix=prefix)
        logging.info(f"Successfully uploaded {len(uploaded)} files to S3.")


if __name__ == "__main__":
    main()