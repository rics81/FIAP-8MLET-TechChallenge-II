import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_data(tickers, days=5):
    """
    Fetch daily OHLCV data for the last `days` trading days for a list of tickers.
    Returns a DataFrame with MultiIndex (Date, Ticker) and columns: Open, High, Low, Close, Volume.
    """
    # Yahoo Finance requires '.SA' suffix for Brazilian stocks
    yf_tickers = [t + '.SA' for t in tickers]
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days * 2)  # fetch more to cover non-trading days
    logging.info(f"Downloading data for {len(tickers)} tickers from {start_date.date()} to {end_date.date()}...")
    data = yf.download(
        tickers=yf_tickers,
        start=start_date,
        end=end_date,
        group_by='ticker',
        auto_adjust=False,
        progress=False
    )
    # yf.download returns a MultiIndex columns if multiple tickers
    if len(tickers) == 1:
        # Single ticker case: convert to same structure
        ticker = yf_tickers[0]
        df = data.copy()
        df.columns = pd.MultiIndex.from_product([[ticker], df.columns])
        data = df

    # Reshape to long format: one row per (Date, Ticker)
    dfs = []
    for ticker in yf_tickers:
        if ticker in data.columns.levels[0]:
            ticker_data = data[ticker].copy()
            ticker_data['Ticker'] = ticker.replace('.SA', '')  # remove suffix for output
            dfs.append(ticker_data)
    if not dfs:
        logging.error("No data retrieved.")
        return pd.DataFrame()
    combined = pd.concat(dfs)
    combined.reset_index(inplace=True)
    combined.rename(columns={
        'Date': 'date',
        'Open': 'open',
        'High': 'high',
        'Low': 'low',
        'Close': 'close',
        'Volume': 'vol'
    }, inplace=True)
    # Keep only last `days` trading days (most recent days with data)
    unique_dates = combined['date'].drop_duplicates().sort_values(ascending=False)
    last_dates = unique_dates.head(days)
    combined = combined[combined['date'].isin(last_dates)]
    return combined

def save_parquet_by_date(df, output_dir='.'):
    """
    Split DataFrame by date and save each date as a separate Parquet file.
    File name: b3_yyyymmdd.parquet
    Returns list of saved file paths.
    """
    if df.empty:
        logging.warning("DataFrame is empty. No files saved.")
        return []

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Check required columns
    required = ['date', 'Ticker', 'open', 'high', 'low', 'close', 'vol']
    missing = [col for col in required if col not in df.columns]
    if missing:
        logging.error(f"Missing columns: {missing}")
        return []

    files = []
    for date, group in df.groupby('date'):
        date_str = date.strftime('%Y%m%d')
        file_name = f"b3_{date_str}.parquet"
        file_path = os.path.join(output_dir, file_name)

        # Prepare output DataFrame with desired column order and names
        out_df = group[['Ticker', 'high', 'low', 'open', 'close', 'vol']].copy()
        out_df.rename(columns={'Ticker': 'ticker'}, inplace=True)

        try:
            out_df.to_parquet(file_path, index=False, engine='pyarrow')
            files.append(file_path)
            logging.info(f"Saved {file_path} with {len(group)} records.")
        except Exception as e:
            logging.error(f"Failed to save {file_path}: {e}")
    return files

if __name__ == "__main__":
    # Example usage – normally called from main
    tickers = ['ITUB4', 'PETR4', 'VALE3']  # sample
    data = fetch_data(tickers, days=5)
    if not data.empty:
        files = save_parquet_by_date(data)
        print(f"Generated files: {files}")