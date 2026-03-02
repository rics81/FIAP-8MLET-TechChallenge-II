import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def _audit_download(data, yf_tickers):
    """
    Inspect the downloaded MultiIndex DataFrame and classify each ticker as:
      - 'ok'       : has at least one row of non-null close prices
      - 'no_data'  : present in columns but all close prices are NaN
      - 'missing'  : not present in columns at all (download silently dropped it)

    Returns a dict: { ticker_symbol: status_string }
    """
    audit = {}

    try:
        available = set(data.columns.get_level_values(0))
    except Exception:
        available = set()

    for ticker in yf_tickers:
        if ticker not in available:
            audit[ticker] = 'missing'
            continue
        try:
            close_series = data[ticker]['Close']
            if close_series.dropna().empty:
                audit[ticker] = 'no_data'
            else:
                audit[ticker] = 'ok'
        except Exception:
            audit[ticker] = 'no_data'

    return audit


def _log_audit_summary(audit):
    """Log a concise summary of problematic tickers at the end of fetch_data."""
    ok_count = sum(1 for s in audit.values() if s == 'ok')
    logging.info(f"Download summary: {ok_count}/{len(audit)} tickers returned valid data.")

    problematic = {t: s for t, s in audit.items() if s != 'ok'}
    if not problematic:
        return

    missing = [t.replace('.SA', '') for t, s in problematic.items() if s == 'missing']
    no_data = [t.replace('.SA', '') for t, s in problematic.items() if s == 'no_data']

    if missing:
        logging.warning(
            f"Tickers dropped by yfinance (possibly delisted or invalid) [{len(missing)}]: "
            + ", ".join(missing)
        )
    if no_data:
        logging.warning(
            f"Tickers with empty price data (all NaN) [{len(no_data)}]: "
            + ", ".join(no_data)
        )


def fetch_data(tickers, start_date=None, max_days=5):
    """
    Fetch daily OHLCV data for a list of tickers.

    Parameters
    ----------
    tickers    : list of ticker strings (without .SA suffix)
    start_date : datetime.date or datetime — fetch from this date onwards.
                 If None, falls back to the last `max_days` calendar days.
    max_days   : fallback window in calendar days when start_date is not provided.
                 Also acts as a safety cap if start_date is unreasonably far back (>30 days).

    Returns a DataFrame with columns: date, Ticker, open, high, low, close, vol.
    """
    yf_tickers = [t + '.SA' for t in tickers]
    end_date = datetime.now() - timedelta(days=1)

    if start_date is None:
        effective_start = end_date - timedelta(days=max_days)
    else:
        # Normalise to datetime if a date object was passed
        if not isinstance(start_date, datetime):
            start_date = datetime.combine(start_date, datetime.min.time())
        # Safety cap: don't fetch more than 30 days back
        if (end_date - start_date).days > 30:
            logging.warning(
                f"start_date {start_date.date()} is more than 30 days back — "
                f"capping to last {max_days} days."
            )
            effective_start = end_date - timedelta(days=max_days)
        else:
            effective_start = start_date

    logging.info(
        f"Downloading data for {len(tickers)} tickers "
        f"from {effective_start.date()} to {end_date.date()}..."
    )

    data = yf.download(
        tickers=yf_tickers,
        start=effective_start,
        end=end_date,
        group_by='ticker',
        auto_adjust=False,
        progress=False
    )

    # yf.download returns flat columns for a single ticker — normalise to MultiIndex
    if len(yf_tickers) == 1:
        ticker = yf_tickers[0]
        df = data.copy()
        df.columns = pd.MultiIndex.from_product([[ticker], df.columns])
        data = df

    # Audit which tickers came back healthy
    audit = _audit_download(data, yf_tickers)

    # Reshape to long format, skipping problematic tickers
    dfs = []
    for ticker in yf_tickers:
        if audit.get(ticker) != 'ok':
            continue
        try:
            ticker_data = data[ticker].copy()
            ticker_data.dropna(subset=['Close'], inplace=True)
            if ticker_data.empty:
                audit[ticker] = 'no_data'
                continue
            ticker_data['Ticker'] = ticker.replace('.SA', '')
            dfs.append(ticker_data)
        except Exception as e:
            logging.debug(f"Skipping {ticker} due to unexpected error: {e}")
            audit[ticker] = 'no_data'

    _log_audit_summary(audit)

    if not dfs:
        logging.error("No valid data retrieved for any ticker.")
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

    os.makedirs(output_dir, exist_ok=True)

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
    tickers = ['ITUB4', 'PETR4', 'VALE3']
    data = fetch_data(tickers, max_days=5)
    if not data.empty:
        files = save_parquet_by_date(data)
        print(f"Generated files: {files}")