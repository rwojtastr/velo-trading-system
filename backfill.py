"""
Binance Historical Data Backfill Job
Downloads data from data.binance.vision and loads to BigQuery
Designed to run as Cloud Run Job (can run for hours)
"""

import os
import io
import zipfile
import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery, storage
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
PROJECT_ID = os.environ.get('PROJECT_ID', 'velo-project-471610')
DATASET_ID = 'market_data'
BUCKET_NAME = 'velo-raw-data'

SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT']
TIMEFRAMES = ['1m', '5m', '15m', '1h']
DAYS_BACK = int(os.environ.get('DAYS_BACK', '180'))  # 6 months default

# Binance data archive base URL
BASE_URL = "https://data.binance.vision/data/futures/um/daily/klines"


def download_and_parse_zip(symbol: str, timeframe: str, date: str) -> pd.DataFrame | None:
    """Download ZIP from Binance, extract CSV, return DataFrame."""
    url = f"{BASE_URL}/{symbol}/{timeframe}/{symbol}-{timeframe}-{date}.zip"
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 404:
            return None  # Data not available for this date
        response.raise_for_status()
        
        # Extract CSV from ZIP
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_name = z.namelist()[0]
            with z.open(csv_name) as f:
                df = pd.read_csv(f, header=None)
        
        # Binance klines CSV columns:
        # 0: Open time, 1: Open, 2: High, 3: Low, 4: Close, 5: Volume,
        # 6: Close time, 7: Quote volume, 8: Trades, 9: Taker buy base,
        # 10: Taker buy quote, 11: Ignore
        df.columns = [
            'open_time', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_volume', 'trades', 'taker_buy_base',
            'taker_buy_quote', 'ignore'
        ]
        
        # Add metadata
        df['symbol'] = symbol
        df['timeframe'] = timeframe
        df['date'] = date
        
        # Convert types
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
        df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
        for col in ['open', 'high', 'low', 'close', 'volume', 'quote_volume', 'taker_buy_base', 'taker_buy_quote']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df['trades'] = df['trades'].astype(int)
        
        # Drop ignore column
        df = df.drop(columns=['ignore'])
        
        return df
        
    except Exception as e:
        print(f"  Error {symbol}/{timeframe}/{date}: {e}")
        return None


def create_bigquery_table(client: bigquery.Client, table_id: str, timeframe: str):
    """Create BigQuery table if not exists, partitioned by date."""
    schema = [
        bigquery.SchemaField("open_time", "TIMESTAMP"),
        bigquery.SchemaField("open", "FLOAT64"),
        bigquery.SchemaField("high", "FLOAT64"),
        bigquery.SchemaField("low", "FLOAT64"),
        bigquery.SchemaField("close", "FLOAT64"),
        bigquery.SchemaField("volume", "FLOAT64"),
        bigquery.SchemaField("close_time", "TIMESTAMP"),
        bigquery.SchemaField("quote_volume", "FLOAT64"),
        bigquery.SchemaField("trades", "INT64"),
        bigquery.SchemaField("taker_buy_base", "FLOAT64"),
        bigquery.SchemaField("taker_buy_quote", "FLOAT64"),
        bigquery.SchemaField("symbol", "STRING"),
        bigquery.SchemaField("timeframe", "STRING"),
        bigquery.SchemaField("date", "DATE"),
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="date"
    )
    table.clustering_fields = ["symbol", "timeframe"]
    
    try:
        client.create_table(table)
        print(f"Created table {table_id}")
    except Exception as e:
        if "Already Exists" in str(e):
            print(f"Table {table_id} already exists")
        else:
            raise


def load_to_bigquery(client: bigquery.Client, df: pd.DataFrame, table_id: str):
    """Load DataFrame to BigQuery."""
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for completion
    return job.output_rows


def process_date(args):
    """Process a single date for all symbols and timeframes."""
    date_str, symbols, timeframes, bq_client, table_id = args
    
    all_data = []
    for symbol in symbols:
        for tf in timeframes:
            df = download_and_parse_zip(symbol, tf, date_str)
            if df is not None:
                all_data.append(df)
    
    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        rows = load_to_bigquery(bq_client, combined, table_id)
        return date_str, len(all_data), rows
    
    return date_str, 0, 0


def main():
    print(f"=" * 60)
    print(f"Binance Backfill Job Started")
    print(f"Days to backfill: {DAYS_BACK}")
    print(f"Symbols: {SYMBOLS}")
    print(f"Timeframes: {TIMEFRAMES}")
    print(f"=" * 60)
    
    # Initialize clients
    bq_client = bigquery.Client(project=PROJECT_ID)
    
    # Create table
    table_id = f"{PROJECT_ID}.{DATASET_ID}.ohlcv"
    create_bigquery_table(bq_client, table_id, "all")
    
    # Generate date list
    dates = []
    for i in range(1, DAYS_BACK + 1):
        date = datetime.utcnow() - timedelta(days=i)
        dates.append(date.strftime('%Y-%m-%d'))
    
    print(f"\nProcessing {len(dates)} days...")
    print(f"Date range: {dates[-1]} to {dates[0]}")
    
    # Process dates sequentially (to avoid overwhelming Binance)
    total_files = 0
    total_rows = 0
    
    for i, date_str in enumerate(dates):
        print(f"\n[{i+1}/{len(dates)}] {date_str}")
        
        day_data = []
        for symbol in SYMBOLS:
            for tf in TIMEFRAMES:
                df = download_and_parse_zip(symbol, tf, date_str)
                if df is not None:
                    day_data.append(df)
                    print(f"  ✓ {symbol}/{tf}: {len(df)} rows")
                else:
                    print(f"  - {symbol}/{tf}: no data")
        
        if day_data:
            combined = pd.concat(day_data, ignore_index=True)
            rows = load_to_bigquery(bq_client, combined, table_id)
            total_files += len(day_data)
            total_rows += rows
            print(f"  Loaded {rows} rows to BigQuery")
    
    print(f"\n" + "=" * 60)
    print(f"BACKFILL COMPLETE")
    print(f"Total files processed: {total_files}")
    print(f"Total rows loaded: {total_rows}")
    print(f"=" * 60)


if __name__ == "__main__":
    main()
