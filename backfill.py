"""
Binance Historical Data Backfill Job - FIXED
Downloads data from data.binance.vision and loads to BigQuery
"""

import os
import io
import zipfile
import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery

# Configuration
PROJECT_ID = os.environ.get('PROJECT_ID', 'velo-project-471610')
DATASET_ID = 'market_data'

SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT']
TIMEFRAMES = ['1m', '5m', '15m', '1h']
DAYS_BACK = int(os.environ.get('DAYS_BACK', '180'))

# Binance data archive base URL
BASE_URL = "https://data.binance.vision/data/futures/um/daily/klines"


def download_and_parse_zip(symbol: str, timeframe: str, date: str) -> pd.DataFrame | None:
    """Download ZIP from Binance, extract CSV, return DataFrame."""
    url = f"{BASE_URL}/{symbol}/{timeframe}/{symbol}-{timeframe}-{date}.zip"
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_name = z.namelist()[0]
            with z.open(csv_name) as f:
                df = pd.read_csv(f, header=None)
        
        # Binance klines columns
        df.columns = [
            'open_time_ms', 'open', 'high', 'low', 'close', 'volume',
            'close_time_ms', 'quote_volume', 'trades', 'taker_buy_base',
            'taker_buy_quote', 'ignore'
        ]
        
        # Keep timestamps as integers for BigQuery TIMESTAMP conversion
        # BigQuery expects microseconds, Binance provides milliseconds
        df['open_time_ms'] = df['open_time_ms'].astype('int64') * 1000  # to microseconds
        df['close_time_ms'] = df['close_time_ms'].astype('int64') * 1000
        
        # Add metadata
        df['symbol'] = symbol
        df['timeframe'] = timeframe
        df['date'] = date
        
        # Convert numeric columns
        for col in ['open', 'high', 'low', 'close', 'volume', 'quote_volume', 'taker_buy_base', 'taker_buy_quote']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df['trades'] = df['trades'].astype('int64')
        
        # Drop ignore column
        df = df.drop(columns=['ignore'])
        
        return df
        
    except Exception as e:
        print(f"  Error {symbol}/{timeframe}/{date}: {e}")
        return None


def create_bigquery_table(client: bigquery.Client, table_id: str):
    """Create BigQuery table if not exists."""
    schema = [
        bigquery.SchemaField("open_time_ms", "INT64"),
        bigquery.SchemaField("open", "FLOAT64"),
        bigquery.SchemaField("high", "FLOAT64"),
        bigquery.SchemaField("low", "FLOAT64"),
        bigquery.SchemaField("close", "FLOAT64"),
        bigquery.SchemaField("volume", "FLOAT64"),
        bigquery.SchemaField("close_time_ms", "INT64"),
        bigquery.SchemaField("quote_volume", "FLOAT64"),
        bigquery.SchemaField("trades", "INT64"),
        bigquery.SchemaField("taker_buy_base", "FLOAT64"),
        bigquery.SchemaField("taker_buy_quote", "FLOAT64"),
        bigquery.SchemaField("symbol", "STRING"),
        bigquery.SchemaField("timeframe", "STRING"),
        bigquery.SchemaField("date", "STRING"),
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="date"
    )
    table.clustering_fields = ["symbol", "timeframe"]
    
    try:
        client.delete_table(table_id, not_found_ok=True)
        client.create_table(table)
        print(f"Created table {table_id}")
    except Exception as e:
        print(f"Table error: {e}")


def load_to_bigquery(client: bigquery.Client, df: pd.DataFrame, table_id: str):
    """Load DataFrame to BigQuery."""
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    return job.output_rows


def main():
    print("=" * 60)
    print("Binance Backfill Job Started")
    print(f"Days to backfill: {DAYS_BACK}")
    print(f"Symbols: {SYMBOLS}")
    print(f"Timeframes: {TIMEFRAMES}")
    print("=" * 60)
    
    bq_client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET_ID}.ohlcv"
    
    # Create fresh table
    create_bigquery_table(bq_client, table_id)
    
    # Generate date list (skip today and yesterday - data not ready)
    dates = []
    for i in range(2, DAYS_BACK + 2):
        date = datetime.utcnow() - timedelta(days=i)
        dates.append(date.strftime('%Y-%m-%d'))
    
    print(f"\nProcessing {len(dates)} days...")
    print(f"Date range: {dates[-1]} to {dates[0]}")
    
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
            try:
                rows = load_to_bigquery(bq_client, combined, table_id)
                total_files += len(day_data)
                total_rows += rows
                print(f"  Loaded {rows} rows to BigQuery")
            except Exception as e:
                print(f"  BigQuery error: {e}")
    
    print("\n" + "=" * 60)
    print("BACKFILL COMPLETE")
    print(f"Total files processed: {total_files}")
    print(f"Total rows loaded: {total_rows}")
    print("=" * 60)


if __name__ == "__main__":
    main()
