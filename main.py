"""
Velo Trading System - Daily Data Collector
Fetches previous day's OHLCV data from Binance and loads to BigQuery
Deployed as Cloud Run Service, triggered by Cloud Scheduler at 01:00 UTC
"""

import functions_framework
import datetime
import time
import ccxt
import pandas as pd
from google.cloud import bigquery

# Configuration
PROJECT_ID = 'velo-project-471610'
DATASET_ID = 'market_data'
TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.ohlcv'

# CCXT format for Binance USD-M perpetual futures
SYMBOLS = ['BTC/USDT:USDT', 'ETH/USDT:USDT', 'SOL/USDT:USDT', 'BNB/USDT:USDT']
TIMEFRAMES = ['1m', '5m', '15m', '1h']
BARS_PER_DAY = {'1m': 1440, '5m': 288, '15m': 96, '1h': 24}

def clean_symbol(symbol: str) -> str:
    """Convert BTC/USDT:USDT to BTCUSDT"""
    return symbol.replace('/USDT:USDT', 'USDT')


@functions_framework.http
def collect_data(request):
    """HTTP Cloud Function to collect Binance OHLCV data."""
    
    request_args = request.args
    days_back = 1
    if request_args and 'days' in request_args:
        days_back = min(int(request_args.get('days')), 7)
    
    exchange = ccxt.binanceusdm({'enableRateLimit': True})
    exchange.load_markets()
    
    bq_client = bigquery.Client(project=PROJECT_ID)
    
    logs = [f"Daily Collection Started", f"Days to collect: {days_back}"]
    success_count = 0
    total_count = len(SYMBOLS) * len(TIMEFRAMES) * days_back
    all_data = []
    
    for day_offset in range(1, days_back + 1):
        target_date = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=day_offset)).date()
        start_dt = datetime.datetime.combine(target_date, datetime.time.min, tzinfo=datetime.timezone.utc)
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = start_ms + 86_400_000
        
        date_str = target_date.strftime('%Y-%m-%d')
        logs.append(f"\n--- {date_str} ---")
        
        for symbol in SYMBOLS:
            symbol_clean = clean_symbol(symbol)
            
            for tf in TIMEFRAMES:
                try:
                    check_query = f"""
                    SELECT COUNT(*) as cnt 
                    FROM `{TABLE_ID}` 
                    WHERE symbol = '{symbol_clean}' 
                      AND timeframe = '{tf}' 
                      AND date = '{date_str}'
                    """
                    check_result = bq_client.query(check_query).result()
                    existing_rows = list(check_result)[0].cnt
                    
                    if existing_rows > 0:
                        logs.append(f"  {symbol_clean}/{tf}: exists ({existing_rows} rows), skipping")
                        success_count += 1
                        continue
                    
                    logs.append(f"  Fetching {symbol_clean}/{tf}...")
                    
                    limit = BARS_PER_DAY[tf] + 10
                    ohlcv = exchange.fetch_ohlcv(symbol, tf, since=start_ms, limit=limit)
                    
                    if not ohlcv:
                        logs.append(f"    No data returned")
                        continue
                    
                    df = pd.DataFrame(ohlcv, columns=['open_time', 'open', 'high', 'low', 'close', 'volume'])
                    df = df[(df['open_time'] >= start_ms) & (df['open_time'] < end_ms)]
                    
                    if len(df) == 0:
                        logs.append(f"    No data for date range")
                        continue
                    
                    df['open_time'] = (df['open_time'] * 1000).astype('int64')
                    tf_minutes = int(tf.replace('m', '').replace('h', '')) * (1 if 'm' in tf else 60)
                    df['close_time'] = (df['open_time'] + tf_minutes * 60 * 1000000).astype('int64')
                    
                    df['quote_volume'] = 0.0
                    df['trades'] = 0
                    df['taker_buy_base'] = 0.0
                    df['taker_buy_quote'] = 0.0
                    df['symbol'] = symbol_clean
                    df['timeframe'] = tf
                    df['date'] = date_str
                    
                    df = df[['open_time', 'open', 'high', 'low', 'close', 'volume', 
                             'close_time', 'quote_volume', 'trades', 'taker_buy_base', 
                             'taker_buy_quote', 'symbol', 'timeframe', 'date']]
                    
                    all_data.append(df)
                    logs.append(f"    Collected {len(df)} bars")
                    success_count += 1
                    
                    time.sleep(0.2)
                    
                except Exception as e:
                    logs.append(f"    ERROR: {str(e)}")
    
    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        try:
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            )
            job = bq_client.load_table_from_dataframe(combined, TABLE_ID, job_config=job_config)
            job.result()
            logs.append(f"\nLoaded {len(combined)} rows to BigQuery")
        except Exception as e:
            logs.append(f"\nBigQuery load error: {str(e)}")
    
    logs.append(f"\nCompleted: {success_count}/{total_count} successful")
    result = "\n".join(logs)
    print(result)
    
    return (result, 200 if success_count == total_count else 207)
