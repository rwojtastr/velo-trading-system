import functions_framework
import datetime
import time
import ccxt
import pandas as pd
from google.cloud import storage

# Configuration
SYMBOLS = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT']
TIMEFRAMES = ['1m', '5m', '15m', '1h']
BUCKET_NAME = 'velo-raw-data'
BARS_PER_DAY = {'1m': 1440, '5m': 288, '15m': 96, '1h': 24}


@functions_framework.http
def collect_data(request):
    """HTTP Cloud Function to collect Binance OHLCV data."""
    
    # Check for backfill parameter
    request_json = request.get_json(silent=True)
    request_args = request.args
    
    days_back = 1  # Default: yesterday only
    if request_args and 'days' in request_args:
        days_back = int(request_args.get('days'))
    elif request_json and 'days' in request_json:
        days_back = int(request_json.get('days'))
    
    # Initialize exchange and load markets
    exchange = ccxt.binanceusdm({'enableRateLimit': True})
    exchange.load_markets()
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    
    logs = [f"Backfill days: {days_back}"]
    success_count = 0
    total_count = len(SYMBOLS) * len(TIMEFRAMES) * days_back
    
    for day_offset in range(1, days_back + 1):
        target_date = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=day_offset)).date()
        start_dt = datetime.datetime.combine(target_date, datetime.time.min, tzinfo=datetime.timezone.utc)
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = start_ms + 86_400_000
        
        logs.append(f"\n--- {target_date} ---")
        
        for symbol in SYMBOLS:
            for tf in TIMEFRAMES:
                try:
                    # Check if file already exists
                    symbol_clean = symbol.replace('/', '_')
                    blob_path = f"{symbol_clean}/{tf}/{target_date}.parquet"
                    blob = bucket.blob(blob_path)
                    
                    if blob.exists():
                        logs.append(f"{symbol} {tf}: exists, skipping")
                        success_count += 1
                        continue
                    
                    logs.append(f"Fetching {symbol} {tf}...")
                    
                    limit = BARS_PER_DAY[tf] + 10
                    ohlcv = exchange.fetch_ohlcv(symbol, tf, since=start_ms, limit=limit)
                    
                    if not ohlcv:
                        logs.append(f"  No data returned")
                        continue
                    
                    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    df = df[(df['timestamp'] >= start_ms) & (df['timestamp'] < end_ms)]
                    
                    if len(df) < BARS_PER_DAY[tf] * 0.9:
                        logs.append(f"  Warning: Only {len(df)} bars (expected {BARS_PER_DAY[tf]})")
                    
                    parquet_data = df.to_parquet(index=False)
                    blob.upload_from_string(parquet_data, content_type='application/octet-stream')
                    
                    logs.append(f"  Saved {len(df)} bars")
                    success_count += 1
                    
                    time.sleep(0.3)
                    
                except Exception as e:
                    logs.append(f"  ERROR: {str(e)}")
        
        time.sleep(1)  # Pause between days
    
    logs.append(f"\nCompleted: {success_count}/{total_count} successful")
    result = "\n".join(logs)
    print(result)
    
    return (result, 200 if success_count == total_count else 207)
