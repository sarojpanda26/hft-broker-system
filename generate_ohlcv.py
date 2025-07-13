import redis
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta
import csv

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
OHLCV_INTERVAL_SECONDS = 60  # 1-minute bars
CSV_FOLDER = 'data/ohlcv_bars'

r = redis.Redis(host='localhost', port=6379, db=0)
symbol_windows = defaultdict(list)  # Stores ticks for each symbol within current window
bar_start_times = {}

def save_bar_to_csv(symbol,bar):
    filename = f"{CSV_FOLDER}/{symbol}_ohlcv.csv"
    file_exists = False
    try:
        with open(filename, 'r') as _:
            file_exists = True
    except FileNotFoundError:
        pass

    with open(filename, 'a', newline='') as csvfile:
        fieldnames = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(bar)

def compute_ohlcv(symbol,ticks):
    prices = [float(t['price']) for t in ticks]
    quantities = [int(t['quantity']) for t in ticks]
    return {
        'timestamp': datetime.utcnow().isoformat(),
        'symbol': symbol,
        'open': prices[0],
        'high': max(prices),
        'low': min(prices),
        'close': prices[-1],
        'volume': sum(quantities)
    }

def generate_ohlcv():
    print(f"[OHLCV] Generator started with {OHLCV_INTERVAL_SECONDS}s bars")
    while True:
        keys = r.keys('*')
        for key in keys:
            symbol = key.decode()
            data = r.hgetall(symbol)
            decoded_data = {k.decode():v.decode() for k,v in data.items()}
            if "price"  or "quantity" not in decoded_data:
                continue  # just skip.. incomplete data

            tick = {
                'timestamp': decoded_data.get('last_updated', datetime.utcnow().isoformat()),
                'symbol': symbol,
                'price': decoded_data['price'],
                'quantity': decoded_data['quantity']
            }
            now = datetime.utcnow()
            window_start =  bar_start_times.get(symbol)
            if not window_start:
                window_start = now
                bar_start_times[symbol] = window_start

            # If within current bar window, append tick
            if (now - window_start) < timedelta(seconds=OHLCV_INTERVAL_SECONDS):
                symbol_windows[symbol].append(tick)
            else:
                # Now Compute and store OHLCV bar
                if symbol_windows[symbol]:
                    bar = compute_ohlcv(symbol, symbol_windows[symbol])
                    save_bar_to_csv(symbol, bar)
                    print(f"[OHLCV] {symbol}: {bar}")

                # Reset the window so that
                symbol_windows[symbol] = [tick]
                bar_start_times[symbol] = now

            time.sleep(1)

if __name__ == '__main__':
    generate_ohlcv()