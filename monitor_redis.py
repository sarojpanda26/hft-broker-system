import redis
import time
import os
from datetime import datetime

def clear_console():
    os.system('cls' if os.name == 'nt' else 'clear')

def monitor_redis(refresh_interval=1):
    r = redis.Redis(host='localhost', port=6379, db=0)

    while True:
        clear_console()
        print(f"=== Redis Tick Monitor ({datetime.now().isoformat()}) ===")
        print(f"{'Symbol':<10} {'Price':<12} {'Quantity':<10} {'Last Updated'}")
        print('-' * 50)

        keys = r.keys('*')
        for key in keys:
            symbol = key.decode()
            data = r.hgetall(key)
            decoded_data = {k.decode(): v.decode() for k, v in data.items()}

            price = decoded_data.get('price', '-')
            quantity = decoded_data.get('quantity', '-')
            last_updated = decoded_data.get('last_updated', '-')

            print(f"{symbol:<10} {price:<12} {quantity:<10} {last_updated}")

        print('-' * 50)
        time.sleep(refresh_interval)

if __name__ == '__main__':
    monitor_redis(refresh_interval=1)