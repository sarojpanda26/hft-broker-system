import socket
# import threading
import time
import random
from datetime import datetime
# import csv
# import os
from nsetools import Nse
import json

nse = Nse()
stock_codes = nse.get_stock_codes()
# print(stock_codes[:5])

# 1st list all the symbol we wanna mock
SYMBOLS = stock_codes[:500]
# Now here i will be creating a map/dictionary of symbol for each symbol from SYMBOLS list and takes a random number
INITIAL_PRICES = {symbol: random.uniform(1000, 2000) for symbol in SYMBOLS}
# To simulate price changing we are here mentioning the timeframe the price changes (i.e 50ms)
TICK_INTERVAL = 0.05  # 50 ms
HOST = '127.0.0.1'
PORT = 9999

# ================================
# Tick Data Generator (Producer)
# ================================

def tick_server():
    prices = INITIAL_PRICES.copy()
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((HOST, PORT))
    server_socket.listen(1)
    print(f"Tick server started listening on {HOST}:{PORT}.........")
    conn, addr = server_socket.accept()
    print(f"Connection established with {addr}")
    while True:
        symbol = random.choice(SYMBOLS)
        prices[symbol] += random.uniform(-0.5, 0.5)
        prices[symbol] = max(prices[symbol], 1)  # To Prevent negative prices
        tick = {
            'timestamp': datetime.utcnow().isoformat(),
            'symbol': symbol,
            'price': round(prices[symbol], 2),
            'quantity': random.randint(1, 100)
        }
        tick_str = json.dumps(tick) + '\n'  # JSON + newline for easy framing
        conn.sendall(tick_str.encode('utf-8'))  # send tick to consumer

        time.sleep(TICK_INTERVAL)