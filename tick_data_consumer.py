import csv
import socket
import json
import os
# from datetime import datetime
import redis

HOST = '127.0.0.1'
PORT = 9999
CSV_FILE = 'tick_data_tcp.csv'
PERSIST_EVERY_N_TICKS = 50
# r = redis.Redis(host='localhost', port=6379, db=0)
# print(r.ping())

def tick_consumer():
    symbol_cache = {}
    tick_buffer = []
    # Create CSV with header if needed
    file_exists = os.path.isfile(CSV_FILE)
    with open(CSV_FILE, 'a', newline='') as csvfile:
        fieldnames = ['timestamp', 'symbol', 'price', 'quantity']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()

        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((HOST, PORT))
        buffer = ''  # buffer for incomplete data chunks
        print(f"Connected to tick server at {HOST}:{PORT}")
        r = redis.Redis(host='localhost', port=6379, db=0)
        print(f"redis is up and running..... {print(r.ping())}")
        while True:
            data = client_socket.recv(1024).decode('utf-8')
            if not data:
                continue
            buffer += data
            while '\n' in buffer:
                line, buffer = buffer.split('\n', 1)
                if not line.strip():
                    continue

                tick = json.loads(line) # loads deserializes the object
                # Update in-memory cache
                symbol_cache[tick['symbol']] = {
                    'price': tick['price'],
                    'quantity': tick['quantity'],
                    'last_updated': tick['timestamp']
                }

                r.hset(tick['symbol'], mapping={
                    'price': tick['price'],
                    'quantity': tick['quantity'],
                    'last_updated': tick['timestamp']
                })
                tick_buffer.append(tick)
                # Batch write
                if len(tick_buffer) >= PERSIST_EVERY_N_TICKS:
                    writer.writerows(tick_buffer)
                    csvfile.flush()
                    tick_buffer.clear()
