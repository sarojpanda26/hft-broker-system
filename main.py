import threading as threading

from tick_data_generator import tick_server
from tick_data_consumer  import tick_consumer

def main():
    # Start tick server in a background thread
    server_thread = threading.Thread(target=tick_server, daemon=True)
    server_thread.start()

    # Start tick consumer in main thread
    try:
        tick_consumer()
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")

if __name__ == '__main__':
    main()
