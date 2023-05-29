import argparse
import socket

from typing import Any, Callable, Tuple
from common import MapFunction, ReduceFunction, send_data, receive_data


class Worker:
    def __init__(self, ip: str, port: int):
        self.ip = ip
        self.port = port
        self.listener = None

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as self.listener:
            self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.listener.bind((self.ip, self.port))
            self.listener.listen(5)
            print(f"Worker is listening on {self.ip}:{self.port}")

            try:
                while True:
                    master, address = self.listener.accept()
                    print(f"Master connection from {address[0]}:{address[1]} accepted")
                    self.process(master)
                    master.close()
            finally:
                self.listener.close()

    def process(self, master: socket.socket) -> Any:
        request = receive_data(master)
        if not request:
            return

        if isinstance(request[0], Callable) and len(request) == 2:
            map_func: MapFunction = request[0]
            data_chunk = request[1]
            result = self.map(map_func, data_chunk)
        elif isinstance(request[0], Callable) and len(request) == 3:
            reduce_func: ReduceFunction = request[0]
            key = request[1]
            values = request[2]
            result = self.reduce(reduce_func, key, values)
        else:
            print("Invalid request")
            return

        send_data(master, result)

    def map(self, function: MapFunction, data) -> Any:
        results = []
        for chunk in data:
            mapped = function(chunk)
            results.extend(mapped)
        return results

    def reduce(self, function: ReduceFunction, key, values) -> Tuple[str, Any]:
        return function(key, values)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Worker for MapReduce example")
    parser.add_argument('--host', type=str, default='localhost', help='Worker host (default: localhost)')
    parser.add_argument('--port', type=int, default=8001, help='Worker port (default: 8001)')
    args = parser.parse_args()

    worker = Worker(args.host, args.port)
    worker.start()
