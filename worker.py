import argparse
import socket

from typing import Any, List
from common import MapFunction, ReduceFunction, send_data, receive_data, RequestType


class Worker:
    def __init__(self, ip: str, port: int):
        self.ip: str = ip
        self.port: int = port
        self.listener: socket.socket = None

    def start(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as self.listener:
            self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.listener.bind((self.ip, self.port))
            self.listener.listen(5)
            print(f"Worker is listening on {self.ip}:{self.port}")

            try:
                while True:
                    master, address = self.listener.accept()
                    print(f"Connection from {address[0]}:{address[1]} accepted")
                    self.process(master)
                    master.close()
            finally:
                self.listener.close()

    def process(self, master: socket.socket) -> Any:
        request = receive_data(master)
        if not request:
            return

        type, function, data = request
        if type == RequestType.MAP:
            result = self.map(function, data)
        elif type == RequestType.REDUCE:
            result = self.reduce(function, data)
        else:
            print(f"Invalid request type {type}")
            return

        send_data(master, result)

    def map(self, function: MapFunction, chunk: List[Any]) -> List[Any]:
        results = []
        for fragment in chunk:
            mapped = function(fragment)
            results.extend(mapped)
        return results

    def reduce(self, function: ReduceFunction, chunk: List[Any]) -> List[Any]:
        results = []
        for key, value in chunk:
            reduced = function(key, value)
            results.append(reduced)
        return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Worker for MapReduce example")
    parser.add_argument('--host', type=str, default='localhost', help='Worker host (default: localhost)')
    parser.add_argument('--port', type=int, default=8001, help='Worker port (default: 8001)')
    args = parser.parse_args()

    worker = Worker(args.host, args.port)
    worker.start()
