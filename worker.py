import argparse
import socket
from typing import Any, Callable, Tuple

from common import MapFunction, ReduceFunction, send_data, receive_data


class Worker:
    def __init__(self, ip: str, port: int):
        self.ip = ip
        self.port = port

    def start(self):
        # Öffne Server-Socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as self.server_socket:
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.ip, self.port))
            self.server_socket.listen(5)
            print(f"Worker listening on {self.ip}:{self.port}")

            try:
                # Warte auf Master-Anfrage
                while True:
                    master_socket, master_addr = self.server_socket.accept()
                    print(f"Accepted connection from {master_addr}")
                    self.process_request(master_socket)
                    master_socket.close()
            finally:
                self.server_socket.close()

    def process_request(self, master_socket: socket.socket) -> Any:
        # Empfange die Anfrage vom Master
        request = receive_data(master_socket)
        if not request:
            return

        # Überprüfe, ob es sich um eine Map- oder Reduce-Funktion handelt
        if isinstance(request[0], Callable) and len(request) == 2:
            map_func: MapFunction = request[0]
            data_chunk = request[1]
            result = self.execute_map(map_func, data_chunk)
        elif isinstance(request[0], Callable) and len(request) == 3:
            reduce_func: ReduceFunction = request[0]
            key = request[1]
            values = request[2]
            result = self.execute_reduce(reduce_func, key, values)
        else:
            print("Invalid request")
            return

        # Sende das Ergebnis zurück an den Master
        send_data(master_socket, result)

    def execute_map(self, map_func: MapFunction, data_chunk) -> Any:
        # Führe die Map-Funktion auf dem Daten-Chunk aus
        map_results = []
        for item in data_chunk:
            map_results.extend(map_func(item))
        return map_results

    def execute_reduce(self, reduce_func: ReduceFunction, key, values) -> Tuple[str, Any]:
        # Führe die Reduce-Funktion auf den gruppierten Key-Value-Paaren aus
        return reduce_func(key, values)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Worker for MapReduce example")
    parser.add_argument('--host', type=str, default='localhost', help='Worker host (default: localhost)')
    parser.add_argument('--port', type=int, default=8001, help='Worker port (default: 8001)')
    args = parser.parse_args()

    worker = Worker(args.host, args.port)
    worker.start()
