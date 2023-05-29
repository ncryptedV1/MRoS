import argparse
import signal
import socket
import threading
import subprocess
import os

from typing import Any, List, Dict, Tuple
from common import MapReduceRequest, send_data, receive_data

MIN_WORKER_AMOUNT = 1
MAX_WORKER_AMOUNT = 20


class Master:
    def __init__(self, ip: str, port: int, worker_host: str, worker_amount: int):
        self.ip = ip
        self.port = port
        self.worker_host = worker_host
        self.worker_amount = int(worker_amount)
        self.workers = None
        self.listener = None

    def find_valid_worker_ports(self):
        ports = list(range(8000, 8000 + self.worker_amount + 1))
        ports.remove(self.port)
        return ports[:self.worker_amount]

    def start_workers(self):
        ports = self.find_valid_worker_ports()
        workers = []
        for port in ports:
            command = ["python3", "worker.py", "--host", self.worker_host, "--port", str(port)]
            worker = subprocess.Popen(command, preexec_fn=os.setsid)
            workers.append(worker)
        self.workers = list(zip(workers, ports))

    def terminate_workers(self):
        for worker in self.workers:
            os.killpg(os.getpgid(worker[0].pid), signal.SIGTERM)

    def start(self):
        self.start_workers()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as self.listener:
            self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.listener.bind((self.ip, self.port))
            self.listener.listen(5)
            print(f"Master is listening on {self.ip}:{self.port}")

            try:
                while True:
                    client, address = self.listener.accept()
                    print(f"Client connection from {address[0]}:{address[1]} accepted")
                    threading.Thread(target=self.handle_client, args=(client,)).start()
            except KeyboardInterrupt:
                self.terminate_workers()
            finally:
                self.listener.close()

    def handle_client(self, client_socket: socket.socket):
        request = receive_data(client_socket)
        if request:
            client_address = client_socket.getpeername()
            print(f"Processing request of {client_address}")
            mapped = self.map(request)
            shuffled = self.shuffle(mapped)
            reduced = self.reduce(request, shuffled)
            print(f"Finished request of {client_address}")
            send_data(client_socket, reduced)
        client_socket.close()

    def chunk(self, request: MapReduceRequest):
        request_size = len(request.data)
        chunk_count = min(len(self.workers), request_size)
        chunk_size = request_size // chunk_count

        chunks = []
        for i in range(0, request_size, chunk_size):
            chunk = request.data[i:i + chunk_size]
            chunks.append(chunk)
        return chunks

    def map(self, request: MapReduceRequest):
        chunks = self.chunk(request)
        mapped = []
        for i, chunk in enumerate(chunks):
            worker_port = self.workers[i][1]
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as worker_socket:
                worker_socket.connect((self.worker_host, worker_port))
                send_data(worker_socket, (request.functions.map_func, chunk))
                mapped.extend(receive_data(worker_socket))
        return mapped

    def shuffle(self, map_result: List[Tuple[Any, Any]]):
        shuffled: Dict[str, List[Any]] = {}
        for key, value in map_result:
            if key not in shuffled:
                shuffled[key] = []
            shuffled[key].append(value)
        return shuffled

    def reduce(self, request: MapReduceRequest, shuffled: Dict[str, List[Any]]):
        reduced = []
        for idx, (key, values) in enumerate(shuffled.items()):
            worker_port = self.workers[idx % len(self.workers)][1]
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as worker_socket:
                worker_socket.connect((self.worker_host, worker_port))
                send_data(worker_socket, (request.functions.reduce_func, key, values))
                reduced.append(receive_data(worker_socket))
        return reduced


def validate_worker_amount(amount):
    if MIN_WORKER_AMOUNT <= int(amount) <= MAX_WORKER_AMOUNT:
        return amount
    else:
        raise argparse.ArgumentTypeError(f"Worker amount must be between {MIN_WORKER_AMOUNT} and {MAX_WORKER_AMOUNT}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start a MapReduce Master")
    parser.add_argument('--host', type=str, default='localhost', help='Host for the master')
    parser.add_argument('--port', type=int, default=8000, help='Port for the master')
    parser.add_argument('--worker-host', type=str, default='localhost', help='Host of workers')
    parser.add_argument('--worker-amount', type=validate_worker_amount, default=5, help='Amount of workers')
    args = parser.parse_args()

    master = Master(args.host, args.port, args.worker_host, args.worker_amount)
    master.start()
