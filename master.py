import argparse
import concurrent
import socket
import subprocess
import threading
from concurrent import futures
from typing import Any, List, Dict, Tuple

from common import MapFunction, MapReduceRequest, send_data, receive_data, RequestType, ReduceFunction

MIN_WORKER_AMOUNT = 1
MAX_WORKER_AMOUNT = 20


def shuffle(map_result: List[Tuple[Any, Any]]) -> List[Any]:
    shuffled: Dict[str, List[Any]] = {}
    for key, value in map_result:
        if key not in shuffled:
            shuffled[key] = []
        shuffled[key].append(value)
    return list(shuffled.items())


def reduce_chunk(host: str, port: int, chunk: Any, function: ReduceFunction) -> List[Any]:
    data = (RequestType.REDUCE, function, chunk)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as worker:
        worker.connect((host, port))
        send_data(worker, data)
        return receive_data(worker)


def map_chunk(host: str, port: int, chunk: Any, function: MapFunction) -> List[Any]:
    data = (RequestType.MAP, function, chunk)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as worker:
        worker.connect((host, port))
        send_data(worker, data)
        return receive_data(worker)


def is_port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(10.0)
        return s.connect_ex(('localhost', port)) == 0


class Master:
    def __init__(self, ip: str, port: int, worker_host: str, worker_amount: int):
        self.ip: str = ip
        self.port: int = port
        self.worker_host: str = worker_host
        self.worker_amount: int = int(worker_amount)
        self.workers: List[Tuple[subprocess.Popen[str], int]] = None
        self.listener: socket.socket = None

    def find_valid_worker_ports(self) -> List[int]:
        ports = []
        cur_port = 7999
        while len(ports) < self.worker_amount:
            cur_port += 1
            if cur_port == self.port or is_port_in_use(cur_port):
                continue
            ports.append(cur_port)
        return ports

    def start_workers(self) -> None:
        print("Looking for worker ports...")
        ports = self.find_valid_worker_ports()
        print(f"Found open ports for workers: {', '.join(str(x) for x in ports)}")
        workers = []
        for port in ports:
            command = ["python", "worker.py", "--host", self.worker_host, "--port", str(port)]
            worker = subprocess.Popen(command)
            workers.append(worker)
        self.workers = list(zip(workers, ports))

    def terminate_workers(self) -> None:
        for worker in self.workers:
            worker[0].terminate()

    def start(self) -> None:
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

    def handle_client(self, client_socket: socket.socket) -> None:
        request = receive_data(client_socket)
        if request:
            address = client_socket.getpeername()
            print(f"Processing request of {address[0]}:{address[1]}")
            mapped = self.map(request)
            shuffled = shuffle(mapped)
            reduced = self.reduce(request, shuffled)
            print(f"Finished request of {address[0]}:{address[1]}")
            send_data(client_socket, reduced)
        client_socket.close()

    def chunk(self, data: List[Any]):
        fragment_count = len(data)
        worker_count = len(self.workers)
        chunk_count = min(worker_count, fragment_count)
        chunk_size = fragment_count // chunk_count

        chunks = []
        for i in range(0, worker_count):
            start = i * chunk_size
            if i == worker_count - 1:
                chunk = data[start:]
            else:
                chunk = data[start:start + chunk_size]
            chunks.append(chunk)
        return chunks

    def map(self, request: MapReduceRequest) -> List[Any]:
        chunks = self.chunk(request.data)
        mapped = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            hosts = [self.worker_host] * len(chunks)
            ports = [self.workers[i][1] for i in range(len(chunks))]
            functions = [request.functions.map_func] * len(chunks)
            results = executor.map(map_chunk, hosts, ports, chunks, functions)
            for result in results:
                mapped.extend(result)
        return mapped

    def reduce(self, request: MapReduceRequest, shuffled: List[Any]) -> Any:
        chunks = self.chunk(shuffled)
        reduced = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            hosts = [self.worker_host] * len(chunks)
            ports = [self.workers[i][1] for i in range(len(chunks))]
            functions = [request.functions.reduce_func] * len(chunks)
            results = executor.map(reduce_chunk, hosts, ports, chunks, functions)
            for result in results:
                reduced.extend(result)
        return reduced


def validate_worker_amount(amount) -> str:
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
