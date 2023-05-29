import argparse
import signal
import socket
import threading
import subprocess
import os

from typing import Any, List, Dict
from common import MapReduceRequest, send_data, receive_data


class Master:
    def __init__(self, ip: str, port: int, worker_host: str, worker_amount: int):
        self.ip = ip
        self.port = port
        self.worker_host = worker_host
        self.worker_amount = worker_amount
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
            self.listener.bind((self.ip, self.port))
            self.listener.listen(5)
            print(f"Master is now listening on {self.ip}:{self.port}")

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
        # Nehme Client-Anfrage entgegen und beginne MapReduce-Prozess
        request = receive_data(client_socket)
        if request:
            print(f"Distributing work from request of {client_socket.getpeername()}")
            result = self.distribute_work(request)
            print(f"Finished MapReduce for request of {client_socket.getpeername()}. Sending results to client.")
            send_data(client_socket, result)
        client_socket.close()

    def distribute_work(self, request: MapReduceRequest):
        # Teile die Daten auf und sende sie an die Worker
        chunk_count = min(len(self.workers), len(request.data))
        chunk_size = len(request.data) // chunk_count
        chunks = [request.data[i:i + chunk_size] for i in range(0, len(request.data), chunk_size)]

        # Sammle die Map-Ergebnisse von den Workern
        map_results = []
        for idx, chunk in enumerate(chunks):
            worker_port = self.workers[idx][1]
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as worker_socket:
                worker_socket.connect((self.worker_host, worker_port))
                send_data(worker_socket, (request.functions.map_func, chunk))
                map_results.extend(receive_data(worker_socket))

        # Shuffle und sortiere die Key-Value-Paare
        shuffle_dict: Dict[str, List[Any]] = {}
        for key, value in map_results:
            if key not in shuffle_dict:
                shuffle_dict[key] = []
            shuffle_dict[key].append(value)

        # Verteile die gruppierten Key-Value-Paare an die Worker und sammle die Reduce-Ergebnisse
        reduce_results = []
        for idx, (key, values) in enumerate(shuffle_dict.items()):
            worker_port = self.workers[idx % len(self.workers)][1]
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as worker_socket:
                worker_socket.connect((self.worker_host, worker_port))
                send_data(worker_socket, (request.functions.reduce_func, key, values))
                reduce_results.append(receive_data(worker_socket))

        return reduce_results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start a MapReduce Master")
    parser.add_argument('--host', type=str, default='localhost', help='Host for the master')
    parser.add_argument('--port', type=int, default=8000, help='Port for the master')
    parser.add_argument('--worker-host', type=str, default='localhost', help='Host of workers')
    parser.add_argument('--worker-amount', type=int, default=5, help='Amount of workers')
    args = parser.parse_args()

    master = Master(args.host, args.port, args.worker_host, args.worker_amount)
    master.start()
