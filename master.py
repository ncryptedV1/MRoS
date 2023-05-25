import argparse
import socket
import threading
from typing import Any, List, Tuple, Dict

from common import MapReduceRequest, send_data, receive_data


class Master:
    def __init__(self, ip: str, port: int, worker_addresses: List[Tuple[str, int]]):
        self.ip = ip
        self.port = port
        self.worker_addresses = worker_addresses

    def start(self):
        # Öffne Server-Socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.ip, self.port))
        self.server_socket.listen(5)
        print(f"Master listening on {self.ip}:{self.port}")

        # Warte auf Client-Anfrage
        while True:
            client_socket, client_addr = self.server_socket.accept()
            print(f"Accepted connection from {client_addr}")
            threading.Thread(target=self.handle_client, args=(client_socket,)).start()

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
        chunk_size = len(request.data) // len(self.worker_addresses)
        chunks = [request.data[i:i + chunk_size] for i in range(0, len(request.data), chunk_size)]

        # Sammle die Map-Ergebnisse von den Workern
        map_results = []
        for idx, chunk in enumerate(chunks):
            worker_ip, worker_port = self.worker_addresses[idx]
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as worker_socket:
                worker_socket.connect((worker_ip, worker_port))
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
            worker_ip, worker_port = self.worker_addresses[idx % len(self.worker_addresses)]
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as worker_socket:
                worker_socket.connect((worker_ip, worker_port))
                send_data(worker_socket, (request.functions.reduce_func, key, values))
                reduce_results.append(receive_data(worker_socket))

        return reduce_results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start a MapReduce Master")
    parser.add_argument('--host', type=str, default='localhost', help='Host for the master')
    parser.add_argument('--port', type=int, default=8000, help='Port for the master')
    parser.add_argument('--worker-hosts', type=str, nargs='+', help='List of worker hosts')
    parser.add_argument('--worker-ports', type=int, nargs='+', help='List of worker ports')
    args = parser.parse_args()

    if len(args.worker_hosts) != len(args.worker_ports):
        raise ValueError("The number of worker hosts and ports must be the same")

    worker_addresses = list(zip(args.worker_hosts, args.worker_ports))

    master = Master(args.host, args.port, worker_addresses)
    master.start()
