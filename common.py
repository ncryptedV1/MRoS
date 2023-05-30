import dill as pickle
import socket

from typing import Any, Callable, List, Tuple
from enum import Enum, auto

import dill as pickle

# Definiere die Typen für die Map- und Reduce-Funktionen
MapFunction = Callable[[Any], List[Tuple[str, Any]]]
ReduceFunction = Callable[[str, List[Any]], Tuple[str, Any]]


class RequestType(Enum):
    MAP = auto()
    REDUCE = auto()


class MapReduceFunctions:
    def __init__(self, map_func: MapFunction, reduce_func: ReduceFunction):
        self.map_func = map_func
        self.reduce_func = reduce_func


class MapReduceRequest:
    def __init__(self, data: List[Any], functions: MapReduceFunctions):
        self.data = data
        self.functions = functions


def send_data(sock: socket.socket, data: Any) -> None:
    serialized_data = pickle.dumps(data)
    sock.sendall(len(serialized_data).to_bytes(4, 'big'))
    sock.sendall(serialized_data)


def receive_data(sock: socket.socket) -> Any:
    data_length = int.from_bytes(sock.recv(4), 'big')
    data = b''
    while len(data) < data_length:
        packet = sock.recv(data_length - len(data))
        if not packet:
            return None
        data += packet
    return pickle.loads(data)
