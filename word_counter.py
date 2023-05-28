import socket
from typing import List, Tuple

from common import MapReduceFunctions, MapReduceRequest, send_data, receive_data


# Definiere die Map-Funktion
def map_func(line: str) -> List[Tuple[str, int]]:
    words = line.split()
    return [(word, 1) for word in words]


# Definiere die Reduce-Funktion
def reduce_func(key: str, values: List[int]) -> Tuple[str, int]:
    return key, sum(values)


def main():
    # Definiere die Daten
    data = [
        "a b c",
        "a a",
        "c c d",
        "d e e f"
    ]

    # Erstelle eine MapReduceRequest
    functions = MapReduceFunctions(map_func, reduce_func)
    request = MapReduceRequest(data, functions)

    # Sende die Anfrage an den Master
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as master_socket:
        master_socket.connect(("localhost", 8000))
        send_data(master_socket, request)

        # Empfange die Ergebnisse vom Master und gib sie aus
        results = receive_data(master_socket)
        for key, count in results:
            print(f"{key}: {count}")


if __name__ == "__main__":
    main()
