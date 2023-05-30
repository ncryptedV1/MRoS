import socket
from typing import List, Tuple

from common import MapReduceFunctions, MapReduceRequest, send_data, receive_data


# Genereller Ablauf:
# 1. Generiere alle Produkte, die während der Matrix-Multiplikation entstehen.
#    Dafür wird allen Werten ein Schlüssel im Format `row:col:pos` zugewiesen,
#    wobei row & col für die Position in der finalen Matrix stehen und pos für die Position innerhalb der Summe.
# 2. Summiere die Werte für eine Zelle im Matrix-Produkt auf.
#    Dafür kann der vorherige Schlüsselwert auf `row:col` reduziert werden, um alle Produkte einer Summe zu gruppieren.
# Credits: https://medium.com/@aw.shubh/matrix-multiplication-through-map-reduce-c72be2f4f90

# Definiere die Map- & Reduce-Funktion für den Multiplikations-Schritt
def mul_map_func(item: Tuple[int, int, int, float, int, int]) -> List[Tuple[str, float]]:
    mat_id, row, col, val, mat1_rows, mat2_cols = item

    if mat_id == 1:  # Für Werte aus Matrix 1, wiederhole Eintrag für jede Spalte aus Matrix 2
        return [(f"{row}:{k}:{col}", val) for k in range(mat2_cols)]
    else:  # Für Werte aus Matrix 2, wiederhole Eintrag für jede Zeile aus Matrix 1
        return [(f"{k}:{col}:{row}", val) for k in range(mat1_rows)]


def mul_reduce_func(key: str, values: List[float]) -> Tuple[str, float]:
    # Values sollte an der Stelle nur 2 Werte enthalten
    return key, values[0] * values[1]


# Definiere die Map- & Reduce-Funktion für den Additions-Schritt
def sum_map_func(item: Tuple[str, float]) -> List[Tuple[str, float]]:
    new_key = ":".join(item[0].split(":")[:2])  # wandelt `row:col:pos` zu `row:col` um
    return [(new_key, item[1])]


def sum_reduce_func(key: str, values: List[float]) -> Tuple[str, float]:
    return key, sum(values)


def main():
    # Definiere die Daten
    mat1 = [
        [1, 2, 3],
        [4, 5, 6]
    ]
    mat2 = [
        [6, 5],
        [4, 3],
        [2, 1]
    ]
    # Transformiere Daten in eine Liste mit dem Format: (Matrix-Index, Row, Col, Val, #Mat1-Rows, #Mat2-Cols)
    mat1_rows = len(mat1)
    mat2_cols = len(mat2[0])
    data = [(1, row, col, val, mat1_rows, mat2_cols) for row, row_values in enumerate(mat1) for col, val in
            enumerate(row_values)] + \
           [(2, row, col, val, mat1_rows, mat2_cols) for row, row_values in enumerate(mat2) for col, val in
            enumerate(row_values)]

    # Erstelle eine MapReduceRequest für den Multiplikations-Schritt
    mul_functions = MapReduceFunctions(mul_map_func, mul_reduce_func)
    mul_request = MapReduceRequest(data, mul_functions)

    # Sende die Anfrage an den Master
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as master_socket:
        master_socket.connect(("localhost", 8000))
        send_data(master_socket, mul_request)

        # Empfange die Ergebnisse vom Master
        mul_results = receive_data(master_socket)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as master_socket:
        # Erstelle eine MapReduceRequest für den Additions-Schritt
        sum_functions = MapReduceFunctions(sum_map_func, sum_reduce_func)
        sum_request = MapReduceRequest(mul_results, sum_functions)

        master_socket.connect(("localhost", 8000))
        send_data(master_socket, sum_request)

        # Empfange die Ergebnisse vom Master und gebe die finale Matrix aus
        sum_results: List[Tuple[str, float]] = receive_data(master_socket)
        sum_results.sort()
        res = []
        for key, value in sum_results:
            row, col = map(int, key.split(":"))
            if len(res) == row:
                res.append([])
            res[row].append(value)
        print(res)


if __name__ == "__main__":
    main()
