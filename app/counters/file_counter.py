import threading
import os
from .acounter import ACounter

class FileCounter(ACounter):
    def __init__(self, filename: str):
        self._filename = filename
        self._lock = threading.Lock()
        self._init_file()

    def _init_file(self):
        if not os.path.exists(self._filename):
            with open(self._filename, "w") as f:
                f.write("0")

    def inc(self) -> None:
        with self._lock:
            value = self.get()
            with open(self._filename, "w") as f:
                f.write(str(value + 1))

    def get(self) -> int:
        with open(self._filename, "r") as f:
            return int(f.read())
