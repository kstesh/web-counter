from abc import ABC, abstractmethod

class ACounter(ABC):

    @abstractmethod
    def inc(self) -> None:
        pass

    @abstractmethod
    def get(self) -> int:
        pass
