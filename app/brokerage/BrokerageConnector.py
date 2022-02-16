from abc import *

class BrokerageConnector(ABC):
    @abstractmethod
    def check_and_wait(self, type):
        pass

    @abstractmethod
    def connect(self):
        pass

