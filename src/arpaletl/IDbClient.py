from abc import ABC, abstractmethod
from sqlalchemy import Engine


class IDbClient(ABC):
    """
    Interface for database client
    """


    engine: Engine


    @abstractmethod
    def __init__(self):
        """
        Constructor for IDbClient
        """


    @abstractmethod
    def __del__(self):
        """
        Destructor for IDbClient
        """


    def get_engine(self) -> Engine:
        """
        Return engine
        """
        return self.engine


class DbClientError(Exception):
    """
    Custom exception for database client errors
    """


    def __init__(self, message: str):
        """
        Constructor for DbClientError
        """
        self.message = message
        super().__init__(self.message)
