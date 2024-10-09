from abc import ABC, abstractmethod


class IDbClient(ABC):
    """
    Interface for database client
    """


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

    @abstractmethod
    def connect(self) -> object:
        """
        Connect method for IDbClient
        @returns: Connection object
        """


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
