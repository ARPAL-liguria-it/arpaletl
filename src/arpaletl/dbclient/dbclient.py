"""
Interface for database client
"""
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
