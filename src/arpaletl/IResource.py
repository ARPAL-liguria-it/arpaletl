from abc import ABC, abstractmethod


class IResource(ABC):
    """
    Interface for resources
    """

    @abstractmethod
    def __init__(self, uri: str):
        """
        Constructor for IResource
        """
        self.uri = uri

    def __del__(self):
        """
        Destructor for IResource
        """

    @abstractmethod
    async def open(self) -> object:
        """
        Open method for IResource that is async
        @returns: Opened resource that can be readed
        """

    @abstractmethod
    async def open_stream(self, chunk: int) -> object:
        """
        Open method for IResource that is async and streams
        @returns: Opened resource that can be parsed in @chunk sized chunks
        """
