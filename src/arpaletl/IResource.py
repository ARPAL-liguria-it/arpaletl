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

    @abstractmethod
    def __del__(self):
        """
        Destructor for IResource
        """

    @abstractmethod
    def open(self) -> object:
        """
        Open method for IResource
        @returns: Opened resource that can be readed with read()
        """

    @abstractmethod
    def open_stream(self, chunk: int) -> object:
        """
        Open method for IResource
        @returns: Opened resource that can be parse in @buffer sized chunks
        """

    @abstractmethod
    async def _async_open_stream(self, chunk: int) -> object:
        """
        Open method for IResource
        @returns: Opened resource that can be parse in @buffer sized chunks
        """

