"""
Module for IResource interface
"""
from abc import ABC, abstractmethod
import io
import zipfile
from arpaletl.utils.arpaletlerrors import ResourceError
from arpaletl.utils.logger import get_logger

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
        self.logger = get_logger(__name__)

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
    async def open_stream(self) -> object:
        """
        Open method for IResource that is async and streams
        @returns: Opened resource that can be parsed in @chunk sized chunks
        """

    async def unzip(self, zipblob: bytes) -> bytes:
        """
        This method will unzip a downloaded resource before returning it as an object
        @param zipblob: Zipped resource
        @returns: Unzipped resource
        """
        try:
            zipobject = io.BytesIO(zipblob)
            with zipfile.ZipFile(zipobject, "r") as zip_ref:
                unzippeddata = zip_ref.read(zip_ref.namelist()[0])
            self.logger.info("Resource successfully unzipped")
            return unzippeddata
        except Exception as e:
            self.logger.error("Error unzipping resource: %s", e)
            raise ResourceError("Error unzipping resource") from e
