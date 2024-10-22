"""
Module for FsResource class
"""
import os
from typing import AsyncIterator
from src.arpaletl.resource.resource import IResource
from src.arpaletl.utils.arpaletlerros import ResourceError
from src.arpaletl.utils.logger import get_logger


class FsResource(IResource):
    """
    Class that takes care of handling file system resources. Implements the IResource interface.
    """

    def __init__(self, uri):
        """
        Constructor for FsResource
        @self.uri: URI of the file system resource
        @self.logger: Logger object
        """
        # check if the uri is a valid path
        if not os.path.exists(uri):
            raise ResourceError("Path does not exist")
        super().__init__(uri)
        self.logger = get_logger(__name__)

    async def open(self, zipped: bool = False) -> bytes:
        """
        Async Open method for FsResource it will open the entire
        resource and make it available for reading
        @raises: ResourceError: If there are problems opening the resource
        @returns: Opened file system resource that can be readed
        """
        try:
            with open(self.uri, "rb") as file:
                data = file.read()
                if zipped:
                    data = await self.unzip(data)
                return data
        except Exception as e:
            self.logger.error("Error opening file system resource: %s", e)
            raise ResourceError("Error opening file system resource") from e

    async def open_stream(self, chunk: int, zipped: bool = False) -> AsyncIterator[bytes]:
        """
        Async Open method for FsResource it will open the
        resource and make it available for reading in chunks
        @raises: ResourceError: If there are problems opening the resource
        @param chunk: Size of the chunks to be readed
        @returns: an Iterator that can be parsed in @chunk sized chunks
        """
        try:
            with open(self.uri, "rb") as file:
                while True:
                    data = file.read(chunk)
                    if not data:
                        break
                    if zipped:
                        data = await self.unzip(data)
                    yield data
        except Exception as e:
            self.logger.error("Error opening file system resource: %s", e)
            raise ResourceError("Error opening file system resource") from e
