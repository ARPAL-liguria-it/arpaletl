"""
Module for FsResource class
"""
import os
from typing import AsyncIterator
from arpaletl.resource.resource import IResource
from arpaletl.utils.arpaletlerrors import ResourceError
from arpaletl.utils.logger import get_logger


class FsResource(IResource):
    """
    Class that takes care of handling file system resources. Implements the IResource interface.
    """

    def __init__(self, uri, zipped: bool = False, chunk: int = 1024):
        """
        Constructor for FsResource
        @self.uri: URI of the file system resource
        @self.logger: Logger object
        @self.zipped: isZipped parameter
        @self.chunk: Chunk size for the stream
        """
        # check if the uri is a valid path
        if not os.path.exists(uri):
            raise ResourceError("Path does not exist")
        super().__init__(uri)
        self.logger = get_logger(__name__)
        self.zipped = zipped
        self.chunk = chunk

    async def open(self) -> bytes:
        """
        Async Open method for FsResource it will open the entire
        resource and make it available for reading
        @raises: ResourceError: If there are problems opening the resource
        @returns: Opened file system resource that can be readed
        """
        try:
            with open(self.uri, "rb") as file:
                data = file.read()
                if self.zipped:
                    data = await self.unzip(data)
                return data
        except Exception as e:
            self.logger.error("Error opening file system resource: %s", e)
            raise ResourceError("Error opening file system resource") from e

    async def open_stream(self) -> AsyncIterator[bytes]:
        """
        Async Open method for FsResource it will open the
        resource and make it available for reading in chunks
        @raises: ResourceError: If there are problems opening the resource
        @returns: an Iterator that can be parsed in @chunk sized chunks
        """
        try:
            with open(self.uri, "rb") as file:
                while True:
                    data = file.read(self.chunk)
                    if not data:
                        break
                    if self.zipped:
                        data = await self.unzip(data)
                    yield data
        except Exception as e:
            self.logger.error("Error opening file system resource: %s", e)
            raise ResourceError("Error opening file system resource") from e
