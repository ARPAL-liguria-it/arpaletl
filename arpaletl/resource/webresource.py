"""
Module for WebResource class
"""
from typing import AsyncIterator
import aiohttp
from arpaletl.resource.resource import IResource
from arpaletl.utils.arpaletlerrors import ResourceError
from arpaletl.utils.logger import get_logger


class WebResource(IResource):
    """
    Class that takes care of handling web resources. Implements the IResource interface.
    """

    def __init__(self, uri: str, timeout: int = 10, headers: dict = None, zipped: bool = False, chunk: int = 1024):
        """
        Constructor for WebResource
        @self.uri: URI of the web resource
        @self.timeout: Timeout for the request
        @self.headers: Headers for the request
        @self.logger: Logger object
        @self.zipped: isZipped parameter
        @self.chunk: Chunk size for the stream
        """
        self.uri = uri
        self.timeout = timeout
        self.headers = headers
        self.zipped = zipped
        self.chunk = chunk
        self.logger = get_logger(__name__)

    async def open(self) -> bytes:
        """
        Async Open method for WebResource it will download the entire
        resource and make it available for reading
        @raises: ResourceError: If there are problems downloading the resource
        @returns: Opened web resource that can be readed
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.uri,
                                       timeout=self.timeout,
                                       headers=self.headers) as response:
                    response.raise_for_status()
                    self.logger.info(
                        "Resource successfully downloaded from %s", self.uri)
                    if self.zipped:
                        return await self.unzip(await response.content.read())
                    else:
                        return await response.content.read()
        except aiohttp.ClientError as e:
            self.logger.error("Error downloading web resource: %s", e)
            raise ResourceError("Error downloading web resource") from e

    async def open_stream(self) -> AsyncIterator[bytes]:
        """
        Async Open method for WebResource it will download the
        resource and make it available for reading in chunks
        @raises: ResourceError: If there are problems downloading the resource
        @returns: an Iterator that can be parsed in @chunk sized chunks
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.uri,
                                       timeout=self.timeout,
                                       headers=self.headers) as response:
                    response.raise_for_status()
                    self.logger.info(
                        "Resource successfully downloaded from %s", self.uri)
                    while True:
                        data = await response.content.read(self.chunk)
                        if not data:
                            break
                        if self.zipped:
                            data = await self.unzip(data)
                        yield data
        except aiohttp.ClientError as e:
            self.logger.error("Error downloading web resource: %s", e)
            raise ResourceError("Error downloading web resource") from e
