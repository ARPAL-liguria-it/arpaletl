from typing import AsyncIterator
import requests
import aiohttp
from src.arpaletl.IResource import IResource
from src.arpaletl.ArpalEtlErrors import ResourceError
from src.arpaletl.utils.logger import get_logger


class WebResource(IResource):
    """
    Class that takes care of handling web resources
    """

    def __init__(self, uri: str, timeout: int = 10, headers: dict = None):
        """
        Constructor for WebResource
        @self.uri: URI of the web resource
        @self.timeout: Timeout for the request
        @self.headers: Headers for the request
        @self.logger: Logger object
        """
        self.uri = uri
        self.timeout = timeout
        self.headers = headers
        self.logger = get_logger(__name__)

    def open(self) -> requests.Response:
        """
        Open method for WebResource it will download the entire 
        resource and make it available for reading
        @returns: Opened web resource that can be readed with read()
        """
        try:
            r = requests.get(self.uri, timeout=self.timeout,
                             headers=self.headers)
            r.raise_for_status()
            self.logger.info(
                "Resource successfully downloaded from %s", self.uri)
        except requests.exceptions.RequestException as e:
            self.logger.error("Error downloading web resource: %s", e)
            raise ResourceError("Error downloading web resource") from e
        return r

    def open_stream(self, chunk: int) -> AsyncIterator:
        """
        Open method for WebResource
        @returns: an Iterator that can be parsed in @chunk sized chunks
        """
        try:
            r = requests.get(self.uri, timeout=self.timeout,
                             headers=self.headers, stream=True)
            r.raise_for_status()
            self.logger.info(
                "Resource successfully downloaded from %s", self.uri)
        except requests.exceptions.RequestException as e:
            self.logger.error("Error downloading web resource: %s", e)
            raise ResourceError("Error downloading web resource") from e
        return r.iter_content(chunk_size=chunk)

    async def async_open(self) -> bytes:
        """
        Async Open method for WebResource it will download the entire
        resource and make it available for reading
        @returns: Opened web resource that can be readed with read()
        """
        async with aiohttp.ClientSession() as session:
            async with session.get(self.uri,
                                   timeout=self.timeout,
                                   headers=self.headers) as response:
                response.raise_for_status()
                return await response.read()

    async def async_open_stream(self, chunk: int) -> AsyncIterator:
        """
        Async Open method for WebResource it will download the
        resource and make it available for reading in chunks
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
                    async for data in response.content.iter_any(chunk):
                        yield data
        except aiohttp.ClientError as e:
            self.logger.error("Error downloading web resource: %s", e)
            raise ResourceError("Error downloading web resource") from e
