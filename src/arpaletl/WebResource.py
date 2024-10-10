from typing import AsyncIterator
import requests
import asyncio
import aiohttp
from src.arpaletl.IResource import IResource, ResourceError


class WebResource(IResource):
    """
    Class that takes care of handling web resources
    """

    def __init__(self, uri: str, timeout: int = 10, headers: dict = None):
        """
        Constructor for WebResource
        @self.uri: URI of the web resource
        """
        self.uri = uri
        self.timeout = timeout
        self.headers = headers

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
        except requests.exceptions.RequestException as e:
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
        except requests.exceptions.RequestException as e:
            raise ResourceError("Error downloading web resource") from e
        return r.iter_content(chunk_size=chunk)

    async def _async_open_stream(self, chunk: int) -> AsyncIterator:
        """
        Async Open method for WebResource
        @returns: an Iterator that can be parsed in @chunk sized chunks
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.uri,
                                       timeout=self.timeout,
                                       headers=self.headers) as response:
                    response.raise_for_status()
                    async for data in response.content.iter_any(chunk):
                        yield data
        except aiohttp.ClientError as e:
            raise ResourceError("Error downloading web resource") from e
