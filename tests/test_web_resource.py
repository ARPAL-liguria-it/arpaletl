import unittest
import asyncio
from src.arpaletl.ArpalEtlErrors import ResourceError
from src.arpaletl.WebResource import WebResource
from typing import AsyncIterator

class TestWebResource(unittest.TestCase):
    """
    Test class for WebResource
    """

    def test_web_resource_open_success(self):
        resource = WebResource('https://jsonplaceholder.typicode.com/posts')
        by = asyncio.run(resource.open())
        self.assertIsInstance(by, bytes)

    def test_web_resource_open_failure(self):
        resource = WebResource('error')
        with self.assertRaises(ResourceError):
            asyncio.run(resource.open())

    def test_web_resource_open_stream_success(self):
        resource = WebResource('https://jsonplaceholder.typicode.com/posts')
        
        async def run_test():
            # Ensure that we're iterating over the async generator
            async for by in resource.open_stream(1024):
                self.assertIsInstance(by, bytes)
        
        # Use asyncio.run() to run the coroutine
        asyncio.run(run_test())

    def test_web_resource_open_stream_failure(self):
        resource = WebResource('error')
        
        async def run_test():
            with self.assertRaises(ResourceError):
                async for _ in resource.open_stream(1024):
                    pass
        
        # Use asyncio.run() to run the coroutine
        asyncio.run(run_test())
