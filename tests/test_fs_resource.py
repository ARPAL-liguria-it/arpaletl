import unittest
import asyncio
import os
from pathlib import Path
from src.arpaletl.ArpalEtlErrors import ResourceError
from src.arpaletl.FsResource import FsResource
from typing import AsyncIterator

class TestFsResource(unittest.TestCase):
    """
    Test class for FsResource
    """
    def test_fs_resource_init_failure(self):
        with self.assertRaises(ResourceError):
            resource = FsResource('error')
            
    def test_fs_resource_open_success(self):
        current_dir = Path(__file__).parent
        file_non_json = current_dir / 'blobs' / 'test_file_non_json'
        resource = FsResource(file_non_json)
        by = asyncio.run(resource.open())
        self.assertIsInstance(by, bytes)

    def test_fs_resource_open_failure(self):
        current_dir = Path(__file__).parent
        test_permissions = current_dir / 'blobs' / 'test_permissions'
        os.chmod(test_permissions, 0o300)
        resource = FsResource(test_permissions)
        with self.assertRaises(ResourceError):
            asyncio.run(resource.open())
        os.chmod(test_permissions, 0o600)

    def test_fs_resource_open_stream_success(self):
        current_dir = Path(__file__).parent
        file_non_json = current_dir / 'blobs' / 'test_file_non_json'
        resource = FsResource(file_non_json)
        
        async def run_test():
            # Ensure that we're iterating over the async generator
            async for by in resource.open_stream(1024):
                self.assertIsInstance(by, bytes)
        
        # Use asyncio.run() to run the coroutine
        asyncio.run(run_test())

    def test_fs_resource_open_stream_failure(self):
        current_dir = Path(__file__).parent
        test_permissions = current_dir / 'blobs' / 'test_permissions'
        os.chmod(test_permissions, 0o200)
        resource = FsResource(test_permissions)
        
        async def run_test():
            with self.assertRaises(ResourceError):
                async for _ in resource.open_stream(1024):
                    pass
        
        # Use asyncio.run() to run the coroutine
        asyncio.run(run_test())
        os.chmod(test_permissions, 0o600)
