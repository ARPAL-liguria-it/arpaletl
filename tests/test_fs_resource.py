import unittest
import asyncio
import os
from pathlib import Path
from arpaletl.utils.arpaletlerrors import ResourceError
from arpaletl.resource.fsresource import FsResource


class TestFsResource(unittest.TestCase):
    """
    Test class for FsResource
    """

    def test_fs_resource_init_failure(self):
        """
        Test that the constructor raises a ResourceError
        """
        with self.assertRaises(ResourceError):
            FsResource('error')

    def test_fs_resource_open_success(self):
        """
        Test that the open method returns a bytes object
        """
        current_dir = Path(__file__).parent
        test_file_non_formatted = current_dir / 'blobs' / 'test_file_non_formatted'
        resource = FsResource(test_file_non_formatted)
        by = asyncio.run(resource.open())
        self.assertIsInstance(by, bytes)

    def test_fs_resource_open_failure(self):
        """
        Test that the open method raises a ResourceError
        """
        current_dir = Path(__file__).parent
        test_permissions = current_dir / 'blobs' / 'test_permissions'
        os.chmod(test_permissions, 0o300)
        resource = FsResource(test_permissions)
        with self.assertRaises(ResourceError):
            asyncio.run(resource.open())
        os.chmod(test_permissions, 0o600)

    def test_fs_resource_open_stream_success(self):
        """
        Test that the open_stream method returns a bytes object
        """
        current_dir = Path(__file__).parent
        test_file_non_formatted = current_dir / 'blobs' / 'test_file_non_formatted'
        resource = FsResource(test_file_non_formatted)

        async def run_test():
            # Ensure that we're iterating over the async generator
            async for by in resource.open_stream(1024):
                self.assertIsInstance(by, bytes)

        # Use asyncio.run() to run the coroutine
        asyncio.run(run_test())

    def test_fs_resource_open_stream_failure(self):
        """
        Test that the open_stream method raises a ResourceError
        """
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
