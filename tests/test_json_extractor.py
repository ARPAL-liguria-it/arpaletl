import unittest
import asyncio
from pathlib import Path
import pandas as pd
from arpaletl.extractor.jsonextractor import JSONExtractor
from arpaletl.utils.arpaletlerrors import ExtractorError
from arpaletl.resource.webresource import WebResource
from arpaletl.resource.fsresource import FsResource


class TestJSONExtractor(unittest.TestCase):
    """
    Test class for JSONExtractor with resources passed as parameters to each function.
    """

    def extract_and_test(self, resource, expected_exception=None, gzip_flag=False):
        """
        Helper method to extract data using the provided resource and handle expected exceptions.
        """
        extractor = JSONExtractor(resource)
        if expected_exception is not None:
            with self.assertRaises(expected_exception):
                asyncio.run(extractor.extract(gzip_flag))
        else:
            df = asyncio.run(extractor.extract(gzip_flag))
            self.assertIsInstance(df, pd.DataFrame)

    def test_json_extractor_parametric(self):
        """
        Parametric test for JSONExtractor.
        """
        current_dir = Path(__file__).parent
        file_non_json = current_dir / 'blobs' / 'test_file_non_json'
        json_tmp_gzip = current_dir / 'blobs' / 'json_tmp.gz'

        test_cases = [
            {
                "resource": WebResource('https://jsonplaceholder.typicode.com/posts'),
                "expected_exception": None,
                "gzip_flag": False,
                "description": "Valid web resource",
            },
            {
                "resource": WebResource('error'),
                "expected_exception": ExtractorError,
                "gzip_flag": False,
                "description": "Invalid web resource - should raise ExtractorError",
            },
            {
                "resource": FsResource(file_non_json),
                "expected_exception": ExtractorError,
                "gzip_flag": True,
                "description": "Invalid file resource (not a JSON)- should raise ExtractorError",
            },
            {
                "resource": FsResource(json_tmp_gzip),
                "expected_exception": None,
                "gzip_flag": True,
                "description": "Valid gzip",
            },
            {
                "resource": FsResource(file_non_json),
                "expected_exception": ExtractorError,
                "gzip_flag": True,
                "description": "Invalid gzip",
            },
        ]

        for case in test_cases:
            with self.subTest(msg=case["description"]):
                self.extract_and_test(
                    resource=case["resource"],
                    expected_exception=case["expected_exception"],
                    gzip_flag=case["gzip_flag"]
                )
