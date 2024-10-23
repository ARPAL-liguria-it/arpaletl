import unittest
import asyncio
from pathlib import Path
import pandas as pd
from arpaletl.extractor.csvextractor import CsvExtractor
from arpaletl.utils.arpaletlerrors import ExtractorError
from arpaletl.resource.fsresource import FsResource


class TestCSVExtractor(unittest.TestCase):
    """
    Test class for JSONExtractor with resources passed as parameters to each function.
    """

    def extract_and_test(self, resource, expected_exception=None):
        """
        Helper method to extract data using the provided resource and handle expected exceptions.
        """
        extractor = CsvExtractor(resource)
        if expected_exception is not None:
            with self.assertRaises(expected_exception):
                asyncio.run(extractor.extract())
        else:
            df = asyncio.run(extractor.extract())
            self.assertIsInstance(df, pd.DataFrame)

    def test_json_extractor_parametric(self):
        """
        Parametric test for JSONExtractor.
        """
        current_dir = Path(__file__).parent
        test_file_non_formatted = current_dir / 'blobs' / 'test_file_non_formatted'
        test_csv = current_dir / 'blobs' / 'test_csv'

        test_cases = [
            {
                "resource": FsResource(test_file_non_formatted),
                "expected_exception": ExtractorError,
                "description": "Error reading CSV resource - should raise ExtractorError",
            },
            {
                "resource": FsResource(test_csv),
                "expected_exception": None,
                "description": "Valid csv",
            },
        ]

        for case in test_cases:
            with self.subTest(msg=case["description"]):
                self.extract_and_test(
                    resource=case["resource"],
                    expected_exception=case["expected_exception"]
                )
