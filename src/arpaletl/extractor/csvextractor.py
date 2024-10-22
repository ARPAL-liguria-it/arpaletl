"""
Module for CsvExtractor class
"""
from io import BytesIO, StringIO
import pandas as pd
from src.arpaletl.extractor.extractor import IExtractor
from src.arpaletl.utils.arpaletlerros import ExtractorError
from src.arpaletl.resource.resource import IResource
from src.arpaletl.utils.logger import get_logger


class CsvExtractor(IExtractor):
    """
    Class that takes care of extracting a CSV resource
    into a Pandas DataFrame object for further use. Implements the IExtractor interface.
    """

    df: pd.DataFrame

    def __init__(self, resource: IResource):
        """
        Constructor for CsvExtractor
        @self.resource: Takes a resource from a IResource object
        @self.logger: Logger object
        """
        self.logger = get_logger(__name__)
        self.resource = resource

    async def extract(self) -> pd.DataFrame:
        """
        Extract method for CsvExtractor that parses the Iterator from IResource async_open()
        @raises: ExtractorError: if there are problems reading the CSV.
        @returns: Extracted data
        """
        try:
            buffer = BytesIO()
            async for chunk in self.resource.open_stream(1024):
                buffer.write(chunk)
            if not buffer.seekable():
                self.logger.error("Buffer is not seekable")
                raise ExtractorError("Buffer is not seekable")
            buffer.seek(0)
            data = buffer.getvalue().decode("utf-8")
            csv_data = StringIO(data)
            self.df = pd.read_csv(csv_data)
            self.logger.info("CSV resource successfully read")
        except Exception as e:
            self.logger.error("Error reading CSV resource: %s", e)
            raise ExtractorError("Error reading CSV resource") from e
        return self.df
