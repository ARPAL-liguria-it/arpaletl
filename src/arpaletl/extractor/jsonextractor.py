"""
Module for extracting JSON resources.
"""
import json
import gzip
from io import BytesIO
import pandas as pd
from src.arpaletl.extractor.extractor import IExtractor
from src.arpaletl.utils.arpaletlerros import ExtractorError, ResourceError
from src.arpaletl.resource.resource import IResource
from src.arpaletl.utils.logger import get_logger


class JSONExtractor(IExtractor):
    """
    Class that takes care of extracting a JSON resource 
    into a Pandas DataFrame object for further use.
    """

    df: pd.DataFrame

    def __init__(self, resource: IResource):
        """
        Constructor for JSONExtractor
        @self.resource: Takes a resource from a IResource object
        @self.logger: Logger object
        """
        self.logger = get_logger(__name__)
        self.resource = resource

    async def extract(self, gzipped: bool = False) -> pd.DataFrame:
        """
        Extract method for JSONExtractor that parses the Iterator from IResource async_open()
        @param gzipped: If the data is gzipped
        @raises: ExtractorError: if there are problems reading the JSON.
        @raises: ResourceError: if there are problems opening the resource.
        @returns: Extracted data
        """
        try:
            buffer = BytesIO()
            async for chunk in self.resource.open_stream(1024):
                buffer.write(chunk)
            if not buffer.seekable():
                self.logger.error("Buffer is not seekable")
                raise ExtractorError("Buffer is not seekable")
            if gzipped is True:
                if not buffer.getvalue().startswith(b"\x1f\x8b"):
                    self.logger.error(
                        "Data is not gzipped please set gzipped to False or check the data source")
                    raise ExtractorError(
                        "Data is not gzipped please set gzipped to False or check the data source")
                buffer.seek(0)
                data = gzip.decompress(buffer.getvalue())
            else:
                buffer.seek(0)
                data = buffer.getvalue()
            json_data = json.loads(data.decode("utf-8"))
            self.df = pd.DataFrame(json_data)
            self.logger.info("JSON resource successfully read")
        except ResourceError as e:
            self.logger.error("Error opening resource: %s", e)
            raise ExtractorError("Error opening resource") from e
        except Exception as e:
            self.logger.error("Error reading JSON: %s", e)
            raise ExtractorError("Error reading JSON") from e
        return self.df
