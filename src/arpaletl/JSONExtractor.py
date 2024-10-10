"""
Extractor for JSON data
"""
import json
import gzip
from io import BytesIO
import pandas as pd
from src.arpaletl.IExtractor import IExtractor
from src.arpaletl.ArpalEtlErrors import ExtractorError, ResourceError
from src.arpaletl.IResource import IResource
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

    async def extract(self, zipped: bool = False) -> pd.DataFrame:
        """
        Extract method for JSONExtractor that parses the Iterator from IResource async_open()
        @raises: ExtractorError: if there are problems reading the JSON.
        @raises: ResourceError: if there are problems opening the resource.
        @returns: Extracted data
        """
        try:
            buffer = BytesIO()
            async for chunk in self.resource.open_stream(1024):
                buffer.write(chunk)
            buffer.seek(0)
            if zipped is True:
                data = gzip.decompress(buffer.getvalue())
            else:
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
