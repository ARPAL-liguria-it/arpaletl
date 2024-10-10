import pandas as pd
from src.arpaletl.IExtractor import IExtractor
from src.arpaletl.ArpalEtlErrors import ExtractorError
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

    def extract(self) -> pd.DataFrame:
        """
        Extract method for JSONExtractor that parses the Iterator from IResource open_stream()
        @raises: ExtractorError: if there are problems reading the JSON.
        @returns: Extracted data
        """
        try:
            data = self.resource.async_open().json()
            self.df = pd.DataFrame(data)
            self.logger.info("JSON resource successfully read")
        except Exception as e:
            self.logger.error("Error reading JSON: %s", e)
            raise ExtractorError("Error reading JSON") from e
        return self.df
