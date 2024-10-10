import logging
import pandas as pd
from src.arpaletl.IExtractor import IExtractor, ExtractorError
from src.arpaletl.IResource import IResource


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
        """
        self.resource = resource

    def extract(self) -> pd.DataFrame:
        """
        Extract method for JSONExtractor that parses the Iterator from IResource open_stream()
        @raises: ExtractorError: if there are problems reading the JSON.
        @returns: Extracted data
        """
        try:
            self.df = pd.read_json(
                self.resource.open_stream(1024), lines=False)
        except Exception as e:
            logging.error("Error reading JSON: %s", e)
            raise ExtractorError("Error reading JSON") from e
        return self.df
