from abc import ABC, abstractmethod
import pandas as pd
from src.arpaletl.IResource import IResource


class IExtractor(ABC):
    """
    Interface for extractor
    """

    @abstractmethod
    def __init__(self, resource: IResource):
        """
        Constructor for IExtractor
        """
        self.resource = resource

    @abstractmethod
    def __del__(self):
        """
        Destructor for IExtractor
        """

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """
        Extract method for IExtractor
        @returns: Extracted data
        """

