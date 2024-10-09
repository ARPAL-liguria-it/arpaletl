from abc import ABC, abstractmethod
import pandas as pd

class IExtractor(ABC):
    """
    Interface for extractor
    """


    @abstractmethod
    def __init__(self):
        """
        Constructor for IExtractor
        """


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


class ExtractorError(Exception):
    """
    Custom exception for extractor errors
    """

    def __init__(self, message: str):
        """
        Constructor for ExtractorError
        """
        self.message = message
        super().__init__(self.message)
