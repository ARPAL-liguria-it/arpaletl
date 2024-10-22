"""
Interface for the Loader class.
"""
from abc import ABC, abstractmethod
import pandas as pd
from sqlalchemy import Table


class ILoader(ABC):
    """
    ILoder interface
    """

    @abstractmethod
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    async def upsert(self, data: pd.DataFrame, table: Table, keys: dict) -> None:
        """
        Abstract method that loads the data into a destination.
        @param data: Data to be loaded
        @param table: Table to load data into
        @param keys: Keys to be used for upserting
        """

    @abstractmethod
    def __del__(self) -> None:
        pass
