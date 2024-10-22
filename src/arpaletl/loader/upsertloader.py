"""
Module for UpsertLoader class
"""
import pandas as pd
from sqlalchemy import Table
from sqlalchemy import select, update, insert
from src.arpaletl.loader.loader import ILoader
from src.arpaletl.utils.arpaletlerros import LoaderError
from src.arpaletl.utils.logger import get_logger


class UpsertLoader(ILoader):
    """
    Class that loads data into Oracle DB. Implements the ILoader interface.
    """

    def __init__(self, db_client) -> None:
        """
        Constructor for OracleDbLoader
        @param db_client: Database client
        """
        self.logger = get_logger(__name__)
        self.db_client = db_client

    async def upsert(self, data: pd.DataFrame, table: Table, keys: dict) -> None:
        """
        Method that loads data into Oracle DB
        @param data: Data to be loaded
        @param table: Table to load data into
        @param keys: Keys to match data with
        """
        try:
            engine = self.db_client.get_engine()
            self.logger.info("Upserting data into DB")
            with engine.connect() as connection:
                for _, row in data.iterrows():
                    data = row.to_dict()
                    data = {k: v for k, v in data.items(
                    ) if k in table.columns.keys()}
                    conditions = [table.c[col] == data[col] for col in keys]
                    stmt = select(table).where(*conditions)
                    result = connection.execute(stmt).fetchone()

                    if result:
                        stmt = update(table).where(*conditions).values(**data)
                    else:
                        stmt = insert(table).values(**data)
                    connection.execute(stmt)
                connection.commit()
        except Exception as e:
            raise LoaderError(
                f"Error loading data into DB: {str(e)}") from e

    def __del__(self) -> None:
        """
        Destructor for OracleDbLoader
        """
        self.logger.info("Closing DB connection")
        self.db_client.close()
