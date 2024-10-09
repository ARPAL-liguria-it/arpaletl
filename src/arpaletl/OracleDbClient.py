import logging
import sqlalchemy
from src.arpaletl.IDbClient import IDbClient, DbClientError


class OracleDbClient(IDbClient):
    """
    Oracle Database client that implements the IDbClient interface
    """


    def __init__(self, db_user: str, db_password: str, db_dsn: str):
        """
        Init method for OracleDbClient
        @raises DbClientError: If the engine creation fails
        @param db_user: Database user
        @param db_password: Database password
        @param db_dsn: Database DSN
        @self.engine: sqlalchemy.Engine object
        """
        if not db_user or not db_password or not db_dsn:
            raise DbClientError("Invalid credentials")
        self.engine = sqlalchemy.create_engine(
            f"oracle+oracledb://{db_user}:{db_password}@{db_dsn}",
            thick_mode=None
        )
        logging.info("Sqlalchemy engine created successfully")


    def __del__(self):
        """
        Delete method for OracleDbClient that disposes of the engine
        """
        self.engine.dispose()
        logging.info("Sqlalchemy engine disposed")


    def connect(self) -> sqlalchemy.Connection:
        """
        Connect method for OracleDbClient
        @returns: Connection object
        """
        try:
            conn = self.engine.connect()
        except Exception as e:
            logging.error("Connection failed: %s", e)
            raise DbClientError("Connection failed") from e
        return conn
