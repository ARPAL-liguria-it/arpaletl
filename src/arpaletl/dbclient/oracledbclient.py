"""
Oracle database client module that implements the IDbClient interface
"""
import sqlalchemy
from src.arpaletl.dbclient.dbclient import IDbClient
from src.arpaletl.utils.arpaletlerros import DbClientError
from src.arpaletl.utils.logger import get_logger


class OracleDbClient(IDbClient):
    """
    Oracle Database client that implements the IDbClient interface
    """

    engine: sqlalchemy.Engine

    def __init__(self, db_user: str, db_password: str, db_dsn: str):
        """
        Init method for OracleDbClient
        @raises DbClientError: If the engine creation fails
        @param db_user: Database user
        @param db_password: Database password
        @param db_dsn: Database DSN
        @self.engine: sqlalchemy.Engine object
        @self.logger: Logger object
        """
        self.logger = get_logger(__name__)
        if not db_user or not db_password or not db_dsn:
            self.logger.error("Invalid credentials")
            raise DbClientError("Invalid credentials")
        self.engine = sqlalchemy.create_engine(
            f"oracle+oracledb://{db_user}:{db_password}@{db_dsn}",
            thick_mode=None
        )
        self.logger.info("Sqlalchemy engine created successfully")

    def __del__(self):
        """
        Delete method for OracleDbClient that disposes of the engine
        """
        try:
            self.engine.dispose()
        except Exception as e:
            self.logger.error("Engine disposal failed: %s", e)
            raise DbClientError("Engine disposal failed") from e
        self.logger.info("Sqlalchemy engine disposed")

    def connect(self) -> sqlalchemy.Connection:
        """
        Connect method for OracleDbClient
        @returns: Connection object
        """
        try:
            conn = self.engine.connect()
        except Exception as e:
            self.logger.error("Connection failed: %s", e)
            raise DbClientError("Connection failed") from e
        return conn
