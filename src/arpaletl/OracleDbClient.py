import os
import logging
import sqlalchemy
import oracledb
from src.arpaletl.IDbClient import IDbClient


class OracleDbClient(IDbClient):
    """
    Oracle Database client that implements the IDbClient interface
    """

    def __init__(self):
        """
        Init method that creates an engine for sqlalchemy
        """
        connection = get_connection()
        if connection is not None:
            self.conn = connection
            self.cursor = connection.cursor()
            try:
                self.engine = sqlalchemy.create_engine(
                    "oracle+oracledb://",
                    creator=lambda: connection
                )
                logging.info("Sqlalchemy engine created successfully")
            except Exception as e:
                logging.error("Sqlachemy engine creation failed: %s", e)
                exit(1)

    def __del__(self):
        """
        Delete method
        """
        self.conn.close()


def get_connection():
    """ 
    Helper function that creates a connection to the DB reading the environmental variables
    """
    try:
        user = os.getenv("DB_USER")
        pwd = os.getenv("DB_PASSWORD")
        dsn = os.getenv("DB_DSN")

        if user is not None and pwd is not None and dsn is not None:
            connection = oracledb.connect(
                user=user,
                password=pwd,
                dsn=dsn
            )
            logging.info("Connection to Oracle Database succeded")
            return connection
        else:
            raise ValueError("Env vars are not set")
    except Exception as e:
        logging.error("Connection to Oracle Database failed: %s", e)
        exit(1)
