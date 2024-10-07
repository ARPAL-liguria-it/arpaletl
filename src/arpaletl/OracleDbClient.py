import os
import sqlalchemy
import oracledb
import logging
from IDbClient import IDbClient

class OracleDbClient(IDbClient):

    def __init__(self):
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
            except:
                logging.error("Sqlachemy engine creation failed")
                exit(1)
    
    def __del__(self):
        self.conn.close()

def get_connection():
    try:
        user = os.getenv("DB_USER")
        pwd = os.getenv("DB_PASSWORD")
        dsn = os.getenv("DB_DSN")

        if user is not None and pwd is not None and dsn is not None:
            connection = oracledb.connect(
                        user = user,
                        password = pwd,
                        dsn = dsn
            )
            logging.info("Connection to Oracle Database succeded")
            return connection
    except:
        logging.error("Connection to Oracle Database failed")
        exit(1)

