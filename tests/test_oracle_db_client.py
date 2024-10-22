import unittest
from unittest.mock import patch, MagicMock
from sqlalchemy import Engine
from arpaletl.dbclient.oracledbclient import OracleDbClient
from arpaletl.utils.arpaletlerrors import DbClientError


class TestOracleDbClient(unittest.TestCase):
    """
    Test class for OracleDbClient
    """

    def test_oracle_db_client_init_success(self):
        """
        Test the constructor of OracleDbClient
        """
        db = OracleDbClient("test_user", "test_password", "test_dsn")
        self.assertIsInstance(db.engine, Engine)

    def test_oracle_db_client_init_failure(self):
        """
        Test the constructor of OracleDbClient with malformed credentials
        """
        with self.assertRaises(DbClientError):
            OracleDbClient("test_user", None, "test_dsn")

    @patch("sqlalchemy.Engine.connect")
    def test_oracle_db_client_connect_success(self, mock_engine_connect):
        """
        Test the connect method of OracleDbClient
        """
        mock_conn = MagicMock(spec=Engine)
        mock_engine_connect.return_value = mock_conn
        db = OracleDbClient("test_user", "test_password", "test_dsn")
        conn = db.connect()
        self.assertIsInstance(conn, Engine)

    def test_oracle_db_client_connect_failure(self):
        """
        Test the connect method of OracleDbClient with invalid credentials
        """
        db = OracleDbClient("test_user", "test_password", "test_dsn")
        with self.assertRaises(DbClientError):
            db.connect()
