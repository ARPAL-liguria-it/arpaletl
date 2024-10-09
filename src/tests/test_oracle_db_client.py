import unittest
from unittest.mock import patch, MagicMock
import os
from src.arpaletl.OracleDbClient import OracleDbClient  # Replace 'your_module' with the actual module name where OracleDbClient is located

class test_oracle_db_client(unittest.TestCase):

    @patch('oracledb.connect')  # Mock the oracledb.connect method
    @patch('sqlalchemy.create_engine')  # Mock the sqlalchemy.create_engine method
    @patch('os.getenv')  # Mock the os.getenv function
    def test_oracle_db_client_init_success(self, mock_getenv, mock_create_engine, mock_connect):
        # Set up the environment variables to return when called
        mock_getenv.side_effect = ['test_user', 'test_password', 'test_dsn']
        
        # Create a mock connection object
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        # Create an instance of OracleDbClient
        client = OracleDbClient()
        
        # Assertions
        mock_connect.assert_called_once_with(
            user='test_user',
            password='test_password',
            dsn='test_dsn'
        )
        self.assertIsNotNone(client.conn)
        self.assertEqual(client.conn, mock_connection)
        self.assertIsNotNone(client.cursor)
        mock_create_engine.assert_called_once()
        
    @patch('oracledb.connect')
    @patch('os.getenv')
    def test_oracle_db_client_init_failure(self, mock_getenv, mock_connect):
        # Simulate a failure in getting environment variables
        mock_getenv.side_effect = [None, None, None]
        
        with self.assertRaises(SystemExit) as cm:  # Capture the SystemExit exception
            OracleDbClient()
        
        # Verify that the exit code is 1
        self.assertEqual(cm.exception.code, 1)  # Ensure the exit code is 1
        
    @patch('oracledb.connect')
    @patch('sqlalchemy.create_engine')
    @patch('os.getenv')
    def test_oracle_db_client_engine_creation_failure(self, mock_getenv, mock_create_engine, mock_connect):
        mock_getenv.side_effect = ['test_user', 'test_password', 'test_dsn']
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        # Simulate a failure in engine creation
        mock_create_engine.side_effect = Exception("Engine creation failed")
        
        with self.assertRaises(SystemExit):  # Expecting exit due to engine creation failure
            OracleDbClient()

if __name__ == '__main__':
    unittest.main()
