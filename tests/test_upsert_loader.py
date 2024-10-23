import unittest
import asyncio
import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
from sqlalchemy import or_
from arpaletl.utils.arpaletlerrors import LoaderError
from arpaletl.loader.upsertloader import UpsertLoader


class TestUpsertLoader(unittest.TestCase):
    """
    Test class for UpsertLoader.
    """

    def setUp(self):
        """
        Set up test database and create test table
        """
        self.engine = create_engine(
            'oracle+oracledb:'
        )
        self.metadata = MetaData()

        self.test_table = Table(
            'test_employees',
            self.metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(100)),
            Column('department', String(100)),
            Column('salary', Integer)
        )

        self.metadata.create_all(self.engine)

        class MockDBClient:
            """
            Mock database client to simulate database operations.
            """

            def __init__(self, engine):
                """
                Constructor to set the engine.
                """
                self._engine = engine

            def get_engine(self):
                """
                Return the engine.
                """
                return self._engine
        self.db_client = MockDBClient(self.engine)

    async def async_upsert(self, test_data, keys, excp=False):
        """
        Helper method to call the async upsert method
        """
        if excp:
            loader = UpsertLoader('che peccato!')
        else:
            loader = UpsertLoader(self.db_client)
        await loader.upsert(test_data, self.test_table, keys)

    def test_insert_new_records(self):
        """
        Test inserting new records into empty table
        """
        test_data = pd.DataFrame({
            'id': [1, 2],
            'name': ['John Doe', 'Jane Smith'],
            'department': ['IT', 'HR'],
            'salary': [75000, 65000]
        })

        keys = ['id']

        asyncio.run(self.async_upsert(test_data, keys))

        with self.engine.connect() as connection:
            result = connection.execute(
                self.test_table.select()
                    .where(or_(self.test_table.c.id == 1, self.test_table.c.id == 2))
            )
            rows = result.fetchall()
            result_df = pd.DataFrame(rows, columns=test_data.columns)

            pd.testing.assert_frame_equal(
                test_data.sort_values('id').reset_index(drop=True),
                result_df.sort_values('id').reset_index(drop=True
                                                        ))

    def test_update_existing_records(self):
        """
        Test updating existing records in table
        """
        initial_data = pd.DataFrame({
            'id': [1],
            'name': ['John Doe'],
            'department': ['IT'],
            'salary': [75000]
        })

        updated_data = pd.DataFrame({
            'id': [1],
            'name': ['John Doe'],
            'department': ['Engineering'],
            'salary': [85000]
        })

        keys = ['id']

        asyncio.run(self.async_upsert(initial_data, keys))

        asyncio.run(self.async_upsert(updated_data, keys))

        with self.engine.connect() as connection:
            result = connection.execute(
                self.test_table.select().where(self.test_table.c.id == 1))
            row = result.fetchone()

            self.assertIsNotNone(row, "No row returned for id = 1.")

            result_df = pd.DataFrame([row], columns=updated_data.columns)

            pd.testing.assert_frame_equal(
                updated_data.sort_values('id').reset_index(drop=True),
                result_df.sort_values('id').reset_index(drop=True)
            )

    def test_mixed_insert_update(self):
        """
        Test both inserting new records and updating existing ones
        """
        initial_data = pd.DataFrame({
            'id': [1, 2],
            'name': ['John Doe', 'Jane Smith'],
            'department': ['IT', 'HR'],
            'salary': [75000, 65000]
        })

        mixed_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['John Doe', 'Jane Wilson', 'Bob Brown'],
            'department': ['Engineering', 'HR', 'Marketing'],
            'salary': [85000, 70000, 60000]
        })

        keys = ['id']

        asyncio.run(self.async_upsert(initial_data, keys))

        asyncio.run(self.async_upsert(mixed_data, keys))

        with self.engine.connect() as connection:
            result = connection.execute(self.test_table.select())
            rows = result.fetchall()
            result_df = pd.DataFrame(rows, columns=mixed_data.columns)

            pd.testing.assert_frame_equal(
                mixed_data.sort_values('id').reset_index(drop=True),
                result_df.sort_values('id').reset_index(drop=True
                                                        ))

    def test_error_handling(self):
        """
        Test error handling for invalid data
        """
        invalid_data = pd.DataFrame({
            'id': [1],
            'invalid_column': ['test']
        })

        keys = ['id']

        with self.assertRaises(LoaderError):
            asyncio.run(self.async_upsert(invalid_data, keys, True))
