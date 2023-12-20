import unittest
import sqlite3
import pandas as pd
from shipping_data_importer import process_shipping_data_0, process_shipping_data_1_and_2


class TestShippingDataImporter(unittest.TestCase):

    def setUp(self):
        self.database_path = '/Users/doepesci/Desktop/forage-walmart-task-4/shipment_database.db'
        self.conn = sqlite3.connect(self.database_path)

    def test_process_shipping_data_0(self):
        file_0 = '/Users/doepesci/Desktop/forage-walmart-task-4/data/shipping_data_0.csv'
        process_shipping_data_0(file_0, self.conn)

        # Test if the table exists and has data
        df = pd.read_sql_query("SELECT * FROM ShippingData0", self.conn)
        self.assertNotEqual(df.empty, True)

    def test_process_shipping_data_1_and_2(self):
        file_1 = '/Users/doepesci/Desktop/forage-walmart-task-4/data/shipping_data_1.csv'
        file_2 = '/Users/doepesci/Desktop/forage-walmart-task-4/data/shipping_data_2.csv'
        process_shipping_data_1_and_2(file_1, file_2, self.conn)

        # Test if the table exists and has data
        df = pd.read_sql_query("SELECT * FROM ShippingData1and2", self.conn)
        self.assertNotEqual(df.empty, True)

    def tearDown(self):
        self.conn.close()


if __name__ == '__main__':
    unittest.main()
