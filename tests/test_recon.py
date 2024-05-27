import os
import unittest
from cred_csv.recon import Recon
import pandas as pd
class TestRecon(unittest.TestCase):
    def setUp(self):

        path = os.path.dirname(__file__)
        self.recon = Recon(f"{path}/data/source.csv", f"{path}/data/source.csv", 'data/output.csv')

    def test_load_csv(self):
        self.assertIsInstance(self.recon.get_source_df(), pd.DataFrame)
        self.assertIsInstance(self.recon.get_target_df(), pd.DataFrame)


    def test_missing_records(self):
        missing_in_source = self.recon.get_missing_records(self.recon.get_source_df(), self.recon.get_target_df())
        self.assertIsInstance(missing_in_source, pd.DataFrame)

        top = missing_in_source['Type']
        #self.assertEqual(top.get('Missing in Target'), 3)
        self.assertEqual(missing_in_source.shape[0], 0)

    def test_compare_field(self):

        column_differences = self.recon.compare_columns(['Name', 'Date'])
        self.assertEqual(column_differences.shape[0], 0)
    def tearDown(self):
        if os.path.exists("data/source.csv"):
            os.remove("data/source.csv")

if __name__ == '__main__':
    unittest.main()