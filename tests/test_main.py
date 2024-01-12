import unittest
import pandas as pd
from pathlib import Path
from main import clean_column_names, load_data, process_routes, get_num_days_in_month, process_data, save_data, upload_to_s3, run_node_script, check_for_directories, data_transform

class TestMain(unittest.TestCase):

    def setUp(self):
        self.df = pd.DataFrame({
            'A 1': [' a ', ' b ', ' c '],
            'B': [' d ', ' e ', ' f '],
            'C': [' g ', ' h ', ' i ']
        })
        
        
        self.output_df = pd.DataFrame({
            'route': ['CityLink Blue', '80', '80'],
            'date': ['2023-04-01', '2020-02-01', '2020-03-01'],
            'date_end': ['2023-04-30', '2020-02-29', '2020-03-31'],
            'ridership': [300000, 29000, 31000],
            'num_days_in_month': [30, 29, 31],
            'ridership_per_day': [10000, 1000, 1000]
        })
        
        self.output_columns = ['route', 'date', 'date_end', 'ridership', 'num_days_in_month', 'ridership_per_day']


    def test_clean_column_names(self):
        result = clean_column_names(self.df)
        self.assertEqual(list(result.columns), ['a_1', 'b', 'c'])

    def test_load_data(self):
        # Assuming you have a test.csv file in your directory
        result = load_data.fn(Path('tests/data/raw/test.csv'))
        self.assertIsInstance(result, pd.DataFrame)

    def test_process_routes(self):
        result = process_routes("CityLink BLUE, CityLink GOLD")
        self.assertEqual(result, "CityLink Blue, CityLink Gold")

    def test_get_num_days_in_month(self):
        result = get_num_days_in_month(pd.to_datetime("2022-02-01"))
        self.assertEqual(result, 28)

    def test_process_data(self):
        # Assuming you have a test.csv file in your directory
        df = load_data.fn(Path('tests/data/raw/test.csv'))
        result = process_data.fn(df)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(list(result.columns), self.output_columns)
        self.assertEqual(result.shape, (3, 6))
        
        

    def test_save_data(self):
        
        save_data.fn(self.output_df, Path('tests/data/test_output.csv'))
        self.assertTrue(Path('tests/data/test_output.csv').exists())

    def test_check_for_directories(self):
        check_for_directories.fn()
        self.assertTrue(Path('data/raw').exists())
        self.assertTrue(Path('data/processed').exists())

if __name__ == '__main__':
    unittest.main()
