import logging
import pandas as pd
import numpy as np
import time


class Recon:
    def __init__(self, source_path, target_path):
        self.source_path = source_path
        self.target_path = target_path

        start = time.time()
        self.source = self.load_csv(path=source_path)
        self.target = self.load_csv(path=target_path)
        end = time.time()
        print("Loading csv  took {} seconds".format(end - start))
        self.source_join_column = self.source.columns[0]
        self.target_join_column = self.target.columns[0]

    def get_source_df(self):
        return self.source

    def get_target_df(self):
        return self.target

    def load_csv(self, path) -> pd.DataFrame:
        '''
        Import CSV file
        Parameters:
            path (str): path to the CSV file

        Returns:
            pd.DataFrame: dataframe from the CSV file
        '''
        try:

            logging.info(f"Loading file from {path}...")
            df = pd.read_csv(path, dtype={0: str},  engine="pyarrow")
            #df.pipe(self.copy_df).pipe(self.remove_trailing_whitespaces)

            return df

        except FileNotFoundError:
            logging.error(f"File not found at {path}. Please check the path and try again.")
        except pd.errors.ParserError as e:
            logging.error(f"Error parsing csv file. Details: {e}")
        except pd.errors.EmptyDataError as e:
            logging.error(f"Empty csv file.")
        except Exception as e:
            logging.error(f"An unexpected error occurred. Details: {e}")

    def get_missing_records(self, source: pd.DataFrame, target: pd.DataFrame, name='Source') -> pd.DataFrame:
        missing = self.source[
            ~self.source[source.columns[0]].isin(self.target[target.columns[0]])].dropna()
        return pd.DataFrame({
            "Type": f"Missing in {name}",
            "Record Identifier": missing[source.columns[0]]
        })

    # Remove leading & trailing whitespaces from dictionary values
    def remove_trailing_whitespaces(self, df):
        for c in df.columns:

            df[c] = df[c].lower()
            df = df.rename(columns={c: c.strip()})
        return df

    def copy_df(self, df):
        return df.copy()
    # Make dict values lower case
    def make_dict_values_case_insensitive(self, dict):
        return {key: val.lower() for key, val in dict.items()}
    def get_all_columns(self) -> set:
        '''
        Get all columns from both files

        Returns:
            columnsset: unique columns from both files

        '''
        source_columns = self.source.columns[1:]
        target_columns = self.target.columns[1:]
        columns = set(source_columns.append(target_columns))
        return columns

    def compare_columns(self, columns) -> pd.DataFrame:
        '''
        Compare columns between the two files

        Parameters:
            columns (list): list of columns to compare

        Returns:
            pd.DataFrame: dataframe of discrepancies
        '''

        logging.info("Comparing source and target columns...")
        results_df = pd.DataFrame()
        for column in columns:
            source_df = self.source[[self.source_join_column, column]].dropna()
            target_df = self.target[[self.target_join_column, column]].dropna()
            comparison_df = source_df.merge(target_df, how='inner', left_on=self.source_join_column,
                                            right_on=self.target_join_column, suffixes=('_source', '_target'))
            comparison_df = comparison_df[comparison_df[column + '_source'] != comparison_df[column + '_target']]
            differences_df = pd.DataFrame({
                "Type": "Field Discrepancy",
                "Record Identifier": comparison_df[self.source_join_column],
                "Field": column,
                "Source Value": comparison_df[column + '_source'],
                "Target Value": comparison_df[column + '_target']
            })
            results_df = pd.concat([results_df, differences_df])
        logging.info("Done!")

        return results_df