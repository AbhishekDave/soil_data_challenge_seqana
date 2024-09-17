# src/data_preprocess_transform/data_preprocessing.py

import pandas as pd
from dateutil import parser


class DataPreprocessing:
    @staticmethod
    def append_preprocessed_rows(new_rows):
        """ Append the transformed rows back to the DataFrame. """
        print('\n**************')
        print(f"Total {len(new_rows)} preprocessed rows to DataFrame.")
        print('**************\n')
        return pd.DataFrame(new_rows)

    @staticmethod
    def drop_duplicates(df, key_columns=None, df_name='DataFrame'):
        """
        Drop duplicate rows based on specified key columns or all columns if not specified.

        Parameters:
        df (Dataframe): The DataFrame to process.
        key_columns (list of str, optional): List of column names to identify duplicates.
                                             If None, all columns from dataframe will be used for duplicate detection.
        df_name (str, optional): Name of the df just for logging purposes.

        Returns:
        df (DataFrame): The DataFrame with duplicates removed.
        """
        print(f"\n**************\nProcessing '{df_name}' for duplicate removal...")

        before_count = len(df)
        print(f"\nTotal rows before removing duplicates: {before_count}")

        if key_columns is None:
            key_columns = df.columns.tolist()  # Use all columns if no key columns specified
            print("\nDropping duplicates based on all columns....")
        else:
            print(f"\nDropping duplicates based on {key_columns} columns....")

        # Drop duplicates based on key_columns and reset index
        df = df.drop_duplicates(subset=key_columns).reset_index(drop=True)

        after_count = len(df)
        deleted_count = before_count - after_count

        if deleted_count > 0:
            print(f"Duplicates removed: {deleted_count}")
        else:
            print(f"\nNo Duplicate found in '{df_name}'.\n**************\n")

        return df

    @staticmethod
    def reformat_dates(df, date_column, desired_format='%Y-%m-%d'):
        """
        Reformat any date column to a consistent format using dateutil for robust parsing.

        Parameters:
        df (pd.DataFrame): The DataFrame containing the date column to process.
        date_column (str): The name of the date column to preprocess.
        desired_format (str): The desired date format (default is '%Y-%m-%d').

        Returns:
        pd.DataFrame: The DataFrame with reformatted dates.
        """
        print('\n**************')
        print(f"Reformatting date column '{date_column}' to format {desired_format}")
        print('**************\n')

        def parse_date_safe(date_str):
            """
            Safely parse a date string using dateutil, and handle any parsing exceptions.
            """
            try:
                parsed_date = parser.parse(date_str)
                return parsed_date.strftime(desired_format)
            except (parser.ParserError, TypeError, ValueError):
                return pd.NaT

        # Replace invalid placeholders with NaT
        df[f'reformat_{date_column}'] = df[date_column].replace(['????-??-??', '', None], pd.NaT)

        # Apply the safe date parsing function
        df[f'reformat_{date_column}'] = df[f'reformat_{date_column}'].apply(parse_date_safe)

        print(f"Finished reformatting date column '{date_column}'.\n")
        return df
