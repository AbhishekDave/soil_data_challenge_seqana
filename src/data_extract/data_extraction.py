# src/data_extract/data_extraction.py

import pandas as pd
import os
import re
import ast


class DataExtraction:
    @staticmethod
    def read_raw_data(file_name):
        """ Load the Excel file into a DataFrame. """
        # Get the current working directory
        project_directory = os.getcwd()

        # Dataset path
        dataset_directory = os.path.join(project_directory, 'dataset')
        file_name = os.path.join(dataset_directory, file_name)

        return pd.read_excel(file_name)

    @staticmethod
    def generate_review_dataframes(input_data):
        """
        Generates review DataFrames for a single DataFrame or a dictionary of DataFrames.

        Parameters:
        - input_data: a single DataFrame or a dictionary of DataFrames.

        Returns:
        A dataframe dictionary of reviews in the form of description for input dataFrame.
        """

        def process_dataframe(df_name):
            """Process a single DataFrame and generate review information."""
            # Check if DataFrame is empty
            if df_name.empty:
                print("Warning: DataFrame is empty.")
                return {
                    'Info Summary': pd.DataFrame(),
                    # 'Summary Statistics - Numerical': pd.DataFrame(),
                    # 'Summary Statistics - Categorical': pd.DataFrame(),
                    'Dataset Info': pd.DataFrame()
                }

            info_df = pd.DataFrame({
                'Data Type': df_name.dtypes,
                'Null/Missing Values': df_name.isna().sum(),
                'Total Values': df_name.count(),
                'Unique Values': df_name.nunique()
            })

            # Summary Statistics for Numerical Columns
            # summary_numerical = df_name.describe()

            # Summary Statistics for Categorical Columns
            """try:
                summary_categorical = df_name.describe(include=[object])
            except ValueError as e:
                print(f"Error generating categorical summary: {e}")
                summary_categorical = pd.DataFrame()"""

            # Dataset Info
            dataset_info = pd.DataFrame({
                'Number of Entries': [df_name.shape[0]],
                'Number of Columns': [df_name.shape[1]],
                'Memory Usage': [df_name.memory_usage(deep=True).sum()]
            })

            return {
                'Info Summary': info_df,
                # 'Summary Statistics - Numerical': summary_numerical,
                # 'Summary Statistics - Categorical': summary_categorical,
                'Dataset Info': dataset_info
            }

        # If the input is a dictionary, process each DataFrame in the dictionary
        if isinstance(input_data, dict):
            review_dataframes = {}
            for name, df in input_data.items():
                review_dataframes[name] = process_dataframe(df)
            return review_dataframes

        # If the input is a single DataFrame, process it directly
        elif isinstance(input_data, pd.DataFrame):
            return process_dataframe(input_data)

        # If input is not a DataFrame or a dictionary of DataFrames, raise an error
        else:
            raise ValueError("Input must be a dataFrame or a dictionary of dataFrames.")

    @staticmethod
    def parse_dict_string(dict_str, is_method=False):
        """ Convert a string representation of a dictionary to an actual dictionary. """

        if pd.isna(dict_str) or isinstance(dict_str, float):  # Check for NaN or float types
            return {}

        dict_str = str(dict_str).strip()  # string conversion and remove any leading/trailing whitespace

        if is_method:
            # For method_dict, use ast.literal_eval for safer evaluation
            try:
                dict_str = dict_str.strip('{}').strip()  # Remove outer braces and whitespace
                dict_str = '{' + dict_str + '}'  # ensure proper dictionary format for literal evaluatin
                return ast.literal_eval(dict_str)
            except (ValueError, SyntaxError, TypeError) as e:
                print(f"Error parsing method_dict: {e}")
                return {}
        else:
            # For value_dict and date_dict, use regex
            pattern = r'(\d+):\s*([^,]+)'
            matches = re.findall(pattern, dict_str)
            return {key.strip(): value.strip() for key, value in matches}

    @staticmethod
    def extract_raw_data_based_on_method_instance(row):

        method_str = row['orgc_method']
        value_str = row['orgc_value']
        date_str = row['orgc_date']

        method_dict = DataExtraction.parse_dict_string(method_str, is_method=True)
        value_dict = DataExtraction.parse_dict_string(value_str)
        date_dict = DataExtraction.parse_dict_string(date_str)

        rows = []
        used_dates = set()

        for method_instance in method_dict:
            instance_key = method_instance.split(":")[0].strip()
            instance_value = value_dict.get(instance_key, None)
            instance_date_value = date_dict.get(instance_key, None)

            if instance_date_value in used_dates:
                value_exists_for_date = any(
                    r['orgc_value_for_instance'] == instance_value and
                    r['orgc_date_for_instance'] == instance_date_value
                    for r in rows
                )
                if value_exists_for_date or instance_value is None:
                    continue

            new_row = row.to_dict()

            try:
                new_row['method_instance'] = int(instance_key)
            except ValueError:
                new_row['method_instance'] = None

            new_row['orgc_value_for_instance'] = instance_value.rstrip('}') if instance_value else None
            new_row['orgc_date_for_instance'] = instance_date_value.rstrip('}') if instance_date_value else None

            rows.append(new_row)
            used_dates.add(instance_date_value)

        return rows
