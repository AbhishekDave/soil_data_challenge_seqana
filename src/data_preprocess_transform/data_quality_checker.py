# src/data_preprocess_transform/data_quality_checker.py

import ast
import re

import pandas as pd


class DataQualityChecker:
    @staticmethod
    def check_column_data_types(df: pd.DataFrame, column_types: dict) -> pd.DataFrame:
        """
        Check if the data types of specified columns match the expected data types.

        Args:
            df (pd.DataFrame): The DataFrame to check.
            column_types (dict): A dictionary where keys are column names and values are the expected data types.
            FOR Example--> X: float, orgc_profile_code: object

        Returns:
            pd.DataFrame: A DataFrame with details of both matched and mismatched data types.
        """
        results = []

        for column, expected_type in column_types.items():
            if column in df.columns:
                # Check data type
                actual_type = df[column].dtype
                if pd.api.types.is_dtype_equal(actual_type, expected_type):
                    issue_check_type = 'data_type_match'
                else:
                    issue_check_type = 'data_type_mismatch'

                results.append({
                    'column_name': column,
                    'expected_type': expected_type,
                    'actual_type': actual_type,
                    'issue_check_type': issue_check_type
                })

        return pd.DataFrame(results)

    @staticmethod
    def check_column_patterns(df: pd.DataFrame, column_patterns: dict) -> pd.DataFrame:
        """
        Check if the values in orgc_method, orgc_date, orgc_value columns match their respective regex patterns.
        """
        results = []
        issue_check_type = None
        for column, pattern in column_patterns.items():
            if column in df.columns:
                for index, value in df[column].dropna().items():
                    dict_str = str(value).strip()  # Convert to string and remove any leading/trailing whitespace

                    if column == 'orgc_method':
                        # Valid dictionary handling for 'orgc_method'
                        dict_str = dict_str.strip('{}').strip()  # Remove outer brackets and whitespace
                        dict_str = '{' + dict_str + '}'  # Ensure proper dictionary format for literal_eval
                        try:
                            if ast.literal_eval(dict_str):  # Check for valid dictionary format
                                issue_check_type = 'regex_pattern_match'
                            else:
                                issue_check_type = 'regex_pattern_mismatch'
                        except (ValueError, SyntaxError):
                            results.append({
                                'column_name': column,
                                'issue_check_type': issue_check_type,
                                'value': value,
                                'row_index_for_value': index
                            })
                    else:
                        # General regex pattern matching for other columns
                        try:
                            if re.fullmatch(pattern, dict_str):  # Check if it's a valid column pattern defined in main.py in function call
                                issue_check_type = 'regex_pattern_match'
                            else:
                                issue_check_type = 'regex_pattern_mismatch'
                        except (ValueError, SyntaxError):
                            results.append({
                                'column_name': column,
                                'issue_check_type': issue_check_type,
                                'value': value,
                                'row_index_for_value': index
                            })

        return pd.DataFrame(results)

    @staticmethod
    def check_outliers(df: pd.DataFrame, numerical_columns: list) -> pd.DataFrame:
        """
        Detect outliers in specified numerical columns using the IQR method.
        """
        results = []
        for column in numerical_columns:
            if column in df.columns:
                Q1 = df[column].quantile(0.25)
                Q3 = df[column].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                outliers = df[(df[column] < lower_bound) | (df[column] > upper_bound)]
                if not outliers.empty:
                    results.append({'column': column, 'outliers_indices': outliers.index.tolist()})
        return pd.DataFrame(results)

    @staticmethod
    def check_missing_values(df: pd.DataFrame, column_list: list) -> pd.DataFrame:
        """
        Detect missing or null values in specified columns.
        """
        missing_data = df[column_list].isnull().sum()
        return missing_data.reset_index().rename(columns={0: 'missing_count', 'index': 'column'})

    @staticmethod
    def check_date_format(df: pd.DataFrame, date_columns: list, expected_format='%Y-%m-%d') -> pd.DataFrame:
        """
        Check if the date columns match the expected format.
        """
        results = []
        for column in date_columns:
            if column in df.columns:
                # invalid_dates = df[~pd.to_datetime(df[column], format=expected_format, errors='raise')]
                # if not invalid_dates.empty:
                # results.append({'column': column, 'invalid_dates': invalid_dates.index.tolist()})
                for index, date_value in df[column].dropna().items():
                    try:
                        pd.to_datetime(date_value, format=expected_format, errors='raise')
                    except (ValueError, TypeError):
                        results.append({
                            'column_name': column,
                            'issue_check_type': 'invalid_date_format',
                            'value': date_value,
                            'row_index': index
                        })
        return pd.DataFrame(results)

    @staticmethod
    def check_lat_long(df: pd.DataFrame, lat_column: str, long_column: str) -> pd.DataFrame:
        """
        Check if latitude and longitude values are valid.
        Latitude should be between -90 and 90, and longitude between -180 and 180.
        """
        results = []

        for index, (lat, long) in df[[lat_column, long_column]].iterrows():
            if pd.isna(lat):
                results.append({
                    'column_name': lat_column,
                    'issue_check_type': 'missing_latitude',
                    'value': lat,
                    'row_index': index
                })
            elif not (-90 <= lat <= 90):
                results.append({
                    'column_name': lat_column,
                    'issue_check_type': 'invalid_latitude',
                    'value': lat,
                    'row_index': index
                })

            if pd.isna(long):
                results.append({
                    'column_name': long_column,
                    'issue_check_type': 'missing_longitude',
                    'value': long,
                    'row_index': index
                })
            elif not (-180 <= long <= 180):
                results.append({
                    'column_name': long_column,
                    'issue_check_type': 'invalid_longitude',
                    'value': long,
                    'row_index': index
                })

        return pd.DataFrame(results)

    @staticmethod
    def check_depth_columns(df: pd.DataFrame, upper_depth_col: str = 'upper_depth',
                            lower_depth_col: str = 'lower_depth') -> pd.DataFrame:
        """
        Check the consistency of upper_depth and lower_depth columns.
        - upper_depth can be 0.
        - lower_depth must always be greater than 0 and greater than to upper_depth.

        Args:
            df (pd.DataFrame): DataFrame to check.
            upper_depth_col (str): The column name for upper depth.
            lower_depth_col (str): The column name for lower depth.

        Returns:
            dataFrame containing details of any depth inconsistencies.
        """
        results = []

        # Check if columns exist
        if upper_depth_col in df.columns and lower_depth_col in df.columns:
            for index, (upper_depth, lower_depth) in df[[upper_depth_col, lower_depth_col]].dropna().iterrows():
                if upper_depth < 0:
                    results.append({
                        'issue_check_type': 'invalid_upper_depth',
                        'row_index': index,
                        'upper_depth': upper_depth,
                        'lower_depth': lower_depth,
                        'message': 'Upper depth cannot be negative.'
                    })
                if lower_depth <= 0:
                    results.append({
                        'issue_check_type': 'invalid_lower_depth',
                        'row_index': index,
                        'upper_depth': upper_depth,
                        'lower_depth': lower_depth,
                        'message': 'Lower depth cannot be 0 or negative, must be greater than 0.'
                    })
                if lower_depth < upper_depth:
                    results.append({
                        'issue_check_type': 'lower_depth_less_than_upper_depth',
                        'row_index': index,
                        'upper_depth': upper_depth,
                        'lower_depth': lower_depth,
                        'message': 'Lower depth must be greater than upper depth.'
                    })

        return pd.DataFrame(results)
