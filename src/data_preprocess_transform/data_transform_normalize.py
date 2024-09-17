# src/data_preprocess_transform/data_transform_normalize.py

import pandas as pd
import numpy as np
from src.data_extract.data_extraction import DataExtraction
from src.data_preprocess_transform.data_preprocessing import DataPreprocessing


class DataTransformNormalize:

    @staticmethod
    def transform_data_with_orgc_method_details(df):
        """ Transform raw data with method instances from 'orgc_method' column. """
        method_str = df['orgc_method']
        method_dict = DataExtraction.parse_dict_string(method_str, is_method=True)

        method_details = []
        for method in method_dict:
            method_instance, method_values = method.split(':')
            method_data = {'method_instance': int(method_instance)}

            for pair in method_values.split(', '):
                key, value = pair.split(' = ')
                method_data[key] = value

            method_data['orgc_method'] = method_str
            method_details.append(method_data)

        return pd.DataFrame(method_details)

    @staticmethod
    def normalize_dataframes(df):

        # Create orgc_method_df
        # orgc_method_df: Extract method-related columns, add id column
        orgc_method_df = pd.concat(
            df.apply(DataTransformNormalize.transform_data_with_orgc_method_details, axis=1).tolist(),
            ignore_index=True)

        orgc_method_df = DataPreprocessing.drop_duplicates(orgc_method_df, ['method_instance', 'orgc_method'],
                                                           "orgc_method_normalized_df")

        # Add 'id' column
        orgc_method_df['id'] = range(1, len(orgc_method_df) + 1)

        # Rename columns
        orgc_method_df = orgc_method_df.rename(columns={'sample pretreatment': 'sample_pretreatment'})
        # Reorder columns to have 'id' as the first column
        orgc_method_df = orgc_method_df[['id', 'method_instance', 'calculation', 'detection', 'reaction',
                                         'sample_pretreatment', 'spectral', 'temperature', 'treatment',
                                         'orgc_method']].reset_index(drop=True)
        # Convert data types
        columns_to_str = ['calculation', 'detection', 'reaction', 'sample_pretreatment',
                          'spectral', 'temperature', 'treatment', 'orgc_method']
        orgc_method_df[columns_to_str] = orgc_method_df[columns_to_str].astype(str)

        # Create orgc_profile_df
        # orgc_profile_df: 'orgc_profile_id', 'orgc_profile_code', 'orgc_dataset_id', 'X', 'Y', 'country_name'
        orgc_profile_df = df[['profile_id', 'orgc_profile_code', 'orgc_dataset_id', 'X', 'Y', 'country_name']]

        # Rename columns
        orgc_profile_df = orgc_profile_df.rename(columns={'X': 'longitude', 'Y': 'latitude'})

        # drop duplicate values
        orgc_profile_df = DataPreprocessing.drop_duplicates(orgc_profile_df, df_name='profile_df')

        # Generate 'id' column for orgc_profile_df
        orgc_profile_df['id'] = range(1, len(orgc_profile_df) + 1)

        # Convert data types
        columns_to_str = ['orgc_profile_code']
        orgc_profile_df[columns_to_str] = orgc_profile_df[columns_to_str].astype(str)

        # Reorder orgc_profile_df
        orgc_profile_df = orgc_profile_df[['id', 'profile_id', 'orgc_profile_code',
                                           'orgc_dataset_id', 'latitude', 'longitude',
                                           'country_name']].reset_index(drop=True)

        # Create orgc_profile_layer_df
        # orgc_profile_layer_df: 'profile_layer_id', 'orgc_profile_id', 'upper_depth', 'lower_depth',
        # 'layer_name', 'litter', 'orgc_method_id', 'orgc_value', 'orgc_value_avg', 'orgc_date'

        orgc_profile_layer_df = df[['profile_layer_id', 'upper_depth', 'lower_depth',
                                    'layer_name', 'litter', 'orgc_value_for_instance', 'orgc_value_avg',
                                    'reformat_orgc_date_for_instance', 'method_instance', 'orgc_method',
                                    'profile_id']].reset_index(drop=True)

        # Rename columns
        orgc_profile_layer_df = orgc_profile_layer_df.rename(columns={'orgc_value_for_instance': 'orgc_value',
                                                                      'X': 'latitude', 'Y': 'longitude',
                                                                      'reformat_orgc_date_for_instance': 'orgc_date'})

        # merge orgc_profile_layer_df with orgc_profile_df to get id as orgc_profile_id for (profile_id)

        # merge profile_layer_df with profile_layer_matrix_df to get id as profile_layer_id for ()
        orgc_profile_layer_df = orgc_profile_layer_df.merge(orgc_profile_df[['id', 'profile_id']],
                                                            left_on='profile_id',
                                                            right_on='profile_id',
                                                            how='left')
        # rename merged id column
        orgc_profile_layer_df = orgc_profile_layer_df.rename(columns={'id': 'orgc_profile_id', })

        # merge orgc_profile_layer_df with orgc_method_df to get id as orgc_method_id for (orgc_method, method_instance)
        orgc_profile_layer_df = orgc_profile_layer_df.merge(orgc_method_df[['id', 'method_instance', 'orgc_method']],
                                                            left_on=['method_instance', 'orgc_method'],
                                                            right_on=['method_instance', 'orgc_method'],
                                                            how='left')
        # rename merged id column
        orgc_profile_layer_df = orgc_profile_layer_df.rename(columns={'id': 'orgc_method_id', })

        # Add 'id' column for orgc_profile_layer_df
        orgc_profile_layer_df['id'] = range(1, len(orgc_profile_layer_df) + 1)

        # Reorder columns
        orgc_profile_layer_df = orgc_profile_layer_df[['id', 'profile_layer_id', 'orgc_profile_id', 'upper_depth',
                                                       'lower_depth', 'layer_name', 'litter',
                                                       'orgc_method_id', 'orgc_value', 'orgc_value_avg',
                                                       'orgc_date']].reset_index(drop=True)

        # Convert data types
        orgc_profile_layer_df['orgc_method_id'] = pd.to_numeric(orgc_profile_layer_df['orgc_method_id'],
                                                                errors='coerce',
                                                                downcast='integer')

        # Reorder data tables as per the sql schema structure

        # Reorder columns as per SQL schema structure
        orgc_method_df = orgc_method_df[['id', 'method_instance', 'calculation', 'detection',
                                         'reaction', 'sample_pretreatment', 'spectral',
                                         'temperature', 'treatment']].reset_index(
            drop=True)

        orgc_profile_df = orgc_profile_df[['id', 'profile_id', 'orgc_profile_code',
                                           'orgc_dataset_id', 'latitude', 'longitude',
                                           'country_name']].reset_index(drop=True)

        orgc_profile_layer_df = orgc_profile_layer_df[['id', 'profile_layer_id', 'orgc_profile_id',
                                                       'upper_depth', 'lower_depth', 'layer_name',
                                                       'litter', 'orgc_method_id', 'orgc_value',
                                                       'orgc_value_avg', 'orgc_date']].reset_index(drop=True)
        return {
            'orgc_method_df': orgc_method_df,
            'orgc_profile_df': orgc_profile_df,
            'orgc_profile_layer_df': orgc_profile_layer_df
        }
