# main.py

from data_extract.data_extraction import DataExtraction

from src.data_preprocess_transform.data_quality_checker import DataQualityChecker
from src.data_preprocess_transform.data_preprocessing import DataPreprocessing
from src.data_preprocess_transform.data_transform_normalize import DataTransformNormalize
from src.data_load.data_loading import DataLoading


def main():
    file_name = 'seqana-data-engineering-challenge-data-wosis-belgium.xlsx'

    # Initialize Preprocessor and transformer and data_load Classes
    extract = DataExtraction()
    dataqualitychecker = DataQualityChecker()
    preprocessor = DataPreprocessing()
    transformer = DataTransformNormalize()
    dataloader = DataLoading()

    # Step 1: Extraction of raw data into raw_df and apply data quality checks

    print(f"\nStarting Extract the raw data from the {file_name}...")
    raw_df = extract.read_raw_data(file_name)
    print("Extraction finishes...\n")

    print("\nAnalyzing the raw data...")
    review_raw_df = extract.generate_review_dataframes(raw_df)
    for name, df in review_raw_df.items():
        print(f"\n{name}:\n", df, "\n" + "-" * 80)

    # DataQuality checks on Metadata Level

    print("\n************\nDataQuality checks on Metadata Level begins.......")
    # Expected Data types check in raw data
    desired_column_types = {
        'X': 'float',
        'Y': 'float',
        'profile_id': 'int',
        'profile_layer_id': 'int',
        'country_name': 'object',
        'upper_depth': 'int',
        'lower_depth': 'int',
        'layer_name': 'object',
        'litter': 'float',
        'orgc_value': 'object',
        'orgc_value_avg': 'float',
        'orgc_method': 'object',
        'orgc_date': 'object',
        'orgc_dataset_id': 'object',
        'orgc_profile_code': 'object',
    }

    # Check column data types
    data_type_check_results = dataqualitychecker.check_column_data_types(raw_df, desired_column_types)
    if (data_type_check_results['issue_check_type'] == 'data_type_match').all():
        print("\nAll columns have the expected data types in raw data.\n")
        print(data_type_check_results)
    else:
        print("\nData type mismatches found in:\n")
        print(data_type_check_results[data_type_check_results['issue_check_type'] == 'data_type_mismatch'].reset_index(drop=True))

    # Check for column patterns in special valued column
    column_patterns = {
        'orgc_value': r'\{(?:\d+:\s*[^,]+(?:,\s*)?)+\}',
        'orgc_date': r'\{(?:\d+:\s*[^,]+(?:,\s*)?)+\}',
        'orgc_method': ''  # only check valid dictionary as valid string in orgc_method
    }

    # Match orgc_value, orgc_date, orgc_method column defined patterns
    pattern_check_results = dataqualitychecker.check_column_patterns(raw_df, column_patterns)

    if pattern_check_results.empty:
        print(f"\nColumns {column_patterns.keys()} match the expected patterns.")
    else:
        print("\nPattern mismatches found:")
        print(pattern_check_results)

    print("\n************\nDataQuality checks on Raw Data Level begins.......")

    # Check for duplicate records
    preprocessor.drop_duplicates(raw_df, df_name='raw_extracted_dataframe')

    # DataQuality checks on Data Level

    # Define columns for different checks
    col_to_check_for_outliers = ['orgc_value_avg']
    col_to_check_for_missing_values = ['X', 'Y', 'profile_id', 'profile_layer_id', 'country_name',
                                       'layer_name', 'orgc_dataset_id', 'orgc_profile_code',
                                       'orgc_value', 'orgc_value_avg', 'orgc_date',
                                       'upper_depth', 'lower_depth', 'orgc_method']
    lat_column = 'Y'  # latitude column name
    long_column = 'X'  # longitude column name
    upper_depth_col = 'upper_depth'  # upper depth column name
    lower_depth_col = 'lower_depth'  # lower depth column name

    # Perform outlier check
    outlier_results = dataqualitychecker.check_outliers(raw_df, col_to_check_for_outliers)
    print("\nOutlier Results:\n", outlier_results)

    # Perform missing values check
    missing_values_results = dataqualitychecker.check_missing_values(raw_df, col_to_check_for_missing_values)
    print("\nMissing Values Results:\n", missing_values_results)

    # Perform latitude and longitude check
    lat_long_results = DataQualityChecker.check_lat_long(raw_df, lat_column, long_column)
    if lat_long_results.empty:
        print("\nLatitude and Longitude columns are consistent.")
    else:
        print("\nInconsistencies in Latitude and Longitude columns:\n", lat_long_results)

    # Perform Depth column check
    depth_check_results = dataqualitychecker.check_depth_columns(raw_df, upper_depth_col, lower_depth_col)
    if depth_check_results.empty:
        print("\nDepth columns are consistent.")
    else:
        print("\nInconsistencies in depth columns:\n", depth_check_results)

    # Step 2- Apply Data Preprocessing

    print('\n**************')
    print("Data Preprocessing begins here....\n")
    print(f"Clean data and normalized raw data itself on orgc_method (method string is in dictionary form)")

    new_rows = []
    for _, row in raw_df.iterrows():
        new_rows.extend(extract.extract_raw_data_based_on_method_instance(row))

    df_to_preprocessed = preprocessor.append_preprocessed_rows(new_rows)

    print("Removing duplicates after cleaning and normalization based on orgc_method...")
    df_preprocessed = preprocessor.drop_duplicates(df_to_preprocessed, df_name='preprocessed_dataframe')

    # Reformat dates first
    print("Reformatting 'orgc_date' to standard format for data consistency...")
    desired_date_format = '%Y-%m-%d'
    date_column = 'orgc_date_for_instance'
    df_preprocessed = preprocessor.reformat_dates(df_preprocessed, date_column, desired_date_format)

    # DataQuality checks on reformat date
    date_format_results = DataQualityChecker.check_date_format(df_preprocessed, ['reformat_orgc_date_for_instance'])
    if not date_format_results.empty:
        print('Date Format Results:\n', date_format_results)
    else:
        print(f"Formatted dates results are consistent in desired format {desired_date_format}.")

    print("\nAnalyzing the preprocessed dataframe...")
    review_raw_df = extract.generate_review_dataframes(df_preprocessed)
    for name, df in review_raw_df.items():
        print(f"\n{name}:\n", df, "\n" + "-" * 80)

    # Step 3 - Aply Data Transformation and Normalization
    print("\nData transformation and Normalization step can proceed here...")
    df_normalized_dict = transformer.normalize_dataframes(df_preprocessed)

    print("\nAnalyzing the 3NF normalized dataframes (orgc_method_df, orgc_profile_df, orgc_profile_layer_df...")
    df_dict_review = extract.generate_review_dataframes(df_normalized_dict)
    # Loop through each DataFrame review (since it's a dictionary of dataframes)
    for df_name, review_data in df_dict_review.items():
        print(f"\nReview for {df_name}:")
        for review_name, df in review_data.items():
            print(f"\n{review_name}:\n", df, "\n" + "-" * 80)

    # Step 4 - Apply Data Load into SQLite DB 3 NF schema structure
    try:
        sql_script_file_name = 'initialize_db.sql'
        dataloader.save_to_sqlite(df_normalized_dict, sql_script_file_name, file_name='seqana_soil_data.db')
    except Exception as error:
        print("Load process stopped.")
        raise error


if __name__ == "__main__":
    main()
