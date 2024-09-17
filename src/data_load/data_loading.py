# src/data_load/data_loading.py
import os
import re
import sqlite3

import pandas as pd


class DataLoading:

    @staticmethod
    def initialize_database(script_file_name, conn):
        """
        Initializes the SQLite database schema by executing the SQL script.
        """
        try:
            project_directory = os.getcwd()
            sql_script_path = os.path.join(project_directory, script_file_name)

            with open(sql_script_path, 'r') as file:
                sql_script = file.read()

            cursor = conn.cursor()
            cursor.executescript(sql_script)
            conn.commit()
            print("\nDatabase schema initialized successfully.")
            return sql_script

        except FileNotFoundError as fnf_error:
            print(f"\nSQL script file not found: {fnf_error}")
            raise
        except sqlite3.Error as db_error:
            print(f"\nError initializing database schema: {db_error}")
            raise
        except Exception as e:
            print(f"\nUnexpected error initializing database schema: {e}")
            raise

    @staticmethod
    def parse_sql_schema(sql_script):
        """
        Parses the SQL script to extract table schema information.
        """
        try:
            schema = {}
            create_table_regex = re.compile(r"CREATE TABLE IF NOT EXISTS (\w+)\s*\((.*?)\);", re.S)
            foreign_key_regex = re.compile(r"FOREIGN KEY\s*\((\w+)\)\s*REFERENCES\s*(\w+)\((\w+)\)")
            primary_key_regex = re.compile(r"PRIMARY KEY\s*\((.*?)\)")

            tables = create_table_regex.findall(sql_script)

            for table_name, table_definition in tables:
                columns = {}
                primary_keys = []
                foreign_keys = []

                for line in table_definition.splitlines():
                    line = line.strip().strip(",")
                    if line.startswith("FOREIGN KEY"):
                        foreign_key_match = foreign_key_regex.search(line)
                        if foreign_key_match:
                            fk_column, ref_table, ref_column = foreign_key_match.groups()
                            foreign_keys.append({
                                "column": fk_column,
                                "ref_table": ref_table,
                                "ref_column": ref_column
                            })
                    elif line.startswith("PRIMARY KEY"):
                        pk_match = primary_key_regex.search(line)
                        if pk_match:
                            primary_keys = pk_match.group(1).split(", ")
                    else:
                        parts = line.split()
                        if len(parts) >= 2:
                            column_name, column_type = parts[0], parts[1]
                            columns[column_name] = column_type

                schema[table_name] = {
                    "columns": columns,
                    "primary_keys": primary_keys,
                    "foreign_keys": foreign_keys
                }

            print("\nSQL schema parsed successfully.")
            return schema

        except Exception as e:
            print(f"\nError parsing SQL schema: {e}")
            raise

    @staticmethod
    def insert_dataframes_to_db(dataframe_dict, conn):
        """
        Insert DataFrames into the SQLite database.
        """
        try:
            for table_name, df in dataframe_dict.items():
                table_name = table_name.replace('_df', '')
                df.to_sql(table_name, conn, if_exists='replace', index=False)
                print(f"\nData inserted into table '{table_name}' successfully with {len(df)} rows.")
        except sqlite3.Error as db_error:
            print(f"\nError inserting data into database: {db_error}")
            raise
        except Exception as e:
            print(f"\nUnexpected error inserting data into tables: {e}")
            raise

    @staticmethod
    def check_column_type(series, expected_type):
        """
        Check if the DataFrame column matches the expected SQL data type.
        """
        try:
            expected_type = expected_type.upper()

            if expected_type == "INTEGER":
                return pd.api.types.is_integer_dtype(series)
            elif expected_type == "FLOAT" or expected_type == "REAL":
                return pd.api.types.is_float_dtype(series)
            elif expected_type == "TEXT":
                return pd.api.types.is_object_dtype(series)
            elif expected_type == "DATETIME":
                return pd.api.types.is_datetime64_any_dtype(series)
            return False
        except Exception as e:
            print(f"\nError checking column type: {e}")
            raise

    @staticmethod
    def correct_column_types(df: pd.DataFrame, schema: dict, df_table_name: str) -> pd.DataFrame:
        """
        Correct the data types of the DataFrame columns to match the SQL schema.
        """
        try:
            corrected_df = df.copy()
            print(f"\nDatatype validation and correction in '{df_table_name}' based on Schema before data insertion.\n")
            for column_name, expected_type in schema["columns"].items():
                if column_name in corrected_df.columns:
                    actual_type = str(corrected_df[column_name].dtype).upper()

                    if not DataLoading.check_column_type(corrected_df[column_name], expected_type):
                        print(f"\nCorrecting column '{column_name}' from type '{actual_type}' to '{expected_type}'")

                        if expected_type == "INTEGER":
                            corrected_df[column_name] = pd.to_numeric(corrected_df[column_name], errors='coerce',
                                                                      downcast='integer')
                        elif expected_type == "FLOAT" or expected_type == "REAL":
                            corrected_df[column_name] = pd.to_numeric(corrected_df[column_name], errors='coerce',
                                                                      downcast='float')
                        elif expected_type == "TEXT":
                            corrected_df[column_name] = corrected_df[column_name].astype('str')
                        elif expected_type == "DATETIME":
                            corrected_df[column_name] = pd.to_datetime(corrected_df[column_name], format='%Y-%m-%d',
                                                                       errors='coerce')

                        new_type = str(corrected_df[column_name].dtype).upper()
                        print(f"Column '{column_name}' has been corrected to type '{new_type}'")
                    else:
                        print(f"Column '{column_name}' already matches the expected type '{expected_type}'.")

            print("\nColumn types corrected successfully.")
            return corrected_df

        except Exception as e:
            print(f"\nError correcting column types: {e}")
            raise

    @staticmethod
    def validate_and_correct_dataframe(schema, df_dict):
        """
        Validate and correct the DataFrames against the SQL schema.
        """
        try:
            for table_name, table_schema in schema.items():
                df_table_name = f"{table_name}_df"

                if df_table_name not in df_dict:
                    raise ValueError(f"\nTable '{table_name}' expected in the DataFrame dictionary but not found.")

                df = df_dict[df_table_name]
                df_columns = set(df.columns)

                schema_columns = set(table_schema["columns"].keys())
                extra_columns = df_columns - schema_columns
                if extra_columns:
                    raise ValueError(
                        f"\nDataFrame for table '{table_name}' has extra columns not in SQL schema: {extra_columns}")

                missing_columns = schema_columns - df_columns
                if missing_columns:
                    raise ValueError(
                        f"\nDataFrame for table '{table_name}' is missing columns from SQL schema: {missing_columns}")

                df_corrected = DataLoading.correct_column_types(df, table_schema, df_table_name)
                df_dict[df_table_name] = df_corrected

                for pk in table_schema["primary_keys"]:
                    if pk not in df_columns:
                        raise ValueError(f"\nPrimary key '{pk}' is missing in DataFrame for table '{table_name}'.")

                for fk in table_schema["foreign_keys"]:
                    if fk["column"] not in df_columns:
                        raise ValueError(
                            f"\nForeign key column '{fk['column']}' is missing in DataFrame for table '{table_name}'.")

            print("\nDataFrames validated and corrected successfully.")
        except Exception as e:
            print(f"\nError validating or correcting DataFrames: {e}")
            raise

    @staticmethod
    def save_to_sqlite(dataframe_dict, sql_script_name, file_name='seqana_soil_data.db'):
        """
        Saves the normalized data (from DataFrames) into SQLite database.
        """
        conn = None  # Initialize conn to None to avoid 'referenced before assignment' issues
        try:
            directory = os.getcwd()
            dataset_directory = os.path.join(directory, 'src')
            db_file_name = os.path.join(dataset_directory, file_name)

            conn = sqlite3.connect(db_file_name)
            print(f"\nConnected to SQLite database at {db_file_name}")

            # Ensure sqlite consider foreign keys checks for the session
            conn.execute('PRAGMA foreign_keys = ON;')

            print("\nInitializing database schema...")
            sql_script = DataLoading.initialize_database(sql_script_name, conn)

            print("\nParsing SQL schema...")
            schema = DataLoading.parse_sql_schema(sql_script)

            print("\nValidating and Correcting DataFrames against SQL schema...")
            DataLoading.validate_and_correct_dataframe(schema, dataframe_dict)

            print("\nInserting data into database tables...")
            DataLoading.insert_dataframes_to_db(dataframe_dict, conn)

            conn.commit()
            print("\nData committed successfully.")

        except sqlite3.Error as db_error:
            print(f"\nSQLite Error: {db_error}")
            if conn:
                conn.rollback()
        except Exception as e:
            print(f"\nUnexpected error: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()
                print(f"\nSQLite connection closed.")
