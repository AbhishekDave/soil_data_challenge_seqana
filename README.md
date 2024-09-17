# Seqana Data Engineering Challenge

## Overview

This project is designed to handle and process data for the Seqana Data Engineering Challenge. It includes the extraction, preprocessing, transformation, and loading of data from an Excel file into a SQLite database with a 3NF schema.

## Project Structure

- `data_extract/`: Contains scripts for data extraction.
- `src/data_preprocess_transform/`: Contains scripts for data quality checking, preprocessing, transformation, and normalization.
- `src/data_load/`: Contains scripts for data loading into SQLite.
- `setup.sh`: Script to set up the virtual environment and install dependencies.
- `main.py`: Main script to execute the data processing pipeline.

## Setup
### PRE-REQUISITES

Before git clone or running the script, ensure you have the following:

1. **Python**: Ensure Python is installed and added to your system's PATH.
    ####
2. **Virtual Environment**: Python's virtual environment tool should be available. This can be installed via:

   - `pip install virtualenv`
    ####
3. **Clone the Repository**

   - `git clone <repository-url>`
   - `cd <repository-directory>`
******
## Data Processing Pipeline

The data processing is handled in the `main.py` script and follows these steps:

### Data Extraction

- **Extracts raw data** from the Excel file `seqana-data-engineering-challenge-data-wosis-belgium.xlsx`.
- - **Check and Removes duplicates** after Extraction.
- **Performs initial data quality checks** on the raw data.

### Data Quality Checks

- **Metadata Level**: Checks column data types for predefined sets and actual from data file and patterns in special columns `orgc_date`,`orgc_value`,`orgc_method`.
- **Raw Data Level**: Checks for duplicate records, outliers, missing values, and validates latitude, longitude, and depth columns.

### Data Preprocessing

- **Cleans and normalizes** the raw data for each orgc_method instances and extract orgc_values and orgc_dates for each instance.
- **Reformats dates** to a standard format %Y-%d-%m (2024-09-17).
- **Check and Removes duplicates** after preprocessing.

### Data Transformation and Normalization

- **Transforms and normalizes** the preprocessed data into a 3NF schema.

### Data Loading

- **Loads the normalized data** into a SQLite database 
  - using the schema defined in `initialize_db.sql` in 3NF structure.
    - orgc_method: (`id`,`method_instance`,`calculation`,`detection`,`reaction`,`sample_pretreatment`,`spectral`,`temperature`, `treatment`)
    - orgc_profile: (`id`,`profile_id`,`orgc_profile_code`,`orgc_dataset_id`,`latitude`,`longitude`,`country_name`)
    - orgc_profile_layer: (`id`,`profile_layer_id`,`orgc_profile_id`,`upper_depth`,`lower_depth`,`layer_name`,`litter`,`orgc_method_id`, `orgc_value`, `orgc_value_avg`, `orgc_date`)
  ####
  - A file (`seqana_soil_data.db`) created after loading finishes and will be available at `<repository-directory>/src/seqana_soil_data.db`to import in your sqlite database.
***************
## Dependencies

The project relies on the following Python packages:

- `pandas`
- `numpy`
- `openpyxl`
- `sqlite3`
- `regex`

Ensure these dependencies are listed in `requirements.txt` to be installed during the setup process.
**************
## Running the Script 
### Automatic run through setup.sh Script 
1. **Run **`<repository-directory>/setup.sh`** from bash tool to perform ETL operation.**
    ####
    This script performs the following steps:

   - Creates a virtual environment if it does not already exist.
   - Activates the virtual environment.
   - Installs dependencies from requirements.txt.
   - Checks and updates pip to the latest version.
   - Sets the PYTHONPATH to include the project root.
   - Runs the main.py script.
   - Deactivates the virtual environment.

### 
2. **Run Manually via terminal or cmd**

If you prefer to run the script manually, follow these steps:

### - Activate the Virtual Environment
`<repository-directory>/venv/bin/activate  # On Linux or macOS
<repository-directory>/venv/Scripts/activate  # On Windows`
### - Install Dependencies
`pip install -r requirements.txt`
### - Run the Main Script
`python src/main.py`
### - Deactivate the Virtual Environment
`deactivate`
