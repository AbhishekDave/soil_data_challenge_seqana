CREATE TABLE IF NOT EXISTS orgc_method (
    id INTEGER PRIMARY KEY,
    method_instance INTEGER,
    calculation TEXT,
    detection TEXT,
    reaction TEXT,
    sample_pretreatment TEXT,
    spectral TEXT,
    temperature TEXT,
    treatment TEXT
);

CREATE TABLE IF NOT EXISTS orgc_profile (
    id INTEGER PRIMARY KEY,
    profile_id INTEGER,
    orgc_profile_code TEXT,
    orgc_dataset_id TEXT,
    latitude REAL,
    longitude REAL,
    country_name TEXT
);

CREATE TABLE IF NOT EXISTS orgc_profile_layer (
    id INTEGER PRIMARY KEY,
    profile_layer_id INTEGER,
    orgc_profile_id INTEGER,
    upper_depth INTEGER,
    lower_depth INTEGER,
    layer_name TEXT,
    litter REAL,
    orgc_method_id INTEGER,
    orgc_value REAL,
    orgc_value_avg REAL,
    orgc_date DATETIME,
    FOREIGN KEY(orgc_profile_id) REFERENCES orgc_profile(id),
    FOREIGN KEY(orgc_method_id) REFERENCES orgc_method(id)
);
