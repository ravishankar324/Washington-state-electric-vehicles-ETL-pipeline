//Creating Database
CREATE OR REPLACE DATABASE ELECTRIC_VEHICLES_DB;

//Creating vehicles fact table
CREATE OR REPLACE TABLE ELECTRIC_VEHICLES_DB.PUBLIC.VEHICLES(
    vin_number STRING PRIMARY KEY,
    base_msrp INT,
    Legislative_district INT,
    vehicle_id INT,
    vehicle_location STRING,
    census_tract INT,
    vehicletype_id INT,
    location_id INT, 
    electric_utility_id INT);

//Creating location Dimension table
CREATE OR REPLACE TABLE ELECTRIC_VEHICLES_DB.PUBLIC.LOCATION(
    county STRING,
    city STRING,
    state STRING,
    postal_code INT,
    location_id INT PRIMARY KEY
);

//Creating Vehicletype Dimension table
CREATE OR REPLACE TABLE ELECTRIC_VEHICLES_DB.PUBLIC.VEHICLETYPE(
    make STRING,
    model STRING,
    model_year INT,
    vehicle_type STRING,
    CAFV_eligibility STRING,
    electric_range STRING,
    vehicletype_id INT PRIMARY KEY
);

//Creating electric utility dimension table
CREATE OR REPLACE TABLE ELECTRIC_VEHICLES_DB.PUBLIC.ELECTRICUTILITY(
electric_utility STRING,
electric_utility_id INT PRIMARY KEY
);

// Creating Stages for Facts and dimension tables

CREATE OR REPLACE STAGE ELECTRIC_VEHICLES_DB.PUBLIC.vehicle_stage
URL='s3://washigton-electricvehicles-data-analysis-spark-etl/output/vehicle_details/'
STORAGE_INTEGRATION = my_s3_integration
FILE_FORMAT = (TYPE = 'PARQUET');

CREATE OR REPLACE STAGE ELECTRIC_VEHICLES_DB.PUBLIC.location_stage
URL='s3://washigton-electricvehicles-data-analysis-spark-etl/output/location_details/'
STORAGE_INTEGRATION = my_s3_integration
FILE_FORMAT = (TYPE = 'PARQUET');

CREATE OR REPLACE STAGE ELECTRIC_VEHICLES_DB.PUBLIC.vehicletype_stage
URL='s3://washigton-electricvehicles-data-analysis-spark-etl/output/vehicletype_details/'
STORAGE_INTEGRATION = my_s3_integration
FILE_FORMAT = (TYPE = 'PARQUET');

CREATE OR REPLACE STAGE ELECTRIC_VEHICLES_DB.PUBLIC.electricutility_stage
URL='s3://washigton-electricvehicles-data-analysis-spark-etl/output/electricutility_details/'
STORAGE_INTEGRATION = my_s3_integration
FILE_FORMAT = (TYPE = 'PARQUET');


// Copying tables from S3 to facts and dims
COPY INTO VEHICLES
FROM @ELECTRIC_VEHICLES_DB.PUBLIC.vehicle_stage
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\.parquet';

COPY INTO LOCATION
FROM @ELECTRIC_VEHICLES_DB.PUBLIC.location_stage
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\.parquet';

COPY INTO VEHICLETYPE
FROM @ELECTRIC_VEHICLES_DB.PUBLIC.vehicletype_stage
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\.parquet';

COPY INTO ELECTRICUTILITY
FROM @ELECTRIC_VEHICLES_DB.PUBLIC.electricutility_stage
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\.parquet';



