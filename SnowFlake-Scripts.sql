-- Creating a warehouse 
CREATE WAREHOUSE YelpDataArchitecture;

-- Creating a database 
CREATE DATABASE Yelp;

-- Creating necessary schemas 
CREATE SCHEMA staging;
CREATE SCHEMA ODS;
CREATE SCHEMA DWS;

-- Creating a file format for CSV
CREATE OR REPLACE FILE FORMAT csv_format type = csv skip_header = 1 empty_field_as_null = true;

-- Creating a snowflake staging area for CSV file format 
CREATE OR REPLACE STAGE CSV_DATA_STAGE file_format= csv_format;

-- Uploading Climate CSV datasets into the staging 
put file:///home/pouya/Desktop/projects/Data-Architecture-Yelp/datasets/usw00023169-las-vegas-mccarran-intl-ap-precipitation-inch.csv @CSV_DATA_STAGE auto_compress=true;

put file:///home/pouya/Desktop/projects/Data-Architecture-Yelp/datasets/usw00023169-temperature-degreef.csv @CSV_DATA_STAGE auto_compress=true;

-- Create a table for Climate temperature degrees
CREATE TABLE ClimateTemperatureDegrees (date number,min float,max float, normal_min float, normal_max float);

CREATE TABLE ClimatePrecipitation (date number,precipitation float,precipitation_normal float);

-- Copying files Climate datasets from CSV staging area to corresponding tables
COPY INTO ClimateTemperatureDegrees FROM @csv_data_stage/usw00023169-temperature-degreef.csv.gz file_format=csv_format; 