-- Creating a warehouse 
CREATE WAREHOUSE YelpDataArchitecture;

-- Creating a database 
CREATE DATABASE Yelp;

-- Creating necessary schemas 
CREATE SCHEMA staging;
CREATE SCHEMA ODS;
CREATE SCHEMA DWS;

-- CSV PART - CLIMATE --

-- Creating a file format for CSV
CREATE OR REPLACE FILE FORMAT csv_format type = csv skip_header = 1 empty_field_as_null = true;

-- Creating a snowflake staging area for CSV file format 
CREATE OR REPLACE STAGE CSV_DATA_STAGE file_format= csv_format;

-- Uploading Climate CSV datasets into the staging 
put file:///home/pouya/Desktop/projects/Data-Architecture-Yelp/datasets/usw00023169-las-vegas-mccarran-intl-ap-precipitation-inch.csv @CSV_DATA_STAGE auto_compress=true;
put file:///home/pouya/Desktop/projects/Data-Architecture-Yelp/datasets/usw00023169-temperature-degreef.csv @CSV_DATA_STAGE auto_compress=true;

-- Create a table for Climate temperature degrees
CREATE TABLE ClimateTemperatureDegrees (date number,min float,max float, normal_min float, normal_max float);
CREATE TABLE ClimatePrecipitation (date number,precipitation varchar,precipitation_normal float);

-- Copying files Climate datasets from CSV staging area to corresponding tables
COPY INTO ClimateTemperatureDegrees FROM @csv_data_stage/usw00023169-temperature-degreef.csv.gz file_format=csv_format; 
COPY INTO ClimatePrecipitation FROM @csv_data_stage/usw00023169-las-vegas-mccarran-intl-ap-precipitation-inch.csv.gz file_format=csv_format;

-- JSON PART - YELP -- 

-- Creating a file format for JSON 
CREATE OR REPLACE FILE FORMAT json_format type = json strip_outer_array=true;

-- Creating a snowflake staging area for JSON file format 
CREATE OR REPLACE STAGE JSON_DATA_STAGE file_format= json_format;

-- Uploading YELP JSON datasets into the staging 
put file:///home/pouya/Desktop/projects/Data-Architecture-Yelp/datasets/x01yelp_academic_dataset_review.json @JSON_DATA_STAGE auto_compress=true;
put file:///home/pouya/Desktop/projects/Data-Architecture-Yelp/datasets/x02yelp_academic_dataset_review.json @JSON_DATA_STAGE auto_compress=true;
put file:///home/pouya/Desktop/projects/Data-Architecture-Yelp/datasets/x03yelp_academic_dataset_review.json @JSON_DATA_STAGE auto_compress=true;
put file:///home/pouya/Desktop/projects/Data-Architecture-Yelp/datasets/x04yelp_academic_dataset_review.json @JSON_DATA_STAGE auto_compress=true;
put file:///home/pouya/Desktop/projects/Data-Architecture-Yelp/datasets/x05yelp_academic_dataset_review.json @JSON_DATA_STAGE auto_compress=true;
put file:///home/pouya/Desktop/projects/Data-Architecture-Yelp/datasets/x06yelp_academic_dataset_review.json @JSON_DATA_STAGE auto_compress=true;
put file:///home/pouya/Desktop/projects/Data-Architecture-Yelp/datasets/x01yelp_academic_dataset_user.json @JSON_DATA_STAGE auto_compress=true;
put file:///home/pouya/Desktop/projects/Data-Architecture-Yelp/datasets/x02yelp_academic_dataset_user.json @JSON_DATA_STAGE auto_compress=true;
put file:///home/pouya/Desktop/projects/Data-Architecture-Yelp/datasets/x03yelp_academic_dataset_user.json @JSON_DATA_STAGE auto_compress=true;
put file:///home/pouya/Desktop/projects/Data-Architecture-Yelp/datasets/x04yelp_academic_dataset_user.json @JSON_DATA_STAGE auto_compress=true;
put file:///home/pouya/Desktop/projects/Data-Architecture-Yelp/datasets/yelp_academic_dataset_business.json @JSON_DATA_STAGE auto_compress=true;
put file:///home/pouya/Desktop/projects/Data-Architecture-Yelp/datasets/yelp_academic_dataset_checkin.json @JSON_DATA_STAGE auto_compress=true;
put file:///home/pouya/Desktop/projects/Data-Architecture-Yelp/datasets/yelp_academic_dataset_covid_features.json @JSON_DATA_STAGE auto_compress=true;
put file:///home/pouya/Desktop/projects/Data-Architecture-Yelp/datasets/yelp_academic_dataset_tip.json @JSON_DATA_STAGE auto_compress=true;


-- Create a table for Yelp customer tips 
CREATE TABLE userTips()