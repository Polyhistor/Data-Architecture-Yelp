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

-- Creating a table for Climate temperature degrees
CREATE TABLE ClimateTemperatureDegrees (date number,min float,max float, normal_min float, normal_max float);

-- Creating a table for Climate preciptations
CREATE TABLE ClimatePrecipitation (date number,precipitation varchar,precipitation_normal float);

-- Copying Climate datasets from CSV staging area to corresponding tables
COPY INTO ClimateTemperatureDegrees FROM @csv_data_stage/usw00023169-temperature-degreef.csv.gz file_format=csv_format; 
COPY INTO ClimatePrecipitation FROM @csv_data_stage/usw00023169-las-vegas-mccarran-intl-ap-precipitation-inch.csv.gz file_format=csv_format;

-- JSON PART - YELP -- 

-- Creating a file format for JSON 
CREATE OR REPLACE FILE FORMAT json_format type = json strip_outer_array=true;

-- Creating a snowflake staging area for JSON file format 
CREATE OR REPLACE STAGE JSON_DATA_STAGE file_format= json_format;

-- Uploading YELP JSON datasets into the staging 
put file:///home/pouya/Desktop/projects/Data-Architecture-Yelp/datasets/yelp_academic_dataset_review.json @JSON_DATA_STAGE auto_compress=true;
put file:///home/pouya/Desktop/projects/Data-Architecture-Yelp/datasets/yelp_academic_dataset_user.json @JSON_DATA_STAGE auto_compress=true;
put file:///home/pouya/Desktop/projects/Data-Architecture-Yelp/datasets/yelp_academic_dataset_business.json @JSON_DATA_STAGE auto_compress=true;
put file:///home/pouya/Desktop/projects/Data-Architecture-Yelp/datasets/yelp_academic_dataset_checkin.json @JSON_DATA_STAGE auto_compress=true;
put file:///home/pouya/Desktop/projects/Data-Architecture-Yelp/datasets/yelp_academic_dataset_covid_features.json @JSON_DATA_STAGE auto_compress=true;
put file:///home/pouya/Desktop/projects/Data-Architecture-Yelp/datasets/yelp_academic_dataset_tip.json @JSON_DATA_STAGE auto_compress=true;


-- Creating a table for Yelp customer tips 
CREATE TABLE userTips(user_id varchar(100), business_id varchar(100), text varchar(500), date TIMESTAMP_NTZ, compliment_count number);

-- Creating a table for Yelp COVID features dataset
CREATE TABLE covidFeatures(business_id varchar(100), highlights varchar(10000), delivery_or_takout boolean, grubhub_enabled boolean, call_to_action_enabled boolean, request_a_quote_enbaled boolean, covid_banner varchar(30000), temporary_closed_Until boolean, virtual_services_offered boolean);

-- Creating a table for Yelp customer check ins
CREATE TABLE checkins(business_id varchar(100), date varchar(10000000));

-- Creating a table for Yelp businesses dataset
CREATE TABLE business(business_id varchar(100), name varchar(500), address varchar(1000), city varchar(500), state varchar(50), postal_code varchar(50), lattitude float, longitude float, stars float, review_count number, is_open number, attributes variant);

-- Creating a table for Yelp customer reviews
CREATE TABLE reviews(review_id varchar(100), user_id varchar(100), business_id varchar(100), stars float,  useful number, funny number, cool number, text varchar(1000000), date TIMESTAMP_NTZ);

-- Creating a table for Yelp users
CREATE TABLE users(user_id varchar(100), name varchar(300), review_count number, yelping_since  TIMESTAMP_NTZ, useful number, funny number, cool number, elite varchar(300), friends varchar(1000000), compliment_hot number, compliment_more number, compliment_profile number, compliment_cute number, compliment_list number, compliment_note number, compliment_plain number, compliment_cool number, compliment_funny number, compliment_writer number, compliment_photos number);

-- Copying Yelp datasets from JSON staging area to corresponding tables
COPY INTO userTips (user_id, business_id, text, date, compliment_count) 
FROM (SELECT $1:user_id::varchar, $1:business_id::varchar, $1:text::varchar, $1:date::TIMESTAMP_LTZ, $1:compliment_count::number from @json_data_stage/yelp_academic_dataset_tip.json.gz t); 

COPY INTO covidFeatures (business_id, highlights, delivery_or_takout, grubhub_enabled, call_to_action_enabled, request_a_quote_enbaled, covid_banner, temporary_closed_Until, virtual_services_offered) 
FROM (SELECT $1:business_id::varchar(100), $1:highlights::varchar(10000), $1:delivery_or_takout::boolean, $1:grubhub_enabled::boolean, $1:call_to_action_enabled::boolean, $1:request_a_quote_enbaled::boolean, $1:covid_banner::varchar(30000), $1:temporary_closed_Until::boolean, $1:virtual_services_offered::boolean from @json_data_stage/yelp_academic_dataset_covid_features.json.gz t);

COPY INTO checkins (business_id, date)
FROM (SELECT $1:business_id::varchar(100), $1:date::varchar(10000000) FROM @json_data_stage/yelp_academic_dataset_checkin.json.gz); 

COPY INTO business (business_id, name, address, city, state, postal_code, lattitude, longitude, stars, review_count, is_open, attributes)
FROM (SELECT $1:business_id::varchar(100), $1:name::varchar(500), $1:address::varchar(1000), $1:city::varchar(500), $1:state::varchar(50), $1:postal_code::varchar(50), $1:lattitude::float, $1:longitude::float, $1:stars::float, $1:review_count::number, $1:is_open::number, $1:attributes::variant FROM @json_data_stage/yelp_academic_dataset_business.json.gz);

COPY INTO reviews (review_id, user_id, business_id, stars, useful, funny, cool, text, date)
FROM (SELECT $1:review_id::varchar(100), $1:user_id::varchar(100), $1:business_id::varchar(100), $1:stars::float, $1:useful::number, $1:funny::number, $1:cool::number, $1:text::varchar(1000000), $1:date::TIMESTAMP_NTZ FROM @json_data_stage/yelp_academic_dataset_review.json.gz);

COPY INTO users (user_id, name, review_count, yelping_since, useful, funny, cool, elite, friends, compliment_hot, compliment_more, compliment_profile, compliment_cute, compliment_list, compliment_note, compliment_plain, compliment_cool, compliment_funny, compliment_writer, compliment_photos)
FROM (SELECT $1:user_id::varchar(100), $1:name::varchar(300), $1:review_count::number, $1:yelping_since::TIMESTAMP_NTZ, $1:useful::number, $1:funny::number, $1:cool::number, $1:elite::varchar(300), $1:friends::varchar(1000000), $1:compliment_hot::number, $1:compliment_more::number, $1:compliment_profile::number, $1:compliment_cute::number, $1:compliment_list::number, $1:compliment_note::number, $1:compliment_plain::number, $1:compliment_cool::number, $1:compliment_funny::number, $1:compliment_writer::number, $1:compliment_photos::number FROM @json_data_stage/yelp_academic_dataset_user.json.gz);


