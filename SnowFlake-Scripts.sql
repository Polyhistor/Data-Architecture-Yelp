--  ############# --
--  ## STAGING ## --  
--  ############# --

-- Creating a warehouse 
CREATE WAREHOUSE YelpDataArchitecture;

-- Creating a database 
CREATE DATABASE Yelp;

-- Creating a schema
CREATE SCHEMA staging;

-- ### CSV PART - CLIMATE ### --

-- Creating a file format for CSV
CREATE OR REPLACE FILE FORMAT csv_format type = csv skip_header = 1 empty_field_as_null = true;

-- Creating a snowflake staging area for CSV file format 
CREATE OR REPLACE STAGE CSV_DATA_STAGE file_format= csv_format;

-- Uploading Climate CSV datasets into the staging 
put file:///home/pouya/Desktop/projects/Data-Architecture-Yelp/datasets/usw00023169-las-vegas-mccarran-intl-ap-precipitation-inch.csv @CSV_DATA_STAGE auto_compress=true;
put file:///home/pouya/Desktop/projects/Data-Architecture-Yelp/datasets/usw00023169-temperature-degreef.csv @CSV_DATA_STAGE auto_compress=true;

-- Creating a table for Climate temperature degrees
CREATE TABLE ClimateTemperatureDegrees (date varchar,min float,max float, normal_min float, normal_max float);

-- Creating a table for Climate preciptations
CREATE TABLE ClimatePrecipitation (date varchar,precipitation varchar,precipitation_normal float);

-- Copying Climate datasets from CSV staging area to corresponding tables
COPY INTO ClimateTemperatureDegrees FROM @csv_data_stage/usw00023169-temperature-degreef.csv.gz file_format=csv_format; 
COPY INTO ClimatePrecipitation FROM @csv_data_stage/usw00023169-las-vegas-mccarran-intl-ap-precipitation-inch.csv.gz file_format=csv_format;

-- ### JSON PART - YELP ### -- 

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
CREATE TABLE userTips(usertip variant);
CREATE TABLE covidFeatures(covidFeature variant);
CREATE TABLE checkins(checkin variant);
CREATE TABLE business(business variant);
CREATE TABLE reviews(review variant);
CREATE TABLE users(user variant);

-- Copying Yelp datasets from JSON staging area to corresponding tables
COPY INTO userTips from @json_data_stage/yelp_academic_dataset_tip.json.gz file_format=json_format;
COPY INTO covidFeatures from @json_data_stage/yelp_academic_dataset_covid_features.json.gz file_format=json_format;
COPY INTO checkins from @json_data_stage/yelp_academic_dataset_checkin.json.gz file_format=json_format;
COPY INTO business from @json_data_stage/yelp_academic_dataset_business.json.gz file_format=json_format;
COPY INTO reviews from @json_data_stage/yelp_academic_dataset_review.json.gz file_format=json_format;
COPY INTO users from @json_data_stage/yelp_academic_dataset_user.json.gz file_format=json_format;

--  ############# --
--  ## ODS ## --  
--  ############# --

-- Creating a schema
CREATE SCHEMA ODS;

-- ### CSV PART - CLIMATE ### --

-- Creating a table for Climate temperature degrees
CREATE TABLE ClimateTemperatureDegrees (
    date date,
    min float,
    max float,
    normal_min float, 
    normal_max float,
        
    constraint pk primary key (date)
);

INSERT INTO ClimateTemperatureDegrees
SELECT to_date(date,'YYYYMMDD'), min, max, normal_min,normal_max
FROM yelp.staging.ClimateTemperatureDegrees;

-- Creating a table for Climate preciptations
CREATE TABLE ClimatePrecipitation (
    date date,
    precipitation varchar,
    precipitation_normal float,
    
    constraint pk primary key (date)
);

INSERT INTO ClimatePrecipitation
SELECT to_date(date,'YYYYMMDD'), precipitation, precipitation_normal 
FROM yelp.staging.ClimatePrecipitation;

-- ### JSON PART - YELP ### -- 

-- Creating a table for Yelp customer tips 
CREATE TABLE userTips(
    user_id varchar(100),
    business_id varchar(1000), 
    text varchar(500), 
    date date, 
    compliment_count number,

    constraint pk primary key (text, date),    
    constraint fk_user_id foreign key (user_id) references users(user_id),
    constraint fk_business_id foreign key (business_id) references business(business_id)
);

-- Creating a table for Yelp COVID features dataset
CREATE TABLE covidFeatures(
    business_id varchar(1000),
    highlights varchar(10000), 
    delivery_or_takout boolean, 
    grubhub_enabled boolean, 
    call_to_action_enabled boolean, 
    request_a_quote_enbaled boolean, 
    covid_banner varchar(30000), 
    temporary_closed_Until varchar(500), 
    virtual_services_offered varchar(500),

    constraint pk primary key (business_id,highlights),
    constraint fk foreign key (business_id) references business(business_id)
);

-- Creating a table for Yelp customer check ins
CREATE TABLE checkins(
    business_id varchar(1000), 
    date varchar(10000000),

    constraint pk primary key (business_id,date),
    constraint fk foreign key (business_id) references business(business_id)
);

-- Creating a table for Yelp businesses dataset
CREATE TABLE business(
    business_id varchar(1000),
    name varchar(500), 
    address varchar(1000), 
    city varchar(500), 
    state varchar(50), 
    postal_code varchar(100), 
    lattitude float, 
    longitude float, 
    stars float, 
    review_count number, 
    is_open number, 
    attributes variant, 
    hours variant, 
    categories varchar(100000),
    
    constraint pk primary key (business_id)
);

-- Creating a table for Yelp customer reviews
CREATE TABLE reviews(
    review_id varchar(100), 
    user_id varchar(100),
    business_id varchar(1000), 
    stars float,  
    useful number, 
    funny number, 
    cool number, 
    text varchar(1000000), 
    date date,

    constraint pk primary key (review_id),    
    constraint fk_user_id foreign key (user_id) references users(user_id),
    constraint fk_business_id foreign key (business_id) references business(business_id)
);

-- Creating a table for Yelp users
CREATE TABLE users(
    user_id varchar(100), 
    name varchar(300), 
    review_count number, 
    yelping_since  TIMESTAMP_NTZ, 
    useful number, 
    funny number, 
    cool number, 
    elite varchar(300), 
    friends varchar(1000000), 
    fans number, 
    average_stars float, 
    compliment_hot number, 
    compliment_more number, 
    compliment_profile number, 
    compliment_cute number, 
    compliment_list number, 
    compliment_note number, 
    compliment_plain number, 
    compliment_cool number, 
    compliment_funny number, 
    compliment_writer number, 
    compliment_photos number,
    
    constraint pk primary key(user_id)
    );


-- Copying Yelp datasets from JSON staging area to corresponding tables
INSERT INTO userTips 
SELECT usertip:user_id, usertip:business_id, usertip:text, usertip:date, usertip:compliment_count FROM yelp.staging.usertips;

-- COPY INTO userTips (user_id, business_id, text, date, compliment_count) 
-- FROM (SELECT $1:user_id::varchar, $1:business_id::varchar, $1:text::varchar, $1:date::TIMESTAMP_LTZ, $1:compliment_count::number from @json_data_stage/yelp_academic_dataset_tip.json.gz t); 

INSERT INTO covidFeatures 
SELECT covidFeature:business_id, covidFeature:highlights, covidFeature:"delivery or takeout", covidFeature:"Grubhub enabled", covidFeature:"Call To Action enabled", covidFeature:"Request a Quote Enabled", covidFeature:"Covid Banner", covidFeature:"Temporary Closed Until", covidFeature:"Virtual Services Offered" 
FROM yelp.staging.covidFeatures;

-- COPY INTO covidFeatures (business_id, highlights, delivery_or_takout, grubhub_enabled, call_to_action_enabled, request_a_quote_enbaled, covid_banner, temporary_closed_Until, virtual_services_offered) 
-- FROM (SELECT $1:business_id::varchar(100), $1:highlights::varchar(10000), $1:delivery_or_takout::boolean, $1:grubhub_enabled::boolean, $1:call_to_action_enabled::boolean, $1:request_a_quote_enbaled::boolean, $1:covid_banner::varchar(30000), $1:temporary_closed_Until::boolean, $1:virtual_services_offered::boolean from @json_data_stage/yelp_academic_dataset_covid_features.json.gz t);

INSERT INTO checkins 
SELECT checkin:business_id, checkin:date 
FROM yelp.staging.checkins;

-- COPY INTO checkins (business_id, date)
-- FROM (SELECT $1:business_id::varchar(100), $1:date::varchar(10000000) FROM @json_data_stage/yelp_academic_dataset_checkin.json.gz); 

INSERT INTO business 
SELECT business:business_id, business:name, business:address, business:city, business:state, business:postal_code, business:latitude, business:longitude, business:stars, business:review_count, business:is_open, business:attributes, business:hours, business:categories FROM yelp.staging.business;

-- COPY INTO business (business_id, name, address, city, state, postal_code, lattitude, longitude, stars, review_count, is_open, attributes)
-- FROM (SELECT $1:business_id::varchar(100), $1:name::varchar(500), $1:address::varchar(1000), $1:city::varchar(500), $1:state::varchar(50), $1:postal_code::varchar(50), $1:lattitude::float, $1:longitude::float, $1:stars::float, $1:review_count::number, $1:is_open::number, $1:attributes::variant FROM @json_data_stage/yelp_academic_dataset_business.json.gz);

INSERT INTO reviews 
SELECT review:review_id, review:user_id, review:business_id, review:stars, review:useful, review:funny, review:cool, review:text, review:date FROM yelp.staging.reviews;

-- COPY INTO reviews (review_id, user_id, business_id, stars, useful, funny, cool, text, date)
-- FROM (SELECT $1:review_id::varchar(100), $1:user_id::varchar(100), $1:business_id::varchar(100), $1:stars::float, $1:useful::number, $1:funny::number, $1:cool::number, $1:text::varchar(1000000), $1:date::TIMESTAMP_NTZ FROM @json_data_stage/yelp_academic_dataset_review.json.gz);

INSERT INTO users 
SELECT user:user_id, user:name, user:review_count, user:yelping_since, user:useful, user:funny, user:cool, user:elite, user:friends, user:fans, user:average_stars, user:compliment_hot, user:compliment_more, user:compliment_profile, user:compliment_cute, user:compliment_list, user:compliment_note, user:compliment_plain, user:compliment_cool, user:compliment_funny, user:compliment_writer, user:compliment_photos FROM yelp.staging.users;


-- COPY INTO users (user_id, name, review_count, yelping_since, useful, funny, cool, elite, friends, compliment_hot, compliment_more, compliment_profile, compliment_cute, compliment_list, compliment_note, compliment_plain, compliment_cool, compliment_funny, compliment_writer, compliment_photos)
-- FROM (SELECT $1:user_id::varchar(100), $1:name::varchar(300), $1:review_count::number, $1:yelping_since::TIMESTAMP_NTZ, $1:useful::number, $1:funny::number, $1:cool::number, $1:elite::varchar(300), $1:friends::varchar(1000000), $1:fans::number, $1:compliment_hot::number, $1:compliment_more::number, $1:compliment_profile::number, $1:compliment_cute::number, $1:compliment_list::number, $1:compliment_note::number, $1:compliment_plain::number, $1:compliment_cool::number, $1:compliment_funny::number, $1:compliment_writer::number, $1:compliment_photos::number FROM @json_data_stage/yelp_academic_dataset_user.json.gz);

-- Displaying relationship between Climate and Yelp data
SELECT * FROM reviews AS r  
JOIN ClimatePrecipitation AS ct 
ON TO_CHAR(TO_TIMESTAMP_NTZ(ct.date), 'YYYY-MM-DD')  = TO_CHAR(r.date, 'YYYY-MM-DD');

--  ############# --
--  ## DWS ## --  
--  ############# --

CREATE SCHEMA DWS;

-- Creating necessary tables
CREATE TABLE DimClimate (
    date date, 
    precipitation varchar,
    precipitation_normal float, 
    min float, 
    max float, 
    normal_min float, 
    normal_max float,

    constraint pk primary key (date)
)

CREATE TABLE DimUserTips(
user_id varchar(100), 
business_id varchar(1000), 
text varchar(500), 
date TIMESTAMP_NTZ, 
compliment_count number,

constraint pk primary key (user_id, business_id)
);

CREATE TABLE DimCovidFeatures(
business_id varchar(1000), 
highlights varchar(10000), 
delivery_or_takout boolean, 
grubhub_enabled boolean, 
call_to_action_enabled boolean, 
request_a_quote_enbaled boolean, 
covid_banner varchar(30000), 
temporary_closed_Until varchar(500), 
virtual_services_offered varchar(500),

constraint pk primary key (business_id)
);

CREATE TABLE DimCheckin(
business_id varchar(1000), 
date varchar(10000000),

constraint pk primary key (business_id)
);

CREATE TABLE DimBusiness(
business_id varchar(1000),
name varchar(500), 
address varchar(1000), 
city varchar(500), 
state varchar(50), 
postal_code varchar(100), 
lattitude float, 
longitude float, 
stars float, 
review_count number, 
is_open number, 
attributes variant, 
hours variant, 
categories varchar(100000),

constraint pk primary key (business_id)
);

CREATE TABLE DimReviews(
review_id varchar(100), 
user_id varchar(100), 
business_id varchar(100), 
stars float, 
useful number, 
funny number, 
cool number, 
text varchar(1000000), 
date TIMESTAMP_NTZ,

constraint pk primary key (review_id)
);

CREATE TABLE DimUsers(
user_id varchar(100), 
name varchar(300),
review_count number,
yelping_since  TIMESTAMP_NTZ,
useful number,
funny number,
cool number,
elite varchar(300),
friends varchar(1000000),
fans number,
average_stars float,
compliment_hot number,
compliment_more number,
compliment_profile number,
compliment_cute number,
compliment_list number,
compliment_note number,
compliment_plain number,
compliment_cool number,
compliment_funny number,
compliment_writer number,
compliment_photos number,

constraint pk primary key (user_id)
);

CREATE TABLE FactTable_Review (
 user_id varchar(100), 
 business_id varchar(1000), 
 review_id varchar(100),
 date date,

 constraint fk_user foreign key (user_id) references yelp.dws.DimUsers(user_id),
 constraint fk_business foreign key (business_id) references yelp.dws.DimBusiness(business_id),  
 constraint fk_user_tip foreign key (user_id, business_id) references yelp.dws.DimUserTips(user_id, business_id),  
 constraint fk_covid_feature foreign key (business_id) references yelp.dws.DimCovidFeatures(business_id),  
 constraint fk_checkin foreign key (business_id) references yelp.dws.DimCheckin(business_id), 
 constraint fk_review foreign key (review_id) references yelp.dws.DimReviews(review_id), 
 constraint fk_climate foreign key (date) references yelp.dws.DimClimate(date)
);


-- Moving data from ODS to DWS 
INSERT INTO DimClimate 
SELECT ct.date, ct.min, ct.max, ct.normal_min, ct.normal_max, cp.precipitation, cp.precipitation_normal
FROM yelp.ods.CLIMATETEMPERATUREDEGREES AS ct 
JOIN yelp.ods.ClimatePrecipitation AS cp
ON ct.date = cp.date;

-- INSERT INTO DimClimateTemperatureDegrees 
-- SELECT date, min, max, normal_min, normal_max 
-- FROM yelp.ods.CLIMATETEMPERATUREDEGREES;

-- INSERT INTO DimClimatePrecipitation 
-- SELECT date, precipitation, precipitation_normal
-- FROM yelp.ods.ClimatePrecipitation;

INSERT INTO DimUserTips 
SELECT user_id, business_id, text, date, COMPLIMENT_COUNT
FROM yelp.ods.usertips;

INSERT INTO DimCovidFeatures 
SELECT business_id, highlights, delivery_or_takout, grubhub_enabled, call_to_action_enabled, request_a_quote_enbaled, covid_banner, temporary_closed_Until, virtual_services_offered 
FROM yelp.ods.covidFeatures;

INSERT INTO DimCheckin 
SELECT business_id, date
FROM yelp.ods.checkins;

INSERT INTO DimBusiness 
SELECT business_id, name, address, city, state, postal_code, lattitude, longitude, stars, review_count, is_open, attributes, hours, categories   
FROM yelp.ods.business;

INSERT INTO DimReviews 
SELECT review_id, user_id, business_id, stars, useful, funny, cool, text, date
FROM yelp.ods.reviews;

INSERT INTO DimUsers 
SELECT user_id, name, review_count, yelping_since, useful, funny, cool, elite, friends, fans, average_stars, compliment_hot, compliment_more, compliment_profile, compliment_cute, compliment_list, compliment_note, compliment_plain, compliment_cool, compliment_funny, compliment_writer, compliment_photos  
FROM yelp.ods.users; 

INSERT INTO FactTable_Review
SELECT u.user_id, b.business_id, r.review_id, cp.date 
FROM DimReviews AS r
JOIN DimUsers AS u 
ON r.user_id = u.user_id
JOIN DimBusiness AS b 
ON r.business_id = b.business_id
JOIN DimClimate AS cp 
ON r.date = cp.date;

-- SQL generated report that clearly includes business name, temperature, precipitation, and ratings.
SELECT b.name as business_name, t.min AS minimum_temperature, t.max AS maximum_temperature, cp.precipitation, r.text 
FROM FactTable_Review AS fr 
JOIN DimBusiness AS b 
ON fr.business_id = b.business_id
JOIN DimClimateTemperatureDegrees AS t
ON fr.date = t.date 
JOIN DimClimate AS cp 
ON fr.date = cp.date 
JOIN DimReviews as r 
ON fr.review_id = r.review_id;
