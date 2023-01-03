# What is this project about?

This is a data architecture project aimed at understanding the relationship of weather to cutomser feedbacks. We have collected customer feedback data from Yelp and weather forecasts in order to understand the relationship.

This will be done through Snowflake cloud.

# Requirements of this project

1. Create a data architecture diagram to visualize how you will ingest and migrate the data into Staging, Operational Data Store (ODS), and Data Warehouse environments, so as to ultimately query the data for relationships between weather and Yelp reviews. Save this so it can be included in your final submission.
2. Create a staging environment(schema) in Snowflake.
3. Upload all Yelp and Climate data to the staging environment. (Screenshots 1,2) (see Screenshot description below)
   NOTE: You may need to SPLIT these datasets into several smaller files (< 3 million records per file in YELP)
4. Create an ODS environment(aka schema).
5. Draw an entity-relationship (ER) diagram to visualize the data structure. Save this so it can be included in your final submission.
6. Migrate the data into the ODS environment. (Screenshots 3,4,5,6)
7. Draw a STAR schema for the Data Warehouse environment. Save this so it can be included in your final submission.
8. Migrate the data to the Data Warehouse. (Screenshot 7)
9. Query the Data Warehouse to determine how weather affects Yelp reviews. ( Screenshot 8)
