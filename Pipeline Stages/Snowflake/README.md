# Loading and Querying Data using SQL

### General Approach
Our team utilized Snowflake to ingest the transformed data from Databricks in order to perform validation queries and create tables/views to be used in Tableau/ThoughtSpot.
Snowflake was extremely helpful for joining related tables together and allowing us to verify data quality. We also reran some cleaning processes in Databricks after finding new outliers from our SQL queries.
With our data being fully cleaned and transformed, we completed joined views that will be used for data visualization later on in our data pipeline.
### DDL Statements
After uploading all of our transformed data to Snowflake via an external stage, we created four tables to house the data and help us verify data accuracy and quality.
These tables include the fully transformed taxi data, the fully transformed HVFHV data, the license number table, and the taxi zone lookup table. Here are the DDL statements for each table:
#### Taxi DDL Code
```sql
CREATE OR REPLACE TRANSIENT TABLE CAPSTONE_DE.GROUP_1.TAXI_TBL (
    VENDOR_ID NUMBER,
    PU_DATETIME DATETIME,
    DO_DATETIME DATETIME,
    YEAR NUMBER,
    MONTH NUMBER,
    DAY NUMBER,
    DAY_OF_WEEK STRING,
    IS_WEEKEND BOOLEAN,
    TRIP_DISTANCE FLOAT,
    TRIP_DURATION FLOAT,
    PU_LOCATIONID NUMBER,
    DO_LOCATIONID NUMBER,
    FARE_AMOUNT FLOAT,
    TIP_AMOUNT FLOAT,
    TOTAL_AMOUNT FLOAT,
    TRIP_TYPE NUMBER,
    TAXI_TYPE STRING
);
```
#### HVFHV DDL Code
```sql
CREATE OR REPLACE TRANSIENT TABLE CAPSTONE_DE.GROUP_1.HIGH_VOLUME_TBL (
    LICENSE_NUM STRING,
    PU_DATETIME DATETIME,
    DO_DATETIME DATETIME,
    REQUEST_DATETIME DATETIME,
    YEAR NUMBER,
    MONTH NUMBER,
    DAY NUMBER,
    DAY_OF_WEEK STRING,
    IS_WEEKEND BOOLEAN,
    TRIP_DISTANCE FLOAT,
    TRIP_DURATION FLOAT,
    PU_LOCATIONID NUMBER,
    DO_LOCATIONID NUMBER,
    FARE_AMOUNT FLOAT,
    TIP_AMOUNT FLOAT,
    TOTAL_AMOUNT FLOAT,
    DRIVER_PAY FLOAT,
    DISPATCHING_BASE_NUM STRING
);
```
#### License Number DDL Code
```sql
CREATE OR REPLACE TRANSIENT TABLE CAPSTONE_DE.GROUP_1.LICENSE_TBL (
    LICENSE_NUM STRING,
    COMPANY_NAME STRING
);
```
#### Taxi Zone Lookup DDL Code
```sql
CREATE OR REPLACE TRANSIENT TABLE CAPSTONE_DE.GROUP_1.TAXI_ZONE_TBL (
    LOCATION_ID NUMBER,
    BOROUGH STRING,
    ZONE STRING,
    SERVICE_ZONE STRING
);
```
### Validation Queries

