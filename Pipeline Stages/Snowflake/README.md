# Loading and Querying Data using SQL

### <ins> General Approach </ins>
Our team utilized Snowflake to ingest the transformed data from Databricks in order to perform validation queries and create tables/views to be used in Tableau/ThoughtSpot.
Snowflake was extremely helpful for joining related tables together and allowing us to verify data quality. We also re-ran some cleaning processes in Databricks after finding new outliers from our SQL queries.
With our data being fully cleaned and transformed, we created joined views that will be used for data visualization later on in our data pipeline.
### DDL Statements
After uploading all of our transformed data to Snowflake via an external stage, we created five tables to house the data and help us verify data accuracy and quality.
These tables include the fully transformed taxi data, the fully transformed HVFHV data, the license number table, the taxi zone lookup table, and the external NYC landmark table. We also created two outlier tables for HVFHV and Taxi so other data/business teams can look into potential data entry issues. Here are the DDL statements for each table:

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
#### NYC Landmark DDL Code
```sql
CREATE OR REPLACE TRANSIENT TABLE CAPSTONE_DE.GROUP_1.LANDMARKS_TBL (
    LANDMARK_NAME STRING,
    LANDMARK_ALTERNATE_NAME STRING,
    BUILDTYPE STRING,
    USE_ORIG STRING,
    USE_SECOND STRING,
    USE_TERTIA STRING,
    BOROUGH STRING,
    NEIGHBORHOOD STRING,
    ADDRESS STRING,
    LATITUDE FLOAT,
    LONGITUDE FLOAT,
    GEOMETRY STRING,
    FID STRING,
    OBJECTID STRING,
    BLOCK STRING,
    LOT STRING,
    BBL_CODE STRING,
    SHAPE_LENG FLOAT,
    SHAPE_AREA FLOAT,
    URL_IMAGE STRING,
    LANDMARK_CATEGORY STRING
);
```
### Table Views for Business Needs
After populating our tables with INSERT INTO statements (found in ```table_creation.sql``` file), we created views of joined tables that we would use to visualize our data.
Here are the views we are visualizing in Tableau/ThoughtSpot:
#### Taxi View for Visualization
```sql
CREATE OR REPLACE VIEW TAXI AS
SELECT PU_DATETIME, DO_DATETIME, YEAR, MONTH, DAY,
PU_LOCATIONID, TZ.BOROUGH AS PU_BOROUGH, DO_LOCATIONID, TZ2.BOROUGH AS DO_BOROUGH
FROM TAXI_TBL AS T JOIN TAXI_ZONE_TBL AS TZ ON T.PU_LOCATIONID = TZ.LOCATION_ID
JOIN TAXI_ZONE_TBL AS TZ2 ON T.DO_LOCATIONID = TZ2.LOCATION_ID;
```
#### HVFHV View for Visualization
```sql
CREATE OR REPLACE VIEW HVFHV AS
SELECT H.LICENSE_NUM, L.COMPANY_NAME AS COMPANY_NAME, PU_DATETIME, DO_DATETIME, YEAR, MONTH, DAY,
 PU_LOCATIONID, TZ.BOROUGH AS PU_BOROUGH, DO_LOCATIONID, TZ2.BOROUGH AS DO_BOROUGH
FROM HIGH_VOLUME_TBL AS H JOIN TAXI_ZONE_TBL AS TZ ON H.PU_LOCATIONID = TZ.LOCATION_ID
JOIN TAXI_ZONE_TBL AS TZ2 ON H.DO_LOCATIONID = TZ2.LOCATION_ID
JOIN LICENSE_TBL AS L ON H.LICENSE_NUM = L.LICENSE_NUM;
```
#### Landmarks View for Visualization
```sql
CREATE OR REPLACE VIEW LANDMARKS AS
SELECT LANDMARK_NAME, ADDRESS, BOROUGH, CATEGORY FROM LANDMARKS_TBL;
```
