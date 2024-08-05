# Cleaning and Transforming Data using PySpark
### General Approach
For our ETL process, we utilized two instances of Databricks to clean and transform our raw data before loading it into Amazon S3 and Snowflake. The first program (Taxi_Data_Ingestion.dbc) was created to process the yellow taxi, green taxi, and HVFHV data and upload it to our transformed bucket. The second program (External_Data_Ingestion.dbc) was designed to clean our external landmark dataset, add a column for Bedrock's categorical output, and upload it as a structured table to S3.
#### Cleaning Raw Data (Taxi_Data_Ingestion.dbc)

#### Transforming for our Use Case (External_Data_Ingestion.dbc)
