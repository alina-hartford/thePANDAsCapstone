# Cleaning and Transforming Data using PySpark
### <ins> General Approach </ins>
For our ETL process, we utilized two instances of Databricks to clean and transform our raw data before loading it into Amazon S3 and Snowflake. The first program (Taxi_Data_Ingestion.dbc) was created to process the yellow taxi, green taxi, and HVFHV data and upload it to our transformed bucket. The second program (External_Data_Ingestion.dbc) was designed to clean our external landmark dataset, add a column for Bedrock's categorical output, and upload it as a parquet file to S3.
### <ins> Cleaning Raw Data</ins>
To clean the NYC taxi data, our team performed a number of transformations on each dataset individually before uploading them to our conformed and transformed S3 buckets. These transformations include normalizing column names, filling in null values, and adding datetime columns for partitioning and timeseries analysis. Below are the transformations applied to each dataset:
#### Yellow Taxi
```python
yellow_taxi = yellow_taxi.dropDuplicates()

yellow_taxi = yellow_taxi.filter(yellow_taxi['trip_distance'] > 0)
yellow_taxi = yellow_taxi.filter((yellow_taxi['fare_amount'] > 0) & (yellow_taxi['total_amount'] > 0))
yellow_taxi = yellow_taxi.na.fill({'passenger_count': 1, 'congestion_surcharge': 0,
'Airport_fee': 0, 'RateCodeID': 1, 'store_and_fwd_flag': 'N'})

yellow_taxi = yellow_taxi.withColumn('trip_type', lit(1))
yellow_taxi = yellow_taxi.withColumn("Taxi_Type", lit("Yellow"))
```
#### Green Taxi
```python
green_taxi = green_taxi.dropDuplicates()
green_taxi = green_taxi.drop('ehail_fee')

green_taxi = green_taxi.filter(green_taxi['trip_distance'] > 0)
green_taxi = green_taxi.filter((green_taxi['fare_amount'] > 0) & (green_taxi['total_amount'] > 0))
green_taxi = green_taxi.na.fill({'passenger_count': 1, 'congestion_surcharge': 0,
'RateCodeID': 1, 'store_and_fwd_flag': 'N', 'payment_type': 5, 'trip_type': 0})

green_taxi = green_taxi.withColumn('Airport_fee', lit(0))
green_taxi = green_taxi.withColumn("Taxi_Type", lit("Green"))
green_taxi = green_taxi.withColumnRenamed("lpep_pickup_datetime", "tpep_pickup_datetime")
green_taxi = green_taxi.withColumnRenamed("lpep_dropoff_datetime", "tpep_dropoff_datetime")
```
#### All Taxi
```python
all_taxi = yellow_taxi.unionByName(green_taxi, allowMissingColumns = True)

all_taxi = all_taxi.withColumn("Year", year(all_taxi['tpep_pickup_datetime']))
all_taxi = all_taxi.withColumn("Month", month(all_taxi['tpep_pickup_datetime']))
all_taxi = all_taxi.withColumn("Day", dayofmonth(all_taxi['tpep_pickup_datetime']))
all_taxi = all_taxi.withColumn("Day_Of_Week_Name", date_format(all_taxi['tpep_pickup_datetime'], 'EEE'))
all_taxi = all_taxi.withColumn("Is_Weekend", date_format("tpep_pickup_datetime", 'EEE').isin(["Sat", "Sun"]))
```
#### HVFHV
```python
hvfhv = hvfhv.dropDuplicates()

hvfhv = hvfhv.withColumnRenamed('pickup_datetime', 'tpep_pickup_datetime')
hvfhv = hvfhv.withColumnRenamed('dropoff_datetime', 'tpep_dropoff_datetime')
hvfhv = hvfhv.withColumnRenamed('trip_miles', 'trip_distance')
hvfhv = hvfhv.withColumnRenamed('base_passenger_fare', 'fare_amount')
hvfhv = hvfhv.withColumnRenamed('tolls', 'tolls_amount')
hvfhv = hvfhv.withColumnRenamed('tips', 'tip_amount')
hvfhv = hvfhv.withColumnRenamed('airport_fee', 'Airport_fee')
hvfhv = hvfhv.withColumnRenamed('trip_time', 'Trip_Duration')

hvfhv = hvfhv.withColumn('Trip_Duration', hvfhv['Trip_Duration'] / 60)
hvfhv = hvfhv.withColumn('total_amount', hvfhv['fare_amount'] + hvfhv['tolls_amount'] +
hvfhv['congestion_surcharge'] + hvfhv['sales_tax'] + hvfhv['bcf'] + hvfhv['Airport_fee'])

hvfhv = hvfhv.withColumn("Year", year(hvfhv['tpep_pickup_datetime']))
hvfhv = hvfhv.withColumn("Month", month(hvfhv['tpep_pickup_datetime']))
hvfhv = hvfhv.withColumn("Day", dayofmonth(hvfhv['tpep_pickup_datetime']))
hvfhv = hvfhv.withColumn("Day_Of_Week_Name", date_format(hvfhv['tpep_pickup_datetime'], 'EEE'))
hvfhv = hvfhv.withColumn("Is_Weekend", date_format("tpep_pickup_datetime", 'EEE').isin(["Sat", "Sun"]))
```
#### Conformed Taxi
```python
conformed_taxi = spark.read.parquet(f"{taxi_partitioned_conformed_path}*/*/*/*.parquet")
conformed_taxi = conformed_taxi.withColumn("file_path", input_file_name()) \
                                   .withColumn("Year", regexp_extract("file_path", r"/Year=(\d+)/", 1)) \
                                   .withColumn("Month", regexp_extract("file_path", r"/Month=(\d+)/", 1)) \
                                   .withColumn("Taxi_Type", regexp_extract("file_path", r"/Taxi_Type=([^/]+)/", 1))

conformed_taxi = conformed_taxi.withColumn('trip_duration', (unix_timestamp("tpep_dropoff_datetime") -
unix_timestamp("tpep_pickup_datetime")) / 60)
conformed_taxi = conformed_taxi.filter(conformed_taxi['trip_distance'] >= 0.1)
conformed_taxi = conformed_taxi.filter(conformed_taxi['trip_duration'] > 0)
conformed_taxi = conformed_taxi.filter(conformed_taxi['trip_duration'] <= 720)
conformed_taxi = conformed_taxi.filter((conformed_taxi['total_amount'] / conformed_taxi['trip_distance']) <= 500)
conformed_taxi = conformed_taxi.filter((conformed_taxi['total_amount'] / conformed_taxi['trip_duration']) <= 1000)
conformed_taxi = conformed_taxi.filter((conformed_taxi['DOLocationID'] != 264) & (conformed_taxi['DOLocationID'] != 265))
conformed_taxi = conformed_taxi.filter((conformed_taxi['Year'] == 2023) | (conformed_taxi['Year'] == 2024))

taxi_transformed = conformed_taxi.select("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance",
"trip_duration", "PULocationID", "DOLocationID", "fare_amount", "tip_amount", "total_amount", "trip_type",
"Year", "Month", "Day", "Day_Of_Week_Name", "Is_Weekend", "Taxi_Type")
```
#### Conformed HVFHV
```python
conformed_hvfhv = spark.read.parquet(f"{hvfhv_partitioned_conformed_path}*/*/*/*.parquet")
conformed_hvfhv = conformed_hvfhv.withColumn("file_path", input_file_name()) \
                                   .withColumn("Year", regexp_extract("file_path", r"/Year=(\d+)/", 1)) \
                                   .withColumn("Month", regexp_extract("file_path", r"/Month=(\d+)/", 1)) \
                                   .withColumn("Day", regexp_extract("file_path", r"/Day=(\d+)/", 1))

conformed_hvfhv = conformed_hvfhv.filter(conformed_hvfhv['trip_distance'] >= 0.1)
conformed_hvfhv = conformed_hvfhv.filter(conformed_hvfhv['trip_duration'] > 0)
conformed_hvfhv = conformed_hvfhv.filter((conformed_hvfhv['total_amount'] / conformed_hvfhv['trip_distance']) <= 500)
conformed_hvfhv = conformed_hvfhv.filter((conformed_hvfhv['total_amount'] / conformed_hvfhv['trip_duration']) <= 1000)
conformed_hvfhv = conformed_hvfhv.filter((conformed_hvfhv['DOLocationID'] != 264) & (conformed_hvfhv['DOLocationID'] != 265))
conformed_hvfhv = conformed_hvfhv.filter(conformed_hvfhv['driver_pay'] >= 0)

hvfhv_transformed = conformed_hvfhv.select('hvfhs_license_num', 'dispatching_base_num', 'request_datetime', 'tpep_pickup_datetime',
'tpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'trip_distance', 'trip_duration', 'fare_amount', 'tip_amount',
'driver_pay', 'total_amount', "Year", "Month", "Day", "Day_Of_Week_Name", "Is_Weekend")
```
### <ins> Transforming for our Use Case</ins>
For our external dataset, we performed similar transformations in this ETL process. We normalized column names, fixed column data types, and dropped records with null in significant columns. Below are the transformations applied to the dataset:
#### Landmarks
```python
landmarks = landmarks.dropDuplicates()
individual_landmarks = landmarks.select('LPC_NAME', 'LPC_Altern', 'BuildType', 'USE_ORIG', 'Use_Second', 'Use_Tertia',
'BORO', 'NEIGHBORHO', 'Address', 'latitude', 'longitude', 'geometry', 'FID', 'OBJECTID', 'Block', 'Lot',
'BBL', 'Shape_Leng', 'Shape_Area', 'URL_IMAGE')

individual_landmarks = individual_landmarks.na.drop(subset=["LPC_NAME", "Address", "Shape_Leng",
"BORO", "NEIGHBORHO", "Shape_Area"])
individual_landmarks = individual_landmarks.withColumnRenamed('LPC_NAME', 'Landmark_Name')
individual_landmarks = individual_landmarks.withColumnRenamed('LPC_Altern', 'Landmark_Alternate_Name')
individual_landmarks = individual_landmarks.withColumnRenamed('BORO', 'Borough')
individual_landmarks = individual_landmarks.withColumnRenamed('NEIGHBORHO', 'Neighborhood')
individual_landmarks = individual_landmarks.withColumnRenamed('BBL', 'BBL_Code')

cols = ['latitude', 'longitude', 'Shape_Leng', 'Shape_Area']
for col_name in cols:
    individual_landmarks = individual_landmarks.withColumn(col_name, col(col_name).cast('float'))
```

