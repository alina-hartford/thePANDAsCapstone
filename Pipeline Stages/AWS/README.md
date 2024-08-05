# AWS Integration

### <ins> General Overview </ins>
To augment our pipeline, we decided to use a few different AWS services and tools. In specific, we used Amazon S3, Amazon Athena, AWS Glue Crawlers, and Amazon Bedrock, which will be expanded on later. The integration of these tools into our ETL pipeline allowed us to store data in a central and easily accessible location, validate our transformations, and implement a twist of AI to enhance our data. 

#### Amazon S3
To store and read our data, we used Amazon S3. The original data sets that were given to us were stored in *capstone-techcatalyst-raw*, from which we loaded into Databricks. Additionally, our external landmark data was also uploaded to this bucket. After initial cleaning was done, data was uploaded into the *capstone-techcatalyst-conformed* bucket. From there, data was loaded back into Databricks for further transformations and uploaded into *capstone-techcatalyst-transformed*. From there, data was loaded into Snowflake for table and view creation. For further details about cleaning and trasnformations, click [here](https://github.com/alina-hartford/thePANDAsCapstone/blob/main/Pipeline%20Stages/Databricks/README.md).
#### Amazon Athena and AWS Glue Crawlers
After uploading data into S3, we validated our cleaning and transformation using AWS Glue Crawlers. For our taxi and HVFH data, we validated after uploading to *capstone-techcatalyst-conformed* and *capstone-techcatalyst-transformed*, which showed us that all columns populated with proper schemas. For our external data, we also validated with AWS Crawlers after uploading to *capstone-techcatalyst-conformed*, which ensured that we had the correct data and data types. We used Amazon Athena as a tool to query this data and ensure that we had the correct number of rows and columns. 
#### Amazon Bedrock
