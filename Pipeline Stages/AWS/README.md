# AWS Integration

### <ins> General Overview </ins>
To augment our pipeline, we decided to use a few different AWS services and tools. In specific, we used Amazon S3, Amazon Athena, AWS Glue Crawlers, and Amazon Bedrock, which will be expanded on later. The integration of these tools into our ETL pipeline allowed us to store data in a central and easily accessible location, validate our transformations, and implement a twist of AI to enhance our data. 

#### Amazon S3
To store and read our data, we used Amazon S3. The original datasets that were given to us were stored in *capstone-techcatalyst-raw*, from which we loaded into Databricks. Additionally, our external landmark data was also uploaded to this bucket. After initial cleaning was done, data was uploaded into the *capstone-techcatalyst-conformed* bucket. From there, data was loaded back into Databricks for further transformations and uploaded into *capstone-techcatalyst-transformed*. From there, data was loaded into Snowflake for table and view creation. For further details about cleaning and transformations, click [here](https://github.com/alina-hartford/thePANDAsCapstone/blob/main/Pipeline%20Stages/Databricks/README.md).
#### Amazon Athena and AWS Glue Crawlers
After uploading data into S3, we validated our cleaning and transformations using AWS Glue Crawlers. For our taxi and HVFHV data, we validated after uploading to *capstone-techcatalyst-conformed* and *capstone-techcatalyst-transformed*, which showed us that all columns populated with proper schemas. For our external data, we also validated with AWS Crawlers after uploading to *capstone-techcatalyst-conformed*, which ensured that we had the correct data and data types. We used Amazon Athena as a tool to query this data and ensure that we had the correct number of rows and columns. 

Below are screenshots of Amazon Athena where we queried our partitioned taxi data and HVFHV data once it has been transformed. We can see all the proper columns populating as expected.
![taxicrawler](images/crawler2.PNG) ![hvfhvcrawler](images/crawler1.PNG)

#### Amazon Bedrock
While deliberating our use case and researching for external data, we found that we needed a way to enhance our data by categorizing our landmarks. Since the dataset we found did not have this information, we figured this would be a good opportuntiy to experiment with using Amazon Bedrock. After getting access granted to Bedrock, we used boto3 to create a program that utilized the Amazon Titan Text Express model to take in a column of landmark names and descriptions and prompted it to output a category designation for that landmark, choosing from a list of 10 categories that we also generated using Amazon Bedrock. Categories include:
* Architecture
* Civic
* Commercial
* Entertainment
* Financial
* Historic
* Industrial
* Institutional
* Religion
* Transportation

The given prompt asked the model to output the categorizations in a JSON formatted string, which we parsed and added to a dictionary. From this, we were able to create a result dataframe that had a column for the landmark name and the landmark category, which was then joined with the rest of our external data for visualizations. All Amazon Bedrock code can be located in ```landmarks.ipynb```.
