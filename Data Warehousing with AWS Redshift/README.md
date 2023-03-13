# Project: Data Warehouse

The basis of this project is to build an ELT pipeline which will extract data from s3 storage, stage the data in Redshift and proceed to transform the
data into dimensional tables for analytics teams to continue finding insights into what songs their users are listening to.

## Database Design

* A STAR schema will be utilised as it will provide fast aggregations of data whilst also simplify queries.

* For our facts table songplays is utilised and users, songs, artists and time will make up the dimensions tables.

## Data Pipeline design

* Python is utilised for the ETL pipeline due to the convenience of its libraries, such as pandas, which make data manipulation simpler. Additionally, Python allows for the connection to Postgres Database.

* Two kinds of data are involved in the pipeline: song and log data. Song data contains information about songs and artists, which is extracted and loaded into the users and artists dimension tables. Log data contains user activity data, such as song plays, which is extracted and loaded into the songplays fact table.

* The first step in the pipeline is to extract the data from the JSON format stored in the S3 data storage system and load it into the staging tables, which are the staging_songs_table and staging_events_table.

* Next ETL will be performed utilising SQL, transferring data from the staging tables to our fact and dimension tables.
