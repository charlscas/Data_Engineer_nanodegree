# Cloud Data Warehouse Project: Sparkify in Redshift
## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

### Key concepts
* Data modeling with Postgres and build an ETL pipeline using Python. 
* Fact and dimension tables for a star schema for a particular analytic focus.
* ETL pipeline transfering data from files in two local directories into Postgres tables using Python and SQL.

## Running the pipeline
### Initiate and terminate redshift cluster
First, clone the repo and copy `dwh.example.cfg` and rename it `dwh.cfg`. Use the AWS credentials and paste them in `KEY` and `SECRET` (you will need to create a user with programmatic access in AWS).

Set a username and a password to connect to the DB. Change any of the pre-set details to your convinience. The `DWH_ENDPOINT` and `ARN` will be set during the creation of the cluster.

Install the python libraries listed in `requirements.txt`.

Run the following command:
```
python3 initiate_cluster.py
```
The terminal will prompt the steps to get the Redshift cluster up and running. It should take between 1.5 and 3 minutes. If no errors are prompted in the console, the cluster is up and running.

At any moment, you can delete the cluster by using the following command:
```
python3 terminate_cluster.py
```

## Create tables
To create the tables in the Postgres Redshift DB you need to run the following command once the cluster is up and running:
```
python3 create_tables.py
```


## Extract, Transform and Load (ETL)
Finally, to extract the data from S3, load it into two staging tables, and transforming it to load it into 5 final tables, you'll need to run the following command:
```
python3 etl.py
```
By default, the configuration value `DWH_PROCESS_INCOMPLETE_DATA` is set to `True`. The ETL loads incomplete data into the final tables. If we wish to load only complete data, then we should set this value to `False`.