# data2bot
## Data Engineer -Talent Pipeline Assessment

This repository contains Python and SQL scripts for extracting, loading, and transforming data from Amazon S3 datalake to PostgreSQL datawarehouse. The data extraction, transformation and loading export tasks are organized into separate python files and a SQL script. The details of each file are as follows:

## orders.py
This python code extracts data from the orders.csv files in the Amazon S3 bucket data lake. The data is streamed and chunked to a specific number of rows and loaded into it  corresponding table in the PostgreSQL database. 
Note: while am loading this orders.csv file into a table, I have to do the column mapping between the files structure and table structure. 
## reviews.py
This python code performs a similar process as orders.py but it focuses on extracting data from the reviews.csv files. Like before, the data is streamed and loaded into the PostgreSQL database.

## shipment_deliveries.py
Similar to the previous python code, shipment_deliveries.py extracts data from the shipment_deliveries.csv files in the Amazon S3 bucket. The streaming and loading process remains consistent across all three python code discussed above.

## transformation_script.sql
This SQL script, transformation.sql, contains the SQL queries used for the transformation process as outlined in the assessment. These queries are an integral part of the data processing and transformation workflow.

## export_tables.py
This python code connects to the PostgreSQL database and exports the transformed data from the database tables into .csv files. These export files are then uploaded to a specified directory on the Amazon S3 data lake.

## Each python code and SQL query plays a crucial role in the end-to-end data extraction, transformation and loading pipeline. The repository aims to provide a clear and organized approach to handling data from Amazon S3 data lake to PostgreSQL datawarehouse while maintaining documentation for transparency and reproducibility.

