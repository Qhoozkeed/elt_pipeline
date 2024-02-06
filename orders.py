import boto3
import io
import psycopg2  
import pandas as pd
from sqlalchemy import create_engine
from botocore import UNSIGNED
from botocore.client import Config
s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
bucket_name = "d2b-internal-assessment-bucket"
response = s3.list_objects(Bucket=bucket_name, Prefix="orders_data")
#file-like object as a storage unit
file_obj = io.BytesIO()
#downloading the csv files from s3 to the file-like obj
s3.download_fileobj(bucket_name, "orders_data/orders.csv", file_obj)
#starting the obj from the beginning
file_obj.seek(0)
#Number of rows per chunk
chunk_size = 1000
#writing csv data in chunks
data_chunks = pd.read_csv(file_obj, chunksize=chunk_size)
 

for data in data_chunks:
    print(data.head())

#db connection
postgre_conn = "postgresql://qudualli9842:Rc1nD3p169@34.89.230.185:5432/d2b_accessment"
engine = create_engine(postgre_conn)
table_name = "orders"
#Mapping the df column with the  table column
column_mapping = {
    "order_id": "order_id",
    "customer_id": "customer_id",
    "order_date": "order_date",
    "customer_id": "customer_id",
    "product_id": "product_id",
    "unit_price": "unit_price",
    "quantity": "quantity",
    "total_price": "amount",    
}

#loading the stream into a postgresql table
for data in data_chunks:
    data = data.rename(columns=column_mapping)
    print("Inserting chunks into order table")
    data.to_sql(table_name, engine, if_exists="append", schema="qudualli9842_staging",index=False)
#this line will be executed after all chunk have been populated successfully
print("All Chunk records inserted succesfully")

engine.dispose()
