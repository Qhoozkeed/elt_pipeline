import boto3
import io
import psycopg2
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from botocore import UNSIGNED
from botocore.client import Config
s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
bucket_name = "d2b-internal-assessment-bucket"
response = s3.list_objects(Bucket=bucket_name, Prefix="orders_data")

file_obj = io.BytesIO()
s3.download_fileobj(bucket_name, "orders_data/reviews.csv", file_obj)
file_obj.seek(0)

chunk_size = 1000
data_chunks = pd.read_csv(file_obj, chunksize=chunk_size)
 

# for data in data_chunks:
#     print(data.count())
#print(data.head())
##print(data.dtypes)

#Loading the extracted files
database_url = "postgresql://qudualli9842:Rc1nD3p169@34.89.230.185:5432/d2b_accessment"

engine = create_engine(database_url)

target_table = "reviews"

for data in data_chunks:
   
    print("Inserting chunks into a Posgresql table")
    data.to_sql(target_table, engine, if_exists="append", schema="qudualli9842_staging",index=False)
    print("Chunk record inserted succesfully")

engine.dispose()
