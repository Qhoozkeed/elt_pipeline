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
#creating a file-like object
file_obj = io.BytesIO()
#downloading the csv files from s3 to the file-like obj
s3.download_fileobj(bucket_name, "orders_data/shipment_deliveries.csv", file_obj)
#starting the obj from the beginning
file_obj.seek(0)
#Number of rows per chunk
chunk_size = 1000
#writing csv data in chunks
data_chunks = pd.read_csv(file_obj, chunksize=chunk_size)

# for data in data_chunks:
#   print(data.count())
