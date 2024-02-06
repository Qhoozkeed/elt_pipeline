import psycopg2
import pandas as pd
import boto3
from sqlalchemy import create_engine
from botocore import UNSIGNED
from botocore.client import Config
s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

# PostgreSQL connection settings
db_connec = {
    'host': '34.89.230.185',
    'port': '5432',
    'database': 'd2b_accessment',
    'user': 'qudualli9842',
    'password': 'Rc1nD3p169'
}


# Amazon S3 settings
s3_bucket_name = 'd2b-internal-assessment-bucket'
s3_object_key = 'analytics_export/qudualli9842/'  



database_url = "postgresql://qudualli9842:Rc1nD3p169@34.89.230.185:5432/d2b_accessment"

engine = create_engine(database_url)
print("db Connected")


# Retrieve and export data
query = 'select * from qudualli9842_analytics.agg_public_holiday'
query2 = 'select * from qudualli9842_analytics.agg_shipments'
query3 = 'select * from qudualli9842_analytics.best_performing_product'
df = pd.read_sql(query, engine)
df2 = pd.read_sql(query2, engine)
df3 = pd.read_sql(query3, engine)
print('Query run')

# Close PostgreSQL connection
engine.dispose()

# Export data to CSV
df.to_csv('agg_public_holiday.csv', index=False)
df2.to_csv('agg_shipments.csv', index=False)
df3.to_csv('best_performing_product.csv', index=False)
# Upload CSV to Amazon S3
s3.upload_file('agg_public_holiday.csv', s3_bucket_name, s3_object_key)
s3.upload_file('agg_shipments.csv', s3_bucket_name, s3_object_key)
s3.upload_file('best_performing_product.csv', s3_bucket_name, s3_object_key)


print("Data exported and uploaded to Amazon S3.")
