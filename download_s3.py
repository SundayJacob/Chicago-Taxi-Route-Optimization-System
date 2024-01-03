from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from datetime import datetime
import boto3
from botocore.exceptions import NoCredentialsError

# Construct credentials from service account key file
credentials = service_account.Credentials.from_service_account_file(
    'fifth-trainer-386908-f6e6e96dc20c.json')  # Replace with your service account file path

# Construct a BigQuery client object
client = bigquery.Client(credentials=credentials)

# Define the date range for the query
start_date = '2020-11-01'
end_date = '2023-11-01'   
# Define the batch size and initial offset
batch_size = 100000  # Number of rows to retrieve per batch
offset = 0
# Parquet file path
csv_file_path = "chicago_taxi_trips.csv" 

print("Starting to Download ....")
# Batch processing
while True:
    QUERY = f"""
        SELECT * FROM bigquery-public-data.chicago_taxi_trips.taxi_trips 
        WHERE trip_start_timestamp >= '{start_date}' 
        AND trip_start_timestamp < '{end_date}' 
        ORDER BY trip_start_timestamp ASC
        LIMIT {batch_size} OFFSET {offset};
        """
    query_job = client.query(QUERY) 
    rows = query_job.result() 
    batch_df = pd.DataFrame([dict(row) for row in rows])
    if offset == 0:
        batch_df.to_csv(csv_file_path, index=False)
    else:
        batch_df.to_csv(csv_file_path, mode='a', header=False, index=False)
    if len(batch_df) < batch_size:
        break
    offset += batch_size
print("Download completing .....")
# AWS S3 Upload
# try:
#     s3 = boto3.client(
#         's3',
#         aws_access_key_id='AKIA3SXAQJ3WBAOKB2MJ',
#         aws_secret_access_key='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'
#     )
#     bucket_name = 'mbd-dataset' 
#     object_name = 'chicago_taxi_trips.parquet' 

#     # Upload the file
#     s3.upload_file(csv_file_path, bucket_name, object_name)
#     print(f"File {csv_file_path} uploaded to {bucket_name}/{object_name}")
# except NoCredentialsError:
#     print("Credentials not available")