from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import os
import boto3
from azure.storage.blob import BlobClient, BlobServiceClient

version = "439b0c5"

# AWS credentials from environment variables
aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

# Azure Storage blob SAS URL from environment variables
blob_sas_url = os.environ['AZURE_BLOB_SAS_URL']

# S3 bucket and file details
s3_bucket = 'rlxabs254test'
s3_file_key = 'test.txt'

# Azure Blob details
blob_container_name = 'airflow'
blob_name = 'test.txt'

# Local file path
local_file_path = '/tmp/test.txt'

def transfer_s3_to_blob():
    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    # Download the file from S3 to the local file system
    s3_client.download_file(s3_bucket, s3_file_key, local_file_path)

    # Create a blob client using the blob SAS URL
    blob_service_client = BlobServiceClient.from_connection_string(blob_sas_url)
    blob_client = blob_service_client.get_blob_client(blob_container_name, blob_name)

    print("\nUploading to Azure Storage as blob:\n\t" + local_file_path)

    # Upload the downloaded file
    with open(local_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

dag = DAG(
    's3_to_blob',
    default_args={'owner': 'airflow'},
    description='An ETL task to move data from S3 to Azure Blob',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['example'],
)

transfer_s3_to_blob_task = PythonOperator(
    task_id='transfer_s3_to_blob',
    python_callable=transfer_s3_to_blob,
    dag=dag,
)
