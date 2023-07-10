from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import os
import boto3
from azure.storage.blob import BlobClient


# AWS credentials from environment variables
aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

# Azure Storage blob SAS URL from environment variables
blob_sas_url = os.environ['AZURE_BLOB_SAS_URL']

# Local file path
local_file_path = '/tmp/my_file'


def transfer_s3_to_blob():
    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    # Download the file from S3 to the local file system
    s3_client.download_file('my-s3-bucket', 'my-s3-object-key', local_file_path)

    # Create a blob client using the blob SAS URL
    blob_client = BlobClient.from_blob_url(blob_sas_url)

    print("\nUploading to Azure Storage as blob:\n\t" + local_file_path)

    # Upload the created file
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
