from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import os
import boto3
from azure.storage.blob import BlobServiceClient
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient


version = "592af7d"

# AWS credentials from environment variables
aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

# S3 bucket and file details
s3_bucket = 'rlxabs254k8stest'
file_name = 'test.txt'

# Azure Blob details
key_vault_name = "kvrlx-abs254001-dev"
sas_token_name = "sas-token-sa-rlxabs254001batchdev"
storage_account_name = 'rlxabs254001batchdev'
container_name = 'abs254001-dev-batch'

# Local file path
local_file_path = '/tmp/test.txt'


def transfer_s3_to_blob():
    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    print("\nDownloading from s3:\n\t" + local_file_path)

    # Download the file from S3 to the local file system
    s3_client.download_file(s3_bucket, file_name, local_file_path)

    #########
    # Get the creds from key vault
    #########
    # TODO this is failing, no permissions to retrieve secret from keyvault, why?
    # credential = ClientSecretCredential(
    #     tenant_id=os.environ["ARM_TENANT_ID"],
    #     client_id=os.environ["ARM_CLIENT_ID"],
    #     client_secret=os.environ["ARM_CLIENT_SECRET"]
    # )
    # vault_url = f"https://{key_vault_name}.vault.azure.net".format()
    # client = SecretClient(vault_url=vault_url, credential=credential)
    # sas_token = client.get_secret(sas_token_name).value
    sas_token = os.environ["SAS_TOKEN"]
    #########
    # Upload the file to Azure Blob
    #########
    account_url = (f"https://{storage_account_name}.blob.core.windows.net")
    blob_service_client = BlobServiceClient(account_url, credential=sas_token)

    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)

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
