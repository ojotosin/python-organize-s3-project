import boto3
from datetime import datetime

def lambda_handler(event, context):

    s3_client = boto3.client('s3')
    bucket_name = "bsu-canvass-data-archive"
    todays_date = datetime.today().strftime("%Y%m%d")
    directory_name = todays_date + "/"

    # Check if directory exists
    try:
        s3_client.head_object(Bucket=bucket_name, Key=directory_name)
    except s3_client.exceptions.ClientError:
        # If the directory doesn't exist, create it
        s3_client.put_object(Bucket=bucket_name, Key=directory_name)

    # Paginate over S3 bucket contents
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name):
        for item in page.get('Contents', []):
            object_creation_date = item.get("LastModified").strftime("%Y%m%d") + "/"
            object_name = item.get("Key")

            if object_creation_date == directory_name and "/" not in object_name:
                new_key = directory_name + object_name
                
                # Copy the object
                s3_client.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': object_name}, Key=new_key)
                
                # Confirm that the object was successfully copied
                try:
                    s3_client.head_object(Bucket=bucket_name, Key=new_key)
                    # If successful, delete the original object
                    s3_client.delete_object(Bucket=bucket_name, Key=object_name)
                except s3_client.exceptions.ClientError as e:
                    print(f"Error verifying the copied object {new_key}. Not deleting the original. Error: {e}")

