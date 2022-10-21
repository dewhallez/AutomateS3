from secrets import access_key, secret_access_key

import boto3
import os

client = boto3.client('s3',
                        aws_access_key_id = access_key,
                        aws_secret_access_key = secret_access_key)

for file in os.listdir():
    if '.py' in file:
        upload_file_bucket = 'my-s3-uploader-test'
        upload_file_key = 'python/' + str(file)
        client.upload_file(file, upload_file_bucket, upload_file_key)

    if '.jpg' in file:
        upload_file_bucket = 'my-s3-uploader-test'
        upload_file_key = 'pictures/' + str(file)
        client.upload_file(file, upload_file_bucket, upload_file_key)



#s3 = boto3.client('s3')
#response = s3.list_buckets()

#print('Existing Buckets: ')
#for bucket in response['Buckets']:
    #print(f'{bucket["Name"]} ')

