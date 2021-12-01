import os
import configparser
from botocore.exceptions import ClientError
import pandas as pd
import boto3
import sys

config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']
os.environ["AWS_REGION"]= config['AWS']['AWS_REGION']

def create_s3_bucket(bucket_name, aws_region, acl="private"):
    '''
    Creates a S3 bucket in the AWS instance.
    
    INPUT:
    - bucket_name: string. Name of the bucket (no spaces allowed).
    - aws_region: Region where the S3 will be hosted.
    - acl: Access control list to apply to the bucket. Default 'private'.
    '''
    try:
        s3 = boto3.client('s3')
        location = {'LocationConstraint': aws_region}
        s3.create_bucket(
            Bucket=bucket_name, 
            CreateBucketConfiguration=location, 
            ACL=acl
        )
        print("- '", bucket_name, "' has been successfully created.", sep='')
    except ClientError as e:
        print(e)
        print("- Try another bucket name.")
        return False
    return True

def delete_s3_empty_bucket(bucket_name):
    """
    Deletes a S3 bucket in the AWS instance.
    
    INPUT:
    -bucket_name: string. Name of the bucket (no spaces allowed).
    """
    try:
        s3 = boto3.client('s3')
        s3.delete_bucket(Bucket=bucket_name)
    except ClientError as e:
        
        if e.response['Error']['Code'] == 'AccessDenied':
            print("- ERROR: The bucket name might not exist or you don't have access.",
                  "You can list the existing buckets",
                 "using the 'python3 bucket.py list' command.")
        elif e.response['Error']['Code'] == 'BucketNotEmpty':
            print("- ERROR: The bucket you tried to delete is not empty. ",
                  "You can access: https://s3.console.aws.amazon.com/s3/buckets/",
                 bucket_name, " and empty the bucket.", sep='')
        else:
            print(e)
            
        return False
    print("- '", bucket_name, "' has been successfully deleted.", sep='')
    return True

def list_s3_buckets():
    """
    Lists the S3 buckets in the AWS instance.
    """
    try:
        s3 = boto3.client('s3')
        pandf = pd.DataFrame(s3.list_buckets()['Buckets'])[['Name','CreationDate']]
        print('S3 Buckets:')
        print(pandf)
        return list(pandf['Name'])
    except AccessDenied as e:
        print(e, "It might not exist the provided bucket.")
        return False
    except ClientError as e:
        print(e)
        return False
    
def main():
    """
    Main function to manage S3 buckets.
    """       
    if len(sys.argv) < 2:
        print("- You need to pass an argument after 'bucket.py'.")
    elif sys.argv[1] == 'create':
        if len(sys.argv) < 3:
            print("- You need to give a name to the bucket.")
        else:
            if len(sys.argv) > 3:
                print("- The bucket name cannot contain any spaces.")
            else:
                print("- Creating S3 bucket: '", sys.argv[2], "'", sep='')
                create_s3_bucket(sys.argv[2], os.environ["AWS_REGION"])
    elif sys.argv[1] == 'list':
        list_s3_buckets()
    elif sys.argv[1] == 'delete':
        if len(sys.argv) < 3:
            print("- You need to specify the bucket name.")
        else:
            if len(sys.argv) > 3:
                print("- The bucket name cannot contain any spaces.")
            else:
                print("- Deleting S3 (empty) bucket: ", sys.argv[2])
                delete_s3_empty_bucket(sys.argv[2])
    else:
        print("Unknown function '", sys.argv[1], "'. Try with: 'create', 'list' or 'delete'.",
             sep='')


if __name__ == "__main__":
    main()