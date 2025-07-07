import boto3
import datetime as dt
from pprint import pprint
from botocore.exceptions import ClientError
import os
import logging
from io import (
    StringIO ,BytesIO)
from dotenv import load_dotenv

load_dotenv()

class s3Client:
    """"A class to handle S3 client operations
    This class initializes an S3 client using the provided AWS credentials and bucket name.
          The class has two modules thatthat allows writing and reding lates files from the s3 bucket
    params : aws_access_keyid: str`
             aws_secret_ket_id: str
             bucketname: str
    """

    def __init__(
            self,
            aws_access_keyid,
            aws_secret_ket_id,
            bucketname
            ):
        if not all([aws_access_keyid, aws_secret_ket_id, bucketname]):
            raise ValueError("All parameters must be provided and not None")
        self.bucketname = bucketname
        self.aws_session = boto3.session.Session()
        try:
            self.s3_client = (
                self.aws_session.client(
                    "s3",
                    aws_access_key_id=aws_access_keyid,
                    aws_secret_access_key=aws_secret_ket_id,
                )
            )
        except ClientError as e:
            logging.error(e)
            raise ValueError("Failed to create S3 client. Check your credentials and bucket name.")

    def s3_write(
            self,
            file_name: str | None,
            file_object: str | None,
    ):
        try:
            res = self.s3_client.put_object(
                Bucket=self.bucketname,
                Key=file_name, 
                Body=file_object
            )
            return print(res)
        except ClientError as e:
            logging.error(e)
            return print("Failed connection and upload")
    
    def s3_get_file(self,
        file_name: str | None = None,
        is_local: bool = False
    ):
        """
        Fet Latest file from the bucket of as they arrive where specifc key are noot specified
        :params file_name the key| Filenname in the s3 bucket"""
        print(self._get_s3_latest_files())
        try:
            res = self.s3_client.get_object(
                Bucket=self.bucketname, 
                Key=str(self._get_s3_latest_files() if not file_name else file_name)
            )
            pprint(res)

            if not res:
                raise ValueError("No content found in the S3 object.")
            else:
                if is_local:
                   body=res.get('Body').read() #  bytes
                   buffer = BytesIO(body)

                   # Read into Spark DataFrame
                #    df = spark.read.csv(buffer, header=True, inferSchema=True)
                #    df.show()

                else:
                        # Save the content to a local file
                        print("I am saving the file locally ")
                        resd = res.get('Body').read().decode('utf-8')
                        with open('file_name.txt', 'w') as f:
                             f.write(resd)
                        return("File downloaded successfully")

        except ClientError as e:
            logging.error(e)
            return print("Failed connection and upload")
        
    def _get_s3_latest_files(self,prefix: str|None =None):
        """
          Get the latest files from the 
           S3 bucket base on my the file arrived
        """
        lastest_key_list = {}
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2') 
            page_iterator = paginator.paginate(Bucket=self.bucketname)  # Adjust prefix if needed
            logging.info("Fetching latest files from S3 bucket...")
            for page in page_iterator: 
                if not page:
                    raise ValueError("No files found in the S3 bucket.")
                for obj in page['Contents']:
                    if not page['Contents']:
                        raise ValueError("No files found in the S3 bucket.")
                    else:
                        if  obj['LastModified'].strftime('%Y%m%d') >= dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d'):
                            lastest_key_list.update({obj['LastModified']:obj['Key']})
            latest_file_date = max(list(lastest_key_list.keys()))
            print(f"Latest file date: {latest_file_date}")
            return lastest_key_list[
                latest_file_date
            ]

        except ClientError as e:
            logging.error(e)
            raise ValueError("Failed to retrieve latest files from S3 bucket.")
        
if __name__ == "__main__":

    # Example usage
    print("S3 Client Example")
    s3 = s3Client(
        aws_access_keyid=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_ket_id=os.getenv('AWS_SECRET_ACCESS_KEY'),
        bucketname="dbtlearn-sam"
    )
    # # Write to S3
    # s3.s3_write("test_file.txt", "This is a test file content.")
    # print("File written to S3 successfully.")
    # Read from S3
    print(s3.s3_get_file())