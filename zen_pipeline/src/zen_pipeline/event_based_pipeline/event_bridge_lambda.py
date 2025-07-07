import json
import os

BASELINE_FILE: str = "2024-09-30-0.json.gz"

file_prefix = os.environ.get("FILE_PREFIX")
bookmark_file = "bookmark"


def lambda_handler(event, context):
    if os.environ.get("ENVIRON") == "DEV":
        os.environ.setdefault("AWS_DEFAULT", "default")
        print(f'Running in {os.environ.get("ENVIRON")}')
    return {"statusCode": s3_upload_res, "body": json.dumps("Download status code!")}
