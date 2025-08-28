import boto3
import requests
import json
import hashlib
import io
from botocore.exceptions import NoCredentialsError

# Replace with your bucket
S3_BUCKET = "my-data-pipeline-bucket"
API_URL = "https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
USER_AGENT = "preeti.dangi@gmail.com"

def fetch_and_upload_api_data():
    # Fetch from API
    r = requests.get(API_URL, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    data = r.json()

    # Generate checksum for versioning
    checksum = hashlib.md5(json.dumps(data, sort_keys=True).encode("utf-8")).hexdigest()
    key = f"acs_population/acs_population-{checksum}.json"

    s3 = boto3.client("s3")

    try:
        # Check existing files in S3
        objs = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="acs_population/")
        existing_keys = [o["Key"] for o in objs.get("Contents", []) or []]

        if key not in existing_keys:
            json_buffer = io.StringIO()
            json.dump(data, json_buffer, indent=2)
            s3.put_object(
                Body=json_buffer.getvalue(),
                Bucket=S3_BUCKET,
                Key=key,
                ContentType="application/json"
            )
            print(f"Uploaded new API result: {key}")
        else:
            print("File already exists in S3, skipping upload.")

    except NoCredentialsError:
        print("AWS credentials not found")

if __name__ == "__main__":
    fetch_and_upload_api_data()
