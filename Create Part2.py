import boto3
import requests
import json
from datetime import datetime

# ---- CONFIG ----
BUCKET_NAME = "your-s3-bucket"
PREFIX = "datausa-population/"
API_URL = "https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"

s3 = boto3.client("s3")

def fetch_api_data():
    """Fetch data from DataUSA API"""
    resp = requests.get(API_URL)
    resp.raise_for_status()
    return resp.json()

def upload_to_s3(data):
    """Save API response as JSON to S3"""
    # Create filename with timestamp
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename = f"{PREFIX}population_{timestamp}.json"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=filename,
        Body=json.dumps(data).encode("utf-8"),
        ContentType="application/json"
    )
    print(f"Uploaded {filename} to S3")

if __name__ == "__main__":
    data = fetch_api_data()
    upload_to_s3(data)
