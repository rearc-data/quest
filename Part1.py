import boto3
import requests
from bs4 import BeautifulSoup
import hashlib
import os

# ---- CONFIG ----
BUCKET_NAME = "your-s3-bucket"
PREFIX = "bls-data/"
SOURCE_URL = "https://download.bls.gov/pub/time.series/pr/"
s3 = boto3.client("s3")

def list_s3_files():
    """List files already in S3"""
    resp = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)
    if "Contents" not in resp:
        return {}
    return {obj["Key"].replace(PREFIX, ""): obj["ETag"].strip('"') for obj in resp["Contents"]}

def get_source_files():
    """Scrape available files from BLS source directory"""
    resp = requests.get(SOURCE_URL)
    soup = BeautifulSoup(resp.text, "html.parser")
    files = {}
    for link in soup.find_all("a"):
        fname = link.get("href")
        if fname and fname.endswith(".txt"):  # adjust if other extensions
            file_url = SOURCE_URL + fname
            file_resp = requests.get(file_url)
            md5 = hashlib.md5(file_resp.content).hexdigest()
            files[fname] = {"url": file_url, "md5": md5, "content": file_resp.content}
    return files

def sync_files():
    s3_files = list_s3_files()
    source_files = get_source_files()

    # Upload new or changed files
    for fname, meta in source_files.items():
        if fname not in s3_files or s3_files[fname] != meta["md5"]:
            print(f"Uploading {fname} to S3...")
            s3.put_object(Bucket=BUCKET_NAME, Key=PREFIX + fname, Body=meta["content"])

    # Remove deleted files
    for fname in s3_files:
        if fname not in source_files:
            print(f"Deleting {fname} from S3...")
            s3.delete_object(Bucket=BUCKET_NAME, Key=PREFIX + fname)

if __name__ == "__main__":
    sync_files()
