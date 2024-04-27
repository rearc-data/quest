import csv
import json
import logging
import os
from tempfile import NamedTemporaryFile

import boto3
import pandas as pd
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger()


BUCKET = "rearc-quest"
REGION = "us-east-1"


def handler(event, context):

    headers = {"User-Agent": "seancowens13@gmail.com"}
    productivity_url = "https://download.bls.gov/pub/time.series/pr/"
    population_url = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"

    get_productivity_data(productivity_url, headers)
    get_population_data(population_url, headers)


def get_productivity_data(url, headers):

    response = requests.get(url=url, headers=headers)
    response.raise_for_status()

    soup = BeautifulSoup(response.content)
    filenames = [
        link.get("href").split("/")[-1]
        for link in soup.findAll("a")
        if not link.get("href").endswith("/")
    ]

    for filename in filenames:
        filename_url = os.path.join(url, filename)
        response = requests.get(url=filename_url, headers=headers)
        response.raise_for_status()
        s3_key = os.path.join("bls", filename)
        write_to_s3(s3_key, response.content)


def get_population_data(url, headers):

    response = requests.get(url=url, headers=headers)
    response.raise_for_status()
    raw_s3_key = os.path.join("population", "raw_data.json")
    cleaned_s3_key = os.path.join("population", "cleaned_data.json")
    write_to_s3(raw_s3_key, response.content)
    write_to_s3(cleaned_s3_key, json.dumps(response.json()["data"]))


def write_to_s3(key, data):

    client = boto3.client("s3", region_name=REGION)
    client.put_object(Bucket=BUCKET, Key=key, Body=data)
