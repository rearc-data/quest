import json
import os

import boto3
import pandas as pd


def handler(event, context):

    for record in event["Records"]:
        print("test")
        payload = record["body"]
        print(str(payload))
