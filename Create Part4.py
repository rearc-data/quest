import aws_cdk as cdk
from cdk_stack import DataPipelineStack

app = cdk.App()
DataPipelineStack(app, "BlsDataPipelineStack", env=cdk.Environment())  # adjust env if needed
app.synth()
----------------------------------------------------------------------------------------------------------------------------------------
#cdk_stack.py
from aws_cdk import (
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as targets,
    aws_sqs as sqs,
    aws_s3_notifications as s3n,
    Duration,
    RemovalPolicy,
    Stack,
)
from constructs import Construct
import os

HERE = os.path.dirname(__file__)
LAMBDA_DIR = os.path.join(HERE, "lambda")

class DataPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # S3 bucket (data store)
        bucket = s3.Bucket(self, "BlsDataBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL
        )

        # SQS queue
        queue = sqs.Queue(self, "PopulationNotificationQueue",
            visibility_timeout=Duration.seconds(300),
            retention_period=Duration.days(4)
        )

        # LAMBDA: Ingest (Part 1 + Part 2)
        ingest_fn = _lambda.Function(self, "IngestFunction",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="ingest_lambda.handler",
            code=_lambda.Code.from_asset(LAMBDA_DIR),
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={
                "S3_BUCKET": bucket.bucket_name,
                "BLS_BASE_URL": "https://download.bls.gov/pub/time.series/pr/",
                "POP_API_URL": "https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population",
                # replace with your contact email to follow BLS policy:
                "USER_AGENT": "your_contact_email@example.com"
            }
        )

        # Permissions for ingest lambda to list/put/get/delete in bucket
        bucket.grant_read_write(ingest_fn)
        bucket.grant_put(ingest_fn)
        ingest_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["s3:ListBucket"],
            resources=[bucket.bucket_arn]
        ))

        # Schedule ingest lambda daily via EventBridge (once per day)
        rule = events.Rule(self, "DailyIngestRule",
            schedule=events.Schedule.rate(Duration.days(1))
        )
        rule.add_target(targets.LambdaFunction(handler=ingest_fn))

        # Configure S3 notification: when population JSON is PUT to acs_population/ prefix, send to SQS
        # Create an SQS destination for notifications
        bucket.add_event_notification(s3.EventType.OBJECT_CREATED, s3n.SqsDestination(queue),
            s3.NotificationKeyFilter(prefix="acs_population/")
        )

        # LAMBDA: Analytics (triggered by SQS)
        analytics_fn = _lambda.Function(self, "AnalyticsFunction",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="analytics_lambda.handler",
            code=_lambda.Code.from_asset(LAMBDA_DIR),
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={
                "S3_BUCKET": bucket.bucket_name
            }
        )

        # Give analytics lambda permission to read from S3
        bucket.grant_read(analytics_fn)
        analytics_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["s3:ListBucket"],
            resources=[bucket.bucket_arn]
        ))

        # Grant SQS permissions for lambda trigger (lambda service uses event source mapping)
        queue.grant_consume_messages(analytics_fn)

        # Add SQS event source mapping for analytics lambda
        analytics_fn.add_event_source_mapping("QueueEventSource",
            event_source_arn=queue.queue_arn,
            batch_size=1,
            enabled=True
        )

        # Outputs 
        self.bucket_name = bucket.bucket_name
        self.queue_url = queue.queue_url
        self.ingest_fn_name = ingest_fn.function_name
        self.analytics_fn_name = analytics_fn.function_name

        #add outputs to the CloudFormation outputs if desired
        from aws_cdk import CfnOutput
        CfnOutput(self, "BucketName", value=bucket.bucket_name)
        CfnOutput(self, "QueueUrl", value=queue.queue_url)
        CfnOutput(self, "IngestLambda", value=ingest_fn.function_name)
        CfnOutput(self, "AnalyticsLambda", value=analytics_fn.function_name)
----------------------------------------------------------------------------------------------------------------------------------------

# ingest_lambda.py
import boto3
import requests
import hashlib
import re
import os
import time
import json
from datetime import datetime
from botocore.exceptions import ClientError

s3 = boto3.client("s3")
BUCKET = os.environ.get("BUCKET_NAME")
BLS_PREFIX = os.environ.get("BLS_PREFIX", "bls-data/")
DATAUSA_PREFIX = os.environ.get("DATAUSA_PREFIX", "datausa-population/")
BLS_URL = "https://download.bls.gov/pub/time.series/pr/"

def list_source_files():
    resp = requests.get(BLS_URL, timeout=30)
    resp.raise_for_status()
    html = resp.text
    # Find hrefs that end with .txt or other typical file types
    hrefs = re.findall(r'href="([^"]+)"', html)
    files = [h for h in hrefs if h and (h.endswith(".txt") or h.endswith(".csv") or h.endswith(".gz"))]
    # Unique, full URL
    return sorted(list(set(files)))

def md5_bytes(content_bytes):
    m = hashlib.md5()
    m.update(content_bytes)
    return m.hexdigest()

def s3_object_etag(bucket, key):
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
        etag = head.get("ETag", "").strip('"')
        return etag
    except ClientError as e:
        if e.response['Error']['Code'] in ("404", "NoSuchKey"):
            return None
        else:
            raise

def sync_bls_files():
    source_files = list_source_files()
    # map of fname -> content bytes
    source_map = {}
    for fname in source_files:
        url = BLS_URL + fname
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        source_map[fname] = r.content

    # List existing S3 objects under prefix
    s3_objs = {}
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET, Prefix=BLS_PREFIX):
        for obj in page.get('Contents', []):
            key = obj['Key']
            short = key[len(BLS_PREFIX):] if key.startswith(BLS_PREFIX) else key
            s3_objs[short] = obj

    # Upload new or changed
    for fname, content in source_map.items():
        key = BLS_PREFIX + fname
        local_md5 = md5_bytes(content)
        s3_etag = None
        if fname in s3_objs:
            # ETag might be md5 for non-multipart uploads; for multipart, it's different.
            s3_etag = s3_object_etag(BUCKET, key)
        if s3_etag == local_md5:
            print(f"Skipping {fname}, unchanged.")
            continue
        print(f"Uploading {fname} (md5: {local_md5}) -> s3://{BUCKET}/{key}")
        s3.put_object(Bucket=BUCKET, Key=key, Body=content)

    # Delete files that exist in S3 but not in source
    for short in list(s3_objs.keys()):
        if short not in source_map:
            del_key = BLS_PREFIX + short
            print(f"Deleting stale object s3://{BUCKET}/{del_key}")
            s3.delete_object(Bucket=BUCKET, Key=del_key)

def fetch_and_write_datausa():
    API_URL = ("https://honolulu-api.datausa.io/tesseract/data.jsonrecords?"
               "cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population")
    r = requests.get(API_URL, timeout=30)
    r.raise_for_status()
    data = r.json()
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    key = f"{DATAUSA_PREFIX}population_{ts}.json"
    print(f"Uploading DataUSA JSON to s3://{BUCKET}/{key}")
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(data).encode("utf-8"), ContentType="application/json")
    return key

def lambda_handler(event, context):
    print("Starting ingest lambda")
    try:
        sync_bls_files()
        datausa_key = fetch_and_write_datausa()
        print("Ingest completed. Wrote:", datausa_key)
    except Exception as e:
        print("Error in ingest:", str(e))
        raise
    return {"status": "ok"}
--------------------------------------------------------------------------------------------------------------------
# report_lambda.py
import json
import os
import boto3
import io
import pandas as pd
import numpy as np
from botocore.exceptions import ClientError

s3 = boto3.client("s3")
BUCKET = os.environ.get("BUCKET_NAME")
BLS_PREFIX = os.environ.get("BLS_PREFIX", "bls-data/")
DATAUSA_PREFIX = os.environ.get("DATAUSA_PREFIX", "datausa-population/")
PRS_SERIES = os.environ.get("PRS_FILTER_SERIES", "PRS30006032")
PRS_PERIOD = os.environ.get("PRS_FILTER_PERIOD", "Q01")

def parse_sqs_event(event):
    # SQS event delivers messages; each message body is a JSON string with S3 event, depending on S3->SQS config
    s3_keys = []
    for rec in event.get('Records', []):
        body = rec.get("body")
        try:
            payload = json.loads(body)
        except Exception:
            payload = body
        # If this is an S3 Event Notification (when S3 -> SQS), payload will have "Records"
        if isinstance(payload, dict) and "Records" in payload:
            for s3rec in payload["Records"]:
                s3_info = s3rec.get("s3", {})
                bucket = s3_info.get("bucket", {}).get("name")
                key = s3_info.get("object", {}).get("key")
                if bucket == BUCKET and key:
                    s3_keys.append(key)
        else:
            # Not standard structure - ignore or log
            continue
    return s3_keys

def read_json_from_s3(key):
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    data = obj['Body'].read().decode('utf-8')
    return json.loads(data)

def read_csv_from_s3(key):
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    data = obj['Body'].read()
    # Pandas read_csv from bytes
    return pd.read_csv(io.BytesIO(data), dtype=str)

def compute_population_stats(pop_df):
    # Expect pop_df rows have at least Year and Population columns.
    pop_df['Year'] = pop_df['Year'].astype(int)
    pop_df['Population'] = pd.to_numeric(pop_df['Population'], errors='coerce')
    subset = pop_df[(pop_df['Year'] >= 2013) & (pop_df['Year'] <= 2018)]
    mean_pop = subset['Population'].mean()
    std_pop = subset['Population'].std()
    return mean_pop, std_pop

def best_year_per_series(ts_df):
    # Clean columns and convert types
    ts_df['value'] = pd.to_numeric(ts_df['value'], errors='coerce').fillna(0)
    ts_df['year'] = ts_df['year'].astype(int)
    grouped = ts_df.groupby(['series_id', 'year'], as_index=False)['value'].sum()
    # For each series_id, pick year with max sum
    idx = grouped.groupby(['series_id'])['value'].transform(max) == grouped['value']
    best = grouped[idx].drop_duplicates(subset=['series_id'])
    return best.rename(columns={'value': 'yearly_sum'})

def join_prs_population(ts_df, pop_df, series_id=PRS_SERIES, period=PRS_PERIOD):
    ts_filtered = ts_df[(ts_df['series_id'] == series_id) & (ts_df['period'] == period)].copy()
    ts_filtered['year'] = ts_filtered['year'].astype(int)
    pop_df['Year'] = pop_df['Year'].astype(int)
    merged = ts_filtered.merge(pop_df, how='left', left_on='year', right_on='Year')
    # Keep desired columns
    return merged[['series_id', 'year', 'period', 'value', 'Population']]

def lambda_handler(event, context):
    print("Report lambda invoked. Event:", json.dumps(event)[:1000])
    try:
        s3_keys = parse_sqs_event(event)
        if not s3_keys:
            print("No s3 keys found in event. Exiting.")
            return {"status": "no_s3_keys"}

        # Read latest pr.data.0.Current from S3
        # Assume its stored exactly at BLS_PREFIX + "pr.data.0.Current" OR maybe multiple objects with that name
        pr_key = BLS_PREFIX + "pr.data.0.Current"
        try:
            ts_df = read_csv_from_s3(pr_key)
        except ClientError:
            # Try list and find file starting with pr.data.0.Current
            listing = s3.list_objects_v2(Bucket=BUCKET, Prefix=BLS_PREFIX)
            pr_key = None
            for o in listing.get('Contents', []):
                name = o['Key']
                if os.path.basename(name).startswith("pr.data.0.Current"):
                    pr_key = name
                    break
            if pr_key is None:
                raise RuntimeError("Could not find pr.data.0.Current in S3")
            ts_df = read_csv_from_s3(pr_key)

        # For each JSON written (DataUSA file), read and compute reports
        results = []
        for key in s3_keys:
            # Only handle DataUSA prefix (we set S3 notification to filter prefix, but double-check)
            if not key.startswith(DATAUSA_PREFIX):
                print("Skipping non-datausa key:", key)
                continue
            data = read_json_from_s3(key)
            # DataUSA JSON older shape: may have top-level 'data' key with list of records under various fields.
            # The ingest writes the API response as-is; we try to handle known structure:
            if isinstance(data, dict) and 'data' in data:
                pop_records = data['data']
            elif isinstance(data, dict) and 'records' in data:
                pop_records = data['records']
            elif isinstance(data, list):
                pop_records = data
            else:
                # attempt to find a nested 'data' key
                pop_records = []
                for v in data.values():
                    if isinstance(v, list):
                        pop_records = v
                        break

            pop_df = pd.DataFrame(pop_records)
            # Normalize column names: ensure Year and Population exist
            # Some records might use 'Year' and 'Population' keys; others might use lowercase
            if 'Year' not in pop_df.columns and 'year' in pop_df.columns:
                pop_df.rename(columns={'year': 'Year'}, inplace=True)
            if 'Population' not in pop_df.columns and 'population' in pop_df.columns:
                pop_df.rename(columns={'population': 'Population'}, inplace=True)

            # Step 2: compute mean & stddev for [2013,2018]
            mean_pop, std_pop = compute_population_stats(pop_df)
            print(f"Population stats for 2013-2018: mean={mean_pop}, stddev={std_pop}")

            # Step 3: Best year per series in time-series
            best_df = best_year_per_series(ts_df)
            print("Best year per series (sample 10):")
            print(best_df.head(10).to_string(index=False))

            # Step 4: Join for series_id PRS30006032 and period Q01
            joined = join_prs_population(ts_df, pop_df)
            print("Joined rows for PRS30006032 Q01 (if any):")
            print(joined.to_string(index=False))

            results.append({
                "s3_key": key,
                "mean_population_2013_2018": float(mean_pop) if mean_pop is not None else None,
                "std_population_2013_2018": float(std_pop) if std_pop is not None else None,
                "best_year_sample_count": int(len(best_df))
            })
        return {"status": "ok", "results": results}
    except Exception as e:
        print("Error in report lambda:", str(e))
        raise
