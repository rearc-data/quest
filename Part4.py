#app.py
#!/usr/bin/env python3
import aws_cdk as cdk
from cdk_stack import DataPipelineStack

app = cdk.App()
DataPipelineStack(app, "BlsDataPipelineStack", env=cdk.Environment())  # adjust env if needed
app.synth()
----------------------------------------------------------------------------------------------------------------------------------------#cdk_stack.py

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
#ingest_lambda.py
import os
import re
import json
import hashlib
import boto3
import urllib.request
import urllib.parse
from html import unescape

S3_BUCKET = os.environ.get("S3_BUCKET") or os.environ.get("S3_BUCKET".upper())  # fallback
# CDK sets S3_BUCKET env var name 'S3_BUCKET'
S3_BUCKET = os.environ.get("S3_BUCKET")
BLS_BASE_URL = os.environ["BLS_BASE_URL"]
POP_API_URL = os.environ["POP_API_URL"]
USER_AGENT = os.environ.get("USER_AGENT", "contact@example.com")

s3 = boto3.client("s3")

def http_get(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read()

def list_bls_files_from_index():
    # fetch index HTML, extract href="pr.*"
    html = http_get(BLS_BASE_URL).decode("utf-8", errors="ignore")
    # simple regex to extract pr.* href values
    files = re.findall(r'href="(pr\.[^"]+)"', html)
    return files

def list_s3_keys(prefix):
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys

def upload_if_new(prefix, filename, content_bytes):
    checksum = hashlib.md5(content_bytes).hexdigest()
    key = f"{prefix}{filename}-{checksum}"
    existing = set(list_s3_keys(prefix))
    if key in existing:
        print(f"SKIP (exists): {key}")
        return False, key
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=content_bytes)
    print(f"Uploaded: s3://{S3_BUCKET}/{key}")
    return True, key

def delete_stale_s3_keys(bls_files, s3_keys):
    # bls_files: list of filenames like 'pr.data.0.Current'
    valid_prefixes = {f"bls/{f}" for f in bls_files}
    to_delete = [k for k in s3_keys if not any(k.startswith(p) for p in valid_prefixes)]
    for k in to_delete:
        s3.delete_object(Bucket=S3_BUCKET, Key=k)
        print(f"Deleted stale: {k}")

def handler(event, context):
    print("Ingest Lambda started.")
    # Part 1: BLS - list files, download, upload to S3 with checksum
    try:
        bls_files = list_bls_files_from_index()
        print("BLS files found:", bls_files)
        existing_s3 = list_s3_keys("bls/")
        for fname in bls_files:
            try:
                content = http_get(BLS_BASE_URL + fname)
            except Exception as e:
                print(f"Failed to download {fname}: {e}")
                continue
            upload_if_new("bls/", fname, content)
        # delete stale S3 items referencing BLS if any
        delete_stale_s3_keys(bls_files, existing_s3)
    except Exception as e:
        print("Error in BLS ingest:", e)

    # Part 2: Population API -> upload JSON to acs_population/
    try:
        pop_bytes = http_get(POP_API_URL)
        # compute checksum and upload
        uploaded, key = upload_if_new("acs_population/", "acs_population.json", pop_bytes)
        # If upload succeeded, S3 notification will send message to SQS (configured)
        print("Population upload key:", key)
    except Exception as e:
        print("Error fetching population API:", e)

    print("Ingest Lambda finished.")
    return {"status": "ok"}
----------------------------------------------------------------------------------------------------------------------------------------
#analytics_lambda.py
import os
import json
import csv
import boto3
import re
from statistics import mean, pstdev  # population stddev
from io import BytesIO, TextIOWrapper

S3_BUCKET = os.environ.get("S3_BUCKET")
s3 = boto3.client("s3")

def list_keys(prefix):
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys

def pick_latest_by_key(keys, startswith):
    # choose the key that has latest LastModified? we only have keys; assume checksum suffix; pick lexicographically last
    if not keys:
        return None
    # pick last by lexicographic (checksum suffix ensures uniqueness); optionally use S3 head to check metadata
    keys_sorted = sorted(keys)
    return keys_sorted[-1]

def read_s3_text(key):
    resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
    body = resp["Body"].read()
    return body.decode("utf-8")

def parse_bls_csv_text(text):
    # BLS file is a whitespace/tab separated text with header row.
    rows = []
    lines = text.splitlines()
    # header may be first line
    # use regex split to split columns by contiguous whitespace
    header_line = None
    for i, line in enumerate(lines):
        if line.strip() == "":
            continue
        header_line = line
        start_idx = i
        break
    if header_line is None:
        return []
    headers = re.split(r"\s+", header_line.strip())
    # remaining lines after header
    for ln in lines[start_idx+1:]:
        if not ln.strip():
            continue
        parts = re.split(r"\s+", ln.strip())
        # if parts length >= headers length, align
        if len(parts) < len(headers):
            # skip malformed lines
            continue
        row = dict(zip(headers, parts[:len(headers)]))
        rows.append(row)
    return rows, headers

def handler(event, context):
    print("Analytics lambda invoked; event:", json.dumps(event))
    # 1) find the latest BLS file under 'bls/'
    bls_keys = list_keys("bls/")
    if not bls_keys:
        print("No BLS files found in bucket.")
    else:
        bls_key = pick_latest_by_key(bls_keys, "bls/")
        print("Using BLS key:", bls_key)
        text = read_s3_text(bls_key)
        bls_rows, headers = parse_bls_csv_text(text)
        # normalize column names
        headers = [h.strip().lower() for h in headers]
        # ensure we have series_id, year, period, value
        # transform rows to normalized dicts
        normalized = []
        for r in bls_rows:
            # header keys may be different (case, spacing)
            nr = {}
            for k,v in r.items():
                nk = k.strip().lower()
                nr[nk] = v
            # parse
            try:
                series_id = nr.get("series_id") or nr.get("series id") or nr.get("series")
                year = int(re.search(r"(\d{4})", nr.get("year","")).group(1)) if nr.get("year") else None
                period = nr.get("period")
                value = float(nr.get("value")) if nr.get("value") not in (None,"") else None
                if series_id and year and period and value is not None:
                    normalized.append({"series_id": series_id.strip(), "year": year, "period": period.strip(), "value": value})
            except Exception:
                continue
        print(f"Parsed {len(normalized)} usable BLS rows.")
    # 2) find the latest population JSON in acs_population/
    pop_keys = list_keys("acs_population/")
    if not pop_keys:
        print("No population JSON found.")
        pop_rows = []
    else:
        pop_key = pick_latest_by_key(pop_keys, "acs_population/")
        print("Using population key:", pop_key)
        pop_text = read_s3_text(pop_key)
        try:
            j = json.loads(pop_text)
            if isinstance(j, dict) and "data" in j:
                pop_rows = j["data"]
            elif isinstance(j, list):
                pop_rows = j
            else:
                # try if wrapped in other keys
                pop_rows = j.get("data", [])
        except Exception as e:
            print("Failed to parse population JSON:", e)
            pop_rows = []
    # convert pop_rows to dict year -> population
    pop_map = {}
    for rec in pop_rows:
        # try to find year and population keys heuristically
        year = None
        popv = None
        for k,v in rec.items():
            lk = k.lower()
            if "year" in lk:
                try:
                    year = int(re.search(r"(\d{4})", str(v)).group(1))
                except Exception:
                    year = None
            if "pop" in lk:
                try:
                    popv = int(float(v))
                except Exception:
                    popv = None
        if year and popv is not None:
            pop_map[year] = popv

    # TASK 1: mean & stddev (population) for 2013-2018
    years = list(range(2013, 2019))
    vals = [pop_map[y] for y in years if y in pop_map]
    if vals:
        avg_pop = mean(vals)
        std_pop = pstdev(vals)   # population stddev
        print("Population stats 2013-2018: mean=", avg_pop, "stddev=", std_pop)
    else:
        print("Population values for 2013-2018 not fully available. available years:", sorted(pop_map.keys()))

    # TASK 2: best year per series
    # group normalized list by series_id and year sum
    from collections import defaultdict
    series_year_sum = defaultdict(lambda: defaultdict(float))
    if normalized:
        for r in normalized:
            sid = r["series_id"]
            yr = r["year"]
            val = float(r["value"])
            series_year_sum[sid][yr] += val

        best_per_series = {}
        for sid, yr_map in series_year_sum.items():
            # pick year with max sum
            best_year = max(yr_map.items(), key=lambda kv: (kv[1], -kv[0]))  # (year,value)
            best_per_series[sid] = {"year": best_year[0], "value": best_year[1]}

        # Log a sample and count
        print(f"Computed best year for {len(best_per_series)} series (sample 20):")
        cnt = 0
        for sid, rec in list(best_per_series.items())[:20]:
            print(sid, rec)
            cnt += 1
    else:
        print("No normalized BLS rows to compute best year.")

    # TASK 3: PRS30006032 Q01 with population
    target = []
    if normalized:
        for r in normalized:
            if r["series_id"] == "PRS30006032" and r["period"].upper() == "Q01":
                row = {"series_id": r["series_id"], "year": r["year"], "period": r["period"], "value": r["value"], "Population": pop_map.get(r["year"])}
                target.append(row)
    print("PRS30006032 Q01 results:")
    for t in target[:50]:
        print(t)

    return {"status": "ok"}
