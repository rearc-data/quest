
"""
Part 3 — Data Analytics (PySpark)
- Fetch BLS file (pr.data.0.Current) using requests with User-Agent
- Fetch population JSON from DataUSA API
- Create Spark DataFrames from the fetched data
- Compute:
    1) Mean & std dev of population for years 2013-2018
    2) For each series_id, the best year (max sum of quarterly value)
    3) For series_id=PRS30006032 and period=Q01, show value and same-year population
- Save outputs as CSV files in the local working directory (and optionally to S3)
"""

import requests
import pandas as pd
from io import StringIO
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------- Configuration ----------
BLS_URL = "https://download.bls.gov/pub/time.series/pr/pr.data.0.Current"
POP_API_URL = "https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
USER_AGENT = "Part3-DataPipeline/1.0 (contact: your_email@example.com)"  # <- put a real email
OUTPUT_DIR = "./part3_outputs"   # will be created locally as I don't S3 Bucket for saving output
WRITE_TO_S3 = False              # As I don't have S3 credentials to write operation
S3_BUCKET = "my-data-pipeline-bucket"

os.makedirs(OUTPUT_DIR, exist_ok=True)


def fetch_bls_to_pandas(url=BLS_URL, user_agent=USER_AGENT, timeout=60):
    headers = {"User-Agent": user_agent}
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    text = resp.text
    
    try:
        df = pd.read_csv(StringIO(text), sep="\t", engine="python", dtype=str)
    except Exception:
        df = pd.read_csv(StringIO(text), sep=r"\s+", engine="python", dtype=str)
        
    df.columns = [c.strip().lower() for c in df.columns]
    needed = {"series_id", "year", "period", "value"}
    if not needed.issubset(set(df.columns)):
        missing = needed - set(df.columns)
        raise RuntimeError(f"BLS file missing expected columns {missing}; columns: {df.columns.tolist()}")
    df['series_id'] = df['series_id'].astype(str).str.strip()
    df['period'] = df['period'].astype(str).str.strip()
    df['year'] = df['year'].astype(str).str.extract(r'(\d{4})')[0].astype(int)
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df = df.dropna(subset=['value'])
    return df

def fetch_population_to_pandas(api_url=POP_API_URL, user_agent=USER_AGENT, timeout=60):
    headers = {"User-Agent": user_agent}
    resp = requests.get(api_url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict) and 'data' in data:
        records = data['data']
    else:
        records = data
    df = pd.DataFrame(records)
    df.columns = [c.strip() for c in df.columns]
    year_col = next((c for c in df.columns if c.lower().startswith('year') or c.lower()=='time'), None)
    pop_col = next((c for c in df.columns if 'pop' in c.lower()), None)
    if (year_col is None) or (pop_col is None):
        raise RuntimeError(f"Population JSON missing expected columns. Found: {df.columns.tolist()}")
    df = df.rename(columns={year_col:'year', pop_col:'Population'})
    df['year'] = df['year'].astype(str).str.extract(r'(\d{4})')[0].astype(int)
    df['Population'] = pd.to_numeric(df['Population'], errors='coerce')
    df = df.dropna(subset=['year','Population']).sort_values('year').reset_index(drop=True)
    return df

print("Fetching BLS file...")
bls_pd = fetch_bls_to_pandas()
print(f"Loaded BLS rows: {len(bls_pd)}")

print("Fetching Population API data...")
pop_pd = fetch_population_to_pandas()
print(f"Loaded Population rows: {len(pop_pd)}")

#Creating Dataframes
bls_df = spark.createDataFrame(bls_pd) \
    .select(
        F.trim(F.col("series_id")).alias("series_id"),
        F.col("year").cast("int").alias("year"),
        F.trim(F.col("period")).alias("period"),
        F.col("value").cast("double").alias("value")
    )

pop_spark_df = spark.createDataFrame(pop_pd) \
    .select(F.col("year").cast("int").alias("year"), F.col("Population").cast("long").alias("Population"))

# ---------- Task 1: Population mean & stddev for 2013-2018 ----------
print("\nTask 1: Population stats (2013-2018)")
pop_stats_df = pop_spark_df.filter((F.col("year") >= 2013) & (F.col("year") <= 2018)) \
    .agg(
        F.mean("Population").alias("mean_population"),
        F.stddev_pop("Population").alias("stddev_population")  
    )

pop_stats_df.show(truncate=False)
# write CSV locally just for refrence
pop_stats_df.toPandas().to_csv(os.path.join(OUTPUT_DIR, "population_stats_2013_2018.csv"), index=False)

# ---------- Task 2: Best year per series_id ----------
print("\nTask 2: Best year per series_id (sum of quarterly values)")


q_df = bls_df.filter(F.col("period").rlike(r"^Q\d{2}$"))

yearly_sum = q_df.groupBy("series_id", "year").agg(F.sum("value").alias("yearly_sum"))

#Using Windows function for calculating yearly sum
w = Window.partitionBy("series_id").orderBy(F.desc("yearly_sum"), F.asc("year"))
best_df = yearly_sum.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).select("series_id", "year", F.col("yearly_sum").alias("value"))

best_df.show(20, truncate=False)
best_df.toPandas().to_csv(os.path.join(OUTPUT_DIR, "best_year_per_series.csv"), index=False)


# ---------- Task 3: PRS30006032 Q01 with population ----------
print("\nTask 3: PRS30006032 period=Q01 with same-year population")

target_df = bls_df.filter((F.col("series_id") == "PRS30006032") & (F.col("period") == "Q01"))

prs_with_pop = target_df.join(pop_spark_df, on="year", how="left").select("series_id", "year", "period", "value", "Population")
prs_with_pop.show(50, truncate=False)
prs_with_pop.toPandas().to_csv(os.path.join(OUTPUT_DIR, "prs30006032_q01_with_population.csv"), index=False)


# ----------Sample for uploading code to s3 ----------
if WRITE_TO_S3:
    import boto3, tempfile
    s3 = boto3.client("s3")
    
    def upload_local_file(local_path, bucket, key):
        s3.upload_file(local_path, bucket, key)
    upload_local_file(os.path.join(OUTPUT_DIR, "population_stats_2013_2018.csv"), S3_BUCKET, "reports/population_stats_2013_2018.csv")
    upload_local_file(os.path.join(OUTPUT_DIR, "best_year_per_series.csv"), S3_BUCKET, "reports/best_year_per_series.csv")
    upload_local_file(os.path.join(OUTPUT_DIR, "prs30006032_q01_with_population.csv"), S3_BUCKET, "reports/prs30006032_q01_with_population.csv")
    print("Uploaded CSVs to S3 bucket:", S3_BUCKET)

print(f"\nAll outputs written to local folder: {os.path.abspath(OUTPUT_DIR)}")
print("Files:")
for f in os.listdir(OUTPUT_DIR):
    print(" -", f)
