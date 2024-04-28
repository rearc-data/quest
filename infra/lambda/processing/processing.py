import logging
import os
from tempfile import TemporaryDirectory

import boto3
import pandas as pd

logger = logging.getLogger(name="processing")


BUCKET = "790890014576-rearc-data-quest-bucket"
REGION = "us-east-1"


def handler(event, context):

    client = boto3.client("s3", region_name=REGION)

    tmpdir = TemporaryDirectory()

    pop_data_file_path = os.path.join(tmpdir.name, "cleaned_data.json")
    bls_data_file_path = os.path.join(tmpdir.name, "pr.data.0.Current")

    client.download_file(
        Bucket=BUCKET,
        Key="data/population/cleaned_data.json",
        Filename=pop_data_file_path,
    )
    client.download_file(
        Bucket=BUCKET, Key="data/bls/pr.data.0.Current", Filename=bls_data_file_path
    )

    population_df = pd.read_json(pop_data_file_path)
    population_df.rename(columns=lambda x: x.lower(), inplace=True)

    years_of_interest_df = population_df[
        population_df["year"].between(2012, 2019, inclusive="neither")
    ]

    mean_usa_population = years_of_interest_df["population"].mean()
    std_dev_usa_population = years_of_interest_df["population"].std()

    productivity_df = pd.read_csv(bls_data_file_path, delimiter="\t")
    productivity_df.rename(columns=lambda x: x.strip(), inplace=True)
    trimmed_productivity_df = productivity_df.apply(
        lambda x: x.str.strip() if x.dtype == "object" else x
    )
    summed_by_year_series_df = (
        trimmed_productivity_df[["series_id", "year", "value"]]
        .groupby(["series_id", "year"], as_index=False)
        .sum()
    )
    greatest_year_by_series_df = summed_by_year_series_df.loc[
        summed_by_year_series_df.groupby("series_id")["value"].idxmax()
    ].reset_index(drop=True)

    population_productivity_df = pd.merge(
        trimmed_productivity_df[["series_id", "year", "period", "value"]],
        population_df[["year", "population"]],
        how="left",
        on=["year"],
    )

    greatest_year_file_path = os.path.join(tmpdir.name, "greatest_year_by_series.csv")
    population_productivity_file_path = os.path.join(
        tmpdir.name, "population_productivity.csv"
    )
    greatest_year_by_series_df.to_csv(greatest_year_file_path)
    population_productivity_df.to_csv(population_productivity_file_path)
    write_to_s3(
        os.path.join("outputs", "greatest_year_by_series.csv"), greatest_year_file_path
    )
    write_to_s3(
        os.path.join("outputs", "population_productivity.csv"),
        population_productivity_file_path,
    )

    logger.info("Mean USA Population from 2013-2018: %s", mean_usa_population)
    logger.info(
        "Standard Deviation of USA Population from 2013-2018: %s",
        std_dev_usa_population,
    )
    logger.info(
        "Greatest Year of Productivity By Series ID: %s", greatest_year_by_series_df
    )
    logger.info(
        "Population For Each Productivity Period: %s", population_productivity_df
    )


def write_to_s3(key, data, **kwargs):

    client = boto3.client("s3", region_name=REGION)
    client.put_object(Bucket=BUCKET, Key=key, Body=data, **kwargs)
    logger.info("Completed writing file to S3: %s", key)
