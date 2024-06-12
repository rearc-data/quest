from data_syncer import DataSyncer
import os

def lambda_handler(event, context):
    """The lambda function is sync'ing data from an API to an S3 bucket. 

    Parameters
    ----------
    event: dict, required
        Input event to the Lambda function

    context: object, required
        Lambda Context runtime methods and attributes

    Returns
    ------
        list: presigned_urls for the files available in the S3 bucket
    """
    url = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
    s3_bucket = os.getenv("REARCQUESTBUCKET_BUCKET_NAME")
    syncer = DataSyncer(url, s3_bucket)
    syncer.extract_census_data().load_census_data_to_s3()
    return "DataSyncer Completed"
