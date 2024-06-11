from random import randint
import file_syncer
from file_syncer import FileSyncer

def lambda_handler(event, context):
    """The lambda function is sync'ing files from a file structure website to an S3 bucket. 
    This lambda function generates a list of pre-signed URLs for the files available in the S3 bucket that can be used to send SES emails.

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
    host_url = "https://download.bls.gov"
    s3_bucket = "noventa-scratch-bucket"
    sync = FileSyncer(host_url, s3_bucket)

    sync.extract_productivity_cost_data()
    sync.load_productivity_cost_data()
    
    return {"presigned_urls": sync.generate_presigned_urls()}
