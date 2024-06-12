from reporter import Reporter
import os

def lambda_handler(event, context):
    """The lambda function is reporting on summary statistics for PR and Census Data. 

    Parameters
    ----------
    event: dict, required
        Input event to the Lambda function

    context: object, required
        Lambda Context runtime methods and attributes

    Returns
    ------
        Completion String
    """
    s3_bucket = os.getenv("REARCQUESTBUCKET_BUCKET_NAME")

    reporter = Reporter()
    reporter.load_data(s3_bucket) \
            .clean_data() \
            .print_census_summary() \
            .print_pr_summary() \
            .print_report()
    
    return "Reporting Completed"
