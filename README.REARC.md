# Rearc Data Quest

### Q. What is this quest?
It is a fun way to assess your data skills. It is also a good representative sample of the work we do at Rearc.

### Q. So what skills should I have?
* Data management / data engineering concepts.
* Programming language (python, java, scala, etc).
* AWS knowledge (Lambda, SQS, CloudWatch logs).
* Infrastructure-as-code (Terraform, CloudFormation, etc)

### Q. What do I have to do?
This quest consists of 4 different parts. Putting all 4 parts together we will have a Data Pipeline architecture.
- Part 1 and Part 2 will showcase your skills with data management, AWS concepts, and your overall data engineering skillset.
  The goal is to source data from different places and store it in-house.
- Part 3 will showcase your data analytics skills. The goal is to find some interesting insights with data.
- Lastly, Part 4 will put all the pieces together. The goal here is to showcase your experience with automation and AWS services.

#### Part 1: AWS S3 & Sourcing Datasets
1. Republish [this open dataset](https://download.bls.gov/pub/time.series/pr/) in Amazon S3 and share with us a link.
    - You may run into 403 Forbidden errors as you test accessing this data. There is a way to comply with the BLS data access policies and re-gain access to fetch this data programatically - we have included some hints as to how to do this at the bottom of this README in the Q/A section.
2. Script this process so the files in the S3 bucket are kept in sync with the source when data on the website is updated, added, or deleted.
    - Don't rely on hard coded names - the script should be able to handle added or removed files.
    - Ensure the script doesn't upload the same file more than once.

#### Part 2: APIs
1. Create a script that will fetch data from [this API](https://datausa.io/api/data?drilldowns=Nation&measures=Population).
   You can read the documentation [here](https://datausa.io/about/api/)
2. Save the result of this API call as a JSON file in S3.

#### Part 3: Data Analytics
0. Load both the csv file from **Part 1** `pr.data.0.Current` and the json file from **Part 2**
   as dataframes ([Spark](https://spark.apache.org/docs/1.6.1/api/java/org/apache/spark/sql/DataFrame.html),
                  [Pyspark](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html),
                  [Pandas](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html),
                  [Koalas](https://koalas.readthedocs.io/en/latest/),
                  etc).

1. Using the dataframe from the population data API (Part 2),
   generate the mean and the standard deviation of the annual US population across the years [2013, 2018] inclusive.

2. Using the dataframe from the time-series (Part 1),
   For every series_id, find the *best year*: the year with the max/largest sum of "value" for all quarters in that year. Generate a report with each series id, the best year for that series, and the summed value for that year.
   For example, if the table had the following values:

    | series_id   | year | period | value |
    |-------------|------|--------|-------|
    | PRS30006011 | 1995 | Q01    | 1     |
    | PRS30006011 | 1995 | Q02    | 2     |
    | PRS30006011 | 1996 | Q01    | 3     |
    | PRS30006011 | 1996 | Q02    | 4     |
    | PRS30006012 | 2000 | Q01    | 0     |
    | PRS30006012 | 2000 | Q02    | 8     |
    | PRS30006012 | 2001 | Q01    | 2     |
    | PRS30006012 | 2001 | Q02    | 3     |

    the report would generate the following table:

    | series_id   | year | value |
    |-------------|------|-------|
    | PRS30006011 | 1996 | 7     |
    | PRS30006012 | 2000 | 8     |

3. Using both dataframes from Part 1 and Part 2, generate a report that will provide the `value`
   for `series_id = PRS30006032` and `period = Q01` and the `population` for that given year (if available in the population dataset)

    | series_id   | year | period | value | Population |
    |-------------|------|--------|-------|------------|
    | PRS30006032 | 2018 | Q01    | 1.9   | 327167439  |

    **Hints:** when working with public datasets you sometimes might have to perform some data cleaning first.
   For example, you might find it useful to perform [trimming](https://stackoverflow.com/questions/35540974/remove-blank-space-from-data-frame-column-values-in-spark) of whitespaces before doing any filtering or joins


4. Submit your analysis, your queries, and the outcome of the reports as a [.ipynb](https://fileinfo.com/extension/ipynb) file.

#### Part 4: Infrastructure as Code & Data Pipeline with AWS CDK
0. Using [AWS CloudFormation](https://aws.amazon.com/cloudformation/), [AWS CDK](https://aws.amazon.com/cdk/) or [Terraform](https://www.terraform.io/), create a data pipeline that will automate the steps above.
1. The deployment should include a Lambda function that executes
   Part 1 and Part 2 (you can combine both in 1 lambda function). The lambda function will be scheduled to run daily.
2. The deployment should include an SQS queue that will be populated every time the JSON file is written to S3. (Hint: [S3 - Notifications](https://docs.aws.amazon.com/AmazonS3/latest/userguide/NotificationHowTo.html))
3. For every message on the queue - execute a Lambda function that outputs the reports from Part 3 (just logging the results of the queries would be enough. No .ipynb is required).


### Q. Do I have to do all these?
You can do as many as you like. We suspect though that once you start you won't be able to stop. It's addictive.

### Q. What do I have to submit?
1. Link to data in S3 and source code (Step 1)
2. Source code (Step 2)
3. Source code in .ipynb file format and results (Step 3)
4. Source code of the data pipeline infrastructure (Step 4)

### Q. What if I successfully complete all the steps?
We have many more for you to solve as a member of the Rearc team!

### Q. What if I fail?
Do. Or do not. There is no fail.

### Q. Can I share this quest with others?
No.

### Q. How do I get around the 403 error when I try to fetch BLS data?
<details>
<summary>Hint 1</summary>
  The BLS data access policies can be found here: https://www.bls.gov/bls/pss.htm
</details>
<details>
<summary>Hint 2</summary>
  The policy page says:

```BLS also reserves the right to block robots that do not contain information that can be used to contact the owner. Blocking may occur in real time.```

How could you add information to your programmatic access requests to let BLS contact you?
</details>
<details>
<summary>Hint 3</summary>
  Adding a <code>User-Agent</code> header to your request with contact information will comply with the BLS data policies and allow you to keep accessing their data programmatically.
</details>
