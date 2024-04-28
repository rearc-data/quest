from aws_cdk import App, BundlingOptions, DockerImage, Duration, Stack
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_lambda_event_sources as lambda_event_sources
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_notifications as s3_notifications
from aws_cdk import aws_sqs as sqs
from constructs import Construct


class RearcQuestStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        ### Create the S3 Bucket

        self.rearc_quest_bucket = s3.Bucket.from_bucket_name(
            self,
            id="rearc-quest-bucket",
            bucket_name="790890014576-rearc-data-quest-bucket",
        )

        ### Create Policies and Roles for the Lambdas

        self.ingestion_policy = iam.PolicyStatement(
            sid="IngestionPolicy",
            actions=["s3:PutObject", "s3:PutObjectAcl", "s3:DeleteObject"],
            resources=[
                self.rearc_quest_bucket.bucket_arn,
                f"{self.rearc_quest_bucket.bucket_arn}/*",
            ],
        )

        self.basic_execution_role_lambda = iam.ManagedPolicy.from_managed_policy_arn(
            self,
            id="AWSLambdaBasicExecutionRole",
            managed_policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
        )

        self.ingestion_role_lambda = iam.Role(
            self,
            id="IngestionLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        )

        self.ingestion_role_lambda.add_to_policy(self.ingestion_policy)
        self.ingestion_role_lambda.add_managed_policy(self.basic_execution_role_lambda)

        self.processing_policy = iam.PolicyStatement(
            sid="ProcessingPolicy",
            actions=[
                "s3:GetObject",
                "s3:GetObjectAcl",
                "s3:PutObject",
                "s3:PutObjectAcl",
                "s3:ListBucket",
                "s3:DeleteObject",
            ],
            resources=[
                self.rearc_quest_bucket.bucket_arn,
                f"{self.rearc_quest_bucket.bucket_arn}/*",
            ],
        )

        self.processing_role_lambda = iam.Role(
            self,
            id="ProcessingLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        )

        self.processing_role_lambda.add_to_policy(self.processing_policy)
        self.processing_role_lambda.add_managed_policy(self.basic_execution_role_lambda)

        ### Create Scheduled Lambda to Poll for Population and Productivity Data

        self.data_ingestion_lambda = lambda_.Function(
            self,
            id="DataIngestionLambda",
            function_name="DataIngestionLambda",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="ingestion.handler",
            code=lambda_.Code.from_asset(
                path="lambda/ingestion/",
                bundling=BundlingOptions(
                    image=DockerImage.from_registry("python:3.11.9-slim-bullseye"),
                    command=[
                        "bash",
                        "-c",
                        "pip install --no-cache -r requirements.txt -t /asset-output && cp -au . /asset-output",
                    ],
                ),
            ),
            role=self.ingestion_role_lambda,
            timeout=Duration.seconds(60),
        )

        ### Create EvenBridge rule to trigger the Ingestion Lambda daily, and set the target of the rule to be the Lambda created above

        self.event_rule = events.Rule(
            self,
            id="DataIngestionRule",
            schedule=events.Schedule.cron(day="*", hour="0", minute="0"),
            targets=[targets.LambdaFunction(self.data_ingestion_lambda)],
        )

        ### Create IAM Role and policy for the EventBridge rule

        self.event_bridge_role = iam.Role(
            self,
            id="RearcDailyCronRole",
            assumed_by=iam.ServicePrincipal("events.amazonaws.com"),
        )

        self.event_bridge_role.add_to_policy(
            iam.PolicyStatement(
                actions=["lambda:InvokeFunction"],
                resources=[self.data_ingestion_lambda.function_arn],
            )
        )

        ### Setup the SQS queue (and DLQ for failed messages) to receive notifications whenever a new file is ingested

        self.new_data_dead_letter_queue = sqs.Queue(
            self, id="NewDataDeadLetterQueue", retention_period=Duration.days(14)
        )

        self.new_data_queue = sqs.Queue(
            self,
            id="NewDataReceivedQueue",
            dead_letter_queue=sqs.DeadLetterQueue(
                queue=self.new_data_dead_letter_queue, max_receive_count=3
            ),
            visibility_timeout=Duration.seconds(60),
        )

        ### Create IAM Role for the S3 Event Notifications to send events to the SQS queue

        self.s3_event_notifications_role = iam.Role(
            self,
            id="S3EventNotificationRole",
            assumed_by=iam.ServicePrincipal("s3.amazonaws.com"),
        )

        self.s3_event_notifications_role.add_to_policy(
            iam.PolicyStatement(
                sid="SendMessagePolicy",
                actions=["sqs:SendMessage"],
                resources=[self.new_data_queue.queue_arn],
            )
        )

        ### Add S3 Event Notifications whenever a new object is created in the bucket

        self.rearc_quest_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3_notifications.SqsDestination(self.new_data_queue),
            s3.NotificationKeyFilter(prefix="data/population", suffix=".json"),
        )

        ### Create a Lambda to process and analyze the data once a notification is received in the queue

        self.data_processing_lambda = lambda_.Function(
            self,
            id="DataProcessingLambda",
            function_name="DataProcessingLambda",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="processing.handler",
            code=lambda_.Code.from_asset(
                path="lambda/processing/",
                bundling=BundlingOptions(
                    image=DockerImage.from_registry("python:3.11.9-slim-bullseye"),
                    command=[
                        "bash",
                        "-c",
                        "pip install --no-cache -r requirements.txt -t /asset-output && cp -au . /asset-output",
                    ],
                ),
            ),
            role=self.processing_role_lambda,
            timeout=Duration.seconds(60),
        )

        self.data_processing_lambda.add_event_source(
            lambda_event_sources.SqsEventSource(
                queue=self.new_data_queue,
                batch_size=1,
            )
        )


# app = App()
# RearcQuestStack(app, "RearcQuestStack")
# app.synth()
