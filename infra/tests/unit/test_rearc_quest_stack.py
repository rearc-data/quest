import aws_cdk as core
from aws_cdk.assertions import Capture, Match, Template
from stacks.rearc_quest_stack import RearcQuestStack


def test_synthesizes_stack():
    app = core.App()
    stack = RearcQuestStack(app, "RearcQuestStack")
    template = Template.from_stack(stack)

    ### Start with a raw resource count from the CDK Template

    template.resource_count_is("AWS::IAM::Role", 5)
    template.resource_count_is("AWS::IAM::Policy", 5)
    template.resource_count_is("AWS::Lambda::Function", 3)
    template.resource_count_is("AWS::SQS::Queue", 2)
    template.resource_count_is("Custom::S3BucketNotifications", 1)
    template.resource_count_is("AWS::Events::Rule", 1)
    template.resource_count_is("AWS::Lambda::Permission", 1)
    template.resource_count_is("AWS::SQS::QueuePolicy", 1)
    template.resource_count_is("AWS::Lambda::EventSourceMapping", 1)

    ### Resource Properties

    template.has_resource_properties(
        "Custom::S3BucketNotifications",
        {
            "ServiceToken": {
                "Fn::GetAtt": [
                    "BucketNotificationsHandler050a0587b7544547bf325f094a3db8347ECC3691",
                    "Arn",
                ]
            },
            "BucketName": "790890014576-rearc-data-quest-bucket",
            "NotificationConfiguration": {
                "QueueConfigurations": [
                    {
                        "Events": ["s3:ObjectCreated:*"],
                        "Filter": {
                            "Key": {
                                "FilterRules": [
                                    {"Name": "suffix", "Value": ".json"},
                                    {"Name": "prefix", "Value": "data/population"},
                                ]
                            }
                        },
                        "QueueArn": {
                            "Fn::GetAtt": ["NewDataReceivedQueueCD990CC6", "Arn"]
                        },
                    }
                ]
            },
            "Managed": False,
        },
    )

    template.has_resource_properties(
        "AWS::IAM::Role",
        {
            "AssumeRolePolicyDocument": {
                "Statement": [
                    {
                        "Action": "sts:AssumeRole",
                        "Effect": "Allow",
                        "Principal": {"Service": "lambda.amazonaws.com"},
                    }
                ],
                "Version": "2012-10-17",
            },
            "ManagedPolicyArns": [
                "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
            ],
        },
    )

    template.has_resource_properties(
        "AWS::IAM::Role",
        {
            "AssumeRolePolicyDocument": {
                "Statement": [
                    {
                        "Action": "sts:AssumeRole",
                        "Effect": "Allow",
                        "Principal": {"Service": "lambda.amazonaws.com"},
                    }
                ],
                "Version": "2012-10-17",
            },
            "ManagedPolicyArns": [
                "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
            ],
        },
    )

    template.has_resource_properties(
        "AWS::Lambda::Function",
        {
            "Code": {
                "S3Bucket": {
                    "Fn::Sub": "cdk-hnb659fds-assets-${AWS::AccountId}-${AWS::Region}"
                },
                "S3Key": "fca1b086b3ad2b0e3e2cd6c5ef76f89b7bade877f24c535ebeed67bd20d2de12.zip",
            },
            "FunctionName": "DataIngestionLambda",
            "Handler": "ingestion.handler",
            "Role": {"Fn::GetAtt": ["IngestionLambdaRole9BF0731D", "Arn"]},
            "Runtime": "python3.11",
            "Timeout": 60,
        },
    )

    template.has_resource_properties(
        "AWS::Events::Rule",
        {
            "ScheduleExpression": "cron(0 0 * * ? *)",
            "State": "ENABLED",
            "Targets": [
                {
                    "Arn": {"Fn::GetAtt": ["DataIngestionLambda0C5F393C", "Arn"]},
                    "Id": "Target0",
                }
            ],
        },
    )

    template.has_resource_properties(
        "AWS::Lambda::Permission",
        {
            "Action": "lambda:InvokeFunction",
            "FunctionName": {"Fn::GetAtt": ["DataIngestionLambda0C5F393C", "Arn"]},
            "Principal": "events.amazonaws.com",
            "SourceArn": {"Fn::GetAtt": ["DataIngestionRule543FBA50", "Arn"]},
        },
    )

    template.has_resource_properties(
        "AWS::IAM::Role",
        {
            "AssumeRolePolicyDocument": {
                "Statement": [
                    {
                        "Action": "sts:AssumeRole",
                        "Effect": "Allow",
                        "Principal": {"Service": "events.amazonaws.com"},
                    }
                ],
                "Version": "2012-10-17",
            }
        },
    )

    template.has_resource_properties(
        "AWS::SQS::Queue", {"MessageRetentionPeriod": 1209600}
    )

    template.has_resource_properties(
        "AWS::SQS::Queue",
        {
            "RedrivePolicy": {
                "deadLetterTargetArn": {
                    "Fn::GetAtt": ["NewDataDeadLetterQueue5E31EFF8", "Arn"]
                },
                "maxReceiveCount": 3,
            },
            "VisibilityTimeout": 60,
        },
    )

    template.has_resource_properties(
        "AWS::IAM::Role",
        {
            "AssumeRolePolicyDocument": {
                "Statement": [
                    {
                        "Action": "sts:AssumeRole",
                        "Effect": "Allow",
                        "Principal": {"Service": "s3.amazonaws.com"},
                    }
                ],
                "Version": "2012-10-17",
            }
        },
    )

    template.has_resource_properties(
        "AWS::Lambda::Function",
        {
            "Description": 'AWS CloudFormation handler for "Custom::S3BucketNotifications" resources (@aws-cdk/aws-s3)',
            "Code": {
                "ZipFile": 'import boto3  # type: ignore\nimport json\nimport logging\nimport urllib.request\n\ns3 = boto3.client("s3")\n\nEVENTBRIDGE_CONFIGURATION = \'EventBridgeConfiguration\'\nCONFIGURATION_TYPES = ["TopicConfigurations", "QueueConfigurations", "LambdaFunctionConfigurations"]\n\ndef handler(event: dict, context):\n  response_status = "SUCCESS"\n  error_message = ""\n  try:\n    props = event["ResourceProperties"]\n    notification_configuration = props["NotificationConfiguration"]\n    managed = props.get(\'Managed\', \'true\').lower() == \'true\'\n    stack_id = event[\'StackId\']\n    old = event.get("OldResourceProperties", {}).get("NotificationConfiguration", {})\n    if managed:\n      config = handle_managed(event["RequestType"], notification_configuration)\n    else:\n      config = handle_unmanaged(props["BucketName"], stack_id, event["RequestType"], notification_configuration, old)\n    s3.put_bucket_notification_configuration(Bucket=props["BucketName"], NotificationConfiguration=config)\n  except Exception as e:\n    logging.exception("Failed to put bucket notification configuration")\n    response_status = "FAILED"\n    error_message = f"Error: {str(e)}. "\n  finally:\n    submit_response(event, context, response_status, error_message)\n\ndef handle_managed(request_type, notification_configuration):\n  if request_type == \'Delete\':\n    return {}\n  return notification_configuration\n\ndef handle_unmanaged(bucket, stack_id, request_type, notification_configuration, old):\n  def with_id(n):\n    n[\'Id\'] = f"{stack_id}-{hash(json.dumps(n, sort_keys=True))}"\n    return n\n\n  external_notifications = {}\n  existing_notifications = s3.get_bucket_notification_configuration(Bucket=bucket)\n  for t in CONFIGURATION_TYPES:\n    if request_type == \'Update\':\n        ids = [with_id(n) for n in old.get(t, [])]\n        old_incoming_ids = [n[\'Id\'] for n in ids]\n        external_notifications[t] = [n for n in existing_notifications.get(t, []) if not n[\'Id\'] in old_incoming_ids]\n    elif request_type == \'Create\':\n        external_notifications[t] = [n for n in existing_notifications.get(t, [])]\n  if EVENTBRIDGE_CONFIGURATION in existing_notifications:\n    external_notifications[EVENTBRIDGE_CONFIGURATION] = existing_notifications[EVENTBRIDGE_CONFIGURATION]\n\n  if request_type == \'Delete\':\n    return external_notifications\n\n  notifications = {}\n  for t in CONFIGURATION_TYPES:\n    external = external_notifications.get(t, [])\n    incoming = [with_id(n) for n in notification_configuration.get(t, [])]\n    notifications[t] = external + incoming\n\n  if EVENTBRIDGE_CONFIGURATION in notification_configuration:\n    notifications[EVENTBRIDGE_CONFIGURATION] = notification_configuration[EVENTBRIDGE_CONFIGURATION]\n  elif EVENTBRIDGE_CONFIGURATION in external_notifications:\n    notifications[EVENTBRIDGE_CONFIGURATION] = external_notifications[EVENTBRIDGE_CONFIGURATION]\n\n  return notifications\n\ndef submit_response(event: dict, context, response_status: str, error_message: str):\n  response_body = json.dumps(\n    {\n      "Status": response_status,\n      "Reason": f"{error_message}See the details in CloudWatch Log Stream: {context.log_stream_name}",\n      "PhysicalResourceId": event.get("PhysicalResourceId") or event["LogicalResourceId"],\n      "StackId": event["StackId"],\n      "RequestId": event["RequestId"],\n      "LogicalResourceId": event["LogicalResourceId"],\n      "NoEcho": False,\n    }\n  ).encode("utf-8")\n  headers = {"content-type": "", "content-length": str(len(response_body))}\n  try:\n    req = urllib.request.Request(url=event["ResponseURL"], headers=headers, data=response_body, method="PUT")\n    with urllib.request.urlopen(req) as response:\n      print(response.read().decode("utf-8"))\n    print("Status code: " + response.reason)\n  except Exception as e:\n      print("send(..) failed executing request.urlopen(..): " + str(e))\n'
            },
            "Handler": "index.handler",
            "Role": {
                "Fn::GetAtt": [
                    "BucketNotificationsHandler050a0587b7544547bf325f094a3db834RoleB6FB88EC",
                    "Arn",
                ]
            },
            "Runtime": "python3.11",
            "Timeout": 300,
        },
    )

    template.has_resource_properties(
        "AWS::Lambda::Function",
        {
            "Code": {
                "S3Bucket": {
                    "Fn::Sub": "cdk-hnb659fds-assets-${AWS::AccountId}-${AWS::Region}"
                },
                "S3Key": "863e2a772b2f9c5ac4d82341238e1b3f75629b31dcf5887c204af6c1ac87afd8.zip",
            },
            "FunctionName": "DataProcessingLambda",
            "Handler": "processing.handler",
            "Role": {"Fn::GetAtt": ["ProcessingLambdaRoleA5BAFD41", "Arn"]},
            "Runtime": "python3.11",
            "Timeout": 60,
        },
    )

    template.has_resource_properties(
        "AWS::Lambda::EventSourceMapping",
        {
            "BatchSize": 1,
            "EventSourceArn": {"Fn::GetAtt": ["NewDataReceivedQueueCD990CC6", "Arn"]},
            "FunctionName": {"Ref": "DataProcessingLambdaADD24C7E"},
        },
    )
