import aws_cdk as core
import aws_cdk.assertions as assertions
from stacks.rearc_quest_stack import RearcQuestStack


# example tests. To run these tests, uncomment this file along with the example
# resource in stacks/rearc_quest_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = RearcQuestStack(app, "RearcQuestStack")
    template = assertions.Template.from_stack(stack)


#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
