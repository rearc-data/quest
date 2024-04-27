#!/usr/bin/env python3

import aws_cdk as cdk
from stacks.rearc_quest_stack import RearcQuestStack
from stacks.trust_stack import TrustStack

app = cdk.App()

TrustStack(app, "TrustStack")
RearcQuestStack(app, "RearcQuestStack")

app.synth()
