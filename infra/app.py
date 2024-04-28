#!/usr/bin/env python3

import aws_cdk as cdk
from stacks.rearc_quest_stack import RearcQuestStack

app = cdk.App()

RearcQuestStack(app, "RearcQuestStack")

app.synth()
