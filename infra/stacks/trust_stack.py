import aws_cdk.aws_iam as iam
from aws_cdk import App, CfnOutput, CfnParameter, Stack
from constructs import Construct


class TrustStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        ### Create parameters to provide at runtime

        # self.github_org = CfnParameter(self, "GitHubOrg", type="String", description="The GitHub organization that owns the repository.")

        self.github_repo = CfnParameter(
            self,
            "GitHubRepo",
            type="String",
            description="The GitHub repository that will run the action.",
        )

        ### Define the OpenID Connect (OIDC) provider for GitHub Actions (GHA).
        ### The provider will be used by GHA workflows to assume a role which
        ### can be used to deploy CDK applications.
        self.github_provider = iam.CfnOIDCProvider(
            self,
            id="GitHubOIDCProvider",
            thumbprint_list=["1b511abead59c6ce207077c0bf0e0043b1382612"],
            url="https://token.actions.githubusercontent.com",
            client_id_list=["sts.amazonaws.com"],
        )

        ### Define the Role to be assumed by GHA workflows
        self.github_actions_role = iam.Role(
            self,
            "GitHubActionsRole",
            assumed_by=iam.FederatedPrincipal(
                federated=self.github_provider.attr_arn,
                conditions={
                    "StringLike": {
                        "token.actions.githubusercontent.com:sub": [
                            f"repo:{self.github_repo.value_as_string}:ref:refs/heads/main",
                        ],
                    },
                    "StringEquals": {
                        "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
                    },
                },
                assume_role_action="sts:AssumeRoleWithWebIdentity",
            ),
        )

        ### Define a policy to permit assuming default CDK roles.
        self.assume_cdk_deployment_roles_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["sts:AssumeRole"],
            resources=["arn:aws:iam::*:role/cdk-*"],
            conditions={
                "StringEquals": {
                    "aws:ResourceTag/aws-cdk:bootstrap-role": [
                        "file-publishing",
                        "lookup",
                        "deploy",
                    ],
                },
            },
        )

        ### Add the policy statement to the previously create GHA Role
        self.github_actions_role.add_to_policy(self.assume_cdk_deployment_roles_policy)

        CfnOutput(
            self,
            id="GitHubActionsRoleArn",
            value=self.github_actions_role.role_arn,
            description="The role ARN for Github Actions to use for the deployment.",
        )
