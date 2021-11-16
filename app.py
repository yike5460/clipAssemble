from aws_cdk import core

from metadata.metadata_stack import MetadataStack


# env = core.Environment(account="xx", region="cn-north-1")

app = core.App()
MetadataStack(app, "metadata")

app.synth()
