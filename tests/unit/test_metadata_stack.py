import json
import pytest

from aws_cdk import core
from metadata.metadata_stack import MetadataStack


def get_template():
    app = core.App()
    MetadataStack(app, "metadata")
    return json.dumps(app.synth().get_stack("metadata").template)


def test_sqs_queue_created():
    assert("AWS::SQS::Queue" in get_template())


def test_sns_topic_created():
    assert("AWS::SNS::Topic" in get_template())
