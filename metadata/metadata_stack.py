import os
from typing import Protocol
from attr import attributes

from aws_cdk import (
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_notifications as s3_notifications,
    aws_lambda_event_sources as lambda_event_sources,
    aws_sqs as sqs,
    aws_sns as sns,
    aws_lambda as lambda_,
    aws_sns_subscriptions as subs,
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_events_targets as targets,
    core
)

class MetadataStack(core.Stack):

    def __init__(self, scope: core.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # lambda role for metadata lambda
        metadata_lambda_role = iam.Role(self, "metadataLambdaRole", assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"), inline_policies=[
            iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "logs:CreateLogGroup",
                            "logs:CreateLogStream",
                            "logs:PutLogEvents"
                        ],
                        # ${AWS::Partition}
                        resources=["arn:aws:logs:*:*:*"]
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "s3:GetObject",
                            "s3:ListBucket",
                            "s3:PutObject"
                        ],
                        resources=["arn:aws:s3:::*"]
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "dynamodb:PutItem",
                            "dynamodb:UpdateItem",
                            "dynamodb:DeleteItem"
                        ],
                        resources=["arn:aws:dynamodb:*:*:table/metaData*"]
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "rekognition:*"
                        ],
                        # to be narrowed down
                        resources=["*"]
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "sqs:*"
                        ],
                        # to be narrowed down
                        resources=["*"]
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "sns:*"
                        ],
                        # to be narrowed down
                        resources=["arn:aws:sns:::*"]
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "iam:PassRole"
                        ],
                        # to be narrowed down
                        resources=["*"]
                    )
                ]
            )
        ])

        video_processor_lambda_role = iam.Role(self, "videoProcessorLambdaRole", assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"), inline_policies=[
            iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "logs:CreateLogGroup",
                            "logs:CreateLogStream",
                            "logs:PutLogEvents"
                        ],
                        # ${AWS::Partition}
                        resources=["arn:aws:logs:*:*:*"]
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "s3:GetObject",
                            "s3:ListBucket",
                            "s3:PutObject"
                        ],
                        resources=["arn:aws:s3:::*"]
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "dynamodb:PutItem",
                            "dynamodb:UpdateItem",
                            "dynamodb:DeleteItem"
                        ],
                        resources=["arn:aws:dynamodb:*:*:table/metaData*"]
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "rekognition:*"
                        ],
                        # to be narrowed down
                        resources=["*"]
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "sqs:*"
                        ],
                        # to be narrowed down
                        resources=["*"]
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "sns:*"
                        ],
                        # to be narrowed down
                        resources=["arn:aws:sns:::*"]
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "iam:PassRole"
                        ],
                        # to be narrowed down
                        resources=["*"]
                    ),
                    # statement for lambda to invoke eventbridge
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "events:PutEvents"
                        ],
                        resources=["arn:aws:events:*:*:*"]
                    )
                ]
            )
        ])

        event_processor_lambda_role = iam.Role(self, "eventProcessorLambdaRole", assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"), inline_policies=[
            iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "logs:CreateLogGroup",
                            "logs:CreateLogStream",
                            "logs:PutLogEvents"
                        ],
                        # ${AWS::Partition}
                        resources=["arn:aws:logs:*:*:*"]
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "s3:GetObject",
                            "s3:ListBucket",
                            "s3:PutObject"
                        ],
                        resources=["arn:aws:s3:::*"]
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "dynamodb:PutItem",
                            "dynamodb:UpdateItem",
                            "dynamodb:DeleteItem"
                        ],
                        resources=["arn:aws:dynamodb:*:*:table/metaData*"]
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "iam:PassRole"
                        ],
                        # to be narrowed down
                        resources=["*"]
                    )
                ]
            )
        ])

        reko_role = iam.Role(self, "rekoRole", assumed_by=iam.ServicePrincipal("rekognition.amazonaws.com"), inline_policies=[
            iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "s3:*"
                        ],
                        resources=["*"]
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "rekognition:*"
                        ],
                        # to be narrowed down
                        resources=["*"]
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "sqs:*"
                        ],
                        # to be narrowed down
                        resources=["*"]
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "sns:*"
                        ],
                        # to be narrowed down
                        resources=["*"]
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "iam:PassRole"
                        ],
                        # to be narrowed down, should be service role iteself
                        resources=["*"]
                    )
                ]
            )
        ])

        # create s3 bucket to store original video
        # get stack name
        stack_name = self.stack_name
        originalBucketName = stack_name + "-original-video"
        mediaAssetsOriginBucket = s3.Bucket(self, "mediaAssetsOrigin", bucket_name=originalBucketName, versioned=True, removal_policy=core.RemovalPolicy.DESTROY, encryption=s3.BucketEncryption.S3_MANAGED, block_public_access=s3.BlockPublicAccess(block_public_acls=True, block_public_policy=True, ignore_public_acls=True, restrict_public_buckets=True))

        # create s3 bucket to store processed video
        processedBucketName = stack_name + "-processed-video"
        mediaAssetsProcessedBucket = s3.Bucket(self, "mediaAssetsProcessed", bucket_name=processedBucketName, versioned=True, removal_policy=core.RemovalPolicy.DESTROY, encryption=s3.BucketEncryption.S3_MANAGED, block_public_access=s3.BlockPublicAccess(block_public_acls=True, block_public_policy=True, ignore_public_acls=True, restrict_public_buckets=True))

        # create sqs queue, adjust duration according to video length
        queue = sqs.Queue(self, "metadataQueue", queue_name="rekQueue", visibility_timeout=core.Duration.seconds(900), retention_period=core.Duration.days(7))

        # create sns topic and subscription to the queue
        topic = sns.Topic(self, "metadataTopic", topic_name="rekTopic")
        topic.add_subscription(subs.SqsSubscription(queue))

        # Set SQS attributes
        # policy = """{{
        # "Version":"2012-10-17",
        # "Statement":[
        #     {{
        #     "Sid":"MyPolicy",
        #     "Effect":"Allow",
        #     "Principal" : {{"AWS" : "*"}},
        #     "Action":"SQS:SendMessage",
        #     "Resource": "{}",
        #     "Condition":{{
        #         "ArnEquals":{{
        #         "aws:SourceArn": "{}"
        #         }}
        #     }}
        #     }}
        # ]
        # }}""".format(queue.queue_arn, topic.topic_arn)

        queue.add_to_resource_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW, 
            principals=[iam.ServicePrincipal('sns.amazonaws.com')], 
            actions=["sqs:SendMessage"], 
            resources=[queue.queue_arn], 
            conditions={"ArnEquals": {"aws:SourceArn": topic.topic_arn}}
        ))  

        # create lambda function and event source from s3 bucket
        fnMetadata = lambda_.DockerImageFunction(self, "metadata", code=lambda_.DockerImageCode.from_image_asset("metadata/meta"), environment={'TOPIC_ARN': topic.topic_arn, 'REKO_ARN':reko_role.role_arn, 'QUEUE_URL': queue.queue_url, 'Processed_Bucket': processedBucketName}, timeout=core.Duration.seconds(900), role=metadata_lambda_role, memory_size=4096)

        mediaAssetsOriginBucket.add_event_notification(s3.EventType.OBJECT_CREATED, s3_notifications.LambdaDestination(fnMetadata))

        # create dynamodb table to store metadata
        ddb = dynamodb.Table(self, "MetadataTable", partition_key=dynamodb.Attribute(name="id", type=dynamodb.AttributeType.STRING), removal_policy=core.RemovalPolicy.DESTROY, table_name="metaData", read_capacity=1, write_capacity=1)

        # create eventbridge rule to trigger lambda function (eventProcessor)
        bus = events.EventBus(self, "eventBus", event_bus_name="eventBus")

        # create lambda function and event source from sqs
        fnVideoProcessor = lambda_.DockerImageFunction(self, "videoProcessor", code=lambda_.DockerImageCode.from_image_asset("metadata/processor"), environment={'QUEUE_URL': queue.queue_url, 'Processed_Bucket': processedBucketName, 'ORIGINAL_BUCKET': originalBucketName, 'DYNAMODB_TABLE': ddb.table_name, "EVENT_BUS_NAME": bus.event_bus_name}, timeout=core.Duration.seconds(900), role=video_processor_lambda_role, memory_size=1024)

        fnVideoProcessor.add_event_source(lambda_event_sources.SqsEventSource(queue, batch_size=1))

        # create lambda function and event source from eventbridge
        fnEventProcessor = lambda_.DockerImageFunction(self, "eventProcessor", code=lambda_.DockerImageCode.from_image_asset("metadata/event"), environment={'Processed_Bucket': processedBucketName, 'ORIGINAL_BUCKET': originalBucketName, 'DYNAMODB_TABLE': ddb.table_name}, timeout=core.Duration.seconds(900), role=event_processor_lambda_role, memory_size=1024)

        rule = events.Rule(self, "eventRule", event_bus=bus, description="eventRule", enabled=True, event_pattern=events.EventPattern(source=["custom"], detail_type=["videoShotsAndGif"])).add_target(targets.LambdaFunction(fnEventProcessor))


        


