import logging
import subprocess
import uuid
import boto3
import os
import json
import sys
import time

from botocore.exceptions import ClientError

s3 = boto3.client('s3')
rek = boto3.client('rekognition')
sqs = boto3.client('sqs')
sns = boto3.client('sns')

dynamoDBTableName = "metaData"
dynamodb = boto3.resource("dynamodb")
dynamoDBTable = dynamodb.Table(dynamoDBTableName)
MediaAssetsSlicedBucket = "media-assets-sliced"

startJobId = ''

SIGNED_URL_EXPIRATION = 300     # The number of seconds that the Signed URL is valid
LOCAL_VIDEO_FILE = '/tmp/'+ 'local-input.mp4'
IFRAME_VIDEO_FILE = '/tmp/'+ str(uuid.uuid1()) + '-iframe-output.mp4'

LAMBDA_TASK_ROOT = os.environ.get('LAMBDA_TASK_ROOT')
# ffmpeg_path = os.path.join(LAMBDA_TASK_ROOT, 'ffmpeg')

CMD = ['ffmpeg', '-y', '-i', LOCAL_VIDEO_FILE, '-strict', '-2', '-qscale', '0', '-intra', IFRAME_VIDEO_FILE]
SHELL_CMD = ' '.join(CMD)
logger = logging.getLogger('boto3')
logger.setLevel(logging.INFO)

# add execution path to ffmpeg
os.environ['PATH'] = os.environ['PATH'] + ':' + os.environ['LAMBDA_TASK_ROOT']

# testing with command aws s3 rm s3://metadata-original-video/SampleVideo_1280x720_30mb.mp4 && aws s3 cp SampleVideo_1280x720_30mb.mp4 s3://metadata-original-video/
def lambda_handler(event, context):
    """

    :param event:
    :param context:
    """
    # dump event from queue
    logger.info("Event: {}".format(json.dumps(event)))
    
    # dump event from queue
    

    # fetch message from sqs
    for record in event['Records']:
        sqsResponse = json.loads(record['body'])
        rekMessage = json.loads(sqsResponse['Message'])

        print('startJobId: {}, status: {}'.format(rekMessage['JobId'], rekMessage['Status']))
        print('receive message from sqsUrl: {} as follows {}'.format(os.environ.get('QUEUE_URL'), json.dumps(sqsResponse)))

        # message validation
        if 'Message' not in sqsResponse:
            logger.error('JobId not found in message')
            return

        sqs.delete_message(QueueUrl=os.environ.get('QUEUE_URL'), ReceiptHandle=record['receiptHandle'])
        GetSegmentDetectionResults(rekMessage['JobId'])

def GetSQSMessageSuccess():
    
    jobFound = False
    succeeded = False
    sqsUrl = os.environ.get('QUEUE_URL')

    while jobFound == False:
        sqsResponse = sqs.receive_message(QueueUrl=sqsUrl, MessageAttributeNames=['ALL'], MaxNumberOfMessages=10)

        if sqsResponse:
            # message format
            # {
            #     "Type" : "Notification",
            #     "MessageId" : "63a3f6b6-d533-4a47-aef9-fcf5cf758c76",
            #     "TopicArn" : "arn:aws:sns:us-west-2:123456789012:MyTopic",
            #     "Subject" : "Testing publish to subscribed queues",
            #     "Message" :
                    # "{"JobId":"d38fb395199e2a91f91c1301d8783d088ee19af66ab82eb1b9f2144bf01a1982",
                    # "Status":"SUCCEEDED",
                    # "API":"StartSegmentDetection",
                    # "Timestamp":1636638674680,
                    # "Video":{
                    #     "S3ObjectName":"SampleVideo_1280x720_30mb.mp4",
                    #     "S3Bucket":"media-assets-origin"}
                    # }",
            #     "Timestamp" : "2021-11-11T13:51:14.814Z",
            #     "SignatureVersion" : "1",
            #     "Signature" : "EXAMPLEnTrFPa3...",
            #     "SigningCertURL" : "https://sns.us-west-2.amazonaws.com/SimpleNotificationService-xxxx.pem",
            #     "UnsubscribeURL" : "https://sns.us-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-2:xxxx:AmazonRekognitionTopic:84656998-faba-45d1-8125-6757a5527e98"
            # }
            if 'Messages' not in sqsResponse:
                print('.', end='')
                # sys.stdout.flush()
                time.sleep(5)
                continue

            for message in sqsResponse['Messages']:
                print('receive message from sqsUrl: {} as follows {}'.format(sqsUrl, json.dumps(sqsResponse)))
                notification = json.loads(message['Body'])
                rekMessage = json.loads(notification['Message'])
                if rekMessage['JobId'] == startJobId:
                    print('Matching Job Found:' + rekMessage['JobId'])
                    jobFound = True
                    if (rekMessage['Status']=='SUCCEEDED'):
                        succeeded=True
                    sqs.delete_message(QueueUrl=sqsUrl, ReceiptHandle=message['ReceiptHandle'])
                else:
                    # should not happen
                    print("Job didn't match:" + str(rekMessage['JobId']) + ' : ' + startJobId)
                # Delete the unknown message. Consider sending to dead letter queue
                sqs.delete_message(QueueUrl=sqsUrl, ReceiptHandle=message['ReceiptHandle'])
    return succeeded

def GetSegmentDetectionResults(jobId, maxRetry=10, retryInterval=5, maxResults=10, nextToken=None):
    paginationToken = ""
    finished = False
    firstTime = True

    while finished == False:
        response = rek.get_segment_detection(
            JobId=jobId, MaxResults=maxResults, NextToken=paginationToken
        )
        print('rekognition response: {}'.format(json.dumps(response)))
        if firstTime == True:
            print(f"Status\n------\n{response['JobStatus']}")
            print("\nRequested Types\n---------------")
            for selectedSegmentType in response['SelectedSegmentTypes']:
                print(f"\tType: {selectedSegmentType['Type']}")
                print(f"\t\tModel Version: {selectedSegmentType['ModelVersion']}")

            print()
            print("\nAudio metadata\n--------------")
            for audioMetadata in response['AudioMetadata']:
                print(f"\tCodec: {audioMetadata['Codec']}")
                print(f"\tDuration: {audioMetadata['DurationMillis']}")
                print(f"\tNumber of Channels: {audioMetadata['NumberOfChannels']}")
                print(f"\tSample rate: {audioMetadata['SampleRate']}")
            print()
            print("\nVideo metadata\n--------------")
            for videoMetadata in response["VideoMetadata"]:
                print(f"\tCodec: {videoMetadata['Codec']}")
                # print(f"\tColor Range: {videoMetadata['ColorRange']}")
                print(f"\tDuration: {videoMetadata['DurationMillis']}")
                print(f"\tFormat: {videoMetadata['Format']}")
                print(f"\tFrame rate: {videoMetadata['FrameRate']}")
                print(f"\tFrameHeight: {videoMetadata['FrameHeight']}")
                print(f"\tFrameWidth: {videoMetadata['FrameWidth']}")
                print("\nSegments\n--------")

            firstTime = False

        for segment in response['Segments']:
            print(f"\tDuration (milliseconds): {segment['DurationMillis']}")
            print(f"\tStart Timestamp (milliseconds): {segment['StartTimestampMillis']}")
            print(f"\tEnd Timestamp (milliseconds): {segment['EndTimestampMillis']}")
            
            print(f"\tStart timecode: {segment['StartTimecodeSMPTE']}")
            print(f"\tEnd timecode: {segment['EndTimecodeSMPTE']}")
            print(f"\tDuration timecode: {segment['DurationSMPTE']}")

            # print(f"\tStart frame number {segment['StartFrameNumber']}")
            # print(f"\tEnd frame number: {segment['EndFrameNumber']}")
            # print(f"\tDuration frames: {segment['DurationFrames']}")
            print()

            # generate a list of segments and using ffmpeg to cancatenate them
            # ffmpeg -f concat -safe 0 -i segments.txt -c copy output.mp4
            # ffmpeg -i segments.txt -c copy output.mp4


            # segments.append(segment)

        if "NextToken" in response:
            paginationToken = response["NextToken"]
        else:
            finished = True



