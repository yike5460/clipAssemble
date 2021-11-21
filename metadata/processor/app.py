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

dynamoDBTableName = "metaData"
dynamodb = boto3.resource("dynamodb")
dynamoDBTable = dynamodb.Table(dynamoDBTableName)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ.get('DYNAMODB_TABLE'))

LAMBDA_TASK_ROOT = os.environ.get('LAMBDA_TASK_ROOT')
# ffmpeg_path = os.path.join(LAMBDA_TASK_ROOT, 'ffmpeg')

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
    # dump event from queue, current parse for rekognition
    logger.info("Event: {}".format(json.dumps(event)))

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

        # download iframe from processed s3 bucket
        s3Object = rekMessage['Video']['S3ObjectName']
        s3Bucket = rekMessage['Video']['S3Bucket']
        iframePath = '/tmp/' + s3Object
        s3.download_file(s3Bucket, s3Object, iframePath)
        logger.info("Download I-Frames {} from S3 {}".format(s3Object, s3Bucket))

        # get shot info from reko result
        GetSegmentDetectionResults(rekMessage['JobId'], s3Object, s3Bucket, iframePath)

        # delete local file
        os.remove(iframePath)
        logger.info("Delete local file {}".format(iframePath))

def GetSegmentDetectionResults(jobId, s3Object, s3Bucket, iframePath, maxRetry=10, retryInterval=5, maxResults=10, nextToken=None):
    paginationToken = ""
    finished = False
    firstTime = True
    slicedFilelist = []

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
            # print(f"\tDuration (milliseconds): {segment['DurationMillis']}")
            # print(f"\tStart Timestamp (milliseconds): {segment['StartTimestampMillis']}")
            # print(f"\tEnd Timestamp (milliseconds): {segment['EndTimestampMillis']}")
            
            # print(f"\tStart timecode: {segment['StartTimecodeSMPTE']}")
            # print(f"\tEnd timecode: {segment['EndTimecodeSMPTE']}")
            # print(f"\tDuration timecode: {segment['DurationSMPTE']}")

            # print(f"\tStart frame number {segment['StartFrameNumber']}")
            # print(f"\tEnd frame number: {segment['EndFrameNumber']}")
            # print(f"\tDuration frames: {segment['DurationFrames']}")

            # generate random video clips from iframe video
            # ffmpeg -ss 00:01:00 -t 00:00:10 -i keyoutput.mp4 -vcodec copy -acodec copy output1.mp4
            RANDOM_VIDEO_FILE = str(uuid.uuid1()) + '-sliced-output.mp4'
            LOCAL_SLICED_VIDEO_FILE = '/tmp/' + RANDOM_VIDEO_FILE

            # archive sliced video to seperate folder with same iframe prefix
            REMOTE_SLICED_VIDEO_FILE = s3Object.split('.')[0] + '/' + RANDOM_VIDEO_FILE
            print('REMOTE_SLICED_VIDEO_FILE is {}'.format(REMOTE_SLICED_VIDEO_FILE))

            CMD = ['ffmpeg', '-ss', segment['StartTimecodeSMPTE'].rsplit(':', 1)[0], '-t', segment['DurationSMPTE'].rsplit(':', 1)[0], '-i', iframePath, '-vcodec copy -acodec copy ', LOCAL_SLICED_VIDEO_FILE]
            SHELL_CMD = ' '.join(CMD)
            try:
                # out_bytes = subprocess.check_output(['./ffmpeg', '-y', '-i', LOCAL_VIDEO_FILE, '-strict', '-2', '-qscale', '0', '-intra', LOCAL_SLICED_VIDEO_FILE])
                out_bytes = subprocess.check_output(SHELL_CMD, shell=True)
                # Upload the transformed I-Frames to S3
                upload_file(LOCAL_SLICED_VIDEO_FILE, s3Bucket, REMOTE_SLICED_VIDEO_FILE)
                logger.info("Uploaded sliced I-Frames {} to S3".format(REMOTE_SLICED_VIDEO_FILE))

                # delete local file
                os.remove(LOCAL_SLICED_VIDEO_FILE)
                slicedFilelist.append(REMOTE_SLICED_VIDEO_FILE)
                print('slicedFilelist is {}'.format(slicedFilelist))

            except subprocess.CalledProcessError as e:
                logger.error("Error: {}, return code {}".format(e.output.decode('utf-8'), e.returncode))

        if "NextToken" in response:
            paginationToken = response["NextToken"]
        else:
            finished = True

            # TBD, generate a list of segments and using ffmpeg to cancatenate them
            # ffmpeg -f concat -safe 0 -i segments.txt -c copy output.mp4
            # ffmpeg -i segments.txt -c copy output.mp4

    # update sliced video info to dynamodb
    print('dynamodb key is {}'.format(s3Object.split('.')[0].rsplit('-')[0] + '.' + s3Object.split('.')[1]))

    response = table.update_item(
        Key={
            # strip -iframe-output to restore original video name, 'SampleVideo_1280x720_30mb-iframe-output.mp4' to 'SampleVideo_1280x720_30mb.mp4'
            'id': s3Object.split('.')[0].rsplit('-')[0] + '.' + s3Object.split('.')[1]
        },
        UpdateExpression="set #s3ObjectName = :s3ObjectName, #s3Bucket = :s3Bucket",
        ExpressionAttributeNames={
            '#s3ObjectName': 's3ObjectName',
            '#s3Bucket': 's3Bucket'
        },
        ExpressionAttributeValues={
            ':s3ObjectName': slicedFilelist,
            ':s3Bucket': s3Bucket
        }
    )
    logger.info("Updated sliced video info to dynamodb")

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    try:
        response = s3.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True



