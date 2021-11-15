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

startJobId = ''

SIGNED_URL_EXPIRATION = 300
LOCAL_VIDEO_FILE = '/tmp/'+ 'local-input.mp4'

LAMBDA_TASK_ROOT = os.environ.get('LAMBDA_TASK_ROOT')
# ffmpeg_path = os.path.join(LAMBDA_TASK_ROOT, 'ffmpeg')

logger = logging.getLogger('boto3')
logger.setLevel(logging.INFO)

# add execution path to ffmpeg
os.environ['PATH'] = os.environ['PATH'] + ':' + os.environ['LAMBDA_TASK_ROOT']

# testing with command aws s3 rm s3://media-assets-origin/SampleVideo_1280x720_30mb.mp4 && aws s3 cp SampleVideo_1280x720_30mb.mp4 s3://media-assets-origin/
def lambda_handler(event, context):
    """

    :param event:
    :param context:
    """
    # Loop through records provided by S3 Event trigger, add for API Gateway invocation in future
    for s3_record in event['Records']:
        logger.info("Working on new s3_record...")
        # Extract the Key and Bucket names for the asset uploaded to S3
        key = s3_record['s3']['object']['key']
        bucket = s3_record['s3']['bucket']['name']
        # Generate a signed URL for the uploaded asset
        signed_url = get_signed_url(SIGNED_URL_EXPIRATION, bucket, key)
        logger.info("original video bucket: {}, key: {}, signed URL: {}".format(bucket, key, signed_url))

        # Launch MediaInfo CLI to extract metadata
        # use ffprobe to fetch metadata (frame number)
        # ffprobe -v error -count_frames -select_streams v:0   -show_entries stream=nb_read_frames -of default=nokey=1:noprint_wrappers=1 SampleVideo_1280x720_30mb.mp4 
        xml_output = subprocess.check_output(["./mediainfo", "--full", "--output=XML", signed_url])
        logger.info("mediainfo output: {}".format(xml_output))
        # save metadata to DynamoDB
        save_record(key, xml_output.decode('utf-8'))

        # invoke rekognition to fetch tech cue and shot info
        StartSegmentDetection(bucket=bucket, key=key)

        # command refer to https://www.jianshu.com/p/cf1e61eb6fc8
        # ffmpeg -i SampleVideo_1280x720_30mb.mp4 -strict -2 -qscale 0 -intra keyoutput.mp4

        # #!/bin/sh
        # for f in ./raw_files/*.mp3; do echo "file '$f'" >> mylist.txt; done
        # printf "file '%s'\n" ./raw_files/*.mp3 > mylist.txt        

        # ffmpeg -ss 00:01:00 -t 00:00:10 -i keyoutput.mp4 -vcodec copy -acodec copy output1.mp4
        # ffmpeg -ss 00:02:00 -t 00:00:10 -i keyoutput.mp4 -vcodec copy -acodec copy output2.mp4

        # #!/bin/sh
        # output=$1
        # echo writing to $output
        # ffmpeg -f concat -safe 0 -i mylist.txt -c copy $output

        # Download the asset to a local file
        s3.download_file(bucket, key, LOCAL_VIDEO_FILE)
        IFRAME_VIDEO_FILE = '/tmp/'+ key + '-iframe-output.mp4'
        CMD = ['ffmpeg', '-y', '-i', LOCAL_VIDEO_FILE, '-strict', '-2', '-qscale', '0', '-intra', IFRAME_VIDEO_FILE]
        SHELL_CMD = ' '.join(CMD)
        # Run ffmpeg to transform I-Frames
        try:
            # out_bytes = subprocess.check_output(['./ffmpeg', '-y', '-i', LOCAL_VIDEO_FILE, '-strict', '-2', '-qscale', '0', '-intra', IFRAME_VIDEO_FILE])
            out_bytes = subprocess.check_output(SHELL_CMD, shell=True)
        except subprocess.CalledProcessError as e:
            logger.error("Error: {}, return code {}".format(e.output.decode('utf-8'), e.returncode))

        # Upload the transformed I-Frames to S3
        upload_file(IFRAME_VIDEO_FILE, os.environ.get('Processed_Bucket'))
        logger.info("Uploaded transformed I-Frames {} to S3".format(IFRAME_VIDEO_FILE))

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

def save_record(key, xml_output):
    """
    Save record to DynamoDB
    :param key:         S3 Key Name
    :param xml_output:  Metadata in XML Format
    :return:
    """
    logger.info("Saving record to DynamoDB...")
    dynamoDBTable.put_item(
       Item={
            'id': key,
            'metaData': xml_output
        }
    )
    logger.info("Saved record to DynamoDB")

def get_signed_url(expires_in, bucket, obj):
    """
    Generate a signed URL
    :param expires_in:  URL Expiration time in seconds
    :param bucket:
    :param obj:         S3 Key name
    :return:            Signed URL
    """
    s3_cli = boto3.client("s3")
    presigned_url = s3_cli.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': obj}, ExpiresIn=expires_in)
    return presigned_url



def StartSegmentDetection(bucket = '', key = ''):
    
    min_Technical_Cue_Confidence = 80.0
    min_Shot_Confidence = 80.0
    max_pixel_threshold = 0.1
    min_coverage_percentage = 60

    print('service role arn: {}, sns topic: {}'.format(os.environ.get('REKO_ARN'), os.environ.get('TOPIC_ARN')))

    # # clean sqs queue before make call to rekognition
    sqsUrl = os.environ.get('QUEUE_URL')
    # sqs.purge_queue(QueueUrl=sqsUrl)
    # # wait for sqs message pruning
    # time.sleep(5)

    # get current sqs message count
    sqsResponse = sqs.get_queue_attributes(QueueUrl=sqsUrl, AttributeNames=['ApproximateNumberOfMessages'])
    print('sqs message count before call to rekognition: {}'.format(sqsResponse['Attributes']['ApproximateNumberOfMessages']))

    response = rek.start_segment_detection(
        Video={"S3Object": {"Bucket": bucket, "Name": key}},
        NotificationChannel={
            "RoleArn": os.environ.get('REKO_ARN'),
            "SNSTopicArn": os.environ.get('TOPIC_ARN'),
        },
        SegmentTypes=["TECHNICAL_CUE", "SHOT"],
        Filters={
            # "TechnicalCueFilter": {
            #     "BlackFrame": {
            #         "MaxPixelThreshold": max_pixel_threshold,
            #         "MinCoveragePercentage": min_coverage_percentage
            #     },
            #     "MinSegmentConfidence": min_Technical_Cue_Confidence
            # },
            "ShotFilter": {"MinSegmentConfidence": min_Shot_Confidence}
        }
    )

    startJobId = response["JobId"]
    print(f"Start Job Id: {startJobId}")