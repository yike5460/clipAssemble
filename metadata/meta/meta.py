import logging
import subprocess
import uuid
import boto3
import os
import json
import sys
import math
import PIL.Image as Image

from botocore.exceptions import ClientError

s3 = boto3.client('s3')
rek = boto3.client('rekognition')
sqs = boto3.client('sqs')

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

# testing with command aws s3 rm s3://metadata-original-video/SampleVideo_1280x720_30mb.mp4 && aws s3 cp SampleVideo_1280x720_30mb.mp4 s3://metadata-original-video/
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

        # Run ffmpeg to transform I-Frames, keep video suffix unchanged
        IFRAME_VIDEO_FILE = '/tmp/'+ key.split('.')[0] + '-iframe-output.' + key.split('.')[1]
        CMD = ['ffmpeg', '-y', '-i', LOCAL_VIDEO_FILE, '-strict', '-2', '-qscale', '0', '-intra', IFRAME_VIDEO_FILE]
        SHELL_CMD = ' '.join(CMD)
        try:
            # out_bytes = subprocess.check_output(['./ffmpeg', '-y', '-i', LOCAL_VIDEO_FILE, '-strict', '-2', '-qscale', '0', '-intra', IFRAME_VIDEO_FILE])
            out_bytes = subprocess.check_output(SHELL_CMD, shell=True)
        except subprocess.CalledProcessError as e:
            logger.error("Error: {}, return code {}".format(e.output.decode('utf-8'), e.returncode))

        # Upload the transformed I-Frames to S3
        upload_file(IFRAME_VIDEO_FILE, os.environ.get('Processed_Bucket'))
        logger.info("Uploaded transformed I-Frames {} to S3".format(IFRAME_VIDEO_FILE))

        # run ffmpeg to snapshot video in 5 second intervals
        # ffmpeg -i SampleVideo_1280x720_30mb.mp4 -vf fps=0.1 key-snapshot-output-%d.jpeg
        SNAPSHOT_VIDEO_FILE = '/tmp/'+ key.split('.')[0] + '-snapshot-output-%d' + '.jpeg'
        CMD = ['ffmpeg', '-y', '-i', LOCAL_VIDEO_FILE, '-vf', 'fps=0.1', SNAPSHOT_VIDEO_FILE]
        SHELL_CMD = ' '.join(CMD)
        try:
            subprocess.check_output(SHELL_CMD, shell=True)
        except subprocess.CalledProcessError as e:
            logger.error("Error: {}, return code {}".format(e.output.decode('utf-8'), e.returncode))

        # calculate image with most entropy and upload to S3 as video cover
        VIDEO_COVER = imageWithMaxEntropy()
        logger.info("Video cover: {}".format(VIDEO_COVER))
        upload_file(VIDEO_COVER, os.environ.get('Processed_Bucket'))
        logger.info("Uploaded video cover {} to S3".format(SNAPSHOT_VIDEO_FILE))

        # remove local file
        os.remove(LOCAL_VIDEO_FILE)
        logger.info("Removed local file {}".format(LOCAL_VIDEO_FILE))

        bucketProcessed = os.environ.get('Processed_Bucket')
        keyProcessed = os.path.basename(IFRAME_VIDEO_FILE)
        logger.info("bucketProcessed: {}, keyProcessed: {}".format(bucketProcessed, keyProcessed))

        # invoke rekognition to fetch tech cue and shot info
        StartSegmentDetection(bucket = str(bucketProcessed), key = str(keyProcessed))

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

    logger.info("service role arn: {}, sns topic: {}".format(os.environ.get('REKO_ARN'), os.environ.get('TOPIC_ARN')))

    # # clean sqs queue before make call to rekognition
    sqsUrl = os.environ.get('QUEUE_URL')
    # sqs.purge_queue(QueueUrl=sqsUrl)
    # # wait for sqs message pruning
    # time.sleep(5)

    # get current sqs message count
    sqsResponse = sqs.get_queue_attributes(QueueUrl=sqsUrl, AttributeNames=['ApproximateNumberOfMessages'])
    logger.info("Current SQS message count: {}".format(sqsResponse['Attributes']['ApproximateNumberOfMessages']))

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
    
def calc_entropy(img):

    # get the image size
    width, height = img.size
    # get the image data
    px = img.load()
    # get the image histogram
    histogram = img.histogram()
    # get the image histogram size
    hist_size = sum(histogram)
    # get the image histogram count
    histogram_count = [float(h) / hist_size for h in histogram]
    # get the image entropy
    entropy = -sum(p * math.log(p, 2) for p in histogram_count if p != 0)

    return entropy

def imageWithMaxEntropy():

    # get the image path
    root_path = "/tmp/"

    # get the max entropy image
    max_entropy_image = None
    max_entropy = 0

    # get the min entropy image
    min_entropy_image = None
    min_entropy = 3

    # get the most entropy image
    most_entropy_image = None
    most_entropy = 0

    # set image list
    image_list = []

    # loop all images in current directory
    for image in os.listdir(root_path):
        if image.endswith(".jpeg"):
            image_list.append(image)

    for image in image_list:
        # get the image path
        image_path = os.path.join(root_path, image)
        # get the image
        img = Image.open(image_path)
        # caculate the entropy
        entropy = calc_entropy(img)
        logger.info("Image: {}, entropy: {}".format(image, entropy))
        # judge the entropy
        if entropy > max_entropy:
            max_entropy = entropy
            max_entropy_image = image
        if entropy < min_entropy:
            min_entropy = entropy
            min_entropy_image = image
    logger.info("Max entropy image is {}".format(max_entropy_image))
    logger.info("Min entropy image is {}".format(min_entropy_image))
    return os.path.join(root_path, max_entropy_image)
