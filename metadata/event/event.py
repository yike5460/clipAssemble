import logging
import subprocess
import uuid
import boto3
import os
import json
import sys
import time

from botocore.exceptions import ClientError

SIGNED_URL_EXPIRATION = 60 * 60 * 24 * 7

s3 = boto3.client('s3')

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
    logger.info("raw event from EventBridge: {}".format(event))

    # parse event from eventbridge
    event_source = event['source']
    event_detail = event['detail']
    event_detail_type = event['detail-type']

    # generate random video clips from iframe video
    if event_detail_type == 'videoShotsAndGif' and event_source == 'custom':
    
        # ffmpeg -ss 00:01:00 -t 00:00:10 -i keyoutput.mp4 -vcodec copy -acodec copy output1.mp4
        RANDOM_VIDEO_FILE = event_detail['randomVideoFile']
        s3Object = event_detail['s3Object']
        s3Bucket = event_detail['s3Bucket']
        startTimecodeSMPTE = event_detail['startTimecodeSMPTE']
        durationSMPTE = event_detail['durationSMPTE']

        # can be any string, just match with predefined process here
        LOCAL_SLICED_VIDEO_FILE = '/tmp/' + RANDOM_VIDEO_FILE
        
        signed_url = get_signed_url(SIGNED_URL_EXPIRATION, s3Bucket, s3Object)
        # double quote the singed url
        signed_url = '"' + signed_url + '"'

        logger.info("iframe video bucket: {}, key: {}, signed URL: {}".format(s3Bucket, s3Object, signed_url))

        # archive sliced video to seperate folder with same iframe prefix
        REMOTE_SLICED_VIDEO_FILE = s3Object.split('.')[0] + '/' + RANDOM_VIDEO_FILE
        logger.info('sliced video file prefix is {}'.format(REMOTE_SLICED_VIDEO_FILE))

        CMD = ['ffmpeg', '-ss', startTimecodeSMPTE, '-t', durationSMPTE, '-i', signed_url, '-vcodec copy -acodec copy ', LOCAL_SLICED_VIDEO_FILE]
        SHELL_CMD = ' '.join(CMD)
        try:
            # out_bytes = subprocess.check_output(['./ffmpeg', '-y', '-i', LOCAL_VIDEO_FILE, '-strict', '-2', '-qscale', '0', '-intra', LOCAL_SLICED_VIDEO_FILE])
            out_bytes = subprocess.check_output(SHELL_CMD, shell=True)
            # Upload the transformed I-Frames to S3
            upload_file(LOCAL_SLICED_VIDEO_FILE, s3Bucket, REMOTE_SLICED_VIDEO_FILE)
            logger.info("Uploaded sliced I-Frames {} to S3".format(REMOTE_SLICED_VIDEO_FILE))

            # delete local file
            os.remove(LOCAL_SLICED_VIDEO_FILE)

        except subprocess.CalledProcessError as e:
            logger.error("Error: {}, return code {}".format(e.output.decode('utf-8'), e.returncode))
            
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
 
    