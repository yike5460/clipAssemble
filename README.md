# video clips
reassemble based on rekognition

## architecture

![architecture](/assets/images/architecture.png)

## installation

```
cdk deploy 
```

## function validation

stack will auto-generate bucket metadata-original-video to store original video assets and bucket metadata-processed-video to store slices video cover, video clips and gifs
```
aws s3 cp SampleVideo_1280x720_30mb.mp4 s3://metadata-original-video/
```

check bucket metadata-processed-video for all generated assets after 5 minutes, all depend on the size of video files
```
aws s3 ls s3://metadata-processed-video
```

## unit test

