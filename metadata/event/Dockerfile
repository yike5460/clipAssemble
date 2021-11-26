FROM public.ecr.aws/lambda/python:3.8

# Install library required to add CURL support to Mediainfo
RUN yum install -y libcurl-devel wget unzip tar xz

# Install static libraries to run ffmpeg 
RUN wget https://www.johnvansickle.com/ffmpeg/old-releases/ffmpeg-4.2.2-amd64-static.tar.xz
RUN tar -xvf ffmpeg-4.2.2-amd64-static.tar.xz
RUN pwd && ls -l ffmpeg-4.2.2-amd64-static
RUN cp ./ffmpeg-4.2.2-amd64-static/ffmpeg ${LAMBDA_TASK_ROOT}
RUN cp ./ffmpeg-4.2.2-amd64-static/ffprobe ${LAMBDA_TASK_ROOT}

COPY *.py ${LAMBDA_TASK_ROOT}

# COPY requirements.txt  .
# RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

CMD ["event.lambda_handler"]