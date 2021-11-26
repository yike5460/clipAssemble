FROM public.ecr.aws/lambda/python:3.8

# Install library required to add CURL support to Mediainfo
RUN yum install -y libcurl-devel wget unzip tar xz
RUN python3 -m pip install --upgrade Pillow

# Intall static libraries required to build Mediainfo
RUN wget https://mediaarea.net/download/binary/mediainfo/21.09/MediaInfo_CLI_21.09_Lambda.zip
RUN unzip MediaInfo_CLI_21.09_Lambda.zip
RUN cp ./bin/mediainfo ${LAMBDA_TASK_ROOT}

# Install static libraries to run ffmpeg 
RUN wget https://www.johnvansickle.com/ffmpeg/old-releases/ffmpeg-4.2.2-amd64-static.tar.xz
RUN tar -xvf ffmpeg-4.2.2-amd64-static.tar.xz
RUN cp ./ffmpeg-4.2.2-amd64-static/ffmpeg ${LAMBDA_TASK_ROOT}
RUN cp ./ffmpeg-4.2.2-amd64-static/ffprobe ${LAMBDA_TASK_ROOT}

COPY *.py ${LAMBDA_TASK_ROOT}

# COPY requirements.txt  .
# RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

CMD ["meta.lambda_handler"]