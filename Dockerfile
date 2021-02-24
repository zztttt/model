FROM python:3.8-slim-buster AS develop

# aliyun source
RUN sed -i 's#http://deb.debian.org#https://mirrors.aliyun.com#g' /etc/apt/sources.list

# time zone
RUN cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    echo 'Asia/Shanghai' >/etc/timezone

# python dependencies
RUN mkdir -p /app
WORKDIR /app
COPY ./Ilib/ /app/Ilib/
ADD ./requirements.txt /app/
RUN pip install -i https://mirrors.aliyun.com/pypi/simple -r requirements.txt
RUN pip install kafka-python

# deploy with hypercorn
ADD ./ /app/
CMD python app.py
