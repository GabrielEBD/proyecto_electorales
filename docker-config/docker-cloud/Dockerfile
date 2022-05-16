FROM python:3.8

ADD ./requirements.txt /requirements.txt

RUN apt-get update 

RUN apt-get install -y --no-install-recommends

RUN pip3 install --no-cache-dir  -r /requirements.txt

ENV LIBRARY_PATH=/lib:/usr/lib

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

ADD . /app

WORKDIR /app

COPY . /app

