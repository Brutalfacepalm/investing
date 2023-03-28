FROM python:3.9.5-slim-buster

RUN mkdir -p /home/app
RUN chmod -R 777 /home/app

RUN apt-get update && apt-get install -y gcc cmake libpq-dev python-dev python-psycopg2

COPY requirements.txt .
RUN pip install -r requirements.txt

WORKDIR /home/app

COPY . /home/app