FROM python:3.9.5-slim-buster

ENV VIRTUAL_ENV "/venv"
ENV PATH "$VIRTUAL_ENV/bin:$PATH"

RUN apt-get update \
    && apt-get install -y --reinstall build-essential \
    && apt-get install -y gcc libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && python -m venv $VIRTUAL_ENV \
    && pip install --no-cache-dir psycopg2 \
        mlflow

RUN pip install boto3==1.19.2

EXPOSE 4040