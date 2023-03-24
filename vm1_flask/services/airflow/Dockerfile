FROM apache/airflow:2.4.3-python3.8

USER root
RUN apt-get update -y && apt-get install -y gcc && \
  apt-get install -y --no-install-recommends gcc git libssl-dev g++ make && \
  cd /tmp && git clone https://github.com/edenhill/librdkafka.git && \
  cd librdkafka && git checkout tags/v2.0.2 && \
  ./configure && make && make install && \
  cd ../ && rm -rf librdkafka
USER airflow

RUN pip install confluent-kafka
RUN pip install airflow-provider-kafka==0.2.2
RUN pip install apache-airflow-providers-mongo==3.0.0
RUN pip install holidays
RUN pip install azure-mgmt-compute
RUN pip install azure-mgmt-storage
RUN pip install azure-mgmt-resource
RUN pip install azure-keyvault-secrets
RUN pip install azure-storage-blob
