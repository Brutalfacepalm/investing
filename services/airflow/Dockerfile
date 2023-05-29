FROM apache/airflow:2.6.1-python3.10

USER root
# WORKDIR /opt/airflow/
# RUN mkdir -p logs dags plugins
# RUN chmod -R 777 logs dags plugins
RUN apt-get update -y && apt-get upgrade -y
RUN apt-get install nano
RUN export PATH=$PATH:~/.local/bin
# RUN apt-get update -y && apt-get install -y gcc && apt-get install -y --no-install-recommends gcc git libssl-dev g++ make && \
#   cd /tmp && git clone https://github.com/edenhill/librdkafka.git && \
#   cd librdkafka && git checkout tags/v2.0.2 && \
#   ./configure && make && make install && \
#   cd ../ && rm -rf librdkafka

USER ${AIRFLOW_UID}

RUN echo $PATH
# RUN pip install apache-airflow
RUN pip install confluent-kafka==2.0.2
RUN pip install airflow-provider-kafka==0.2.2
RUN pip install apache-airflow-providers-mongo==3.0.0
RUN pip install holidays==0.14.2
RUN pip install numba==0.56.4
# RUN pip install torch==2.0.1
RUN pip install numpy==1.23.5
# RUN pip install scikit-learn
