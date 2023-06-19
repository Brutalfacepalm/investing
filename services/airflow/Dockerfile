FROM apache/airflow:2.6.1-python3.10

USER root
WORKDIR /opt/airflow/
RUN mkdir -p logs dags plugins
RUN chmod -R 777 logs dags plugins

#   cd /tmp && git clone https://github.com/edenhill/librdkafka.git && \
#   cd librdkafka && git checkout tags/v2.0.2 && \
#   ./configure && make && make install && \
#   cd ../ && rm -rf librdkafka

RUN apt-get update -y && apt-get upgrade -y
RUN apt-get install -yf nano
# RUN apt-get update -y && apt-get install -y gcc && apt-get install -y --no-install-recommends gcc git libssl-dev g++ make

### Install Google Chrome
RUN apt-get install -yf wget xvfb unzip
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
RUN echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list
RUN apt-get update -y
RUN apt-get install -y google-chrome-stable

### Install ChromeDriver
RUN apt-get install -yqq unzip
RUN wget -O /tmp/chromedriver.zip http://chromedriver.storage.googleapis.com/`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE`/chromedriver_linux64.zip
RUN unzip /tmp/chromedriver.zip chromedriver -d /usr/local/bin/
RUN chmod 777 /usr/local/bin/chromedriver
RUN chmod 777 /usr/bin/google-chrome

RUN export PATH=$PATH:~/.local/bin

USER 1000

RUN pip install --upgrade requests
RUN pip install apache-airflow==2.6.1
RUN pip install confluent-kafka==2.0.2
RUN pip install airflow-provider-kafka==0.2.2
RUN pip install apache-airflow-providers-mongo==3.0.0
RUN pip install holidays==0.14.2
RUN pip install numba==0.56.4
RUN pip install torch==2.0.1
RUN pip install numpy==1.23.5
RUN pip install scikit-learn==1.2.2
RUN pip install selenium==4.10.0
RUN pip install webdriver_manager
