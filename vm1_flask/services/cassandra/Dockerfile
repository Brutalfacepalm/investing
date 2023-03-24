FROM cassandra:3.11

RUN set -x \
    && echo "deb http://pkg.ci.collectd.org/deb jessie collectd-5.7" \
        > /etc/apt/sources.list.d/pkg.ci.collectd.org.list \
    && gpg --keyserver hkp://pgp.mit.edu:80 --recv-keys 3994D24FB8543576 \
        && gpg --export -a 3994D24FB8543576 | apt-key add - \
    && apt-get update \
        && apt-get install -y --no-install-recommends \
            collectd=5.7.1-1.1 \
            collectd-utils \
            libprotobuf-c-dev \
            libmicrohttpd-dev \
    && echo "deb http://deb.debian.org/debian jessie main" \
        >> /etc/apt/sources.list.d/pkg.ci.collectd.org.list \
    && apt-get update \
        && apt-get install -y \
            librrd4 \
            libmicrohttpd10 \
        && rm -rf /var/lib/apt/lists/*
RUN touch /var/log/collectd.log \
    && chown cassandra:cassandra /var/log/collectd.log

# install Prometheus JMX exporter
# NOTE: 0.10 will not work until this issue is resolved:
#       https://github.com/prometheus/jmx_exporter/issues/170
ENV JMX_EXPORTER_VERSION 0.9
COPY lib/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar \
         /prometheus/
RUN echo 'JVM_OPTS="$JVM_OPTS -javaagent:'/prometheus/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar=7070:/prometheus/prometheus.yml'"' \
    | tee -a /etc/cassandra/cassandra-env.sh

# add JMX authentication files for Reaper access
COPY config/jmxremote.access /usr/lib/jvm/java-8-openjdk-amd64/jre/lib/management/jmxremote.access
COPY config/jmxremote.password /etc/cassandra/jmxremote.password
RUN chown cassandra:cassandra \
        /usr/lib/jvm/java-8-openjdk-amd64/jre/lib/management/jmxremote.access \
        /etc/cassandra/jmxremote.password \
    && chmod 600 \
        /usr/lib/jvm/java-8-openjdk-amd64/jre/lib/management/jmxremote.access \
        /etc/cassandra/jmxremote.password

# overwrite the base docker-entrypoint.sh with modified one, for filebeat perms
COPY docker-entrypoint.sh /docker-entrypoint.sh

# does not work for some reason
#RUN echo "exec service collectd start &" \
#    >> /etc/cassandra/cassandra-env.sh