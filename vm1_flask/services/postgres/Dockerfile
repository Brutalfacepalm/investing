FROM postgres

RUN chmod -R 777 /docker-entrypoint-initdb.d

COPY create-multiple-postgresql-databases.sh /docker-entrypoint-initdb.d/
