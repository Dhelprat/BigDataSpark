FROM apache/spark:3.5.1

USER root

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    pip3 install --no-cache-dir clickhouse-connect==0.8.18

RUN mkdir -p /opt/spark-extra-jars /opt/spark-apps /opt/spark-scripts && \
    curl -fsSL -o /opt/spark-extra-jars/postgresql-42.7.4.jar https://jdbc.postgresql.org/download/postgresql-42.7.4.jar && \
    curl -fsSL -o /opt/spark-extra-jars/clickhouse-jdbc-0.6.0-all.jar https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.0/clickhouse-jdbc-0.6.0-all.jar

ENV SPARK_CLASSPATH="/opt/spark-extra-jars/postgresql-42.7.4.jar:/opt/spark-extra-jars/clickhouse-jdbc-0.6.0-all.jar"
ENV PYSPARK_PYTHON=python3

WORKDIR /opt/spark-apps
