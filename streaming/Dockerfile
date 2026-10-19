# Spark 4.0 with the Kafka connector baked in (no Ivy downloads at start-up)
# and the platform's Python packages.
FROM apache/spark:4.0.1-scala2.13-java17-python3-ubuntu

USER root
ARG MAVEN=https://repo1.maven.org/maven2
# Set to true for an S3 data lake (LAKE_PATH / CHECKPOINT_ROOT = s3a://...):
# adds hadoop-aws and the AWS SDK v2 bundle (~600 MB, so off by default).
ARG WITH_S3=false
RUN set -eux; cd /opt/spark/jars; \
    for jar in \
      org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.1/spark-sql-kafka-0-10_2.13-4.0.1.jar \
      org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.1/spark-token-provider-kafka-0-10_2.13-4.0.1.jar \
      org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar \
      org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar; \
    do curl -fsSLO "$MAVEN/$jar"; done; \
    if [ "$WITH_S3" = "true" ]; then \
      curl -fsSLO "$MAVEN/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar"; \
      curl -fsSLO "$MAVEN/software/amazon/awssdk/bundle/2.24.6/bundle-2.24.6.jar"; \
    fi; \
    mkdir -p /data/lake /data/checkpoints /app && chown -R spark:spark /data /app

COPY requirements-streaming.txt /tmp/requirements-streaming.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements-streaming.txt

WORKDIR /app
COPY --chown=spark:spark producer/ producer/
COPY --chown=spark:spark ml/ ml/
COPY --chown=spark:spark streaming/ streaming/
COPY --chown=spark:spark storage/ storage/

ENV PYTHONPATH=/app \
    PYSPARK_PYTHON=python3 \
    PYTHONWARNINGS=ignore::FutureWarning \
    HOME=/tmp
USER spark

CMD ["/opt/spark/bin/spark-submit", "--master", "local[*]", "--driver-memory", "1g", \
     "--conf", "spark.ui.port=4040", "/app/streaming/fraud_stream.py"]
