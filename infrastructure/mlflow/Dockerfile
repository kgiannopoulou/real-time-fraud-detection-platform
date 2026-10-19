# MLflow tracking server for the cloud deployment: PostgreSQL backend store
# (RDS) and S3 artefact store, both configured through environment variables.
FROM python:3.12-slim
RUN pip install --no-cache-dir mlflow==3.17.1 psycopg2-binary==2.9.13 boto3 \
 && useradd --create-home --uid 10001 mlflow
COPY infrastructure/mlflow/entrypoint.sh /entrypoint.sh
USER mlflow
EXPOSE 5000
ENTRYPOINT ["bash", "/entrypoint.sh"]
