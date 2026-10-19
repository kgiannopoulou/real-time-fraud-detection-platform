# Airflow with the training code in an isolated virtualenv, so ML library
# versions never fight Airflow's own pinned dependencies.
FROM apache/airflow:2.11.2-python3.12

USER root
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 \
 && rm -rf /var/lib/apt/lists/*
COPY requirements-ml.txt /tmp/requirements-ml.txt
RUN python -m venv /opt/ml-venv \
 && /opt/ml-venv/bin/pip install --no-cache-dir -r /tmp/requirements-ml.txt \
 && mkdir -p /opt/fraud/data && chown -R airflow:root /opt/fraud

COPY --chown=airflow:root producer/ /opt/fraud/producer/
COPY --chown=airflow:root ml/ /opt/fraud/ml/
COPY --chown=airflow:root streaming/__init__.py streaming/features.py /opt/fraud/streaming/
COPY --chown=airflow:root storage/ /opt/fraud/storage/
COPY --chown=airflow:root airflow/entrypoint.sh /opt/fraud/entrypoint.sh

USER airflow
ENV PYTHONPATH=/opt/fraud
ENTRYPOINT ["/usr/bin/dumb-init", "--", "bash", "/opt/fraud/entrypoint.sh"]
