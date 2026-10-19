FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app
WORKDIR /app
RUN pip install --no-cache-dir confluent-kafka==2.16.0 prometheus-client==0.26.0 \
 && useradd --create-home --uid 10001 app
COPY producer/ producer/
USER app
EXPOSE 8001
ENTRYPOINT ["python", "-m", "producer.producer"]
CMD ["--rate", "20", "--quiet", "--metrics-port", "8001"]
