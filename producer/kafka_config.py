"""Kafka connection settings shared by every client.

Locally the broker is plaintext. On a managed cluster (e.g. Amazon MSK with
SASL/SCRAM over TLS) the same images are configured through environment
variables only:

    KAFKA_SECURITY_PROTOCOL=SASL_SSL
    KAFKA_SASL_MECHANISM=SCRAM-SHA-512
    KAFKA_SASL_USERNAME=...   KAFKA_SASL_PASSWORD=...   (from a secrets manager)
"""

from __future__ import annotations

import os


def _env() -> dict[str, str | None]:
    return {
        "protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
        "mechanism": os.getenv("KAFKA_SASL_MECHANISM"),
        "username": os.getenv("KAFKA_SASL_USERNAME"),
        "password": os.getenv("KAFKA_SASL_PASSWORD"),
    }


def librdkafka_settings() -> dict[str, str]:
    """Settings for confluent-kafka clients (producer, consumers)."""
    env = _env()
    settings = {"security.protocol": env["protocol"]}
    if env["mechanism"]:
        settings.update(
            {
                "sasl.mechanism": env["mechanism"],
                "sasl.username": env["username"] or "",
                "sasl.password": env["password"] or "",
            }
        )
    return settings


def spark_kafka_options(bootstrap: str) -> dict[str, str]:
    """Options for Spark's Kafka source/sink (prefixed with ``kafka.``)."""
    env = _env()
    options = {"kafka.bootstrap.servers": bootstrap, "kafka.security.protocol": env["protocol"]}
    if env["mechanism"]:
        login = (
            "org.apache.kafka.common.security.scram.ScramLoginModule"
            if env["mechanism"].startswith("SCRAM")
            else "org.apache.kafka.common.security.plain.PlainLoginModule"
        )
        options.update(
            {
                "kafka.sasl.mechanism": env["mechanism"],
                "kafka.sasl.jaas.config": (
                    f'{login} required username="{env["username"]}" password="{env["password"]}";'
                ),
            }
        )
    return options
