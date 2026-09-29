"""Kafka consumer loop, pipeline executor, retry and DLQ.

The names below are the engine Kafka link API every data-plane entry point uses (`engine-kafka/v0`,
ADR-008); `tributary.runtime.engine_kafka`'s module doc has the migration recipe.
"""

from .engine_kafka import load_engine_kafka, load_engine_kafka_or_exit
from .errors import EngineKafkaError, KafkaSecurityError, RuntimeConfigurationError
from .secrets import DirSecretResolver, SecretResolutionError, SecretResolver

__all__ = [
    "DirSecretResolver",
    "EngineKafkaError",
    "KafkaSecurityError",
    "RuntimeConfigurationError",
    "SecretResolutionError",
    "SecretResolver",
    "load_engine_kafka",
    "load_engine_kafka_or_exit",
]
