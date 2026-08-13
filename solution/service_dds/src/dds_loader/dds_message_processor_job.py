from datetime import datetime
from logging import Logger
from lib.kafka_connect import KafkaConsumer

class DdsMessageProcessor:
    def __init__(
        self,
        consumer: KafkaConsumer,
        logger: Logger
    ) -> None:
        self._consumer = consumer
        self._logger = logger
        self._batch_size = 30

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            message = self._consumer.consume()

            if message is None:
                break

            self._logger.info(f"Kafka message: {message}")

        self._logger.info(f"{datetime.utcnow()}: FINISH")
