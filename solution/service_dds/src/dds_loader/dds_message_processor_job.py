from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer
from dds_loader.repository.dds_repository import DdsRepository


class DdsMessageProcessor:
    def __init__(
        self,
        consumer: KafkaConsumer,
        repository: DdsRepository,
        logger: Logger
    ) -> None:
        self._consumer = consumer
        self._repository = repository
        self._logger = logger
        self._batch_size = 100

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        messages = []

        for _ in range(self._batch_size):
            message = self._consumer.consume()

            if message is None:
                break

            messages.append(message)

        if not messages:
            self._logger.info(f"{datetime.utcnow()}: FINISH")
            return

        try:
            self._repository.save_orders(messages)
            self._consumer.commit()

        except Exception:
            self._logger.exception(
                "Ошибка обработки batch сообщений из Kafka"
            )

        self._logger.info(f"{datetime.utcnow()}: FINISH")