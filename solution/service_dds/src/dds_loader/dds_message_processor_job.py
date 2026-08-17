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
        self._batch_size = 300

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            message = self._consumer.consume()

            if message is None:
                break

            try:
                # Сохраняем заказ в DDS и Outbox одной транзакцией
                self._repository.save_order(message)

                # Фиксируем offset только после успешной записи в БД
                self._consumer.commit()

            except Exception:
                self._logger.exception(
                    "Ошибка обработки сообщения из Kafka"
                )
                break

        self._logger.info(f"{datetime.utcnow()}: FINISH")