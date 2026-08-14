from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository


class OutboxPublisher:
    def __init__(
        self,
        producer: KafkaProducer,
        repository: DdsRepository,
        logger: Logger
    ) -> None:
        self._producer = producer
        self._repository = repository
        self._logger = logger

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: OUTBOX START")

        event = self._repository.get_unsent_event()

        if event is None:
            self._logger.info(f"{datetime.utcnow()}: OUTBOX FINISH")
            return

        try:
            self._producer.produce(
                event['payload']
            )

            self._repository.mark_event_sent(
                event['id']
            )

            self._logger.info(
                f"Событие id={event['id']} успешно отправлено"
            )

        except Exception:
            self._logger.exception(
                f"Не удалось отправить событие id={event['id']}"
            )

        self._logger.info(f"{datetime.utcnow()}: OUTBOX FINISH")