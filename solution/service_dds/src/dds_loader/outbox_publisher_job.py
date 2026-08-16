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

        batch_size = 50

        # Забираем пачку событий из Outbox
        events = self._repository.get_unsent_events(
            batch_size
        )

        if not events:
            self._logger.info(f"{datetime.utcnow()}: OUTBOX FINISH")
            return

        try:
            # Отправляем всю пачку в Kafka одним flush
            sent_event_ids = self._producer.produce_batch(
                events
            )

            # Одним UPDATE отмечаем успешно отправленные события
            self._repository.mark_events_sent(
                sent_event_ids
            )

            self._logger.info(
                f"Успешно отправлено событий: {len(sent_event_ids)}"
            )

        except Exception:
            self._logger.exception(
                "Ошибка отправки пачки событий в Kafka"
            )

        self._logger.info(f"{datetime.utcnow()}: OUTBOX FINISH")