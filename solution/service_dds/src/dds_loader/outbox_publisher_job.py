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

        events = self._repository.get_unsent_events(
            batch_size
        )

        sent_event_ids = []

        for event in events:
            try:
                self._producer.produce(
                    event['payload']
                )

                sent_event_ids.append(
                    event['id']
                )

            except Exception:
                self._logger.exception(
                    f"Не удалось отправить событие id={event['id']}"
                )
                break

        self._repository.mark_events_sent(
            sent_event_ids
        )

        self._logger.info(
            f"Успешно отправлено событий: {len(sent_event_ids)}"
        )

        self._logger.info(f"{datetime.utcnow()}: OUTBOX FINISH")