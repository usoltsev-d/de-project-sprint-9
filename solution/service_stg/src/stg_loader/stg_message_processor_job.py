import json
from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.redis import RedisClient
from stg_loader.repository.stg_repository import StgRepository


class StgMessageProcessor:
    def __init__(
        self,
        consumer: KafkaConsumer,
        producer: KafkaProducer,
        redis_client: RedisClient,
        stg_repository: StgRepository,
        batch_size: int,
        logger: Logger
    ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._redis = redis_client
        self._stg_repository = stg_repository
        self._batch_size = 100
        self._logger = logger

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            message = self._consumer.consume()

            # В Kafka больше нет сообщений — завершаем batch раньше.
            if message is None:
                break

            payload = message["payload"]

            # 1. Сохраняем оригинальное событие as-is в STG.
            self._stg_repository.order_events_insert(
                object_id=message["object_id"],
                object_type=message["object_type"],
                sent_dttm=message["sent_dttm"],
                payload=json.dumps(payload)
            )

            # 2. Получаем пользователя из Redis.
            user_id = payload["user"]["id"]
            user = self._redis.get(user_id)

            # 3. Получаем ресторан из Redis.
            restaurant_id = payload["restaurant"]["id"]
            restaurant = self._redis.get(restaurant_id)

            # 4. Обогащаем продукты.
            products = []

            for order_item in payload["order_items"]:
                product_id = order_item["id"]

                product = next(
                    product
                    for product in restaurant["menu"]
                    if product["_id"] == product_id
                )

                products.append(
                    {
                        "id": product_id,
                        "price": order_item["price"],
                        "quantity": order_item["quantity"],
                        "name": product["name"],
                        "category": product["category"]
                    }
                )

            # 5. Формируем выходное сообщение.
            output_message = {
                "object_id": message["object_id"],
                "object_type": message["object_type"],
                "payload": {
                    "id": message["object_id"],
                    "date": payload["date"],
                    "cost": payload["cost"],
                    "payment": payload["payment"],
                    "status": payload["final_status"],
                    "restaurant": {
                        "id": restaurant["_id"],
                        "name": restaurant["name"]
                    },
                    "user": {
                        "id": user["_id"],
                        "name": user["name"]
                    },
                    "products": products
                }
            }

            # 6. Отправляем обогащённое сообщение в Kafka.
            self._producer.produce(output_message)

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")
