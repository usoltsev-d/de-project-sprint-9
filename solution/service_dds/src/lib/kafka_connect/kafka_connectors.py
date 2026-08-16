import json
from typing import Dict, Optional

from confluent_kafka import Consumer, Producer


def error_callback(err):
    print('Something went wrong: {}'.format(err))


class KafkaProducer:
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        topic: str,
        cert_path: str
    ) -> None:
        params = {
            'bootstrap.servers': f'{host}:{port}',
            'security.protocol': 'SASL_SSL',
            'ssl.ca.location': cert_path,
            'sasl.mechanism': 'SCRAM-SHA-512',
            'sasl.username': user,
            'sasl.password': password,
            'error_cb': error_callback,
        }

        self.topic = topic
        self.p = Producer(params)

    def produce_batch(self, events: list[dict]) -> list[int]:
        sent_event_ids = []
        # Kafka вызовет callback после завершения доставки сообщения
        def delivery_callback(err, msg, event_id: int) -> None:
            if err is None:
                sent_event_ids.append(
                    event_id
                )

        for event in events:
            event_id = event['id']

            self.p.produce(
                self.topic,
                json.dumps(
                    event['payload'],
                    ensure_ascii=False
                ),
                callback=lambda err, msg, event_id=event_id:
                    delivery_callback(
                        err,
                        msg,
                        event_id
                    )
            )

        self.p.flush(10)

        return sent_event_ids

class KafkaConsumer:
    def __init__(self,
                 host: str,
                 port: int,
                 user: str,
                 password: str,
                 topic: str,
                 group: str,
                 cert_path: str
                 ) -> None:
        params = {
            'bootstrap.servers': f'{host}:{port}',
            'security.protocol': 'SASL_SSL',
            'ssl.ca.location': cert_path,
            'sasl.mechanism': 'SCRAM-SHA-512',
            'sasl.username': user,
            'sasl.password': password,
            'group.id': group,  # '',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'error_cb': error_callback,
            'client.id': 'someclientkey'
        }

        self.topic = topic
        self.c = Consumer(params)
        self.c.subscribe([topic])

    def consume(self, timeout: float = 3.0) -> Optional[Dict]:
        msg = self.c.poll(timeout=timeout)
        if not msg:
            return None
        if msg.error():
            raise Exception(msg.error())
        val = msg.value().decode()
        return json.loads(val)

    def commit(self) -> None:
        self.c.commit(asynchronous=False)