from confluent_kafka import Consumer, TopicPartition

conf = {
    'bootstrap.servers': 'rc1a-nfekjh0bqondm8kr.mdb.yandexcloud.net:9091',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'SCRAM-SHA-512',
    'sasl.username': 'producer_consumer',
    'sasl.password': 'kafka_2026',
    'ssl.ca.location': '/home/usoltsev/.cert/YandexInternalRootCA.crt',
    'group.id': 'cdm-service-consumer-group',
    'enable.auto.commit': False,
}

consumer = Consumer(conf)

tp = TopicPartition('dds-service-orders', 0, 19913)

result = consumer.commit(
    offsets=[tp],
    asynchronous=False
)

print(result)

consumer.close()