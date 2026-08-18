# Проект 9-го спринта

Проект реализует потоковую обработку заказов.

Данные проходят через три слоя:
`STG -> DDS -> CDM`

## Архитектура

Поток данных:

`order-service_orders`
↓
STG
↓
`stg-service-orders`
↓
DDS
↓
Outbox
↓
`dds-service-orders`
↓
CDM

### STG

Сервис STG:

- читает сообщения из Kafka-топика `order-service_orders`;
- сохраняет исходные события в `stg.order_events`;
- получает дополнительные данные из Redis/Valkey;
- формирует обогащённое сообщение;
- отправляет результат в Kafka-топик `stg-service-orders`.

### DDS

Сервис DDS:

- читает сообщения из Kafka-топика `stg-service-orders`;
- сохраняет данные в модель Data Vault;
- обрабатывает заказы со статусами `CLOSED` и `CANCELLED`;
- для CDM формирует только завершённые заказы со статусом `CLOSED`;
- сохраняет сообщения для отправки в `dds.outbox`;
- публикует их в Kafka-топик `dds-service-orders`.

В DDS используются:

- хабы;
- линки;
- сателлиты;
- transactional outbox.

### CDM

Сервис CDM:

- читает сообщения из Kafka-топика `dds-service-orders`;
- формирует две аналитические витрины:

`cdm.user_product_counters`

Количество заказов пользователя по каждому продукту.

`cdm.user_category_counters`

Количество заказов пользователя по каждой категории.

Если в одном заказе присутствует несколько товаров одной категории, категория учитывается только один раз.

## Kafka

Используемые топики:

- `order-service_orders` — исходные заказы;
- `stg-service-orders` — сообщения STG -> DDS;
- `dds-service-orders` — сообщения DDS -> CDM.

Consumer groups:

- `stg-service-consumer-group`;
- `dds-service-consumer-group`;
- `cdm-service-consumer-group`.

## PostgreSQL

Используются три логических слоя:

- `stg` — staging;
- `dds` — Data Vault;
- `cdm` — аналитические витрины.

DDL находится в:

```text
migrations/DDL/


## Container Registry

Актуальные образы сервисов, используемые при развёртывании:

```text
STG:
cr.yandex/crpqg2ustmet33rjclbf/stg_service:v2026-08-17-r1

DDS:
cr.yandex/crpqg2ustmet33rjclbf/dds_service:v2026-08-18-r7

CDM:
cr.yandex/crpqg2ustmet33rjclbf/cdm_service:v2026-08-18-r3