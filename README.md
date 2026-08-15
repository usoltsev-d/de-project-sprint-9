# Проект 9-го спринта

## Сервисы

- STG — читает `order-service_orders`, сохраняет данные в STG и отправляет сообщения в `stg-service-orders`.
- DDS — читает `stg-service-orders`, сохраняет данные в DDS и отправляет сообщения в `dds-service-orders`.
- CDM — читает `dds-service-orders` и формирует витрины CDM.

## Container Registry

STG:
cr.yandex/crpqg2ustmet33rjclbf/stg_service:v2026-08-13-r2

DDS:
cr.yandex/crpqg2ustmet33rjclbf/dds_service:v2026-08-15-r1