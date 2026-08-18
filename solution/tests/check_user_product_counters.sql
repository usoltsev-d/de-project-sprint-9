-- Сквозная проверка витрины cdm.user_product_counters.
-- Запрос пересчитывает количество CLOSED-заказов по продуктам напрямую из DDS
-- и сравнивает результат с данными в CDM для указанного пользователя.

-- Ожидаемый результат: 0 строк.
-- Если строки вернулись, значит значения в DDS и CDM отличаются.

WITH params AS (
    SELECT
        'cd16bcdc-77f9-549f-9114-a7c1566d76df'::uuid AS user_id
),
latest_status AS (
    SELECT DISTINCT ON (h_order_pk)
        h_order_pk,
        status
    FROM dds.s_order_status
    ORDER BY h_order_pk, load_dt DESC
),
latest_product_names AS (
    SELECT DISTINCT ON (h_product_pk)
        h_product_pk,
        name
    FROM dds.s_product_names
    ORDER BY h_product_pk, load_dt DESC
),
dds_counts AS (
    SELECT
        spn.name AS product_name,
        COUNT(DISTINCT lou.h_order_pk) AS order_cnt
    FROM dds.l_order_user lou
    JOIN latest_status ls
        ON ls.h_order_pk = lou.h_order_pk
    JOIN dds.l_order_product lop
        ON lop.h_order_pk = lou.h_order_pk
    JOIN latest_product_names spn
        ON spn.h_product_pk = lop.h_product_pk
    CROSS JOIN params p
    WHERE lou.h_user_pk = p.user_id
      AND ls.status = 'CLOSED'
    GROUP BY spn.name
),
cdm_counts AS (
    SELECT
        upc.product_name,
        upc.order_cnt
    FROM cdm.user_product_counters upc
    CROSS JOIN params p
    WHERE upc.user_id = p.user_id
)
SELECT
    COALESCE(d.product_name, c.product_name) AS product_name,
    d.order_cnt AS dds_order_cnt,
    c.order_cnt AS cdm_order_cnt
FROM dds_counts d
FULL JOIN cdm_counts c
    ON c.product_name = d.product_name
WHERE d.order_cnt IS DISTINCT FROM c.order_cnt
ORDER BY product_name;